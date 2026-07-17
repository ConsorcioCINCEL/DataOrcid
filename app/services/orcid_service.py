"""ORCID API client utilities used by cache builders and profile views."""

import logging
import time
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests
from flask import current_app

logger = logging.getLogger(__name__)

_CACHED_TOKEN: Optional[str] = None
_TOKEN_EXPIRY: float = 0
_TOKEN_LOCK = threading.Lock()
_PROFILE_CACHE: dict[tuple[str, str], tuple[float, Dict]] = {}
_PROFILE_CACHE_LOCK = threading.Lock()


class OrcidSearchError(RuntimeError):
    """Raised when an institutional ORCID search cannot be completed safely."""


def get_client_credentials_token() -> Optional[str]:
    """
    Return a cached ORCID client-credentials token, refreshing it when needed.

    The token is shared across threads, so refreshes are guarded by a lock.
    """
    global _CACHED_TOKEN, _TOKEN_EXPIRY

    if _CACHED_TOKEN and time.time() < _TOKEN_EXPIRY:
        return _CACHED_TOKEN

    with _TOKEN_LOCK:
        if _CACHED_TOKEN and time.time() < _TOKEN_EXPIRY:
            return _CACHED_TOKEN

        conf = current_app.config
        token_url = conf.get("ORCID_TOKEN_URL")
        client_id = conf.get("ORCID_CLIENT_ID")
        client_secret = conf.get("ORCID_CLIENT_SECRET")

        if not all([token_url, client_id, client_secret]):
            logger.error("Configuration Error: Missing ORCID Token URL, Client ID, or Secret.")
            return None

        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': '/read-public',
        }
        headers = {'Accept': 'application/json'}

        try:
            response = requests.post(token_url, data=payload, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            token = data.get("access_token")
            # Refresh one minute before ORCID's advertised expiration.
            expires_in = data.get("expires_in", 6300)
            
            if token:
                _CACHED_TOKEN = token
                _TOKEN_EXPIRY = time.time() + expires_in - 60 
                return token
                
        except requests.exceptions.RequestException as exc:
            logger.error("ORCID Token Exchange Failed: %s", exc)
        
        return None


def safe_get(url: str, headers: Dict = None, timeout: int = 30, retries: int = 3) -> Optional[requests.Response]:
    """
    GET a URL with ORCID defaults, bearer normalization, and retry handling.

    Returns 404 responses to callers because a missing ORCID resource is a valid
    application outcome, not a transport failure.
    """
    req_headers = (headers or {}).copy()

    if 'Accept' not in req_headers:
        req_headers['Accept'] = 'application/vnd.orcid+json'

    if 'Authorization' in req_headers:
        auth_val = req_headers['Authorization'].strip()
        if not auth_val.lower().startswith('bearer '):
            req_headers['Authorization'] = f"Bearer {auth_val}"

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=req_headers, timeout=timeout)

            if response.status_code == 200:
                return response

            if response.status_code == 429:
                sleep_duration = 2 * attempt
                logger.warning("Rate limit hit (429). Sleeping %ds before retry %d/%d...", 
                               sleep_duration, attempt, retries)
                time.sleep(sleep_duration)
                continue

            if response.status_code == 404:
                return response

            if response.status_code in (401, 403):
                logger.error("Authentication error (%d) accessing: %s", response.status_code, url)
                return None

        except requests.exceptions.RequestException as exc:
            logger.warning("[API Attempt %d/%d Failed] %s: %s", attempt, retries, url, exc)
        
        if attempt < retries:
            time.sleep(0.5 * attempt)

    logger.error("Failed to fetch URL after %d attempts: %s", retries, url)
    return None


def list_orcids_for_institution(
    ror_id: Optional[str],
    grid_id: Optional[str],
    base_url: Optional[str] = None, 
    headers: Optional[Dict] = None,
    rows: int = 1000,
    delay: float = 0.0,
    ringgold_ids: Optional[List[str]] = None,
    grid_ids: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Search ORCID separately by ROR, GRID, and Ringgold identifiers.

    Separate searches preserve match provenance while results are deduplicated
    by ORCID iD. Any incomplete page raises an error so callers can keep their
    previous cache instead of replacing it with a truncated result set.
    """
    unique_results = {}
    search_url = base_url or current_app.config.get('ORCID_SEARCH_URL')
    
    if not search_url:
        logger.error("Configuration Error: No ORCID Search URL defined.")
        raise OrcidSearchError("No ORCID Search URL is configured.")
    
    search_headers = {'Accept': 'application/json'}
    if headers:
        search_headers.update(headers)

    token = get_client_credentials_token()
    if token and 'Authorization' not in search_headers:
        search_headers['Authorization'] = f"Bearer {token}"

    resolved_grid_ids = _unique_values([grid_id] + list(grid_ids or []))
    resolved_ringgold_ids = _unique_values(ringgold_ids or [])

    if not resolved_ringgold_ids or not resolved_grid_ids:
        try:
            from .institution_registry_service import get_institution_identifiers

            stored_identifiers = get_institution_identifiers(ror_id or "")
            resolved_grid_ids = _unique_values(
                resolved_grid_ids + stored_identifiers.get("grid", [])
            )
            resolved_ringgold_ids = _unique_values(
                resolved_ringgold_ids + stored_identifiers.get("ringgold", [])
            )
        except Exception as exc:
            logger.debug("Stored institutional identifiers could not be loaded: %s", exc)

    searches = []
    if ror_id:
        clean_ror = ror_id.strip().rstrip('/').split('/')[-1].lower()
        searches.append(("ror", clean_ror, f'ror-org-id:"https://ror.org/{clean_ror}"'))
    searches.extend(
        ("grid", value, f'grid-org-id:"{_escape_query_value(value)}"')
        for value in resolved_grid_ids
    )
    searches.extend(
        ("ringgold", value, f'ringgold-org-id:"{_escape_query_value(value)}"')
        for value in resolved_ringgold_ids
    )
    
    if not searches:
        logger.warning("Search aborted: No institutional identifier was provided.")
        return []

    for scheme, identifier, raw_query in searches:
        logger.info("Executing institutional %s search: %s", scheme.upper(), raw_query)
        records = _expanded_search(
            search_url,
            raw_query,
            search_headers,
            rows=rows,
            delay=delay,
        )
        for record in records:
            orcid_id = (record.get("orcid-id") or "").strip()
            if not orcid_id:
                continue

            stored = unique_results.setdefault(orcid_id, dict(record))
            matches = stored.setdefault("matched_identifiers", {})
            values = matches.setdefault(scheme, [])
            if identifier not in values:
                values.append(identifier)

            for key in ("given-names", "family-names", "credit-name", "other-name", "email", "institution-name"):
                if not stored.get(key) and record.get(key):
                    stored[key] = record[key]

    logger.info("Search Completed. Total Unique Researchers Found: %d", len(unique_results))
    return list(unique_results.values())


def _expanded_search(
    search_url: str,
    raw_query: str,
    headers: Dict,
    *,
    rows: int,
    delay: float,
) -> List[Dict]:
    """Return every expanded-search page for one exact institutional query."""
    page_size = max(1, min(int(rows or 1000), 1000))
    endpoint = f"{search_url.rstrip('/')}/expanded-search/"
    encoded_query = urllib.parse.quote(raw_query)
    start = 0
    results = []

    while True:
        url = f"{endpoint}?q={encoded_query}&start={start}&rows={page_size}"
        response = safe_get(url, headers, timeout=30)
        if response is None or response.status_code != 200:
            status = response.status_code if response is not None else "unavailable"
            raise OrcidSearchError(
                f"ORCID search failed for query {raw_query!r} at offset {start} "
                f"with status {status}."
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise OrcidSearchError(
                f"ORCID returned invalid JSON for query {raw_query!r} at offset {start}."
            ) from exc

        chunk = data.get("expanded-result") or []
        total = int(data.get("num-found") or 0)
        if not chunk:
            if start < total:
                raise OrcidSearchError(
                    f"ORCID returned an empty page before all {total} results were read "
                    f"for query {raw_query!r}."
                )
            break

        results.extend(chunk)
        start += len(chunk)
        if start >= total:
            break
        if delay > 0:
            time.sleep(delay)

    return results


def _escape_query_value(value: str) -> str:
    return str(value).replace('\\', '\\\\').replace('"', '\\"')


def _unique_values(values) -> List[str]:
    result = []
    for value in values:
        clean_value = (value or "").strip()
        if clean_value and clean_value not in result:
            result.append(clean_value)
    return result


def fetch_single_profile(orcid_id: str, base_url: str, token: str) -> Dict:
    """
    Fetch one full ORCID record without relying on Flask request context.

    Keeping the worker context-free makes it safe to use in thread pools.
    """
    url = f"{base_url.rstrip('/')}/{orcid_id}/record"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    
    response = safe_get(url, headers=headers)
    if response and response.status_code == 200:
        data = response.json()
        if 'activities-summary' in data:
            data['activities'] = data['activities-summary']
        return data
        
    return {}


def get_full_orcid_profile(orcid_id: str, force_refresh: bool = False) -> Dict:
    """Return a short-lived cached ORCID profile from the configured Member API."""
    member_url = current_app.config.get('ORCID_MEMBER_URL')
    if not member_url:
        logger.error("Configuration Error: Missing Member API URL.")
        return {}

    normalized_orcid = (orcid_id or "").strip()
    cache_key = (member_url.rstrip("/"), normalized_orcid)
    now = time.monotonic()
    ttl = max(int(current_app.config.get("ORCID_PROFILE_CACHE_TTL", 900)), 0)

    if not force_refresh and ttl:
        with _PROFILE_CACHE_LOCK:
            cached = _PROFILE_CACHE.get(cache_key)
            if cached and cached[0] > now:
                return cached[1]

    token = get_client_credentials_token()
    if not token:
        logger.error("Configuration Error: Missing ORCID API token.")
        return {}

    profile = fetch_single_profile(normalized_orcid, member_url, token)
    if profile and ttl:
        with _PROFILE_CACHE_LOCK:
            _PROFILE_CACHE[cache_key] = (now + ttl, profile)
            expired_keys = [key for key, value in _PROFILE_CACHE.items() if value[0] <= now]
            for key in expired_keys:
                _PROFILE_CACHE.pop(key, None)
    return profile


def get_all_profiles_concurrently(orcid_ids: List[str], max_workers: int = 10) -> Dict[str, Dict]:
    """
    Fetch full ORCID profiles in parallel and return them keyed by ORCID iD.
    """
    results = {}
    
    member_url = current_app.config.get('ORCID_MEMBER_URL')
    token = get_client_credentials_token()
    
    if not member_url or not token:
        logger.error("Configuration Error: Cannot start concurrent fetch without API credentials.")
        return {}

    total = len(orcid_ids)
    logger.info("Starting concurrent fetch for %d profiles using %d workers...", total, max_workers)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_orcid = {
            executor.submit(fetch_single_profile, oid, member_url, token): oid 
            for oid in orcid_ids
        }
        
        count = 0
        for future in as_completed(future_to_orcid):
            oid = future_to_orcid[future]
            count += 1
            try:
                profile_data = future.result()
                if profile_data:
                    results[oid] = profile_data

                if count % 25 == 0:
                    logger.info("Progress: %d/%d profiles fetched.", count, total)
                    
            except Exception as exc:
                logger.error("Thread Error: Failed to fetch profile for %s: %s", oid, exc)
                
    logger.info("Concurrent fetch completed. Success Rate: %d/%d", len(results), total)
    return results
