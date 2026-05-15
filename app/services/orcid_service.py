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
) -> List[Dict]:
    """
    Search ORCID for researchers affiliated with a ROR and/or GRID identifier.

    ORCID expanded search is paginated; results are deduplicated by ORCID iD.
    """
    start = 0
    unique_results = {} 
    
    search_url = current_app.config.get('ORCID_SEARCH_URL') or base_url
    
    if not search_url:
        logger.error("Configuration Error: No ORCID Search URL defined.")
        return []
    
    search_headers = {'Accept': 'application/json'}
    if headers:
        search_headers.update(headers)

    token = get_client_credentials_token()
    if token and 'Authorization' not in search_headers:
        search_headers['Authorization'] = f"Bearer {token}"

    query_parts = []
    if ror_id:
        query_parts.append(f'ror-org-id:"https://ror.org/{ror_id}"')
    if grid_id:
        query_parts.append(f'grid-org-id:"{grid_id}"')
    
    if not query_parts:
        logger.warning("Search aborted: No ROR or GRID ID provided.")
        return []

    raw_query = " OR ".join(query_parts)
    logger.info("Executing Institutional Search: %s", raw_query)

    while True:
        try:
            encoded_query = urllib.parse.quote(raw_query)
            endpoint = f"{search_url.rstrip('/')}/expanded-search/"
            url = f"{endpoint}?q={encoded_query}&start={start}&rows={rows}"
            
            response = safe_get(url, search_headers, timeout=30)
            if not response or response.status_code != 200:
                logger.error("Search request failed at start index %d", start)
                break 

            data = response.json()
            chunk = data.get("expanded-result", []) or []
            if not chunk:
                break

            for record in chunk:
                orcid_id = (record.get("orcid-id") or "").strip()
                if orcid_id:
                    unique_results[orcid_id] = record

            if len(chunk) < rows:
                break
                
            start += rows
            
            if delay > 0:
                time.sleep(delay)

        except Exception as exc:
            logger.exception("Critical error during ORCID search pagination loop: %s", exc)
            break

    logger.info("Search Completed. Total Unique Researchers Found: %d", len(unique_results))
    return list(unique_results.values())


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


def get_full_orcid_profile(orcid_id: str) -> Dict:
    """Fetch a single ORCID profile using the configured Member API."""
    member_url = current_app.config.get('ORCID_MEMBER_URL')
    token = get_client_credentials_token()
    
    if not member_url or not token:
        logger.error("Configuration Error: Missing Member API URL or Token.")
        return {}

    return fetch_single_profile(orcid_id, member_url, token)


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
