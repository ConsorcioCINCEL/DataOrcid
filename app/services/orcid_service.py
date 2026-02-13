"""
Module: orcid_service.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Core ORCID API Integration Service.
    
    This module handles all direct communication with the ORCID Public and Member APIs.
    It is designed for high-throughput environments where thousands of researcher profiles
    need to be synchronized efficiently.

    Key Features:
    1. **Thread-Safe Token Caching**: Implements a singleton pattern with locking to share 
       OAuth2 tokens across threads, minimizing unnecessary authentication requests.
    2. **Resilience**: Includes a custom `safe_get` wrapper with exponential backoff 
       to gracefully handle API rate limits (HTTP 429) and transient network errors.
    3. **Concurrency**: Uses `ThreadPoolExecutor` to fetch full profile data in parallel,
       significantly reducing the time required for bulk institutional synchronization.
    4. **Context Decoupling**: Worker functions are designed to operate outside the Flask 
       application context where necessary to avoid threading issues.
"""

import logging
import time
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests
from flask import current_app

logger = logging.getLogger(__name__)

# --- Global Token Management ---
# Shared state for the OAuth2 access token to avoid re-authenticating on every request.
_CACHED_TOKEN: Optional[str] = None
_TOKEN_EXPIRY: float = 0
_TOKEN_LOCK = threading.Lock()


def get_client_credentials_token() -> Optional[str]:
    """
    Retrieves a valid OAuth2 access token using the Client Credentials Grant flow.
    
    This function implements a thread-safe caching mechanism:
    - If a valid token exists in memory, it returns immediately.
    - If the token is expired or missing, it acquires a lock to refresh it from the ORCID API.
    
    Returns:
        Optional[str]: The Bearer Access Token string, or None if authentication failed.
    """
    global _CACHED_TOKEN, _TOKEN_EXPIRY
    
    # 1. Fast Path: Check if valid token exists (Read-only, no lock needed yet)
    if _CACHED_TOKEN and time.time() < _TOKEN_EXPIRY:
        return _CACHED_TOKEN

    # 2. Slow Path: Acquire lock to refresh token
    with _TOKEN_LOCK:
        # Double-check inside lock to prevent race conditions
        if _CACHED_TOKEN and time.time() < _TOKEN_EXPIRY:
            return _CACHED_TOKEN

        # Retrieve credentials from Flask App Config
        conf = current_app.config
        token_url = conf.get("ORCID_TOKEN_URL")
        client_id = conf.get("ORCID_CLIENT_ID")
        client_secret = conf.get("ORCID_CLIENT_SECRET")

        if not all([token_url, client_id, client_secret]):
            logger.error("Configuration Error: Missing ORCID Token URL, Client ID, or Secret.")
            return None

        # Prepare OAuth2 Request
        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': '/read-public' # Requesting read access to public data
        }
        headers = {'Accept': 'application/json'}

        try:
            response = requests.post(token_url, data=payload, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            token = data.get("access_token")
            # Calculate expiry time (Default 6300s buffer if not provided)
            # We subtract 60 seconds to create a safety window before actual expiration
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
    A robust wrapper for `requests.get` that handles common API issues.
    
    Features:
    - **Automatic Header Injection**: Adds specific Accept headers if missing.
    - **Bearer Token Formatting**: Ensures the Authorization header is correctly formatted.
    - **Rate Limit Handling**: Detects HTTP 429 responses and sleeps (backoff) before retrying.
    - **Retry Logic**: Retries on transient errors up to `retries` times.
    
    Args:
        url (str): The target API endpoint.
        headers (Dict, optional): Custom HTTP headers.
        timeout (int): Request timeout in seconds.
        retries (int): Maximum number of retry attempts.
        
    Returns:
        Optional[requests.Response]: The successful response object, or None if all retries failed.
    """
    req_headers = (headers or {}).copy()
    
    # ORCID specific content type
    if 'Accept' not in req_headers:
        req_headers['Accept'] = 'application/vnd.orcid+json'

    # Fix common issue where 'Bearer ' prefix is missing in Authorization header
    if 'Authorization' in req_headers:
        auth_val = req_headers['Authorization'].strip()
        if not auth_val.lower().startswith('bearer '):
            req_headers['Authorization'] = f"Bearer {auth_val}"

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=req_headers, timeout=timeout)
            
            # Success
            if response.status_code == 200:
                return response

            # Handle Rate Limiting (429 Too Many Requests)
            if response.status_code == 429:
                sleep_duration = 2 * attempt # Exponential backoff (2s, 4s, 6s...)
                logger.warning("Rate limit hit (429). Sleeping %ds before retry %d/%d...", 
                               sleep_duration, attempt, retries)
                time.sleep(sleep_duration)
                continue
            
            # Handle Not Found (Do not retry)
            if response.status_code == 404:
                return response
            
            # Handle Auth Errors (Do not retry - likely configuration issue)
            if response.status_code in (401, 403):
                logger.error("Authentication error (%d) accessing: %s", response.status_code, url)
                return None

        except requests.exceptions.RequestException as exc:
            logger.warning("[API Attempt %d/%d Failed] %s: %s", attempt, retries, url, exc)
        
        # Wait before next retry for transient network errors
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
    Searches the ORCID Registry for researchers affiliated with a specific institution.
    
    Supports searching by ROR ID (Research Organization Registry) and/or GRID ID.
    Uses pagination to retrieve the complete list of affiliated researchers.
    
    Args:
        ror_id (str): The ROR identifier (e.g., '02ap3w078').
        grid_id (str): The GRID identifier (e.g., 'grid.424112.0').
        base_url (str, optional): Override for the Search API endpoint.
        headers (Dict, optional): Extra headers (e.g., Auth token).
        rows (int): Number of results per page (Max 1000 for Expanded Search).
        delay (float): Time to sleep between pages to avoid rate limits.
        
    Returns:
        List[Dict]: A list of 'expanded-search' result objects containing ORCID iDs.
    """
    start = 0
    unique_results = {} 
    
    # Resolve API Endpoint
    search_url = current_app.config.get('ORCID_SEARCH_URL') or base_url
    
    if not search_url:
        logger.error("Configuration Error: No ORCID Search URL defined.")
        return []
    
    # Prepare Headers
    search_headers = {'Accept': 'application/json'}
    if headers:
        search_headers.update(headers)
    
    # Inject Token if not present
    token = get_client_credentials_token()
    if token and 'Authorization' not in search_headers:
        search_headers['Authorization'] = f"Bearer {token}"
    
    # Build Lucene Query Syntax for ORCID
    query_parts = []
    if ror_id:
        query_parts.append(f'ror-org-id:"https://ror.org/{ror_id}"')
    if grid_id:
        query_parts.append(f'grid-org-id:"{grid_id}"')
    
    if not query_parts:
        logger.warning("Search aborted: No ROR or GRID ID provided.")
        return []
        
    # Combine identifiers with OR logic to maximize recall
    raw_query = " OR ".join(query_parts)
    logger.info("Executing Institutional Search: %s", raw_query)

    # Pagination Loop
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
                break # No more results

            # Deduplicate results based on ORCID iD
            for record in chunk:
                orcid_id = (record.get("orcid-id") or "").strip()
                if orcid_id:
                    unique_results[orcid_id] = record

            # Check if we have reached the end of the result set
            if len(chunk) < rows:
                break
                
            start += rows
            
            # Optional throttling
            if delay > 0:
                time.sleep(delay)

        except Exception as exc:
            logger.exception("Critical error during ORCID search pagination loop: %s", exc)
            break

    logger.info("Search Completed. Total Unique Researchers Found: %d", len(unique_results))
    return list(unique_results.values())


def fetch_single_profile(orcid_id: str, base_url: str, token: str) -> Dict:
    """
    Worker function to fetch the full record of a single researcher.
    
    Design Note:
    This function is designed to be 'pure' (no dependency on Flask's `current_app` context)
    so it can be safely executed by background worker threads.
    
    Args:
        orcid_id (str): The researcher's ORCID iD.
        base_url (str): The API base URL (Member or Public).
        token (str): Valid Bearer token.
        
    Returns:
        Dict: The full profile JSON, or an empty dict on failure.
    """
    url = f"{base_url.rstrip('/')}/{orcid_id}/record"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    
    response = safe_get(url, headers=headers)
    if response and response.status_code == 200:
        data = response.json()
        # Normalize data structure: Some older API versions use 'activities-summary'
        if 'activities-summary' in data:
            data['activities'] = data['activities-summary']
        return data
        
    return {}


def get_full_orcid_profile(orcid_id: str) -> Dict:
    """
    Convenience wrapper to fetch a single profile using the configured Member API.
    Used primarily for single-record views (e.g., 'View Profile' modal).
    
    Args:
        orcid_id (str): The target ORCID iD.
        
    Returns:
        Dict: Full profile data.
    """
    member_url = current_app.config.get('ORCID_MEMBER_URL')
    token = get_client_credentials_token()
    
    if not member_url or not token:
        logger.error("Configuration Error: Missing Member API URL or Token.")
        return {}

    return fetch_single_profile(orcid_id, member_url, token)


def get_all_profiles_concurrently(orcid_ids: List[str], max_workers: int = 10) -> Dict[str, Dict]:
    """
    High-Performance Bulk Fetching Service.
    
    Uses a `ThreadPoolExecutor` to fetch multiple ORCID profiles in parallel.
    This reduces the total time for synchronizing an institution by an order of magnitude
    compared to sequential processing.
    
    Args:
        orcid_ids (List[str]): A list of ORCID iDs to fetch.
        max_workers (int): The number of concurrent threads (Default: 10).
                           Adjust based on server CPU and Network capabilities.
        
    Returns:
        Dict[str, Dict]: A dictionary mapping ORCID iD -> Profile Data JSON.
    """
    results = {}
    
    # Retrieve configuration once from the main thread
    member_url = current_app.config.get('ORCID_MEMBER_URL')
    token = get_client_credentials_token()
    
    if not member_url or not token:
        logger.error("Configuration Error: Cannot start concurrent fetch without API credentials.")
        return {}

    total = len(orcid_ids)
    logger.info("Starting concurrent fetch for %d profiles using %d workers...", total, max_workers)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map futures to ORCID IDs for tracking
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
                
                # Log progress periodically
                if count % 25 == 0:
                    logger.info("Progress: %d/%d profiles fetched.", count, total)
                    
            except Exception as exc:
                logger.error("Thread Error: Failed to fetch profile for %s: %s", oid, exc)
                
    logger.info("Concurrent fetch completed. Success Rate: %d/%d", len(results), total)
    return results