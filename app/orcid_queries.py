"""
Module: orcid_queries.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    ORCID API Data Retrieval Services.
    
    This module provides a specialized abstraction layer to interact with 
    the ORCID Member and Public APIs. It decomposes the complex ORCID record 
    into individual biographical and activity-based queries.
    
    Architecture:
    - **Modular Fetching**: Specific functions for every ORCID sub-endpoint.
    - **Security**: Centralized OAuth2 token management via Client Credentials.
    - **Stability**: Robust exception handling for network timeouts and API errors.
"""

import requests
import logging
from flask import current_app as app
from requests.exceptions import RequestException, Timeout
from typing import Optional, Dict, Any

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


# ============================================================
# AUTHENTICATION PROTOCOL
# ============================================================

def get_orcid_token() -> Optional[str]:
    """
    Retrieves an OAuth2 access token via the Client Credentials flow.

    Uses the institutional credentials (Client ID and Secret) defined in 
    the application configuration to obtain a temporary Bearer token 
    scoped for '/read-public' access.

    Returns:
        Optional[str]: A valid access token if successful, otherwise None.
    """
    conf = app.config
    token_url = conf.get("ORCID_TOKEN_URL")
    client_id = conf.get("ORCID_CLIENT_ID")
    client_secret = conf.get("ORCID_CLIENT_SECRET")

    # Safety check: Verify configuration exists
    if not all([token_url, client_id, client_secret]):
        app.logger.error("CRITICAL: Missing ORCID configuration (ID, Secret, or Token URL).")
        return None

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    }
    headers = {"Accept": "application/json"}

    try:
        # Request token with a 15s timeout
        response = requests.post(token_url, data=payload, headers=headers, timeout=15)
        
        if response.status_code == 200:
            token = response.json().get("access_token")
            if token:
                return token
            app.logger.warning("ORCID API response successful but 'access_token' is missing in payload.")
            return None
        else:
            app.logger.error("ORCID Auth Error [%d]: %s", response.status_code, response.text)
            return None
            
    except (RequestException, Timeout) as exc:
        app.logger.exception("Network error during ORCID token retrieval: %s", exc)
        return None


# ============================================================
# GENERIC DATA FETCHING (MEMBER API)
# ============================================================

def fetch_orcid_data(orcid_id: str, endpoint: str) -> Optional[Dict[str, Any]]:
    """
    Base function to query a specific sub-route of a researcher's ORCID record.

    Designed to work with the Member API to leverage higher rate limits 
    and better performance during institutional bulk processing.

    Args:
        orcid_id (str): The target ORCID identifier (e.g., '0000-0002-1825-0097').
        endpoint (str): The specific record section (e.g., 'person', 'works', 'employments').

    Returns:
        Optional[Dict[str, Any]]: The parsed JSON response or None on failure/404.
    """
    token = get_orcid_token()
    if not token:
        app.logger.error("Fetch aborted: Unable to obtain a valid ORCID access token.")
        return None

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    
    # Prioritize Member API base URL for institutional stability
    base_url = app.config.get('ORCID_BASE_URL_MEMBER', 'https://api.orcid.org/v3.0/')
    target_url = f"{base_url.rstrip('/')}/{orcid_id}/{endpoint.strip()}"

    try:
        response = requests.get(target_url, headers=headers, timeout=20)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            app.logger.info("ORCID Resource not found: %s/%s", orcid_id, endpoint)
            return None
        else:
            app.logger.warning("ORCID API Response [%d] for %s/%s", 
                               response.status_code, orcid_id, endpoint)
            return None
            
    except (RequestException, Timeout) as exc:
        app.logger.exception("Network error querying ORCID %s/%s: %s", orcid_id, endpoint, exc)
        return None


# ============================================================
# BIOGRAPHICAL ENDPOINTS (PERSON SECTION)
# ============================================================
# These functions fetch specific parts of the 'Person' metadata.

def fetch_person(orcid_id: str): 
    """Fetches the full biographical summary (Person)."""
    return fetch_orcid_data(orcid_id, "person")

def fetch_address(orcid_id: str): 
    """Fetches researcher's physical/institutional addresses."""
    return fetch_orcid_data(orcid_id, "address")

def fetch_email(orcid_id: str): 
    """Fetches publicly available email addresses."""
    return fetch_orcid_data(orcid_id, "email")

def fetch_external_identifiers(orcid_id: str): 
    """Fetches IDs from other systems (Scopus, ResearcherID, etc.)."""
    return fetch_orcid_data(orcid_id, "external-identifiers")

def fetch_keywords(orcid_id: str): 
    """Fetches research keywords defined in the profile."""
    return fetch_orcid_data(orcid_id, "keywords")

def fetch_other_names(orcid_id: str): 
    """Fetches known aliases or alternative name spellings."""
    return fetch_orcid_data(orcid_id, "other-names")

def fetch_personal_details(orcid_id: str): 
    """Fetches basic personal metadata (Given/Family names)."""
    return fetch_orcid_data(orcid_id, "personal-details")

def fetch_researcher_urls(orcid_id: str): 
    """Fetches profile-linked websites and social links."""
    return fetch_orcid_data(orcid_id, "researcher-urls")


# ============================================================
# ACTIVITIES & RESEARCH ENDPOINTS (ACTIVITIES SECTION)
# ============================================================
# These functions fetch scholarly activity summaries.

def fetch_activities(orcid_id: str): 
    """Fetches a high-level summary of all scholarly activities."""
    return fetch_orcid_data(orcid_id, "activities")

def fetch_educations(orcid_id: str): 
    """Fetches educational background and degrees."""
    return fetch_orcid_data(orcid_id, "educations")

def fetch_employments(orcid_id: str): 
    """Fetches institutional employment history."""
    return fetch_orcid_data(orcid_id, "employments")

def fetch_fundings(orcid_id: str): 
    """Fetches research grants and financial awards."""
    return fetch_orcid_data(orcid_id, "fundings")

def fetch_peer_reviews(orcid_id: str): 
    """Fetches verified peer-review contributions."""
    return fetch_orcid_data(orcid_id, "peer-reviews")

def fetch_works(orcid_id: str): 
    """Fetches the complete list of scholarly publications."""
    return fetch_orcid_data(orcid_id, "works")

# Specialized Sections
def fetch_research_resources(orcid_id: str): 
    """Fetches use of external research facilities or resources."""
    return fetch_orcid_data(orcid_id, "research-resources")

def fetch_services(orcid_id: str): 
    """Fetches service-based institutional affiliations."""
    return fetch_orcid_data(orcid_id, "services")

def fetch_qualifications(orcid_id: str): 
    """Fetches professional or academic qualifications."""
    return fetch_orcid_data(orcid_id, "qualifications")

def fetch_memberships(orcid_id: str): 
    """Fetches memberships in professional societies."""
    return fetch_orcid_data(orcid_id, "memberships")

def fetch_distinctions(orcid_id: str): 
    """Fetches honors, awards, and prizes."""
    return fetch_orcid_data(orcid_id, "distinctions")

def fetch_invited_positions(orcid_id: str): 
    """Fetches visiting or invited academic positions."""
    return fetch_orcid_data(orcid_id, "invited-positions")