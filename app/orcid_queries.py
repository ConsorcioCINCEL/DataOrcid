"""Small wrappers for individual ORCID record endpoints."""

import requests
import logging
from flask import current_app as app
from requests.exceptions import RequestException, Timeout
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def get_orcid_token() -> Optional[str]:
    """Request an ORCID client-credentials token for public-read access."""
    conf = app.config
    token_url = conf.get("ORCID_TOKEN_URL")
    client_id = conf.get("ORCID_CLIENT_ID")
    client_secret = conf.get("ORCID_CLIENT_SECRET")

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


def fetch_orcid_data(orcid_id: str, endpoint: str) -> Optional[Dict[str, Any]]:
    """
    Fetch one ORCID record sub-endpoint, returning None for missing resources.
    """
    token = get_orcid_token()
    if not token:
        app.logger.error("Fetch aborted: Unable to obtain a valid ORCID access token.")
        return None

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    
    base_url = app.config.get('ORCID_MEMBER_URL', 'https://api.orcid.org/v3.0/')
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


def fetch_person(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "person")

def fetch_address(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "address")

def fetch_email(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "email")

def fetch_external_identifiers(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "external-identifiers")

def fetch_keywords(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "keywords")

def fetch_other_names(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "other-names")

def fetch_personal_details(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "personal-details")

def fetch_researcher_urls(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "researcher-urls")


def fetch_activities(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "activities")

def fetch_educations(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "educations")

def fetch_employments(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "employments")

def fetch_fundings(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "fundings")

def fetch_peer_reviews(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "peer-reviews")

def fetch_works(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "works")

def fetch_research_resources(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "research-resources")

def fetch_services(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "services")

def fetch_qualifications(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "qualifications")

def fetch_memberships(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "memberships")

def fetch_distinctions(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "distinctions")

def fetch_invited_positions(orcid_id: str): 
    return fetch_orcid_data(orcid_id, "invited-positions")
