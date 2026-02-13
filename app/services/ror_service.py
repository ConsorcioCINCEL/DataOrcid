"""
Module: ror_service.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Research Organization Registry (ROR) Integration Service.
    
    This module provides utilities to interact with the ROR API, primarily to
    resolve legacy identifiers like GRID (Global Research Identifier Database).
    
    Key Features:
    - Resolves institutional ROR IDs to GRID IDs (required for some ORCID queries).
    - Robust parsing of ROR API responses, handling schema changes (v1 vs v2).
    - Error handling for network issues and invalid identifiers.
"""

import requests
import logging
from typing import Optional

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


def fetch_grid_from_ror(ror_id: str) -> Optional[str]:
    """
    Queries the ROR API to find the equivalent GRID identifier for a given institution.
    
    Why is this needed?
    While ROR is the modern standard, the ORCID API still relies heavily on GRID IDs
    for searching affiliations. This function acts as a bridge.

    Logic:
    1. Sanitizes the input ROR ID to extract the raw identifier.
    2. Queries the public ROR API.
    3. Parses the 'external_ids' field, supporting both:
       - Schema v2 (List of dictionaries).
       - Schema v1 (Dictionary of keys).

    Args:
        ror_id (str): The ROR identifier (e.g., '02ap3w078' or full URL).

    Returns:
        Optional[str]: The resolved GRID ID (e.g., 'grid.424112.0') if found, else None.
    """
    if not ror_id:
        return None

    # 1. Input Sanitization
    # Remove URL prefixes and trailing slashes to get the clean ID
    clean_ror = ror_id.strip().rstrip('/').split('/')[-1]
    
    # Construct the ROR API endpoint
    api_url = f"https://api.ror.org/organizations/https://ror.org/{clean_ror}"

    try:
        # 2. API Request
        # Set a 10s timeout to prevent hanging the application on network issues
        response = requests.get(api_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # 3. Response Parsing
            # The 'external_ids' field structure changed in recent ROR API versions.
            # We must handle both to ensure stability.
            external_ids = data.get('external_ids', [])

            # Case A: Modern Schema (List of dictionaries)
            # Structure: [{"type": "grid", "preferred": "grid.xxx", "all": ["grid.xxx"]}]
            if isinstance(external_ids, list):
                for identifier in external_ids:
                    # Look specifically for the 'grid' type entry
                    if identifier.get('type') == 'grid':
                        # Return the 'preferred' ID or fallback to the first in 'all' list
                        return identifier.get('preferred') or (
                            identifier.get('all') and identifier['all'][0]
                        )

            # Case B: Legacy Schema Fallback (Dictionary)
            # Structure: {"GRID": {"preferred": "grid.xxx", "all": ["grid.xxx"]}}
            elif isinstance(external_ids, dict):
                grid_data = external_ids.get('GRID', {})
                return grid_data.get('preferred') or (
                    grid_data.get('all') and grid_data['all'][0]
                )
        
        elif response.status_code == 404:
            logger.info("ROR Identifier '%s' not found in registry.", clean_ror)
        else:
            logger.warning("ROR API returned unexpected status code %d for %s", 
                           response.status_code, clean_ror)
                
    except requests.exceptions.RequestException as exc:
        logger.error("Network error communicating with ROR API for %s: %s", clean_ror, exc)
    except Exception as exc:
        logger.exception("Unexpected error parsing ROR response for %s: %s", clean_ror, exc)
    
    return None