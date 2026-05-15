"""Helpers for resolving ROR metadata used by ORCID searches."""

import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def fetch_grid_from_ror(ror_id: str) -> Optional[str]:
    """
    Return the GRID identifier associated with a ROR record, when available.

    ROR has exposed `external_ids` as both list and dict shapes over time, so
    both formats are accepted here.
    """
    if not ror_id:
        return None

    clean_ror = ror_id.strip().rstrip('/').split('/')[-1]
    api_url = f"https://api.ror.org/organizations/https://ror.org/{clean_ror}"

    try:
        response = requests.get(api_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            external_ids = data.get('external_ids', [])

            if isinstance(external_ids, list):
                for identifier in external_ids:
                    if identifier.get('type') == 'grid':
                        return identifier.get('preferred') or (
                            identifier.get('all') and identifier['all'][0]
                        )

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
