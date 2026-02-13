"""
Module: orcid_cache.py
Author: Gastón Olivares
Project: DataOrcid-Chile
License: MIT
Description: Persistence services for aggregated ORCID datasets. 
             Handles the retrieval and storage of annual statistical 
             data in JSON format within the local database.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from flask import current_app
from .models import OrcidCache, db


def get_cached_data(year: int) -> Optional[OrcidCache]:
    """
    Retrieves a historical ORCID cache entry for a specific year.

    Args:
        year (int): The target year for the aggregated dataset.

    Returns:
        Optional[OrcidCache]: The database model instance if found, otherwise None.
    """
    try:
        return OrcidCache.query.filter_by(year=year).first()
    except Exception as exc:
        current_app.logger.exception("Failed to retrieve ORCID cache for year %s: %s", year, exc)
        return None


def save_cache(year: int, data: Dict[str, Any]) -> Optional[OrcidCache]:
    """
    Persists or updates the aggregated ORCID data for a specific year.

    This function implements an 'upsert' logic: it updates the existing 
    entry and its timestamp if the year is already present, or creates 
    a new record if it is not.

    Args:
        year (int): The target year for the cache entry.
        data (Dict[str, Any]): The statistical or aggregated data to be stored.

    Returns:
        Optional[OrcidCache]: The updated or newly created model instance, 
            or None if the transaction failed.
    """
    try:
        cache_entry = OrcidCache.query.filter_by(year=year).first()
        
        if cache_entry:
            # Update existing record
            cache_entry.data = data
            cache_entry.created_at = datetime.utcnow()
            current_app.logger.debug("Updating existing ORCID cache entry for year %s", year)
        else:
            # Create new record
            cache_entry = OrcidCache(year=year, data=data)
            db.session.add(cache_entry)
            current_app.logger.debug("Creating new ORCID cache entry for year %s", year)

        db.session.commit()
        current_app.logger.info("Aggregated ORCID cache successfully synchronized for year %s", year)
        return cache_entry

    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("CRITICAL: Failed to save ORCID cache for year %s: %s", year, exc)
        return None