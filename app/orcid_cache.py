"""Persistence helpers for aggregated yearly ORCID datasets."""

from datetime import datetime
from typing import Optional, Dict, Any
from flask import current_app
from .models import OrcidCache, db


def get_cached_data(year: int) -> Optional[OrcidCache]:
    """Return the cached dataset for a year, if present."""
    try:
        return OrcidCache.query.filter_by(year=year).first()
    except Exception as exc:
        current_app.logger.exception("Failed to retrieve ORCID cache for year %s: %s", year, exc)
        return None


def save_cache(year: int, data: Dict[str, Any]) -> Optional[OrcidCache]:
    """Create or update the cached dataset for a year."""
    try:
        cache_entry = OrcidCache.query.filter_by(year=year).first()
        
        if cache_entry:
            cache_entry.data = data
            cache_entry.created_at = datetime.utcnow()
            current_app.logger.debug("Updating existing ORCID cache entry for year %s", year)
        else:
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
