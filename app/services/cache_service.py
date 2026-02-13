"""
Module: cache_service.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Core Cache Construction Service.
    
    This module contains the heavy-lifting logic for synchronizing local database tables
    with the external ORCID API. It is optimized for high-volume data processing.
    
    Key Optimization Strategies:
    1. **Concurrency**: Uses `concurrent.futures` to fetch thousands of ORCID profiles in parallel,
       bypassing the latency of sequential HTTP requests.
    2. **Bulk Persistence**: Uses SQLAlchemy's `bulk_save_objects` to insert records in batches,
       significantly reducing database transaction overhead.
    3. **In-Memory Transformation**: Parses large JSON blobs in memory to extract only relevant 
       metadata (Works, Fundings) before touching the database.
    4. **Data Healing**: Automatically resolves missing identifiers (GRID IDs) via ROR API lookup.
"""

import logging
from flask import current_app
from sqlalchemy import or_
from .. import db
from ..models import WorkCache, FundingCache, ResearcherStatus, User, ResearcherCache
from .orcid_service import list_orcids_for_institution, get_all_profiles_concurrently, get_full_orcid_profile
from .ror_service import fetch_grid_from_ror

logger = logging.getLogger(__name__)

# ============================================================
# DATA PERSISTENCE HELPERS
# ============================================================

def _flush_bulk(bulk: list, model_name: str) -> int:
    """
    Persists a batch of SQLAlchemy objects to the database in a single transaction.
    
    This function is critical for performance when processing thousands of records.
    It clears the list after committing to free up memory.
    
    Args:
        bulk (list): A list of model instances (e.g., [WorkCache(...), ...]).
        model_name (str): The name of the model (for error logging context).
        
    Returns:
        int: The number of records successfully saved.
    """
    if not bulk:
        return 0
    try:
        db.session.bulk_save_objects(bulk)
        db.session.commit()
        count = len(bulk)
        bulk.clear() # Clear memory immediately after commit
        return count
    except Exception as exc:
        db.session.rollback()
        logger.exception("CRITICAL: Failed to save %s batch: %s", model_name, exc)
        return 0


def ensure_and_heal_grid_for_ror(ror_id: str) -> str or None:
    """
    Ensures that a ROR ID has an associated GRID ID in the local database.
    
    ORCID's API often relies on GRID IDs for affiliation searches. If our local 
    user record has a ROR but no GRID, this function attempts to 'heal' the data 
    by fetching the GRID ID from the external ROR API.
    
    Args:
        ror_id (str): The ROR identifier to verify.
        
    Returns:
        str or None: The resolved GRID ID or None if not found.
    """
    if not ror_id:
        return None
        
    # 1. Check local database for an existing mapping
    existing = User.query.filter(
        User.ror_id == ror_id, 
        User.grid_id.isnot(None), 
        User.grid_id != ""
    ).first()
    
    grid_id = existing.grid_id if existing else None

    # 2. If missing locally, fetch from external ROR API (Self-Healing)
    if not grid_id:
        grid_id = fetch_grid_from_ror(ror_id)

    # 3. Propagate the resolved GRID ID to all users with this ROR
    if grid_id:
        users_to_update = User.query.filter(
            User.ror_id == ror_id, 
            or_(User.grid_id.is_(None), User.grid_id == "")
        ).all()
        
        if users_to_update:
            for user in users_to_update:
                user.grid_id = grid_id
            try:
                db.session.commit()
            except Exception as exc:
                db.session.rollback()
                logger.error("Failed to sync GRID ID for ROR %s: %s", ror_id, exc)
                
    return grid_id


def _extract_status_from_profile(profile_data: dict, ror_id: str, orcid: str, trusted_ids: list) -> ResearcherStatus:
    """
    Analyzes a researcher's full ORCID profile to determine if their affiliation 
    is 'Managed' by the institution.
    
    A profile is considered 'Managed' if it contains an affiliation entry (Employment, Education, etc.)
    that was added or updated by a trusted API Client ID (Affiliation Manager).
    
    Args:
        profile_data (dict): The full ORCID profile JSON.
        ror_id (str): The institutional ROR ID context.
        orcid (str): The researcher's ORCID iD.
        trusted_ids (list): List of API Client IDs authorized by the institution.
        
    Returns:
        ResearcherStatus: A model instance representing the management status.
    """
    is_managed = False
    activities = profile_data.get('activities-summary') or {}
    
    # ORCID sections where affiliation data can reside
    sections_to_check = [
        'employments', 'educations', 'qualifications', 
        'invited-positions', 'distinctions', 'memberships', 'services'
    ]

    for section in sections_to_check:
        section_data = activities.get(section) or {}
        for group in section_data.get('affiliation-group', []):
            for summary in group.get('summaries', []):
                # Dynamically locate the summary item (keys vary by section, e.g., 'employment-summary')
                item_data = next(
                    (val for val in summary.values() if isinstance(val, dict) and 'source' in val), 
                    None
                )
                
                if not item_data:
                    continue

                source = item_data.get('source') or {}
                source_client_path = (source.get('source-client-id') or {}).get('path')

                # Verification: Does the record source match our institutional App Keys?
                if source_client_path and source_client_path in trusted_ids:
                    is_managed = True
                    break
            if is_managed:
                break
        if is_managed:
            break

    return ResearcherStatus(ror_id=ror_id, orcid=orcid, is_managed_by_am=is_managed)


# ============================================================
# CACHE BUILDER: WORKS (PUBLICATIONS)
# ============================================================

def build_works_cache_for_ror(ror_id: str, base_url: str, headers: dict) -> int:
    """
    Rebuilds the local cache of Publications (Works) for a specific institution.
    
    Process:
    1. Resolves GRID ID for the ROR.
    2. Searches ORCID for all researchers affiliated with the institution.
    3. Fetches full profiles concurrently.
    4. Extracts Works metadata and Affiliation Status.
    5. Bulk inserts data into `work_cache` and `researcher_status` tables.
    
    Args:
        ror_id (str): Target ROR ID.
        base_url (str): ORCID API base URL.
        headers (dict): Authorization headers.
        
    Returns:
        int: Total number of publications cached.
    """
    grid_id = ensure_and_heal_grid_for_ror(ror_id)
    
    # 1. Discovery Phase
    researchers = list_orcids_for_institution(ror_id, grid_id, base_url, headers) or []
    logger.info("Works Cache: Processing %d researchers for %s", len(researchers), ror_id)
    
    if not researchers:
        return 0

    # 2. Configuration Phase (Trusted Client IDs)
    system_client_id = current_app.config.get("ORCID_CLIENT_ID")
    trusted_ids = [system_client_id] if system_client_id else []
    
    # Add institution-specific Affiliation Manager ID if configured
    manager_user = User.query.filter_by(ror_id=ror_id).filter(User.am_client_id.isnot(None)).first()
    if manager_user and manager_user.am_client_id:
        trusted_ids.append(manager_user.am_client_id)
        logger.info("ROR %s using custom AM Key: %s", ror_id, manager_user.am_client_id)

    orcid_ids = [r.get('orcid-id') for r in researchers if r.get('orcid-id')]

    # 3. Cleanup Phase (Purge old cache)
    WorkCache.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)
    ResearcherStatus.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)
    db.session.commit()

    # 4. Fetch Phase (Multithreaded)
    profiles_map = get_all_profiles_concurrently(orcid_ids, max_workers=10)

    total_works_cached = 0
    works_buffer = []
    status_buffer = []
    
    # 5. Processing Phase (In-Memory)
    for orcid_id, profile in profiles_map.items():
        if not profile:
            continue

        # A. Determine Management Status (AM Badge)
        status_buffer.append(_extract_status_from_profile(profile, ror_id, orcid_id, trusted_ids))

        # B. Parse Publication Metadata
        activities = profile.get('activities-summary') or {}
        works_container = activities.get('works') or {}
        
        for group in works_container.get('group', []):
            for work in (group.get('work-summary') or []):
                title_node = work.get('title') or {}
                pub_date = work.get('publication-date') or {}
                external_ids = (work.get('external-ids') or {}).get('external-id') or []
                
                # Prioritize DOI and ISSN extraction
                doi, issn, others = None, None, []
                for eid in external_ids:
                    id_type = (eid.get('external-id-type') or '').lower()
                    id_val = eid.get('external-id-value')
                    if id_type == 'doi' and not doi:
                        doi = id_val
                    elif id_type == 'issn' and not issn:
                        issn = id_val
                    elif id_val:
                        others.append(f"{id_type}:{id_val}")

                works_buffer.append(WorkCache(
                    ror_id=ror_id, orcid=orcid_id, 
                    title=(title_node.get('title') or {}).get('value'),
                    type=work.get('type'),
                    put_code=work.get('put-code'), 
                    journal_title=(work.get('journal-title') or {}).get('value'),
                    pub_year=((pub_date.get('year') or {}).get('value')),
                    pub_month=((pub_date.get('month') or {}).get('value')),
                    pub_day=((pub_date.get('day') or {}).get('value')),
                    doi=doi, issn=issn,
                    other_external_ids='; '.join(others) if others else None,
                    source=((work.get('source', {}).get('source-name') or {}).get('value')),
                    url=(work.get('url') or {}).get('value'), 
                    visibility=work.get('visibility')
                ))
                total_works_cached += 1

        # Memory Optimization: Periodic bulk flush prevents RAM exhaustion
        if len(works_buffer) >= 2000:
            _flush_bulk(works_buffer, "WorkCache")
        if len(status_buffer) >= 1000:
            _flush_bulk(status_buffer, "ResearcherStatus")

    # Final database synchronization
    _flush_bulk(works_buffer, "WorkCache")
    _flush_bulk(status_buffer, "ResearcherStatus")
    
    logger.info("Finished Works Cache for %s. Total: %d", ror_id, total_works_cached)
    return total_works_cached


# ============================================================
# CACHE BUILDER: FUNDINGS (GRANTS)
# ============================================================

def build_fundings_cache_for_ror(ror_id: str, base_url: str, headers: dict) -> int:
    """
    Rebuilds the local cache of Funding (Grants) for a specific institution.
    Follows a similar logic to `build_works_cache_for_ror`.
    
    Returns:
        int: Total number of funding records cached.
    """
    grid_id = ensure_and_heal_grid_for_ror(ror_id)
    researchers = list_orcids_for_institution(ror_id, grid_id, base_url, headers) or []
    
    if not researchers:
        return 0

    orcid_ids = [r.get('orcid-id') for r in researchers if r.get('orcid-id')]
    
    # Purge existing cache
    FundingCache.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)
    db.session.commit()

    # Concurrent Fetch
    profiles_map = get_all_profiles_concurrently(orcid_ids, max_workers=10)
    
    total_fundings_cached = 0
    funding_buffer = []
    
    for orcid_id, profile in profiles_map.items():
        if not profile:
            continue

        activities = profile.get('activities-summary') or {}
        funding_container = activities.get('fundings') or {}
        
        for group in funding_container.get('group', []):
            for summary in (group.get('funding-summary') or []):
                org = summary.get('organization') or {}
                address = org.get('address') or {}
                start_date = summary.get('start-date') or {}
                end_date = summary.get('end-date') or {}
                amount_node = summary.get('amount') or {}
                external_ids = (summary.get('external-ids') or {}).get('external-id') or []
                
                # Extract Grant Number
                grant_id = next(
                    (eid.get('external-id-value') for eid in external_ids 
                     if 'grant' in (eid.get('external-id-type') or '').lower()), 
                    None
                )

                funding_buffer.append(FundingCache(
                    ror_id=ror_id, orcid=orcid_id,
                    title=((summary.get('title', {}).get('title') or {}).get('value')),
                    type=summary.get('type'), 
                    org_name=org.get('name'),
                    city=address.get('city'), 
                    country=address.get('country'),
                    start_y=((start_date.get('year') or {}).get('value')),
                    start_m=((start_date.get('month') or {}).get('value')),
                    start_d=((start_date.get('day') or {}).get('value')),
                    end_y=((end_date.get('year') or {}).get('value')),
                    end_m=((end_date.get('month') or {}).get('value')),
                    end_d=((end_date.get('day') or {}).get('value')),
                    grant_number=grant_id, 
                    currency=amount_node.get('currency-code'), 
                    amount=amount_node.get('value'),
                    source=((summary.get('source', {}).get('source-name') or {}).get('value')),
                    visibility=summary.get('visibility'), 
                    url=(summary.get('url') or {}).get('value')
                ))
                total_fundings_cached += 1

        if len(funding_buffer) >= 2000:
            _flush_bulk(funding_buffer, "FundingCache")

    _flush_bulk(funding_buffer, "FundingCache")
    logger.info("Finished Funding Cache for %s. Total: %d", ror_id, total_fundings_cached)
    return total_fundings_cached


# ============================================================
# CACHE BUILDER: RESEARCHER PROFILES (NAMES)
# ============================================================

def build_researcher_names_cache(ror_id: str):
    """
    Synchronizes researcher profile details (Names, Bio) into the ResearcherCache table.
    
    Optimization:
    - Uses multithreading to fetch profiles concurrently.
    - Pre-fetches existing records to perform upserts (Update if exists, Insert if new) efficiently.
    - Commits in batches to avoid locking the database for too long.
    """
    # 1. Identify Target ORCIDs
    # Get all unique ORCIDs present in the Work and Funding caches for this ROR
    w_orcids = db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id)
    f_orcids = db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id)
    all_orcids = [r[0] for r in w_orcids.union(f_orcids).all()]
    
    total = len(all_orcids)
    if total == 0:
        return 0

    logger.info("Profile Sync: Fetching %d profiles concurrently for ROR %s", total, ror_id)

    # 2. Concurrent Fetch (Multithreaded)
    profiles_map = get_all_profiles_concurrently(all_orcids, max_workers=10)

    # 3. Process and Buffer Updates
    updated_count = 0
    
    # Pre-fetch existing researcher records into a dictionary for O(1) lookup
    # This avoids N+1 SELECT queries inside the loop
    existing_researchers = {r.orcid: r for r in ResearcherCache.query.filter(ResearcherCache.orcid.in_(all_orcids)).all()}

    for orcid, profile in profiles_map.items():
        if not profile:
            continue

        person = profile.get('person', {})
        name = person.get('name', {})
        
        given = (name.get('given-names') or {}).get('value')
        family = (name.get('family-name') or {}).get('value')
        credit = (name.get('credit-name') or {}).get('value')
        
        # Upsert Logic: Update if exists, else Create
        researcher = existing_researchers.get(orcid)
        if not researcher:
            researcher = ResearcherCache(orcid=orcid)
            db.session.add(researcher) # Register new object with session
        
        researcher.given_names = given
        researcher.family_name = family
        researcher.credit_name = credit
        
        updated_count += 1
        
        # Batch commit every 500 records
        if updated_count % 500 == 0:
            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.error("Error during partial commit for researcher names: %s", e)

    # Final commit for remaining records
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error("Error during final commit for researcher names: %s", e)

    logger.info("Successfully synchronized %d researcher names for ROR %s", updated_count, ror_id)
    return updated_count