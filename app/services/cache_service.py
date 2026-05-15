"""Build and refresh local ORCID caches for works, fundings, and profiles."""

import logging
from flask import current_app
from sqlalchemy import or_
from .. import db
from ..models import WorkCache, FundingCache, ResearcherStatus, User, ResearcherCache
from .orcid_service import list_orcids_for_institution, get_all_profiles_concurrently, get_full_orcid_profile
from .ror_service import fetch_grid_from_ror
from .institution_registry_service import get_institution_by_ror

logger = logging.getLogger(__name__)

def _flush_bulk(bulk: list, model_name: str) -> int:
    """Flush a batch while keeping the caller's transaction open."""
    if not bulk:
        return 0
    try:
        db.session.bulk_save_objects(bulk)
        db.session.flush()
        count = len(bulk)
        bulk.clear()
        return count
    except Exception as exc:
        db.session.rollback()
        logger.exception("CRITICAL: Failed to save %s batch: %s", model_name, exc)
        raise


def ensure_and_heal_grid_for_ror(ror_id: str) -> str or None:
    """
    Resolve the GRID ID for a ROR and persist it to local user records.

    ORCID searches still benefit from GRID identifiers, so the cache builders
    use ROR as the canonical local ID and fill GRID as compatibility metadata.
    """
    if not ror_id:
        return None
        
    existing = User.query.filter(
        User.ror_id == ror_id, 
        User.grid_id.isnot(None), 
        User.grid_id != ""
    ).first()
    
    grid_id = existing.grid_id if existing else None

    if not grid_id:
        institution = get_institution_by_ror(ror_id)
        grid_id = institution.get("grid_id") if institution else None

    if not grid_id:
        grid_id = fetch_grid_from_ror(ror_id)

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
    Mark whether a profile has affiliation records written by trusted clients.
    """
    is_managed = False
    activities = profile_data.get('activities-summary') or {}
    
    # ORCID stores affiliation-like records under several activity sections.
    sections_to_check = [
        'employments', 'educations', 'qualifications', 
        'invited-positions', 'distinctions', 'memberships', 'services'
    ]

    for section in sections_to_check:
        section_data = activities.get(section) or {}
        for group in section_data.get('affiliation-group', []):
            for summary in group.get('summaries', []):
                # Section summary keys vary, e.g. "employment-summary".
                item_data = next(
                    (val for val in summary.values() if isinstance(val, dict) and 'source' in val), 
                    None
                )
                
                if not item_data:
                    continue

                source = item_data.get('source') or {}
                source_client_path = (source.get('source-client-id') or {}).get('path')

                if source_client_path and source_client_path in trusted_ids:
                    is_managed = True
                    break
            if is_managed:
                break
        if is_managed:
            break

    return ResearcherStatus(ror_id=ror_id, orcid=orcid, is_managed_by_am=is_managed)


def build_works_cache_for_ror(ror_id: str, base_url: str, headers: dict) -> int:
    """
    Rebuild works and affiliation status caches for one institution.

    Existing rows are replaced only after ORCID profiles have been fetched, so
    network failures do not wipe the previous cache.
    """
    grid_id = ensure_and_heal_grid_for_ror(ror_id)
    
    researchers = list_orcids_for_institution(ror_id, grid_id, base_url, headers) or []
    logger.info("Works Cache: Processing %d researchers for %s", len(researchers), ror_id)
    
    if not researchers:
        return 0

    system_client_id = current_app.config.get("ORCID_CLIENT_ID")
    trusted_ids = [system_client_id] if system_client_id else []
    
    manager_user = User.query.filter_by(ror_id=ror_id).filter(User.am_client_id.isnot(None)).first()
    if manager_user and manager_user.am_client_id:
        trusted_ids.append(manager_user.am_client_id)
        logger.info("ROR %s using custom AM Key: %s", ror_id, manager_user.am_client_id)

    orcid_ids = [r.get('orcid-id') for r in researchers if r.get('orcid-id')]

    profiles_map = get_all_profiles_concurrently(orcid_ids, max_workers=10)

    # Replace rows in the same transaction as the inserts.
    WorkCache.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)
    ResearcherStatus.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)

    total_works_cached = 0
    works_buffer = []
    status_buffer = []
    
    for orcid_id, profile in profiles_map.items():
        if not profile:
            continue

        status_buffer.append(_extract_status_from_profile(profile, ror_id, orcid_id, trusted_ids))

        activities = profile.get('activities-summary') or {}
        works_container = activities.get('works') or {}
        
        for group in works_container.get('group', []):
            for work in (group.get('work-summary') or []):
                title_node = work.get('title') or {}
                pub_date = work.get('publication-date') or {}
                external_ids = (work.get('external-ids') or {}).get('external-id') or []
                
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

        if len(works_buffer) >= 2000:
            _flush_bulk(works_buffer, "WorkCache")
        if len(status_buffer) >= 1000:
            _flush_bulk(status_buffer, "ResearcherStatus")

    _flush_bulk(works_buffer, "WorkCache")
    _flush_bulk(status_buffer, "ResearcherStatus")
    db.session.commit()
    
    logger.info("Finished Works Cache for %s. Total: %d", ror_id, total_works_cached)
    return total_works_cached


def build_fundings_cache_for_ror(ror_id: str, base_url: str, headers: dict) -> int:
    """
    Rebuild funding cache rows for one institution.

    Existing rows are replaced only after profile fetches have completed.
    """
    grid_id = ensure_and_heal_grid_for_ror(ror_id)
    researchers = list_orcids_for_institution(ror_id, grid_id, base_url, headers) or []
    
    if not researchers:
        return 0

    orcid_ids = [r.get('orcid-id') for r in researchers if r.get('orcid-id')]
    
    profiles_map = get_all_profiles_concurrently(orcid_ids, max_workers=10)

    # Replace rows in the same transaction as the inserts.
    FundingCache.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)
    
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
    db.session.commit()
    logger.info("Finished Funding Cache for %s. Total: %d", ror_id, total_fundings_cached)
    return total_fundings_cached


def build_researcher_names_cache(ror_id: str):
    """
    Refresh display names for researchers already present in works/fundings caches.
    """
    w_orcids = db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id)
    f_orcids = db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id)
    all_orcids = [r[0] for r in w_orcids.union(f_orcids).all()]
    
    total = len(all_orcids)
    if total == 0:
        return 0

    logger.info("Profile Sync: Fetching %d profiles concurrently for ROR %s", total, ror_id)

    profiles_map = get_all_profiles_concurrently(all_orcids, max_workers=10)

    updated_count = 0

    # Preload rows to avoid one SELECT per ORCID.
    existing_researchers = {r.orcid: r for r in ResearcherCache.query.filter(ResearcherCache.orcid.in_(all_orcids)).all()}

    for orcid, profile in profiles_map.items():
        if not profile:
            continue

        person = profile.get('person', {})
        name = person.get('name', {})
        
        given = (name.get('given-names') or {}).get('value')
        family = (name.get('family-name') or {}).get('value')
        credit = (name.get('credit-name') or {}).get('value')
        
        researcher = existing_researchers.get(orcid)
        if not researcher:
            researcher = ResearcherCache(orcid=orcid)
            db.session.add(researcher) # Register new object with session
        
        researcher.given_names = given
        researcher.family_name = family
        researcher.credit_name = credit
        
        updated_count += 1
        
        if updated_count % 500 == 0:
            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                logger.error("Error during partial commit for researcher names: %s", e)

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error("Error during final commit for researcher names: %s", e)

    logger.info("Successfully synchronized %d researcher names for ROR %s", updated_count, ror_id)
    return updated_count
