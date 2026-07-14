"""Build and refresh local ORCID caches for institutions and researchers."""

from datetime import datetime, timezone
import logging
import re

from flask import current_app
from sqlalchemy import or_

from .. import db
from ..models import (
    FundingCache,
    InstitutionResearcher,
    ResearcherCache,
    ResearcherStatus,
    User,
    WorkCache,
)
from .institution_registry_service import (
    ensure_institution_registry,
    get_institution_by_ror,
    get_institution_identifiers,
    upsert_institution_identifier,
)
from .orcid_service import get_all_profiles_concurrently, list_orcids_for_institution
from .ror_service import fetch_grid_from_ror

logger = logging.getLogger(__name__)
ISSN_RE = re.compile(r"^\d{4}-?\d{3}[\dXx]$")
PROFILE_BATCH_SIZE = 250


def _clean_external_id_value(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def _is_valid_issn(value: str | None) -> bool:
    text = _clean_external_id_value(value)
    return bool(text and len(text) <= 64 and ISSN_RE.match(text))


def _serialize_external_id(id_type: str, value: str) -> str:
    label = (id_type or "external-id").strip() or "external-id"
    return f"{label}:{value}"


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


def _chunks(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def ensure_and_heal_grid_for_ror(ror_id: str) -> str | None:
    """Resolve a GRID ID for a ROR and persist it as institutional metadata."""
    if not ror_id:
        return None

    identifiers = get_institution_identifiers(ror_id)
    grid_id = identifiers.get("grid", [None])[0] if identifiers.get("grid") else None

    if not grid_id:
        existing = User.query.filter(
            User.ror_id == ror_id,
            User.grid_id.isnot(None),
            User.grid_id != "",
        ).first()
        grid_id = existing.grid_id if existing else None

    if not grid_id:
        grid_id = fetch_grid_from_ror(ror_id)

    if not grid_id:
        return None

    institution = ensure_institution_registry(ror_id)
    if not institution.grid_id:
        institution.grid_id = grid_id
    upsert_institution_identifier(
        ror_id,
        "grid",
        grid_id,
        source="ror",
        is_verified=True,
    )

    users_to_update = User.query.filter(
        User.ror_id == ror_id,
        or_(User.grid_id.is_(None), User.grid_id == ""),
    ).all()
    for user in users_to_update:
        user.grid_id = grid_id

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        logger.error("Failed to persist GRID ID for ROR %s: %s", ror_id, exc)
    return grid_id


def _extract_status_from_profile(
    profile_data: dict,
    ror_id: str,
    orcid: str,
    trusted_ids: list[str],
) -> ResearcherStatus:
    """Mark whether a profile has affiliation records written by trusted clients."""
    is_managed = False
    activities = profile_data.get("activities-summary") or {}
    sections_to_check = [
        "employments",
        "educations",
        "qualifications",
        "invited-positions",
        "distinctions",
        "memberships",
        "services",
    ]

    for section in sections_to_check:
        section_data = activities.get(section) or {}
        for group in section_data.get("affiliation-group", []):
            for summary in group.get("summaries", []):
                item_data = next(
                    (
                        value
                        for value in summary.values()
                        if isinstance(value, dict) and "source" in value
                    ),
                    None,
                )
                if not item_data:
                    continue

                source = item_data.get("source") or {}
                source_client_path = (source.get("source-client-id") or {}).get("path")
                if source_client_path and source_client_path in trusted_ids:
                    is_managed = True
                    break
            if is_managed:
                break
        if is_managed:
            break

    return ResearcherStatus(
        ror_id=ror_id,
        orcid=orcid,
        is_managed_by_am=is_managed,
    )


def discover_researchers_for_ror(
    ror_id: str,
    base_url: str | None = None,
    headers: dict | None = None,
) -> tuple[list[dict], int]:
    """Search all institutional IDs and persist a complete association snapshot."""
    ensure_and_heal_grid_for_ror(ror_id)
    identifiers = get_institution_identifiers(ror_id)
    researchers = list_orcids_for_institution(
        ror_id,
        _first(identifiers.get("grid", [])),
        base_url=base_url,
        headers=headers,
        grid_ids=identifiers.get("grid", []),
        ringgold_ids=identifiers.get("ringgold", []),
    )
    institution_id = _persist_discovered_researchers(ror_id, researchers)
    return researchers, institution_id


def _persist_discovered_researchers(ror_id: str, researchers: list[dict]) -> int:
    """Store every search hit before any potentially failing profile download."""
    institution = ensure_institution_registry(ror_id)
    db.session.flush()
    now = _utc_now()
    existing = {
        row.orcid: row
        for row in InstitutionResearcher.query.filter_by(
            institution_id=institution.id
        ).all()
    }

    for association in existing.values():
        association.is_active = False

    orcid_ids = [
        record.get("orcid-id")
        for record in researchers
        if record.get("orcid-id")
    ]
    researcher_cache = _load_researcher_cache(orcid_ids)

    for record in researchers:
        orcid = (record.get("orcid-id") or "").strip()
        if not orcid:
            continue

        association = existing.get(orcid)
        if not association:
            association = InstitutionResearcher(
                institution_id=institution.id,
                orcid=orcid,
                first_seen_at=now,
            )
            db.session.add(association)
            existing[orcid] = association

        matches = record.get("matched_identifiers") or {}
        association.matched_by_ror = bool(matches.get("ror"))
        association.matched_by_grid = bool(matches.get("grid"))
        association.matched_by_ringgold = bool(matches.get("ringgold"))
        association.is_active = True
        association.profile_status = "pending"
        association.profile_error = None
        association.last_seen_at = now

        _update_researcher_from_expanded(record, researcher_cache)

    db.session.commit()
    return institution.id


def build_full_cache_for_ror(
    ror_id: str,
    base_url: str,
    headers: dict,
    max_orcids: int | None = None,
) -> dict:
    """Discover researchers once and rebuild works, fundings, names, and status."""
    return _build_cache_for_ror(
        ror_id,
        base_url=base_url,
        headers=headers,
        include_works=True,
        include_fundings=True,
        max_orcids=max_orcids,
    )


def build_works_cache_for_ror(
    ror_id: str,
    base_url: str,
    headers: dict,
    max_orcids: int | None = None,
) -> int:
    """Rebuild works while preserving every discovered researcher association."""
    result = _build_cache_for_ror(
        ror_id,
        base_url=base_url,
        headers=headers,
        include_works=True,
        include_fundings=False,
        max_orcids=max_orcids,
    )
    return result["works"]


def build_fundings_cache_for_ror(
    ror_id: str,
    base_url: str,
    headers: dict,
    max_orcids: int | None = None,
) -> int:
    """Rebuild fundings while preserving every discovered researcher association."""
    result = _build_cache_for_ror(
        ror_id,
        base_url=base_url,
        headers=headers,
        include_works=False,
        include_fundings=True,
        max_orcids=max_orcids,
    )
    return result["fundings"]


def _build_cache_for_ror(
    ror_id: str,
    *,
    base_url: str,
    headers: dict,
    include_works: bool,
    include_fundings: bool,
    max_orcids: int | None = None,
) -> dict:
    researchers, institution_id = discover_researchers_for_ror(
        ror_id,
        base_url=base_url,
        headers=headers,
    )
    result = {
        "researchers": len(researchers),
        "profiles": 0,
        "works": 0,
        "fundings": 0,
    }
    orcid_ids = [
        record.get("orcid-id")
        for record in researchers
        if record.get("orcid-id")
    ]

    work_cleanup = WorkCache.query.filter_by(ror_id=ror_id)
    funding_cleanup = FundingCache.query.filter_by(ror_id=ror_id)
    status_cleanup = ResearcherStatus.query.filter_by(ror_id=ror_id)
    if orcid_ids:
        work_cleanup = work_cleanup.filter(WorkCache.orcid.notin_(orcid_ids))
        funding_cleanup = funding_cleanup.filter(FundingCache.orcid.notin_(orcid_ids))
        status_cleanup = status_cleanup.filter(ResearcherStatus.orcid.notin_(orcid_ids))

    if include_works:
        work_cleanup.delete(synchronize_session=False)
    if include_fundings:
        funding_cleanup.delete(synchronize_session=False)
    status_cleanup.delete(synchronize_session=False)

    if not researchers:
        db.session.commit()
        return result

    if max_orcids:
        logger.info(
            "Cache build: limiting profile fetch to %d ORCID iDs for %s",
            max_orcids,
            ror_id,
        )
        orcid_ids = orcid_ids[:max_orcids]

    associations = {
        row.orcid: row
        for row in InstitutionResearcher.query.filter_by(
            institution_id=institution_id,
            is_active=True,
        ).all()
    }
    trusted_ids = _trusted_client_ids(ror_id)

    try:
        work_buffer = []
        funding_buffer = []
        status_buffer = []

        for batch in _chunks(orcid_ids, PROFILE_BATCH_SIZE):
            profiles = get_all_profiles_concurrently(batch, max_workers=10)
            researcher_cache = _load_researcher_cache(batch)
            now = _utc_now()
            successful_orcids = [orcid for orcid in batch if profiles.get(orcid)]

            if successful_orcids:
                if include_works:
                    WorkCache.query.filter(
                        WorkCache.ror_id == ror_id,
                        WorkCache.orcid.in_(successful_orcids),
                    ).delete(synchronize_session=False)
                if include_fundings:
                    FundingCache.query.filter(
                        FundingCache.ror_id == ror_id,
                        FundingCache.orcid.in_(successful_orcids),
                    ).delete(synchronize_session=False)
                ResearcherStatus.query.filter(
                    ResearcherStatus.ror_id == ror_id,
                    ResearcherStatus.orcid.in_(successful_orcids),
                ).delete(synchronize_session=False)

            for orcid in batch:
                association = associations.get(orcid)
                profile = profiles.get(orcid)
                if not profile:
                    if association:
                        association.profile_status = "failed"
                        association.profile_error = "No public ORCID profile data was returned."
                    continue

                result["profiles"] += 1
                if association:
                    association.profile_status = "success"
                    association.profile_error = None
                    association.profile_updated_at = now

                _update_researcher_from_profile(orcid, profile, researcher_cache)
                status_buffer.append(
                    _extract_status_from_profile(profile, ror_id, orcid, trusted_ids)
                )

                if include_works:
                    rows = _work_rows_from_profile(ror_id, orcid, profile)
                    work_buffer.extend(rows)
                    result["works"] += len(rows)
                if include_fundings:
                    rows = _funding_rows_from_profile(ror_id, orcid, profile)
                    funding_buffer.extend(rows)
                    result["fundings"] += len(rows)

            if len(work_buffer) >= 2000:
                _flush_bulk(work_buffer, "WorkCache")
            if len(funding_buffer) >= 2000:
                _flush_bulk(funding_buffer, "FundingCache")
            if len(status_buffer) >= 1000:
                _flush_bulk(status_buffer, "ResearcherStatus")
            db.session.flush()

        _flush_bulk(work_buffer, "WorkCache")
        _flush_bulk(funding_buffer, "FundingCache")
        _flush_bulk(status_buffer, "ResearcherStatus")
        db.session.commit()

        if include_works:
            result["works"] = WorkCache.query.filter_by(ror_id=ror_id).count()
        if include_fundings:
            result["fundings"] = FundingCache.query.filter_by(ror_id=ror_id).count()
    except Exception as exc:
        db.session.rollback()
        _mark_pending_associations_failed(institution_id, str(exc))
        raise

    logger.info(
        "Finished cache build for %s: %d researchers, %d profiles, %d works, %d fundings.",
        ror_id,
        result["researchers"],
        result["profiles"],
        result["works"],
        result["fundings"],
    )
    return result


def build_researcher_names_cache(ror_id: str) -> int:
    """Refresh names for every active institutional researcher association."""
    institution = get_institution_by_ror(ror_id)
    all_orcids = []
    institution_id = institution.get("institution_id") if institution else None

    if institution_id:
        all_orcids = [
            orcid
            for (orcid,) in db.session.query(InstitutionResearcher.orcid)
            .filter_by(institution_id=institution_id, is_active=True)
            .all()
        ]

    if not all_orcids:
        work_orcids = db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id)
        funding_orcids = db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id)
        status_orcids = db.session.query(ResearcherStatus.orcid).filter_by(ror_id=ror_id)
        all_orcids = sorted(
            {
                value
                for (value,) in work_orcids.union(funding_orcids, status_orcids).all()
                if value
            }
        )

    if not all_orcids:
        return 0

    updated_count = 0
    for batch in _chunks(all_orcids, PROFILE_BATCH_SIZE):
        profiles = get_all_profiles_concurrently(batch, max_workers=10)
        researcher_cache = _load_researcher_cache(batch)
        associations = {}
        if institution_id:
            associations = {
                row.orcid: row
                for row in InstitutionResearcher.query.filter(
                    InstitutionResearcher.institution_id == institution_id,
                    InstitutionResearcher.orcid.in_(batch),
                ).all()
            }

        now = _utc_now()
        for orcid in batch:
            profile = profiles.get(orcid)
            association = associations.get(orcid)
            if not profile:
                if association:
                    association.profile_status = "failed"
                    association.profile_error = "No public ORCID profile data was returned."
                continue

            _update_researcher_from_profile(orcid, profile, researcher_cache)
            if association:
                association.profile_status = "success"
                association.profile_error = None
                association.profile_updated_at = now
            updated_count += 1
        db.session.commit()

    logger.info(
        "Successfully synchronized %d researcher profiles for ROR %s",
        updated_count,
        ror_id,
    )
    return updated_count


def _load_researcher_cache(orcid_ids: list[str]) -> dict[str, ResearcherCache]:
    result = {}
    for batch in _chunks(list(dict.fromkeys(orcid_ids)), 500):
        rows = ResearcherCache.query.filter(ResearcherCache.orcid.in_(batch)).all()
        result.update({row.orcid: row for row in rows})
    return result


def _update_researcher_from_expanded(
    record: dict,
    researcher_cache: dict[str, ResearcherCache],
) -> None:
    orcid = (record.get("orcid-id") or "").strip()
    if not orcid:
        return

    researcher = researcher_cache.get(orcid)
    if not researcher:
        researcher = ResearcherCache(orcid=orcid)
        db.session.add(researcher)
        researcher_cache[orcid] = researcher

    researcher.given_names = record.get("given-names") or researcher.given_names
    researcher.family_name = record.get("family-names") or researcher.family_name
    researcher.credit_name = record.get("credit-name") or researcher.credit_name
    emails = record.get("email") or []
    if emails:
        researcher.email = emails[0]


def _update_researcher_from_profile(
    orcid: str,
    profile: dict,
    researcher_cache: dict[str, ResearcherCache],
) -> None:
    researcher = researcher_cache.get(orcid)
    if not researcher:
        researcher = ResearcherCache(orcid=orcid)
        db.session.add(researcher)
        researcher_cache[orcid] = researcher

    person = profile.get("person") or {}
    name = person.get("name") or {}
    researcher.given_names = (
        (name.get("given-names") or {}).get("value") or researcher.given_names
    )
    researcher.family_name = (
        (name.get("family-name") or {}).get("value") or researcher.family_name
    )
    researcher.credit_name = (
        (name.get("credit-name") or {}).get("value") or researcher.credit_name
    )

    emails = (person.get("emails") or {}).get("email") or []
    public_email = next((item.get("email") for item in emails if item.get("email")), None)
    if public_email:
        researcher.email = public_email
    researcher.updated_at = _utc_now()


def _work_rows_from_profile(ror_id: str, orcid: str, profile: dict) -> list[WorkCache]:
    rows = []
    works = ((profile.get("activities-summary") or {}).get("works") or {}).get("group") or []
    for group in works:
        for work in group.get("work-summary") or []:
            title_node = work.get("title") or {}
            publication_date = work.get("publication-date") or {}
            external_ids = (work.get("external-ids") or {}).get("external-id") or []
            doi, issn, others = None, None, []
            for external_id in external_ids:
                identifier_type = (external_id.get("external-id-type") or "").lower()
                identifier_value = _clean_external_id_value(
                    external_id.get("external-id-value")
                )
                if not identifier_value:
                    continue
                if identifier_type == "doi" and not doi:
                    doi = identifier_value
                elif identifier_type == "issn" and not issn and _is_valid_issn(identifier_value):
                    issn = identifier_value
                else:
                    others.append(_serialize_external_id(identifier_type, identifier_value))

            rows.append(
                WorkCache(
                    ror_id=ror_id,
                    orcid=orcid,
                    title=(title_node.get("title") or {}).get("value"),
                    type=work.get("type"),
                    put_code=work.get("put-code"),
                    journal_title=(work.get("journal-title") or {}).get("value"),
                    pub_year=((publication_date.get("year") or {}).get("value")),
                    pub_month=((publication_date.get("month") or {}).get("value")),
                    pub_day=((publication_date.get("day") or {}).get("value")),
                    doi=doi,
                    issn=issn,
                    other_external_ids="; ".join(others) if others else None,
                    source=((work.get("source") or {}).get("source-name") or {}).get("value"),
                    url=(work.get("url") or {}).get("value"),
                    visibility=work.get("visibility"),
                )
            )
    return rows


def _funding_rows_from_profile(
    ror_id: str,
    orcid: str,
    profile: dict,
) -> list[FundingCache]:
    rows = []
    fundings = ((profile.get("activities-summary") or {}).get("fundings") or {}).get("group") or []
    for group in fundings:
        for summary in group.get("funding-summary") or []:
            organization = summary.get("organization") or {}
            address = organization.get("address") or {}
            start_date = summary.get("start-date") or {}
            end_date = summary.get("end-date") or {}
            amount = summary.get("amount") or {}
            external_ids = (summary.get("external-ids") or {}).get("external-id") or []
            grant_id = next(
                (
                    _clean_external_id_value(external_id.get("external-id-value"))
                    for external_id in external_ids
                    if "grant" in (external_id.get("external-id-type") or "").lower()
                ),
                None,
            )

            rows.append(
                FundingCache(
                    ror_id=ror_id,
                    orcid=orcid,
                    title=((summary.get("title") or {}).get("title") or {}).get("value"),
                    type=summary.get("type"),
                    org_name=organization.get("name"),
                    city=address.get("city"),
                    country=address.get("country"),
                    start_y=((start_date.get("year") or {}).get("value")),
                    start_m=((start_date.get("month") or {}).get("value")),
                    start_d=((start_date.get("day") or {}).get("value")),
                    end_y=((end_date.get("year") or {}).get("value")),
                    end_m=((end_date.get("month") or {}).get("value")),
                    end_d=((end_date.get("day") or {}).get("value")),
                    grant_number=grant_id,
                    currency=amount.get("currency-code"),
                    amount=amount.get("value"),
                    source=((summary.get("source") or {}).get("source-name") or {}).get("value"),
                    visibility=summary.get("visibility"),
                    url=(summary.get("url") or {}).get("value"),
                )
            )
    return rows


def _trusted_client_ids(ror_id: str) -> list[str]:
    identifiers = []
    system_client_id = current_app.config.get("ORCID_CLIENT_ID")
    if system_client_id:
        identifiers.append(system_client_id)

    manager = (
        User.query.filter_by(ror_id=ror_id)
        .filter(User.am_client_id.isnot(None), User.am_client_id != "")
        .first()
    )
    if manager and manager.am_client_id and manager.am_client_id not in identifiers:
        identifiers.append(manager.am_client_id)
    return identifiers


def _mark_pending_associations_failed(institution_id: int, error: str) -> None:
    try:
        rows = InstitutionResearcher.query.filter_by(
            institution_id=institution_id,
            is_active=True,
            profile_status="pending",
        ).all()
        for row in rows:
            row.profile_status = "failed"
            row.profile_error = (error or "Cache build failed.")[:2000]
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("Failed to record researcher profile errors for institution %s", institution_id)


def _first(values: list[str]) -> str | None:
    return values[0] if values else None


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for database columns without time zones."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
