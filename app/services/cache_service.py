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
from .data_trust_service import refresh_affiliation_evidence
from .institution_lock import institutional_writer

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
    *, persist: bool = True,
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
    institution_id = (_persist_discovered_researchers(ror_id, researchers) if persist
                      else ensure_institution_registry(ror_id).id)
    return researchers, institution_id


def _persist_discovered_researchers(ror_id: str, researchers: list[dict], *, commit: bool = True) -> int:
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
        association.evidence_type = "verified_search"
        association.evidence_sources = [
            scheme
            for scheme in ("ror", "grid", "ringgold")
            if matches.get(scheme)
        ]
        association.is_verified = True
        association.is_active = True
        association.profile_status = "pending"
        association.profile_error = None
        association.last_seen_at = now

        _update_researcher_from_expanded(record, researcher_cache)

    db.session.flush()
    if commit:
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
    *, return_result: bool = False,
) -> int | dict:
    """Rebuild works while preserving every discovered researcher association."""
    result = _build_cache_for_ror(
        ror_id,
        base_url=base_url,
        headers=headers,
        include_works=True,
        include_fundings=False,
        max_orcids=max_orcids,
    )
    return result if return_result else result["works"]


def build_fundings_cache_for_ror(
    ror_id: str,
    base_url: str,
    headers: dict,
    max_orcids: int | None = None,
    *, return_result: bool = False,
) -> int | dict:
    """Rebuild fundings while preserving every discovered researcher association."""
    result = _build_cache_for_ror(
        ror_id,
        base_url=base_url,
        headers=headers,
        include_works=False,
        include_fundings=True,
        max_orcids=max_orcids,
    )
    return result if return_result else result["fundings"]


@institutional_writer
def _build_cache_for_ror(
    ror_id: str,
    *,
    base_url: str,
    headers: dict,
    include_works: bool,
    include_fundings: bool,
    max_orcids: int | None = None,
    retry_failed: bool = False,
) -> dict:
    """Stage remote data, then publish source and derived tables atomically."""
    from ..models import InstitutionSyncProfile
    from .sync_version_service import prepare_sync_version, publish_sync_version, fail_sync_version
    from .canonical_work_service import rebuild_canonical_works
    from .analytics_service import refresh_openalex_facts

    if retry_failed:
        institution_id = ensure_institution_registry(ror_id).id
        researchers = [
            {"orcid-id": row.orcid}
            for row in InstitutionResearcher.query.filter_by(institution_id=institution_id, is_active=True).all()
        ]
        orcid_ids = [row.orcid for row in InstitutionResearcher.query.filter_by(
            institution_id=institution_id, is_active=True, profile_status="failed",
        ).all()]
    else:
        researchers, institution_id = discover_researchers_for_ror(
            ror_id, base_url=base_url, headers=headers, persist=False,
        )
        orcid_ids = list(dict.fromkeys(record["orcid-id"] for record in researchers if record.get("orcid-id")))
    all_orcids = [record["orcid-id"] for record in researchers if record.get("orcid-id")]
    if max_orcids:
        orcid_ids = orcid_ids[:max_orcids]
    result = {
        "researchers": len(researchers), "profiles": 0, "works": 0, "fundings": 0,
        "requested_profiles": len(orcid_ids), "failed_profiles": [], "retained_profiles": 0,
        "errors": [],
    }
    version_id = prepare_sync_version(ror_id, researchers, orcid_ids, get_all_profiles_concurrently, PROFILE_BATCH_SIZE)
    result["version_id"] = version_id
    try:
        if not retry_failed:
            institution_id = _persist_discovered_researchers(ror_id, researchers, commit=False)
            for model, enabled in ((WorkCache, include_works), (FundingCache, include_fundings), (ResearcherStatus, True)):
                if enabled:
                    cleanup = model.query.filter_by(ror_id=ror_id)
                    if all_orcids:
                        cleanup = cleanup.filter(model.orcid.notin_(all_orcids))
                    cleanup.delete(synchronize_session=False)
        associations = {row.orcid: row for row in InstitutionResearcher.query.filter_by(
            institution_id=institution_id, is_active=True,
        ).all()}
        trusted_ids = _trusted_client_ids(ror_id)
        cached_profiles = {
            value for (value,) in db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id).union(
                db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id),
                db.session.query(ResearcherStatus.orcid).filter_by(ror_id=ror_id),
            ).all()
        }
        for batch in _chunks(orcid_ids, PROFILE_BATCH_SIZE):
            profiles = {row.orcid: row.payload_json for row in InstitutionSyncProfile.query.filter(
                InstitutionSyncProfile.version_id == version_id, InstitutionSyncProfile.orcid.in_(batch),
            ).all()}
            successful = [orcid for orcid in batch if profiles.get(orcid)]
            for model, enabled in ((WorkCache, include_works), (FundingCache, include_fundings), (ResearcherStatus, True)):
                if enabled and successful:
                    model.query.filter(model.ror_id == ror_id, model.orcid.in_(successful)).delete(synchronize_session=False)
            researcher_cache = _load_researcher_cache(batch)
            works, fundings, statuses = [], [], []
            now = _utc_now()
            for orcid in batch:
                association = associations.get(orcid)
                profile = profiles.get(orcid)
                if not profile:
                    result["failed_profiles"].append(orcid)
                    if association:
                        if association.profile_updated_at or orcid in cached_profiles:
                            result["retained_profiles"] += 1
                        association.profile_status = "failed"
                        association.profile_error = "No public ORCID profile data was returned."
                    continue
                result["profiles"] += 1
                if association:
                    association.profile_status = "success"
                    association.profile_error = None
                    association.profile_updated_at = now
                _update_researcher_from_profile(orcid, profile, researcher_cache)
                refresh_affiliation_evidence(institution_id, ror_id, orcid, profile)
                statuses.append(_extract_status_from_profile(profile, ror_id, orcid, trusted_ids))
                if include_works:
                    works.extend(_work_rows_from_profile(ror_id, orcid, profile))
                if include_fundings:
                    fundings.extend(_funding_rows_from_profile(ror_id, orcid, profile))
            _flush_bulk(works, "WorkCache")
            _flush_bulk(fundings, "FundingCache")
            _flush_bulk(statuses, "ResearcherStatus")
        if include_works:
            result["works"] = WorkCache.query.filter_by(ror_id=ror_id).count()
            result["unique_works"] = rebuild_canonical_works(ror_id, commit=False)["unique_outputs"]
            result["analytics_rows"] = refresh_openalex_facts(ror_id, commit=False)["rows"]
        else:
            # Funding-only imports can also update the names used by OAI.
            from .oai_publication_service import refresh_oai_publication
            refresh_oai_publication(ror_id)
        if include_fundings:
            result["fundings"] = FundingCache.query.filter_by(ror_id=ror_id).count()
        if result["failed_profiles"]:
            result["errors"].append("Some ORCID profiles could not be refreshed.")
        publish_sync_version(version_id, result)
    except Exception as exc:
        fail_sync_version(version_id, exc)
        raise
    logger.info("Published institution version %s for %s: %d refreshed, %d failed profiles.",
                version_id, ror_id, result["profiles"], len(result["failed_profiles"]))
    return result


def retry_failed_profiles_for_ror(ror_id: str, base_url: str, headers: dict, job_id=None) -> dict:
    """Refresh only failed active profiles, preserving the rest of the snapshot."""
    return _build_cache_for_ror(ror_id, base_url=base_url, headers=headers,
                              include_works=True, include_fundings=True, retry_failed=True)


@institutional_writer
def build_researcher_names_cache(ror_id: str, *, return_result: bool = False) -> int | dict:
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
        return {"profiles": 0, "failed_profiles": [], "errors": []} if return_result else 0

    updated_count = 0
    failed_profiles = []
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
                failed_profiles.append(orcid)
                if association:
                    association.profile_status = "failed"
                    association.profile_error = "No public ORCID profile data was returned."
                continue

            _update_researcher_from_profile(orcid, profile, researcher_cache)
            # A name-only refresh must not clear a failed full-profile import.
            updated_count += 1
        db.session.commit()

    from .oai_publication_service import refresh_oai_publication
    refresh_oai_publication(ror_id)
    db.session.commit()
    logger.info(
        "Synchronized %d researcher profile names for ROR %s",
        updated_count,
        ror_id,
    )
    result = {"profiles": updated_count, "failed_profiles": failed_profiles,
              "errors": ["Some ORCID profile names could not be refreshed."] if failed_profiles else []}
    return result if return_result else updated_count


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
