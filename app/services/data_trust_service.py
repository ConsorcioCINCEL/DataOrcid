"""Backfill explicit evidence and normalize public ORCID affiliation records."""

from __future__ import annotations

from collections import defaultdict
import re

from .. import db
from ..models import (
    FundingCache,
    InstitutionRegistry,
    InstitutionResearcher,
    ResearcherAffiliationEvidence,
    ResearcherCache,
    ResearcherStatus,
    WorkCache,
    utc_now,
)
from .institution_registry_service import get_institution_identifiers

AFFILIATION_SECTIONS = (
    "employments",
    "educations",
    "qualifications",
    "invited-positions",
    "distinctions",
    "memberships",
    "services",
)


def backfill_inferred_associations(ror_id: str | None = None) -> dict:
    """Persist cache-derived researcher relationships without calling them verified."""
    institution_query = InstitutionRegistry.query.filter_by(is_active=True)
    if ror_id:
        institution_query = institution_query.filter_by(ror_id=ror_id)
    institutions = institution_query.all()
    registry_by_ror = {row.ror_id: row for row in institutions}
    allowed_rors = set(registry_by_ror)
    if not allowed_rors:
        return {"associations": 0, "created": 0, "updated": 0, "institutions": 0}

    sources_by_pair: dict[tuple[str, str], set[str]] = defaultdict(set)
    for source_name, model in (
        ("works_cache", WorkCache),
        ("funding_cache", FundingCache),
        ("researcher_status", ResearcherStatus),
    ):
        rows = db.session.query(model.ror_id, model.orcid).filter(
            model.ror_id.in_(allowed_rors),
            model.orcid.isnot(None),
            model.orcid != "",
        ).distinct().all()
        for source_ror, orcid in rows:
            sources_by_pair[(source_ror, orcid)].add(source_name)

    institution_ids = [row.id for row in institutions]
    existing = {
        (row.institution_id, row.orcid): row
        for row in InstitutionResearcher.query.filter(
            InstitutionResearcher.institution_id.in_(institution_ids)
        ).all()
    }
    profile_orcids = set()
    source_orcids = sorted({pair[1] for pair in sources_by_pair})
    for start in range(0, len(source_orcids), 500):
        chunk = source_orcids[start:start + 500]
        profile_orcids.update(
            orcid
            for (orcid,) in db.session.query(ResearcherCache.orcid)
            .filter(ResearcherCache.orcid.in_(chunk))
            .all()
        )

    created = 0
    updated = 0
    now = utc_now()
    for (source_ror, orcid), evidence_sources in sorted(sources_by_pair.items()):
        institution = registry_by_ror[source_ror]
        association = existing.get((institution.id, orcid))
        if association:
            if not association.is_verified:
                association.evidence_type = "cache_inference"
                association.evidence_sources = sorted(evidence_sources)
                association.is_active = True
                association.last_seen_at = now
                updated += 1
            continue

        association = InstitutionResearcher(
            institution_id=institution.id,
            orcid=orcid,
            matched_by_ror=False,
            matched_by_grid=False,
            matched_by_ringgold=False,
            evidence_type="cache_inference",
            evidence_sources=sorted(evidence_sources),
            is_verified=False,
            is_active=True,
            profile_status="success" if orcid in profile_orcids else "pending",
            first_seen_at=now,
            last_seen_at=now,
        )
        db.session.add(association)
        existing[(institution.id, orcid)] = association
        created += 1
        if created % 2000 == 0:
            db.session.flush()

    db.session.commit()
    return {
        "associations": len(sources_by_pair),
        "created": created,
        "updated": updated,
        "institutions": len(institutions),
    }


def refresh_affiliation_evidence(
    institution_id: int,
    ror_id: str,
    orcid: str,
    profile_data: dict,
) -> int:
    """Replace normalized affiliation evidence matching the active institution."""
    identifiers = get_institution_identifiers(ror_id)
    trusted = {
        scheme: {_normalized_identifier(value) for value in values if value}
        for scheme, values in identifiers.items()
    }
    institution = db.session.get(InstitutionRegistry, institution_id)
    institution_name_key = _normalized_name(institution.name if institution else "")

    ResearcherAffiliationEvidence.query.filter_by(
        institution_id=institution_id,
        orcid=orcid,
    ).delete(synchronize_session=False)

    activities = profile_data.get("activities-summary") or {}
    rows = []
    now = utc_now()
    for section in AFFILIATION_SECTIONS:
        section_data = activities.get(section) or {}
        for group in section_data.get("affiliation-group", []):
            for summary in group.get("summaries", []):
                item = next(
                    (
                        value
                        for value in summary.values()
                        if isinstance(value, dict) and value.get("organization")
                    ),
                    None,
                )
                if not item:
                    continue

                organization = item.get("organization") or {}
                organization_name = organization.get("name") or ""
                disambiguated = organization.get("disambiguated-organization") or {}
                source_type = (disambiguated.get("disambiguation-source") or "").strip().lower()
                identifier_value = _normalized_identifier(
                    disambiguated.get("disambiguated-organization-identifier")
                )
                scheme = {
                    "ror": "ror",
                    "grid": "grid",
                    "ringgold": "ringgold",
                }.get(source_type)
                identifier_match = bool(
                    scheme and identifier_value and identifier_value in trusted.get(scheme, set())
                )
                name_match = bool(
                    institution_name_key
                    and _normalized_name(organization_name) == institution_name_key
                )
                if not identifier_match and not name_match:
                    continue

                end_year = _date_year(item.get("end-date"))
                source = item.get("source") or {}
                source_client_id = (source.get("source-client-id") or {}).get("path")
                rows.append(
                    ResearcherAffiliationEvidence(
                        institution_id=institution_id,
                        orcid=orcid,
                        source_section=section,
                        organization_name=organization_name or None,
                        role_title=(item.get("role-title") or "").strip() or None,
                        department_name=(item.get("department-name") or "").strip() or None,
                        start_year=_date_year(item.get("start-date")),
                        end_year=end_year,
                        source_client_id=source_client_id,
                        organization_identifiers={scheme: identifier_value} if scheme and identifier_value else None,
                        evidence_type="verified_identifier" if identifier_match else "organization_name",
                        is_current=end_year is None,
                        observed_at=now,
                    )
                )

    if rows:
        db.session.bulk_save_objects(rows)
    return len(rows)


def _date_year(value: dict | None) -> int | None:
    raw = ((value or {}).get("year") or {}).get("value")
    try:
        year = int(raw)
    except (TypeError, ValueError):
        return None
    return year if 1000 <= year <= 9999 else None


def _normalized_identifier(value: str | None) -> str:
    text = (value or "").strip().lower().rstrip("/")
    return re.sub(r"^https?://(?:www\.)?ror\.org/", "", text)


def _normalized_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").strip().lower()).strip()
