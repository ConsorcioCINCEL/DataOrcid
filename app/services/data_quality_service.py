"""Aggregate data quality and cross-module opportunity signals."""

from __future__ import annotations

from sqlalchemy import case, func

from .. import db
from ..models import (
    CanonicalWork,
    DuplicateProfileReview,
    FundingCache,
    InstitutionRegistry,
    InstitutionResearcher,
    OpenAlexWorkAuthor,
    OpenAlexWorkInstitution,
    OpenAlexWorkMetadata,
    ResearcherAffiliationEvidence,
    WorkCache,
    WorkRecordLink,
)
from .canonical_work_service import canonical_work_counts
from .data_health_service import institution_data_health


def institution_quality_report(ror_id: str) -> dict:
    """Return quality, provenance, and safe cross-module indicators."""
    institution = InstitutionRegistry.query.filter_by(ror_id=ror_id).first()
    institution_id = institution.id if institution else None
    health = institution_data_health(ror_id)
    canonical = canonical_work_counts(ror_id)

    work_total, work_with_doi, work_with_year = db.session.query(
        func.count(WorkCache.id),
        func.count(case((func.trim(func.coalesce(WorkCache.doi, "")) != "", 1))),
        func.count(case((func.trim(func.coalesce(WorkCache.pub_year, "")) != "", 1))),
    ).filter(WorkCache.ror_id == ror_id).one()

    funding_total, funding_with_grant, funding_with_amount = db.session.query(
        func.count(FundingCache.id),
        func.count(case((func.trim(func.coalesce(FundingCache.grant_number, "")) != "", 1))),
        func.count(case((
            (func.trim(func.coalesce(FundingCache.amount, "")) != "")
            & (func.trim(func.coalesce(FundingCache.currency, "")) != ""),
            1,
        ))),
    ).filter(FundingCache.ror_id == ror_id).one()

    local_doi_ids = (
        db.session.query(WorkRecordLink.canonical_work_id)
        .filter(WorkRecordLink.ror_id == ror_id)
        .distinct()
        .subquery()
    )
    doi_rows = (
        db.session.query(CanonicalWork.doi_normalized)
        .join(local_doi_ids, local_doi_ids.c.canonical_work_id == CanonicalWork.id)
        .filter(CanonicalWork.doi_normalized.isnot(None))
        .subquery()
    )
    eligible_dois = db.session.query(func.count()).select_from(doi_rows).scalar() or 0
    enriched_dois = (
        db.session.query(func.count(func.distinct(OpenAlexWorkMetadata.doi_normalized)))
        .join(doi_rows, doi_rows.c.doi_normalized == OpenAlexWorkMetadata.doi_normalized)
        .scalar()
        or 0
    )

    association_total = 0
    verified_associations = 0
    inferred_associations = 0
    current_affiliations = 0
    historical_affiliations = 0
    if institution_id:
        association_total, verified_associations, inferred_associations = db.session.query(
            func.count(InstitutionResearcher.id),
            func.count(case((InstitutionResearcher.is_verified.is_(True), 1))),
            func.count(case((InstitutionResearcher.is_verified.is_(False), 1))),
        ).filter(
            InstitutionResearcher.institution_id == institution_id,
            InstitutionResearcher.is_active.is_(True),
        ).one()
        current_affiliations, historical_affiliations = db.session.query(
            func.count(case((ResearcherAffiliationEvidence.is_current.is_(True), 1))),
            func.count(case((ResearcherAffiliationEvidence.is_current.is_(False), 1))),
        ).filter(
            ResearcherAffiliationEvidence.institution_id == institution_id
        ).one()

    funding_context = _funding_output_context(ror_id)
    review_counts = dict(
        db.session.query(DuplicateProfileReview.status, func.count(DuplicateProfileReview.id))
        .filter(DuplicateProfileReview.ror_id == ror_id)
        .group_by(DuplicateProfileReview.status)
        .all()
    )

    orphan_authors = (
        db.session.query(func.count(OpenAlexWorkAuthor.id))
        .outerjoin(
            OpenAlexWorkMetadata,
            OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkAuthor.doi_normalized,
        )
        .filter(OpenAlexWorkMetadata.id.is_(None))
        .scalar()
        or 0
    )
    orphan_institutions = (
        db.session.query(func.count(OpenAlexWorkInstitution.id))
        .outerjoin(
            OpenAlexWorkMetadata,
            OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkInstitution.doi_normalized,
        )
        .filter(OpenAlexWorkMetadata.id.is_(None))
        .scalar()
        or 0
    )

    return {
        "ror_id": ror_id,
        "institution_name": institution.name if institution else ror_id,
        "health": health,
        "canonical": canonical,
        "works": {
            "records": int(work_total or 0),
            "with_doi": int(work_with_doi or 0),
            "with_year": int(work_with_year or 0),
            "doi_percent": _percent(work_with_doi, work_total),
            "year_percent": _percent(work_with_year, work_total),
            "excess_percent": _percent(canonical["excess_records"], work_total),
        },
        "openalex": {
            "eligible_dois": int(eligible_dois),
            "enriched_dois": int(enriched_dois),
            "coverage_percent": _percent(enriched_dois, eligible_dois),
            "orphan_authors": int(orphan_authors),
            "orphan_institutions": int(orphan_institutions),
        },
        "fundings": {
            "records": int(funding_total or 0),
            "with_grant": int(funding_with_grant or 0),
            "with_amount": int(funding_with_amount or 0),
            "grant_percent": _percent(funding_with_grant, funding_total),
            "amount_percent": _percent(funding_with_amount, funding_total),
        },
        "researchers": {
            "relationships": int(association_total or 0),
            "verified": int(verified_associations or 0),
            "inferred": int(inferred_associations or 0),
            "verified_percent": _percent(verified_associations, association_total),
            "current_affiliations": int(current_affiliations or 0),
            "historical_affiliations": int(historical_affiliations or 0),
        },
        "funding_context": funding_context,
        "reviews": {
            "pending": int(review_counts.get("pending", 0)),
            "confirmed": int(review_counts.get("confirmed", 0)),
            "false_positive": int(review_counts.get("false_positive", 0)),
            "resolved": int(review_counts.get("resolved", 0)),
        },
    }


def _funding_output_context(ror_id: str) -> dict:
    """Compare activity at researcher grain without implying grant causality."""
    work_rows = (
        db.session.query(
            WorkRecordLink.orcid.label("orcid"),
            func.count(func.distinct(WorkRecordLink.canonical_work_id)).label("works"),
        )
        .filter(WorkRecordLink.ror_id == ror_id)
        .group_by(WorkRecordLink.orcid)
        .subquery()
    )
    funding_rows = (
        db.session.query(
            FundingCache.orcid.label("orcid"),
            func.count(FundingCache.id).label("fundings"),
        )
        .filter(FundingCache.ror_id == ror_id)
        .group_by(FundingCache.orcid)
        .subquery()
    )
    rows = (
        db.session.query(
            work_rows.c.orcid,
            work_rows.c.works,
            funding_rows.c.fundings,
        )
        .join(funding_rows, funding_rows.c.orcid == work_rows.c.orcid)
        .order_by(funding_rows.c.fundings.desc(), work_rows.c.works.desc())
        .limit(10)
        .all()
    )
    total_with_both = (
        db.session.query(func.count())
        .select_from(work_rows.join(funding_rows, funding_rows.c.orcid == work_rows.c.orcid))
        .scalar()
        or 0
    )
    return {
        "researchers_with_both": int(total_with_both),
        "top_rows": [
            {
                "orcid": row.orcid,
                "unique_outputs": int(row.works or 0),
                "funding_records": int(row.fundings or 0),
            }
            for row in rows
        ],
    }


def _percent(numerator, denominator) -> float:
    return round((float(numerator or 0) / float(denominator or 0)) * 100, 1) if denominator else 0.0
