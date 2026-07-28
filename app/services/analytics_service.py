"""Derived analytics data used by filterable OpenAlex views."""

from __future__ import annotations

from sqlalchemy import case, func

from .. import db
from ..models import (
    AnalyticsDataVersion,
    OpenAlexInstitutionWorkFact,
    OpenAlexWorkAuthor,
    OpenAlexWorkInstitution,
    OpenAlexWorkMetadata,
    OpenAlexWorkRawCache,
    WorkCache,
    utc_now,
)
from .doi_service import normalize_doi


FACT_BATCH_SIZE = 4000


def analytics_scope_key(ror_id: str | None = None) -> str:
    return f"openalex:ror:{ror_id}" if ror_id else "openalex:global"


def get_analytics_data_version(ror_id: str | None = None) -> dict | None:
    row = db.session.get(AnalyticsDataVersion, analytics_scope_key(ror_id))
    if not row:
        return None
    return {
        "scope": row.scope_key,
        "version": int(row.version or 0),
        "updated_at": row.updated_at.isoformat() if row.updated_at else "",
    }


def bump_analytics_data_versions(
    ror_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    include_global: bool = True,
) -> None:
    scope_keys = {
        analytics_scope_key(ror_id)
        for ror_id in (ror_ids or [])
        if ror_id
    }
    if include_global:
        scope_keys.add(analytics_scope_key())

    now = utc_now()
    for scope_key in sorted(scope_keys):
        row = (
            AnalyticsDataVersion.query
            .filter_by(scope_key=scope_key)
            .with_for_update()
            .first()
        )
        if not row:
            row = AnalyticsDataVersion(scope_key=scope_key, version=1)
            db.session.add(row)
        else:
            row.version = int(row.version or 0) + 1
        row.updated_at = now
    db.session.flush()


def openalex_fact_available(ror_id: str | None = None) -> bool:
    if ror_id:
        return (
            db.session.query(OpenAlexInstitutionWorkFact.id)
            .filter(OpenAlexInstitutionWorkFact.ror_id == ror_id)
            .first()
            is not None
        )

    source_rors = (
        db.session.query(func.count(func.distinct(WorkCache.ror_id)))
        .filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.type == "journal-article",
        )
        .scalar()
        or 0
    )
    fact_rors = (
        db.session.query(func.count(func.distinct(OpenAlexInstitutionWorkFact.ror_id)))
        .scalar()
        or 0
    )
    return bool(source_rors and fact_rors >= source_rors)


def refresh_openalex_facts(ror_id: str | None = None) -> dict:
    if ror_id:
        return _refresh_institution_openalex_facts(ror_id, include_global=True)

    ror_ids = [
        value
        for (value,) in (
            db.session.query(WorkCache.ror_id)
            .filter(WorkCache.ror_id.isnot(None), WorkCache.ror_id != "")
            .distinct()
            .order_by(WorkCache.ror_id)
            .all()
        )
        if value
    ]
    total_rows = 0
    for institutional_ror in ror_ids:
        summary = _refresh_institution_openalex_facts(
            institutional_ror,
            include_global=False,
        )
        total_rows += summary["rows"]

    bump_analytics_data_versions(include_global=True)
    db.session.commit()
    return {
        "institutions": len(ror_ids),
        "rows": total_rows,
    }


def _refresh_institution_openalex_facts(
    ror_id: str,
    include_global: bool,
) -> dict:
    source_rows = (
        db.session.query(
            WorkCache.id,
            WorkCache.doi,
            WorkCache.doi_normalized,
            WorkCache.title,
        )
        .filter(
            WorkCache.ror_id == ror_id,
            WorkCache.type == "journal-article",
        )
        .order_by(WorkCache.id)
        .all()
    )
    source_by_key = {}
    for row in source_rows:
        normalized_doi = row.doi_normalized or normalize_doi(row.doi)
        cache_key = normalized_doi or f"work:{row.id}"
        source = source_by_key.setdefault(
            cache_key,
            {
                "representative_work_cache_id": row.id,
                "source_record_count": 0,
                "has_valid_doi": bool(normalized_doi),
                "has_local_title": False,
            },
        )
        source["source_record_count"] += 1
        source["has_local_title"] = source["has_local_title"] or bool(
            (row.title or "").strip()
        )

    OpenAlexInstitutionWorkFact.query.filter_by(ror_id=ror_id).delete(
        synchronize_session=False
    )
    refreshed_at = utc_now()
    cache_keys = sorted(source_by_key)
    batch_size = 500 if db.engine.dialect.name == "sqlite" else FACT_BATCH_SIZE
    inserted_rows = 0
    for start in range(0, len(cache_keys), batch_size):
        key_batch = cache_keys[start:start + batch_size]
        metadata_by_key = {
            row.doi_normalized: row
            for row in (
                OpenAlexWorkMetadata.query
                .filter(OpenAlexWorkMetadata.doi_normalized.in_(key_batch))
                .all()
            )
        }
        raw_by_key = {
            row.doi_normalized: row
            for row in (
                db.session.query(
                    OpenAlexWorkRawCache.doi_normalized,
                    OpenAlexWorkRawCache.status,
                    OpenAlexWorkRawCache.error,
                )
                .filter(OpenAlexWorkRawCache.doi_normalized.in_(key_batch))
                .all()
            )
        }
        institution_by_key = _institution_metrics(key_batch, ror_id)
        author_by_key = _author_metrics(key_batch)
        mappings = []
        for cache_key in key_batch:
            source = source_by_key[cache_key]
            metadata = metadata_by_key.get(cache_key)
            raw = raw_by_key.get(cache_key)
            institution = institution_by_key.get(cache_key, {})
            has_chile = bool(institution.get("has_chile_affiliation"))
            has_non_chile = bool(institution.get("has_non_chile_affiliation"))
            mappings.append({
                "ror_id": ror_id,
                "openalex_cache_key": cache_key,
                "representative_work_cache_id": source["representative_work_cache_id"],
                "source_record_count": source["source_record_count"],
                "has_valid_doi": source["has_valid_doi"],
                "has_local_title": source["has_local_title"],
                "raw_status": raw.status if raw else None,
                "raw_error": raw.error if raw else None,
                "openalex_id": metadata.openalex_id if metadata else None,
                "title": metadata.title if metadata else None,
                "publication_year": metadata.publication_year if metadata else None,
                "document_type": metadata.type if metadata else None,
                "language": metadata.language if metadata else None,
                "cited_by_count": int(metadata.cited_by_count or 0) if metadata else 0,
                "fwci": metadata.fwci if metadata else None,
                "is_oa": bool(metadata.is_oa) if metadata else False,
                "oa_status": metadata.oa_status if metadata else None,
                "source_name": metadata.source_name if metadata else None,
                "source_issn_l": metadata.source_issn_l if metadata else None,
                "primary_topic_field": metadata.primary_topic_field if metadata else None,
                "primary_topic_domain": metadata.primary_topic_domain if metadata else None,
                "has_selected_affiliation": bool(institution.get("has_selected_affiliation")),
                "has_chile_affiliation": has_chile,
                "has_non_chile_affiliation": has_non_chile,
                "has_international_collaboration": has_chile and has_non_chile,
                "author_count": int(author_by_key.get(cache_key, 0)),
                "institution_count": int(institution.get("institution_count", 0)),
                "refreshed_at": refreshed_at,
            })
        if mappings:
            db.session.bulk_insert_mappings(OpenAlexInstitutionWorkFact, mappings)
            inserted_rows += len(mappings)
            db.session.flush()

    bump_analytics_data_versions([ror_id], include_global=include_global)
    db.session.commit()
    return {
        "ror_id": ror_id,
        "source_records": len(source_rows),
        "rows": inserted_rows,
    }


def _institution_metrics(cache_keys: list[str], ror_id: str) -> dict:
    institution_key = func.coalesce(
        OpenAlexWorkInstitution.institution_id,
        OpenAlexWorkInstitution.ror_id,
        OpenAlexWorkInstitution.institution_name,
    )
    rows = (
        db.session.query(
            OpenAlexWorkInstitution.doi_normalized,
            func.max(case(
                (OpenAlexWorkInstitution.ror_id == ror_id, 1),
                else_=0,
            )).label("has_selected_affiliation"),
            func.max(case(
                (OpenAlexWorkInstitution.country_code == "CL", 1),
                else_=0,
            )).label("has_chile_affiliation"),
            func.max(case(
                (
                    OpenAlexWorkInstitution.country_code.isnot(None)
                    & (OpenAlexWorkInstitution.country_code != "CL"),
                    1,
                ),
                else_=0,
            )).label("has_non_chile_affiliation"),
            func.count(func.distinct(institution_key)).label("institution_count"),
        )
        .filter(OpenAlexWorkInstitution.doi_normalized.in_(cache_keys))
        .group_by(OpenAlexWorkInstitution.doi_normalized)
        .all()
    )
    return {
        row.doi_normalized: {
            "has_selected_affiliation": bool(row.has_selected_affiliation),
            "has_chile_affiliation": bool(row.has_chile_affiliation),
            "has_non_chile_affiliation": bool(row.has_non_chile_affiliation),
            "institution_count": int(row.institution_count or 0),
        }
        for row in rows
    }


def _author_metrics(cache_keys: list[str]) -> dict:
    author_key = func.coalesce(
        OpenAlexWorkAuthor.author_id,
        OpenAlexWorkAuthor.orcid,
        OpenAlexWorkAuthor.author_name,
    )
    return {
        doi_normalized: int(author_count or 0)
        for doi_normalized, author_count in (
            db.session.query(
                OpenAlexWorkAuthor.doi_normalized,
                func.count(func.distinct(author_key)),
            )
            .filter(OpenAlexWorkAuthor.doi_normalized.in_(cache_keys))
            .group_by(OpenAlexWorkAuthor.doi_normalized)
            .all()
        )
    }
