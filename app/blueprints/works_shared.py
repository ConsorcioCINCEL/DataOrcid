"""Work/funding cache management views and exports."""

import json
import logging
import math
from datetime import datetime as dt, timedelta, timezone
from io import BytesIO

import pandas as pd
from flask import current_app, request, send_file
from sqlalchemy import String, case, cast, func, literal
from sqlalchemy.orm import load_only

from .. import db, plain_text
from ..spreadsheet import excel_cell as _excel_cell, excel_safe_dataframe
logger = logging.getLogger(__name__)
def _format_datetime(value):
    """Return a stable string for spreadsheet exports."""
    return value.isoformat() if value else None


def _attach_download_token(response):
    """Let the browser-side export modal know the download response started."""
    token = request.args.get("download_token")
    if token:
        response.set_cookie(
            "orcid_download_token",
            token,
            max_age=120,
            samesite="Lax",
        )
    return response


def _send_dataframe_export(data_frame: pd.DataFrame, base_name: str, sheet_name: str):
    """Send a dataframe as CSV by default or Excel when requested."""
    export_format = (request.args.get('format') or '').lower()

    if export_format == 'excel':
        output = BytesIO()
        excel_frame = excel_safe_dataframe(data_frame)
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            excel_frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        output.seek(0)
        return _attach_download_token(send_file(
            output,
            as_attachment=True,
            download_name=f"{base_name}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        ))

    output = BytesIO(data_frame.to_csv(index=False).encode('utf-8-sig'))
    output.seek(0)
    return _attach_download_token(send_file(
        output,
        as_attachment=True,
        download_name=f"{base_name}.csv",
        mimetype='text/csv',
    ))


def _institution_lookup() -> dict:
    """Map ROR IDs to readable institution names."""
    from ..services.institution_registry_service import get_institution_options

    return {
        item["ror_id"]: item.get("name") or item["ror_id"]
        for item in get_institution_options()
        if item.get("ror_id")
    }


def _institution_metadata_lookup() -> dict:
    """Map ROR IDs to institutional names and external identifiers."""
    from ..services.institution_registry_service import get_institution_options

    return {
        item["ror_id"]: item
        for item in get_institution_options()
        if item.get("ror_id")
    }


def _institution_name(ror_id: str, institutions: dict) -> str:
    return institutions.get(ror_id) or ror_id or ""


def _chunks(items: list, size: int = 500):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _openalex_normalized_doi_expr(model):
    """Return the validated DOI key stored with the source record."""
    return model.doi_normalized


def _openalex_cache_key_expr(model):
    """Return the OpenAlex local cache key: DOI when present, otherwise work:<id>."""
    has_doi = model.doi_normalized.isnot(None)
    return case(
        (has_doi, model.doi_normalized),
        else_=literal("work:") + cast(model.id, String),
    )


def _page_params(default_per_page: int = 100) -> tuple[int, int]:
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except (TypeError, ValueError):
        page = 1

    try:
        per_page = int(request.args.get("per_page", default_per_page))
    except (TypeError, ValueError):
        per_page = default_per_page

    return page, min(max(per_page, 10), 250)


def _table_page_params(prefix: str, default_per_page: int = 25, max_per_page: int = 100) -> tuple[int, int]:
    from .works_openalex_data import _is_export_request

    if _is_export_request():
        return 1, 100000

    try:
        page = max(int(request.args.get(f"{prefix}_page", 1)), 1)
    except (TypeError, ValueError):
        page = 1

    try:
        per_page = int(request.args.get(f"{prefix}_per_page", default_per_page))
    except (TypeError, ValueError):
        per_page = default_per_page

    return page, min(max(per_page, 10), max_per_page)


def _table_sort_params(prefix: str, allowed: set[str], default_sort: str, default_dir: str = "desc") -> tuple[str, str]:
    sort = (request.args.get(f"{prefix}_sort") or default_sort).strip()
    if sort not in allowed:
        sort = default_sort

    direction = (request.args.get(f"{prefix}_dir") or default_dir).strip().lower()
    if direction not in {"asc", "desc"}:
        direction = default_dir

    return sort, direction


def _pagination_dict(page: int, per_page: int, total_rows: int) -> dict:
    pages = max(math.ceil(total_rows / per_page), 1) if total_rows else 1
    page = min(max(page, 1), pages)
    page_numbers = []
    for value in range(1, pages + 1):
        if value == 1 or value == pages or abs(value - page) <= 2:
            page_numbers.append(value)
        elif page_numbers and page_numbers[-1] is not None:
            page_numbers.append(None)

    return {
        "page": page,
        "per_page": per_page,
        "total_rows": total_rows,
        "pages": pages,
        "has_prev": page > 1,
        "has_next": page < pages,
        "prev_page": max(page - 1, 1),
        "next_page": min(page + 1, pages),
        "start": ((page - 1) * per_page + 1) if total_rows else 0,
        "end": min(page * per_page, total_rows),
        "page_numbers": page_numbers,
    }


def _json_list(value) -> list:
    if isinstance(value, list):
        return [item for item in value if item]
    if isinstance(value, tuple):
        return [item for item in value if item]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            return [item for item in parsed if item]
        return [value] if value else []
    return []


def _association_summary(counter: dict, limit: int = 3) -> dict:
    rows = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return {
        "labels": [label for label, _ in rows[:limit]],
        "total": len(rows),
        "extra": max(len(rows) - limit, 0),
    }


def _sorted_counter_rows(counter: dict, limit: int = 10) -> tuple[list[str], list[int]]:
    rows = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [row[0] for row in rows], [row[1] for row in rows]


def _summary_labels(summary: dict | None) -> str:
    if not summary:
        return ""
    return "; ".join(summary.get("labels") or [])


def _priority_source_export_rows(priority_open_access: dict) -> list[dict]:
    """Return export-ready Diamond and Green source metrics."""
    return [
        {
            "source": row["source_name"],
            "issn_l": row["source_issn_l"],
            "open_access_type": row["oa_status_label"],
            "articles": row["articles"],
            "citations": row["citations"],
            "average_citations": row["average_citations"],
        }
        for row in priority_open_access["source_rows"]
    ]


def _priority_article_export_rows(priority_open_access: dict) -> list[dict]:
    """Return export-ready top-cited Diamond and Green articles."""
    return [
        {
            "title": plain_text(row["title"]),
            "doi": row["doi"],
            "openalex_id": row["openalex_id"],
            "publication_year": row["publication_year"],
            "source": row["source_name"],
            "issn_l": row["source_issn_l"],
            "open_access_type": row["oa_status_label"],
            "citations": row["citations"],
            "fwci": row["fwci"],
        }
        for row in priority_open_access["top_cited_articles"]
    ]


def _priority_institution_export_rows(priority_open_access: dict) -> list[dict]:
    """Return export-ready institutional Diamond and Green metrics."""
    return [
        {
            "institution": row["institution"],
            "ror_id": row["ror_id"],
            "diamond_articles": row["diamond_articles"],
            "green_articles": row["green_articles"],
            "diamond_and_green_articles": row["articles"],
            "diamond_citations": row["diamond_citations"],
            "green_citations": row["green_citations"],
            "diamond_and_green_citations": row["citations"],
        }
        for row in priority_open_access["institution_rows"]
    ]


def _researcher_pairs(ror_id: str | None = None):
    """Return unique (ror_id, orcid) pairs known by any local cache."""
    from ..models import (
        FundingCache,
        InstitutionRegistry,
        InstitutionResearcher,
        ResearcherStatus,
        WorkCache,
    )

    pairs = set()
    association_query = db.session.query(
        InstitutionRegistry.ror_id,
        InstitutionResearcher.orcid,
    ).join(
        InstitutionResearcher,
        InstitutionResearcher.institution_id == InstitutionRegistry.id,
    ).filter(
        InstitutionRegistry.is_active.is_(True),
        InstitutionResearcher.is_active.is_(True),
    )
    if ror_id:
        association_query = association_query.filter(InstitutionRegistry.ror_id == ror_id)
    pairs.update(association_query.distinct().all())

    sources = (
        db.session.query(WorkCache.ror_id, WorkCache.orcid).filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.orcid.isnot(None),
            WorkCache.orcid != "",
        ),
        db.session.query(FundingCache.ror_id, FundingCache.orcid).filter(
            FundingCache.ror_id.isnot(None),
            FundingCache.ror_id != "",
            FundingCache.orcid.isnot(None),
            FundingCache.orcid != "",
        ),
        db.session.query(ResearcherStatus.ror_id, ResearcherStatus.orcid).filter(
            ResearcherStatus.ror_id.isnot(None),
            ResearcherStatus.ror_id != "",
            ResearcherStatus.orcid.isnot(None),
            ResearcherStatus.orcid != "",
        ),
    )
    for query in sources:
        if ror_id:
            query = query.filter_by(ror_id=ror_id)
        pairs.update((ror_id, orcid) for ror_id, orcid in query.distinct().all())
    return sorted(pairs)


def _researcher_pair_union(ror_id: str | None = None):
    """Build the union of institution/researcher pairs across local sources."""
    from ..models import (
        FundingCache,
        InstitutionRegistry,
        InstitutionResearcher,
        ResearcherStatus,
        WorkCache,
    )

    association_query = db.session.query(
        InstitutionRegistry.ror_id.label("ror_id"),
        InstitutionResearcher.orcid.label("orcid"),
    ).join(
        InstitutionResearcher,
        InstitutionResearcher.institution_id == InstitutionRegistry.id,
    ).filter(
        InstitutionRegistry.is_active.is_(True),
        InstitutionResearcher.is_active.is_(True),
    )

    source_queries = [
        db.session.query(
            WorkCache.ror_id.label("ror_id"),
            WorkCache.orcid.label("orcid"),
        ).filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.orcid.isnot(None),
            WorkCache.orcid != "",
        ),
        db.session.query(
            FundingCache.ror_id.label("ror_id"),
            FundingCache.orcid.label("orcid"),
        ).filter(
            FundingCache.ror_id.isnot(None),
            FundingCache.ror_id != "",
            FundingCache.orcid.isnot(None),
            FundingCache.orcid != "",
        ),
        db.session.query(
            ResearcherStatus.ror_id.label("ror_id"),
            ResearcherStatus.orcid.label("orcid"),
        ).filter(
            ResearcherStatus.ror_id.isnot(None),
            ResearcherStatus.ror_id != "",
            ResearcherStatus.orcid.isnot(None),
            ResearcherStatus.orcid != "",
        ),
    ]

    if ror_id:
        association_query = association_query.filter(InstitutionRegistry.ror_id == ror_id)
        source_queries = [query.filter_by(ror_id=ror_id) for query in source_queries]

    return association_query.union(*source_queries)


def _researcher_count(ror_id: str | None = None) -> int:
    """Count unique institution/researcher pairs without loading them in Python."""
    combined = _researcher_pair_union(ror_id).subquery()
    return int(db.session.query(func.count()).select_from(combined).scalar() or 0)


def _institution_cache_summaries(institutions: list[dict] | None = None) -> list[dict]:
    """Return cache counts and freshness for every known institution."""
    from ..models import (
        FundingCache,
        FundingCacheRun,
        OpenAlexSyncRun,
        OpenAlexWorkMetadata,
        WorkCache,
        WorkCacheRun,
    )
    from ..services.institution_registry_service import get_institution_options

    institutions = institutions if institutions is not None else get_institution_options()
    ror_ids = [item.get("ror_id") for item in institutions if item.get("ror_id")]
    if not ror_ids:
        return []

    def grouped_counts(model) -> dict[str, int]:
        return {
            ror_id: int(count or 0)
            for ror_id, count in (
                db.session.query(model.ror_id, func.count(model.id))
                .filter(model.ror_id.in_(ror_ids))
                .group_by(model.ror_id)
                .all()
            )
        }

    def latest_success(model) -> dict[str, dt]:
        return dict(
            db.session.query(model.ror_id, func.max(model.finished_at))
            .filter(
                model.ror_id.in_(ror_ids),
                model.status == "success",
                model.finished_at.isnot(None),
            )
            .group_by(model.ror_id)
            .all()
        )

    work_counts = grouped_counts(WorkCache)
    funding_counts = grouped_counts(FundingCache)

    researcher_pairs = _researcher_pair_union().subquery()
    researcher_counts = {
        ror_id: int(count or 0)
        for ror_id, count in (
            db.session.query(researcher_pairs.c.ror_id, func.count())
            .filter(researcher_pairs.c.ror_id.in_(ror_ids))
            .group_by(researcher_pairs.c.ror_id)
            .all()
        )
    }

    normalized_doi = _openalex_normalized_doi_expr(WorkCache).label("doi_normalized")
    institution_dois = (
        db.session.query(WorkCache.ror_id.label("ror_id"), normalized_doi)
        .filter(
            WorkCache.ror_id.in_(ror_ids),
            WorkCache.type == "journal-article",
            WorkCache.doi_normalized.isnot(None),
        )
        .distinct()
        .subquery()
    )
    openalex_coverage = {
        ror_id: {
            "candidates": int(candidates or 0),
            "matched": int(matched or 0),
        }
        for ror_id, candidates, matched in (
            db.session.query(
                institution_dois.c.ror_id,
                func.count(),
                func.count(OpenAlexWorkMetadata.id),
            )
            .outerjoin(
                OpenAlexWorkMetadata,
                OpenAlexWorkMetadata.doi_normalized == institution_dois.c.doi_normalized,
            )
            .group_by(institution_dois.c.ror_id)
            .all()
        )
    }

    work_updates = latest_success(WorkCacheRun)
    funding_updates = latest_success(FundingCacheRun)
    openalex_updates = latest_success(OpenAlexSyncRun)
    running_rors = {
        row[0]
        for model in (WorkCacheRun, FundingCacheRun)
        for row in (
            db.session.query(model.ror_id)
            .filter(
                model.ror_id.in_(ror_ids),
                model.status.in_(("pending", "running")),
            )
            .distinct()
            .all()
        )
    }
    stale_days = max(int(current_app.config.get("CACHE_STALE_DAYS", 30)), 1)
    stale_before = dt.now(timezone.utc).replace(tzinfo=None) - timedelta(days=stale_days)

    summaries = []
    for institution in institutions:
        ror_id = institution.get("ror_id")
        if not ror_id:
            continue
        coverage = openalex_coverage.get(ror_id, {"candidates": 0, "matched": 0})
        candidates = coverage["candidates"]
        matched = coverage["matched"]
        updates = [
            value
            for value in (
                work_updates.get(ror_id),
                funding_updates.get(ror_id),
                openalex_updates.get(ror_id),
            )
            if value
        ]
        required_updates = [work_updates.get(ror_id), funding_updates.get(ror_id)]
        last_update = max(updates) if updates else None
        works = work_counts.get(ror_id, 0)
        fundings = funding_counts.get(ror_id, 0)
        researchers = researcher_counts.get(ror_id, 0)
        has_data = any((works, fundings, researchers, candidates))

        if ror_id in running_rors:
            health = "running"
        elif not has_data:
            health = "empty"
        elif any(value is None for value in required_updates):
            health = "attention"
        elif min(required_updates) < stale_before:
            health = "stale"
        else:
            health = "ready"

        summaries.append({
            "ror_id": ror_id,
            "name": institution.get("name") or ror_id,
            "researchers": researchers,
            "works": works,
            "fundings": fundings,
            "openalex_candidates": candidates,
            "openalex_matched": matched,
            "openalex_percent": round((matched / candidates * 100), 1) if candidates else 0,
            "last_update": last_update,
            "health": health,
            "has_data": has_data,
        })

    return sorted(summaries, key=lambda item: item["name"].casefold())


def _build_researchers_dataframe(ror_id: str | None = None) -> pd.DataFrame:
    from ..models import (
        FundingCache,
        InstitutionRegistry,
        InstitutionResearcher,
        ResearcherCache,
        ResearcherStatus,
        WorkCache,
    )

    institutions = _institution_metadata_lookup()
    pairs = _researcher_pairs(ror_id)
    orcids = sorted({orcid for _, orcid in pairs})

    metadata = {}
    if orcids:
        for chunk in _chunks(orcids):
            rows = ResearcherCache.query.filter(ResearcherCache.orcid.in_(chunk)).all()
            metadata.update({row.orcid: row for row in rows})

    status_rows = []
    for chunk in _chunks(orcids):
        status_rows.extend(ResearcherStatus.query.filter(
            ResearcherStatus.orcid.in_(chunk)
        ).all())
    status_map = {
        (row.ror_id, row.orcid): bool(row.is_managed_by_am)
        for row in status_rows
    }

    association_query = db.session.query(
        InstitutionRegistry.ror_id,
        InstitutionResearcher,
    ).join(
        InstitutionResearcher,
        InstitutionResearcher.institution_id == InstitutionRegistry.id,
    ).filter(InstitutionResearcher.is_active.is_(True))
    if ror_id:
        association_query = association_query.filter(InstitutionRegistry.ror_id == ror_id)
    association_map = {
        (institution_ror, association.orcid): association
        for institution_ror, association in association_query.all()
    }

    works_counts = {
        (ror_id, orcid): count
        for ror_id, orcid, count in db.session.query(
            WorkCache.ror_id, WorkCache.orcid, func.count(WorkCache.id)
        ).filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.orcid.isnot(None),
            WorkCache.orcid != "",
        ).group_by(WorkCache.ror_id, WorkCache.orcid).all()
    }
    funding_counts = {
        (ror_id, orcid): count
        for ror_id, orcid, count in db.session.query(
            FundingCache.ror_id, FundingCache.orcid, func.count(FundingCache.id)
        ).filter(
            FundingCache.ror_id.isnot(None),
            FundingCache.ror_id != "",
            FundingCache.orcid.isnot(None),
            FundingCache.orcid != "",
        ).group_by(FundingCache.ror_id, FundingCache.orcid).all()
    }

    rows = []
    for institution_ror, orcid in pairs:
        institution = institutions.get(institution_ror, {})
        association = association_map.get((institution_ror, orcid))
        matched_by_ror = bool(getattr(association, 'matched_by_ror', False))
        matched_by_grid = bool(getattr(association, 'matched_by_grid', False))
        matched_by_ringgold = bool(getattr(association, 'matched_by_ringgold', False))
        match_sources = [
            scheme
            for scheme, matched in (
                ('ROR', matched_by_ror),
                ('GRID', matched_by_grid),
                ('Ringgold', matched_by_ringgold),
            )
            if matched
        ]
        rows.append({
            'institution': institution.get('name') or institution_ror,
            'ror_id': institution_ror,
            'grid_ids': '; '.join(institution.get('grid_ids') or []),
            'ringgold_ids': '; '.join(institution.get('ringgold_ids') or []),
            'orcid': orcid,
            'orcid_url': f'https://orcid.org/{orcid}',
            'given_names': getattr(metadata.get(orcid), 'given_names', None),
            'family_name': getattr(metadata.get(orcid), 'family_name', None),
            'credit_name': getattr(metadata.get(orcid), 'credit_name', None),
            'email': getattr(metadata.get(orcid), 'email', None),
            'matched_by_ror': matched_by_ror,
            'matched_by_grid': matched_by_grid,
            'matched_by_ringgold': matched_by_ringgold,
            'match_sources': '; '.join(match_sources),
            'is_managed_by_am': status_map.get((institution_ror, orcid), False),
            'works_count': works_counts.get((institution_ror, orcid), 0),
            'fundings_count': funding_counts.get((institution_ror, orcid), 0),
            'profile_status': getattr(association, 'profile_status', None),
            'profile_error': getattr(association, 'profile_error', None),
            'association_first_seen_at': _format_datetime(getattr(association, 'first_seen_at', None)),
            'association_last_seen_at': _format_datetime(getattr(association, 'last_seen_at', None)),
            'profile_updated_at': _format_datetime(getattr(metadata.get(orcid), 'updated_at', None)),
        })
    return pd.DataFrame(rows)


def _build_works_dataframe(records, institutions: dict | None = None) -> pd.DataFrame:
    institutions = institutions or _institution_lookup()
    return pd.DataFrame([{
        'institution': _institution_name(r.ror_id, institutions),
        'ror_id': r.ror_id,
        'orcid': r.orcid,
        'title': r.title,
        'type': r.type,
        'put_code': r.put_code,
        'journal_title': r.journal_title,
        'pub_year': r.pub_year,
        'pub_month': r.pub_month,
        'pub_day': r.pub_day,
        'doi': r.doi,
        'issn': r.issn,
        'other_external_ids': r.other_external_ids,
        'source': r.source,
        'url': r.url,
        'visibility': r.visibility,
        'created_at': _format_datetime(r.created_at),
    } for r in records])


def _build_fundings_dataframe(records, institutions: dict | None = None) -> pd.DataFrame:
    institutions = institutions or _institution_lookup()
    return pd.DataFrame([{
        'institution': _institution_name(r.ror_id, institutions),
        'ror_id': r.ror_id,
        'orcid': r.orcid,
        'title': r.title,
        'type': r.type,
        'org_name': r.org_name,
        'city': r.city,
        'country': r.country,
        'start_y': r.start_y,
        'start_m': r.start_m,
        'start_d': r.start_d,
        'end_y': r.end_y,
        'end_m': r.end_m,
        'end_d': r.end_d,
        'grant_number': r.grant_number,
        'currency': r.currency,
        'amount': r.amount,
        'source': r.source,
        'url': r.url,
        'visibility': r.visibility,
        'created_at': _format_datetime(r.created_at),
    } for r in records])


OPENALEX_EXPORT_BASE_COLUMNS = [
    'doi_normalized',
    'source_doi',
    'openalex_id',
    'openalex_url',
    'raw_status',
    'http_status',
    'raw_error',
    'raw_fetched_at',
    'raw_created_at',
    'raw_oa_updated_date',
    'title',
    'publication_year',
    'publication_date',
    'type',
    'language',
    'cited_by_count',
    'fwci',
    'is_retracted',
    'is_oa',
    'oa_status',
    'oa_url',
    'best_pdf_url',
    'source_name',
    'source_issn_l',
    'source_type',
    'source_is_in_doaj',
    'primary_topic_name',
    'primary_topic_field',
    'primary_topic_domain',
    'pmid',
    'pmcid',
    'volume',
    'issue',
    'first_page',
    'last_page',
    'source_id',
    'source_issns',
    'source_host_organization_name',
    'primary_landing_page_url',
    'primary_pdf_url',
    'primary_license',
    'primary_version',
    'referenced_works_count',
    'citation_normalized_percentile',
    'is_in_top_1_percent',
    'is_in_top_10_percent',
    'cited_by_percentile_min',
    'cited_by_percentile_max',
    'author_count',
    'institution_count',
    'country_count',
    'location_count',
    'has_abstract',
    'has_fulltext',
    'indexed_in',
    'topics',
    'keywords',
    'sustainable_development_goals',
    'funders',
    'awards',
    'apc_list_value',
    'apc_list_currency',
    'apc_list_value_usd',
    'apc_paid_value',
    'apc_paid_currency',
    'apc_paid_value_usd',
    'author_ids',
    'author_names',
    'author_orcids',
    'corresponding_author_names',
    'institution_names',
    'institution_rors',
    'countries',
    'raw_affiliation_strings',
    'raw_created_date',
    'metadata_fetched_at',
    'metadata_updated_at',
    'has_raw_json',
]

OPENALEX_EXPORT_CSV_COLUMNS = OPENALEX_EXPORT_BASE_COLUMNS + ['raw_json_length', 'raw_json']
OPENALEX_EXPORT_XLSX_COLUMNS = OPENALEX_EXPORT_BASE_COLUMNS


def _openalex_export_load_options(raw_model, metadata_model):
    """Load only columns required by the fast spreadsheet export."""
    return (
        load_only(
            raw_model.doi_normalized,
            raw_model.source_doi,
            raw_model.openalex_id,
            raw_model.status,
            raw_model.http_status,
            raw_model.error,
            raw_model.fetched_at,
            raw_model.created_at,
            raw_model.oa_updated_date,
        ),
        load_only(*(
            [
                getattr(metadata_model, column)
                for column in OPENALEX_EXPORT_XLSX_COLUMNS
                if hasattr(metadata_model, column)
            ]
            + [metadata_model.fetched_at, metadata_model.updated_at]
        )),
    )


def _openalex_export_row(
    raw,
    metadata,
    include_raw_json: bool = True,
    cache_key: str | None = None,
) -> dict:
    """Return one exportable OpenAlex raw-cache row with derived metadata."""
    openalex_id = (metadata.openalex_id if metadata else None) or (raw.openalex_id if raw else None)
    raw_payload = raw.raw_json if include_raw_json and raw else None
    raw_json = json.dumps(raw_payload, ensure_ascii=False) if raw_payload else None
    row = {
        'doi_normalized': raw.doi_normalized if raw else cache_key,
        'source_doi': raw.source_doi if raw else None,
        'openalex_id': openalex_id,
        'openalex_url': f"https://openalex.org/{openalex_id}" if openalex_id else None,
        'raw_status': raw.status if raw else 'pending',
        'http_status': raw.http_status if raw else None,
        'raw_error': raw.error if raw else None,
        'raw_fetched_at': _format_datetime(raw.fetched_at) if raw else None,
        'raw_created_at': _format_datetime(raw.created_at) if raw else None,
        'raw_oa_updated_date': _format_datetime(raw.oa_updated_date) if raw else None,
        'title': plain_text(metadata.title) if metadata else None,
        'publication_year': metadata.publication_year if metadata else None,
        'publication_date': metadata.publication_date if metadata else None,
        'type': metadata.type if metadata else None,
        'language': metadata.language if metadata else None,
        'cited_by_count': metadata.cited_by_count if metadata else None,
        'fwci': metadata.fwci if metadata else None,
        'is_retracted': metadata.is_retracted if metadata else None,
        'is_oa': metadata.is_oa if metadata else None,
        'oa_status': metadata.oa_status if metadata else None,
        'oa_url': metadata.oa_url if metadata else None,
        'best_pdf_url': metadata.best_pdf_url if metadata else None,
        'source_name': metadata.source_name if metadata else None,
        'source_issn_l': metadata.source_issn_l if metadata else None,
        'source_type': metadata.source_type if metadata else None,
        'source_is_in_doaj': metadata.source_is_in_doaj if metadata else None,
        'primary_topic_name': metadata.primary_topic_name if metadata else None,
        'primary_topic_field': metadata.primary_topic_field if metadata else None,
        'primary_topic_domain': metadata.primary_topic_domain if metadata else None,
        'pmid': metadata.pmid if metadata else None,
        'pmcid': metadata.pmcid if metadata else None,
        'volume': metadata.volume if metadata else None,
        'issue': metadata.issue if metadata else None,
        'first_page': metadata.first_page if metadata else None,
        'last_page': metadata.last_page if metadata else None,
        'source_id': metadata.source_id if metadata else None,
        'source_issns': metadata.source_issns if metadata else None,
        'source_host_organization_name': metadata.source_host_organization_name if metadata else None,
        'primary_landing_page_url': metadata.primary_landing_page_url if metadata else None,
        'primary_pdf_url': metadata.primary_pdf_url if metadata else None,
        'primary_license': metadata.primary_license if metadata else None,
        'primary_version': metadata.primary_version if metadata else None,
        'referenced_works_count': metadata.referenced_works_count if metadata else None,
        'citation_normalized_percentile': metadata.citation_normalized_percentile if metadata else None,
        'is_in_top_1_percent': metadata.is_in_top_1_percent if metadata else None,
        'is_in_top_10_percent': metadata.is_in_top_10_percent if metadata else None,
        'cited_by_percentile_min': metadata.cited_by_percentile_min if metadata else None,
        'cited_by_percentile_max': metadata.cited_by_percentile_max if metadata else None,
        'author_count': metadata.author_count if metadata else None,
        'institution_count': metadata.institution_count if metadata else None,
        'country_count': metadata.country_count if metadata else None,
        'location_count': metadata.location_count if metadata else None,
        'has_abstract': metadata.has_abstract if metadata else None,
        'has_fulltext': metadata.has_fulltext if metadata else None,
        'indexed_in': metadata.indexed_in if metadata else None,
        'topics': metadata.topics if metadata else None,
        'keywords': metadata.keywords if metadata else None,
        'sustainable_development_goals': metadata.sustainable_development_goals if metadata else None,
        'funders': metadata.funders if metadata else None,
        'awards': metadata.awards if metadata else None,
        'apc_list_value': metadata.apc_list_value if metadata else None,
        'apc_list_currency': metadata.apc_list_currency if metadata else None,
        'apc_list_value_usd': metadata.apc_list_value_usd if metadata else None,
        'apc_paid_value': metadata.apc_paid_value if metadata else None,
        'apc_paid_currency': metadata.apc_paid_currency if metadata else None,
        'apc_paid_value_usd': metadata.apc_paid_value_usd if metadata else None,
        'author_ids': metadata.author_ids if metadata else None,
        'author_names': metadata.author_names if metadata else None,
        'author_orcids': metadata.author_orcids if metadata else None,
        'corresponding_author_names': metadata.corresponding_author_names if metadata else None,
        'institution_names': metadata.institution_names if metadata else None,
        'institution_rors': metadata.institution_rors if metadata else None,
        'countries': metadata.countries if metadata else None,
        'raw_affiliation_strings': metadata.raw_affiliation_strings if metadata else None,
        'raw_created_date': metadata.raw_created_date if metadata else None,
        'metadata_fetched_at': _format_datetime(metadata.fetched_at) if metadata else None,
        'metadata_updated_at': _format_datetime(metadata.updated_at) if metadata else None,
        'has_raw_json': bool(raw_json) if include_raw_json else bool(raw and raw.status == 'found'),
    }
    if include_raw_json:
        row['raw_json_length'] = len(raw_json) if raw_json else 0
        row['raw_json'] = raw_json
    return row


def _build_openalex_dataframe(records) -> pd.DataFrame:
    """Return exportable OpenAlex raw-cache rows with derived work metadata."""
    rows = [_openalex_export_row(raw, metadata) for raw, metadata in records]
    return pd.DataFrame(rows, columns=OPENALEX_EXPORT_CSV_COLUMNS)
