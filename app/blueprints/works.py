"""Work/funding cache management views and exports."""

import copy
import csv
import hashlib
import json
import logging
import math
import time
from collections import OrderedDict
from datetime import datetime as dt
from io import BytesIO, StringIO
from threading import RLock

import pandas as pd
from flask import (
    Blueprint, request, redirect, url_for,
    Response, send_file, current_app, render_template, session, stream_with_context
)
from flask_babel import _
from sqlalchemy import String, and_, case, cast, func, literal, or_, select

from .. import db
from ..decorators import admin_required, login_required, staff_required
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

bp_works = Blueprint("works", __name__)
logger = logging.getLogger(__name__)

_OPENALEX_GLOBAL_ANALYTICS_CACHE = OrderedDict()
_OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK = RLock()
_OPENALEX_GLOBAL_ANALYTICS_CACHE_MAX_SIZE = 40
_OPENALEX_GLOBAL_ANALYTICS_CACHE_TTL = 900


def _format_datetime(value):
    """Return a stable string for spreadsheet exports."""
    return value.isoformat() if value else None


def _excel_cell(value):
    """Keep Excel exports inside the worksheet cell length limit."""
    if isinstance(value, str) and len(value) > 32767:
        return f"{value[:32740]}... [truncated for Excel]"
    return value


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
        excel_frame = data_frame.copy()
        for column in excel_frame.select_dtypes(include=["object"]).columns:
            excel_frame[column] = excel_frame[column].map(_excel_cell)
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


def _institution_name(ror_id: str, institutions: dict) -> str:
    return institutions.get(ror_id) or ror_id or ""


def _chunks(items: list, size: int = 500):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _openalex_normalized_doi_expr(model):
    trimmed = func.lower(func.trim(model.doi))
    without_url = func.regexp_replace(trimmed, r"^https?://(dx\.)?doi\.org/", "")
    without_prefix = func.regexp_replace(without_url, r"^doi:\s*", "")
    return func.rtrim(without_prefix, ".")


def _openalex_cache_key_expr(model):
    """Return the OpenAlex local cache key: DOI when present, otherwise work:<id>."""
    has_doi = and_(model.doi.isnot(None), func.trim(model.doi) != "")
    return case(
        (has_doi, _openalex_normalized_doi_expr(model)),
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

    return page, min(max(per_page, 25), 250)


def _table_page_params(prefix: str, default_per_page: int = 25, max_per_page: int = 100) -> tuple[int, int]:
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


def _openalex_global_cache_ttl() -> int:
    try:
        return max(int(current_app.config.get("OPENALEX_ANALYTICS_CACHE_TTL", _OPENALEX_GLOBAL_ANALYTICS_CACHE_TTL)), 0)
    except (TypeError, ValueError):
        return _OPENALEX_GLOBAL_ANALYTICS_CACHE_TTL


def _signature_row(model, timestamp_column) -> dict:
    count, latest = db.session.query(func.count(model.id), func.max(timestamp_column)).one()
    return {
        "count": int(count or 0),
        "latest": latest.isoformat() if latest else "",
    }


def _openalex_global_data_signature() -> dict:
    from ..models import OpenAlexWorkAuthor, OpenAlexWorkInstitution, OpenAlexWorkMetadata, WorkCache

    return {
        "works": _signature_row(WorkCache, WorkCache.created_at),
        "metadata": _signature_row(OpenAlexWorkMetadata, OpenAlexWorkMetadata.updated_at),
        "authors": _signature_row(OpenAlexWorkAuthor, OpenAlexWorkAuthor.created_at),
        "institutions": _signature_row(OpenAlexWorkInstitution, OpenAlexWorkInstitution.created_at),
    }


def _openalex_global_request_cache_key(filters: dict) -> str:
    request_args = {
        key: request.args.getlist(key)
        for key in sorted(request.args.keys())
        if key not in {"lang", "refresh_cache"}
    }
    payload = {
        "filters": filters,
        "request_args": request_args,
        "locale": session.get("locale") or current_app.config.get("BABEL_DEFAULT_LOCALE", "es"),
        "data_signature": _openalex_global_data_signature(),
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _openalex_global_analytics_with_cache(filters: dict) -> dict:
    ttl = _openalex_global_cache_ttl()
    bypass_cache = request.args.get("refresh_cache") == "1" or ttl <= 0
    now = time.monotonic()
    cache_key = None if bypass_cache else _openalex_global_request_cache_key(filters)

    if cache_key:
        with _OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK:
            cached = _OPENALEX_GLOBAL_ANALYTICS_CACHE.get(cache_key)
            if cached and now - cached["stored_at"] <= ttl:
                _OPENALEX_GLOBAL_ANALYTICS_CACHE.move_to_end(cache_key)
                analytics = copy.deepcopy(cached["analytics"])
                analytics["cache"] = {
                    "hit": True,
                    "generated_at": cached["generated_at"],
                    "ttl_seconds": ttl,
                }
                return analytics
            if cached:
                _OPENALEX_GLOBAL_ANALYTICS_CACHE.pop(cache_key, None)

    analytics = _openalex_global_analytics(filters)
    generated_at = dt.utcnow().isoformat()

    if cache_key:
        with _OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK:
            _OPENALEX_GLOBAL_ANALYTICS_CACHE[cache_key] = {
                "analytics": copy.deepcopy(analytics),
                "stored_at": now,
                "generated_at": generated_at,
            }
            while len(_OPENALEX_GLOBAL_ANALYTICS_CACHE) > _OPENALEX_GLOBAL_ANALYTICS_CACHE_MAX_SIZE:
                _OPENALEX_GLOBAL_ANALYTICS_CACHE.popitem(last=False)

    analytics["cache"] = {
        "hit": False,
        "generated_at": generated_at,
        "ttl_seconds": ttl,
    }
    return analytics


def _sorted_counter_rows(counter: dict, limit: int = 10) -> tuple[list[str], list[int]]:
    rows = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [row[0] for row in rows], [row[1] for row in rows]


def _summary_labels(summary: dict | None) -> str:
    if not summary:
        return ""
    return "; ".join(summary.get("labels") or [])


def _researcher_pairs():
    """Return unique (ror_id, orcid) pairs known by any local cache."""
    from ..models import FundingCache, ResearcherStatus, WorkCache

    pairs = set()
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
        pairs.update((ror_id, orcid) for ror_id, orcid in query.distinct().all())
    return sorted(pairs)


def _researcher_count() -> int:
    return len(_researcher_pairs())


def _build_researchers_dataframe() -> pd.DataFrame:
    from ..models import FundingCache, ResearcherCache, ResearcherStatus, WorkCache

    institutions = _institution_lookup()
    pairs = _researcher_pairs()
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

    return pd.DataFrame([{
        'institution': _institution_name(ror_id, institutions),
        'ror_id': ror_id,
        'orcid': orcid,
        'given_names': getattr(metadata.get(orcid), 'given_names', None),
        'family_name': getattr(metadata.get(orcid), 'family_name', None),
        'credit_name': getattr(metadata.get(orcid), 'credit_name', None),
        'email': getattr(metadata.get(orcid), 'email', None),
        'is_managed_by_am': status_map.get((ror_id, orcid), False),
        'works_count': works_counts.get((ror_id, orcid), 0),
        'fundings_count': funding_counts.get((ror_id, orcid), 0),
        'profile_updated_at': _format_datetime(getattr(metadata.get(orcid), 'updated_at', None)),
    } for ror_id, orcid in pairs])


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
    'metadata_fetched_at',
    'metadata_updated_at',
    'has_raw_json',
]

OPENALEX_EXPORT_CSV_COLUMNS = OPENALEX_EXPORT_BASE_COLUMNS + ['raw_json_length', 'raw_json']
OPENALEX_EXPORT_XLSX_COLUMNS = OPENALEX_EXPORT_BASE_COLUMNS


def _openalex_export_row(raw, metadata, include_raw_json: bool = True) -> dict:
    """Return one exportable OpenAlex raw-cache row with derived metadata."""
    openalex_id = (metadata.openalex_id if metadata else None) or raw.openalex_id
    raw_json = json.dumps(raw.raw_json, ensure_ascii=False) if include_raw_json and raw.raw_json else None
    row = {
        'doi_normalized': raw.doi_normalized,
        'source_doi': raw.source_doi,
        'openalex_id': openalex_id,
        'openalex_url': f"https://openalex.org/{openalex_id}" if openalex_id else None,
        'raw_status': raw.status,
        'http_status': raw.http_status,
        'raw_error': raw.error,
        'raw_fetched_at': _format_datetime(raw.fetched_at),
        'raw_created_at': _format_datetime(raw.created_at),
        'raw_oa_updated_date': _format_datetime(raw.oa_updated_date),
        'title': metadata.title if metadata else None,
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
        'metadata_fetched_at': _format_datetime(metadata.fetched_at) if metadata else None,
        'metadata_updated_at': _format_datetime(metadata.updated_at) if metadata else None,
        'has_raw_json': bool(raw_json) if include_raw_json else None,
    }
    if include_raw_json:
        row['raw_json_length'] = len(raw_json) if raw_json else 0
        row['raw_json'] = raw_json
    return row


def _build_openalex_dataframe(records) -> pd.DataFrame:
    """Return exportable OpenAlex raw-cache rows with derived work metadata."""
    rows = [_openalex_export_row(raw, metadata) for raw, metadata in records]
    return pd.DataFrame(rows, columns=OPENALEX_EXPORT_CSV_COLUMNS)


def _send_openalex_export(records_query, base_name: str = 'openalex_articles_all_institutions'):
    """Send OpenAlex exports without materializing the full dataset as a DataFrame."""
    export_format = (request.args.get('format') or '').lower()

    if export_format == 'excel':
        from openpyxl import Workbook

        workbook = Workbook(write_only=True)
        worksheet = workbook.create_sheet('OpenAlex')
        worksheet.append(OPENALEX_EXPORT_XLSX_COLUMNS)
        for raw, metadata in records_query.yield_per(500):
            row = _openalex_export_row(raw, metadata, include_raw_json=False)
            worksheet.append([_excel_cell(row.get(column)) for column in OPENALEX_EXPORT_XLSX_COLUMNS])

        output = BytesIO()
        workbook.save(output)
        output.seek(0)
        return _attach_download_token(send_file(
            output,
            as_attachment=True,
            download_name=f"{base_name}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        ))

    def generate_csv():
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=OPENALEX_EXPORT_CSV_COLUMNS, extrasaction='ignore')
        buffer.write('\ufeff')
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for raw, metadata in records_query.yield_per(500):
            writer.writerow(_openalex_export_row(raw, metadata))
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    response = Response(
        stream_with_context(generate_csv()),
        mimetype='text/csv; charset=utf-8',
    )
    response.headers['Content-Disposition'] = f'attachment; filename="{base_name}.csv"'
    return _attach_download_token(response)


def _openalex_cache_summary(ror_id: str) -> dict:
    """Summarize OpenAlex sync status for DOI-backed journal articles."""
    from ..models import OpenAlexSyncRun, OpenAlexWorkMetadata, OpenAlexWorkRawCache, WorkCache
    from ..services.openalex_service import TITLE_MATCH_NOT_FOUND_ERROR

    normalized = _openalex_normalized_doi_expr(WorkCache).label("doi_normalized")
    cache_key = _openalex_cache_key_expr(WorkCache).label("openalex_cache_key")
    article_filters = (
        WorkCache.ror_id == ror_id,
        WorkCache.type == "journal-article",
    )
    doi_filters = (
        WorkCache.ror_id == ror_id,
        WorkCache.doi.isnot(None),
        WorkCache.doi != "",
        WorkCache.type == "journal-article",
    )
    doi_subquery = (
        db.session.query(normalized)
        .filter(*doi_filters)
        .distinct()
        .subquery()
    )
    cache_key_subquery = (
        db.session.query(cache_key)
        .filter(*article_filters)
        .distinct()
        .subquery()
    )

    article_works = db.session.query(func.count(WorkCache.id)).filter(*article_filters).scalar() or 0
    article_doi_works = db.session.query(func.count(WorkCache.id)).filter(*doi_filters).scalar() or 0
    candidate_dois = db.session.query(func.count()).select_from(doi_subquery).scalar() or 0
    processed = (
        db.session.query(func.count())
        .select_from(doi_subquery)
        .join(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    matched = (
        db.session.query(func.count())
        .select_from(doi_subquery)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    not_found = (
        db.session.query(func.count())
        .select_from(doi_subquery)
        .join(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkRawCache.status == "not_found")
        .scalar()
        or 0
    )
    errors = (
        db.session.query(func.count())
        .select_from(doi_subquery)
        .join(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkRawCache.status == "error")
        .scalar()
        or 0
    )
    matched_cache_keys = (
        db.session.query(func.count())
        .select_from(cache_key_subquery)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == cache_key_subquery.c.openalex_cache_key)
        .scalar()
        or 0
    )
    no_doi_title_candidates = (
        db.session.query(func.count())
        .select_from(WorkCache)
        .outerjoin(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == cache_key)
        .filter(
            *article_filters,
            WorkCache.title.isnot(None),
            WorkCache.title != "",
            (WorkCache.doi.is_(None)) | (WorkCache.doi == ""),
            (OpenAlexWorkRawCache.id.is_(None)) |
            (
                (OpenAlexWorkRawCache.status != "found") &
                (
                    (OpenAlexWorkRawCache.error.is_(None)) |
                    (OpenAlexWorkRawCache.error != TITLE_MATCH_NOT_FOUND_ERROR)
                )
            ),
        )
        .scalar()
        or 0
    )
    doi_not_found_title_subquery = (
        db.session.query(normalized)
        .select_from(WorkCache)
        .join(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == normalized)
        .outerjoin(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == normalized)
        .filter(
            *doi_filters,
            WorkCache.title.isnot(None),
            WorkCache.title != "",
            OpenAlexWorkRawCache.status == "not_found",
            OpenAlexWorkMetadata.id.is_(None),
            (OpenAlexWorkRawCache.error.is_(None)) |
            (OpenAlexWorkRawCache.error != TITLE_MATCH_NOT_FOUND_ERROR),
        )
        .distinct()
        .subquery()
    )
    doi_not_found_title_candidates = (
        db.session.query(func.count())
        .select_from(doi_not_found_title_subquery)
        .scalar()
        or 0
    )
    last_run = (
        OpenAlexSyncRun.query
        .filter_by(ror_id=ror_id)
        .order_by(OpenAlexSyncRun.finished_at.desc())
        .first()
    )

    return {
        "article_works": article_works,
        "article_doi_works": article_doi_works,
        "candidate_dois": candidate_dois,
        "processed_dois": processed,
        "matched_dois": matched,
        "matched_openalex_keys": matched_cache_keys,
        "not_found_dois": not_found,
        "error_dois": errors,
        "pending_dois": max(candidate_dois - processed, 0),
        "title_candidate_works": no_doi_title_candidates + doi_not_found_title_candidates,
        "no_doi_title_candidates": no_doi_title_candidates,
        "doi_not_found_title_candidates": doi_not_found_title_candidates,
        "processed_percent": round((processed / candidate_dois * 100), 1) if candidate_dois else 0,
        "matched_percent": round((matched / candidate_dois * 100), 1) if candidate_dois else 0,
        "last_run": last_run,
    }


def _openalex_work_rows(
    ror_id: str,
    coverage: str = "all",
    page: int = 1,
    per_page: int = 100,
) -> tuple[list[dict], dict, dict]:
    """Join local ORCID articles with OpenAlex metadata by DOI or local work key."""
    from ..models import OpenAlexWorkMetadata, OpenAlexWorkRawCache, WorkCache

    summary = _openalex_cache_summary(ror_id)
    normalized = _openalex_normalized_doi_expr(WorkCache).label("doi_normalized")
    cache_key = _openalex_cache_key_expr(WorkCache).label("openalex_cache_key")
    query = (
        db.session.query(
            WorkCache.id.label("work_cache_id"),
            WorkCache.title,
            WorkCache.orcid,
            WorkCache.type,
            WorkCache.pub_year,
            WorkCache.journal_title,
            WorkCache.doi,
            normalized,
            cache_key,
            OpenAlexWorkRawCache.status.label("raw_status"),
            OpenAlexWorkRawCache.error.label("raw_error"),
            OpenAlexWorkMetadata.openalex_id,
            OpenAlexWorkMetadata.cited_by_count,
            OpenAlexWorkMetadata.is_oa,
            OpenAlexWorkMetadata.oa_status,
            OpenAlexWorkMetadata.source_name,
            OpenAlexWorkMetadata.source_issn_l,
            OpenAlexWorkMetadata.primary_topic_field,
            OpenAlexWorkMetadata.primary_topic_domain,
        )
        .outerjoin(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == cache_key)
        .outerjoin(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == cache_key)
        .filter(
            WorkCache.ror_id == ror_id,
            WorkCache.type == "journal-article",
        )
    )

    if coverage == "enriched":
        query = query.filter(OpenAlexWorkMetadata.id.isnot(None))
    elif coverage == "missing":
        query = query.filter(OpenAlexWorkRawCache.id.is_(None))

    total_rows = query.count()
    query = query.order_by(WorkCache.pub_year.desc(), WorkCache.title.asc())
    result_rows = query.offset((page - 1) * per_page).limit(per_page).all()

    rows = []
    for row in result_rows:
        raw_status = row.raw_status or "pending"
        openalex_id = row.openalex_id
        has_doi = bool(row.doi)
        rows.append({
            "work_cache_id": row.work_cache_id,
            "title": row.title or "",
            "orcid": row.orcid,
            "type": row.type or "",
            "pub_year": row.pub_year or "",
            "journal_title": row.journal_title or "",
            "doi": row.doi or "",
            "doi_normalized": row.doi_normalized,
            "openalex_cache_key": row.openalex_cache_key,
            "has_doi": has_doi,
            "matched": bool(openalex_id),
            "raw_status": raw_status,
            "raw_error": row.raw_error or "",
            "openalex_id": openalex_id or "",
            "openalex_url": f"https://openalex.org/{openalex_id}" if openalex_id else "",
            "cited_by_count": row.cited_by_count,
            "is_oa": bool(row.is_oa) if openalex_id else None,
            "oa_status": row.oa_status or "",
            "source_name": row.source_name or "",
            "source_issn_l": row.source_issn_l or "",
            "primary_topic_field": row.primary_topic_field or "",
            "primary_topic_domain": row.primary_topic_domain or "",
        })

    summary.update({
        "total_article_works": summary["article_works"],
        "total_doi_works": summary["article_doi_works"],
        "unique_dois": summary["candidate_dois"],
        "enriched_unique_dois": summary["matched_openalex_keys"],
        "processed_unique_dois": summary["processed_dois"],
        "coverage_percent": round((summary["matched_openalex_keys"] / summary["article_works"] * 100), 1) if summary["article_works"] else 0,
        "total_rows": total_rows,
        "total_citations": (
            db.session.query(func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0))
            .filter(OpenAlexWorkMetadata.doi_normalized.in_(
                db.session.query(_openalex_cache_key_expr(WorkCache))
                .filter(
                    WorkCache.ror_id == ror_id,
                    WorkCache.type == "journal-article",
                )
                .distinct()
            ))
            .scalar()
            or 0
        ),
        "open_access_works": (
            db.session.query(func.count())
            .filter(OpenAlexWorkMetadata.doi_normalized.in_(
                db.session.query(_openalex_cache_key_expr(WorkCache))
                .filter(
                    WorkCache.ror_id == ror_id,
                    WorkCache.type == "journal-article",
                )
                .distinct()
            ))
            .filter(OpenAlexWorkMetadata.is_oa.is_(True))
            .scalar()
            or 0
        ),
    })

    pagination = {
        "page": page,
        "per_page": per_page,
        "total_rows": total_rows,
        "pages": max(math.ceil(total_rows / per_page), 1) if total_rows else 1,
        "has_prev": page > 1,
        "has_next": page * per_page < total_rows,
        "prev_page": max(page - 1, 1),
        "next_page": page + 1,
        "start": ((page - 1) * per_page + 1) if total_rows else 0,
        "end": min(page * per_page, total_rows),
    }
    return rows, summary, pagination


def _int_filter(value) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _list_filter(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        values = list(value)
    return [str(item).strip() for item in values if str(item).strip()]


def _request_list_arg(name: str) -> list[str]:
    values = request.args.getlist(name)
    if values:
        return _list_filter(values)
    return _list_filter(request.args.get(name))


def _selected_values(selected: list[str], allowed: list[str], default: list[str]) -> list[str]:
    allowed_set = set(allowed)
    values = [value for value in selected if value in allowed_set]
    return values or default


def _is_export_request() -> bool:
    return request.endpoint in {"works.openalex_global_export", "works.openalex_analytics_export", "works.openalex_works_export"}


def _chart_color(index: int) -> str:
    colors = [
        "#2a69b8", "#28a745", "#f39c12", "#dc3545", "#17a2b8",
        "#6f42c1", "#20c997", "#6c757d", "#fd7e14", "#6610f2",
    ]
    return colors[index % len(colors)]


def _openalex_analytics(ror_id: str, filters: dict | None = None) -> dict:
    """Build chart-ready analytics from OpenAlex-enriched journal articles."""
    from ..models import (
        OpenAlexWorkAuthor,
        OpenAlexWorkInstitution,
        OpenAlexWorkMetadata,
        WorkCache,
    )

    filters = filters or {}
    year_from = _int_filter(filters.get("year_from"))
    year_to = _int_filter(filters.get("year_to"))
    selected_types = _list_filter(filters.get("type"))
    selected_oa_statuses = _list_filter(filters.get("oa_status"))
    selected_affiliations = [
        value.lower()
        for value in _list_filter(filters.get("affiliation"))
        if value.lower() in {"all", "selected", "chile", "international", "not_selected"}
    ]
    if not selected_affiliations or "all" in selected_affiliations:
        selected_affiliations = []

    summary = _openalex_cache_summary(ror_id)
    cache_key = _openalex_cache_key_expr(WorkCache).label("doi_normalized")
    local_key_subquery = (
        db.session.query(cache_key)
        .filter(
            WorkCache.ror_id == ror_id,
            WorkCache.type == "journal-article",
        )
        .distinct()
        .subquery()
    )
    base_metadata_query = (
        db.session.query(OpenAlexWorkMetadata)
        .join(local_key_subquery, OpenAlexWorkMetadata.doi_normalized == local_key_subquery.c.doi_normalized)
    )

    option_years = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.publication_year)
            .join(local_key_subquery, OpenAlexWorkMetadata.doi_normalized == local_key_subquery.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
            .distinct()
            .order_by(OpenAlexWorkMetadata.publication_year.desc())
            .all()
        )
    ]
    option_types = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.type)
            .join(local_key_subquery, OpenAlexWorkMetadata.doi_normalized == local_key_subquery.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.type.isnot(None), OpenAlexWorkMetadata.type != "")
            .distinct()
            .order_by(OpenAlexWorkMetadata.type.asc())
            .all()
        )
    ]
    option_oa_statuses = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.oa_status)
            .join(local_key_subquery, OpenAlexWorkMetadata.doi_normalized == local_key_subquery.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.oa_status.isnot(None), OpenAlexWorkMetadata.oa_status != "")
            .distinct()
            .order_by(OpenAlexWorkMetadata.oa_status.asc())
            .all()
        )
    ]

    selected_inst_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized)
        .filter(OpenAlexWorkInstitution.ror_id == ror_id)
        .distinct()
        .subquery()
    )
    chile_inst_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code == "CL")
        .distinct()
        .subquery()
    )
    non_chile_inst_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code.isnot(None))
        .filter(OpenAlexWorkInstitution.country_code != "CL")
        .distinct()
        .subquery()
    )
    selected_inst_select = select(selected_inst_subquery.c.doi_normalized)
    chile_inst_select = select(chile_inst_subquery.c.doi_normalized)
    non_chile_inst_select = select(non_chile_inst_subquery.c.doi_normalized)

    filtered_query = base_metadata_query
    if year_from is not None:
        filtered_query = filtered_query.filter(OpenAlexWorkMetadata.publication_year >= year_from)
    if year_to is not None:
        filtered_query = filtered_query.filter(OpenAlexWorkMetadata.publication_year <= year_to)
    if selected_types:
        filtered_query = filtered_query.filter(OpenAlexWorkMetadata.type.in_(selected_types))
    if selected_oa_statuses:
        filtered_query = filtered_query.filter(OpenAlexWorkMetadata.oa_status.in_(selected_oa_statuses))
    affiliation_conditions = []
    if "selected" in selected_affiliations:
        affiliation_conditions.append(OpenAlexWorkMetadata.doi_normalized.in_(selected_inst_select))
    if "chile" in selected_affiliations:
        affiliation_conditions.append(OpenAlexWorkMetadata.doi_normalized.in_(chile_inst_select))
    if "international" in selected_affiliations:
        affiliation_conditions.append(and_(
            OpenAlexWorkMetadata.doi_normalized.in_(chile_inst_select),
            OpenAlexWorkMetadata.doi_normalized.in_(non_chile_inst_select),
        ))
    if "not_selected" in selected_affiliations:
        affiliation_conditions.append(~OpenAlexWorkMetadata.doi_normalized.in_(selected_inst_select))
    if affiliation_conditions:
        filtered_query = filtered_query.filter(or_(*affiliation_conditions))

    doi_subquery = (
        filtered_query
        .with_entities(OpenAlexWorkMetadata.doi_normalized.label("doi_normalized"))
        .distinct()
        .subquery()
    )

    def _counter_query(column, fallback: str, limit: int = 10):
        label_expr = func.coalesce(column, fallback)
        count_expr = func.count(OpenAlexWorkMetadata.id)
        rows = (
            db.session.query(label_expr, count_expr)
            .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
            .group_by(label_expr)
            .order_by(count_expr.desc())
            .limit(limit)
            .all()
        )
        return [row[0] for row in rows], [row[1] for row in rows]

    enriched_count = db.session.query(func.count()).select_from(doi_subquery).scalar() or 0
    year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            func.count(OpenAlexWorkMetadata.id),
            func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0),
        )
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year)
        .order_by(OpenAlexWorkMetadata.publication_year.asc())
        .all()
    )
    sorted_years = [str(row.publication_year) for row in year_rows]

    total_citations = (
        db.session.query(func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0))
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    open_access_count = (
        db.session.query(func.count(OpenAlexWorkMetadata.id))
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.is_oa.is_(True))
        .scalar()
        or 0
    )
    average_fwci = (
        db.session.query(func.avg(OpenAlexWorkMetadata.fwci))
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.fwci.isnot(None))
        .scalar()
    )

    selected_institution_count = (
        db.session.query(func.count())
        .select_from(doi_subquery)
        .join(selected_inst_subquery, selected_inst_subquery.c.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    cl_doi_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
        .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code == "CL")
        .distinct()
        .subquery()
    )
    non_cl_doi_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
        .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code.isnot(None))
        .filter(OpenAlexWorkInstitution.country_code != "CL")
        .distinct()
        .subquery()
    )
    chile_affiliation_count = (
        db.session.query(func.count())
        .select_from(cl_doi_subquery)
        .scalar()
        or 0
    )
    international_collaboration_count = (
        db.session.query(func.count())
        .select_from(cl_doi_subquery)
        .join(non_cl_doi_subquery, cl_doi_subquery.c.doi_normalized == non_cl_doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    selected_international_count = (
        db.session.query(func.count())
        .select_from(doi_subquery)
        .join(selected_inst_subquery, selected_inst_subquery.c.doi_normalized == doi_subquery.c.doi_normalized)
        .join(non_chile_inst_subquery, non_chile_inst_subquery.c.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    chile_without_selected_count = max(chile_affiliation_count - selected_institution_count, 0)
    no_chile_count = max(enriched_count - chile_affiliation_count, 0)

    unique_authors = (
        db.session.query(func.count(func.distinct(func.coalesce(OpenAlexWorkAuthor.author_id, OpenAlexWorkAuthor.author_name))))
        .join(doi_subquery, OpenAlexWorkAuthor.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )
    unique_institutions = (
        db.session.query(func.count(func.distinct(func.coalesce(OpenAlexWorkInstitution.institution_id, OpenAlexWorkInstitution.ror_id, OpenAlexWorkInstitution.institution_name))))
        .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
        .scalar()
        or 0
    )

    type_labels, type_values = _counter_query(OpenAlexWorkMetadata.type, _("Unknown type"), limit=8)
    oa_labels, oa_values = _counter_query(OpenAlexWorkMetadata.oa_status, _("Unknown OA status"), limit=8)
    field_labels, field_values = _counter_query(OpenAlexWorkMetadata.primary_topic_field, _("Unknown field"), limit=10)
    domain_labels, domain_values = _counter_query(OpenAlexWorkMetadata.primary_topic_domain, _("Unknown domain"), limit=10)
    source_labels, source_values = _counter_query(OpenAlexWorkMetadata.source_name, _("Unknown source"), limit=10)
    language_labels, language_values = _counter_query(OpenAlexWorkMetadata.language, _("Unknown language"), limit=8)

    type_year_label_expr = func.coalesce(OpenAlexWorkMetadata.type, _("Unknown type"))
    type_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            type_year_label_expr,
            func.count(OpenAlexWorkMetadata.id),
        )
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year, type_year_label_expr)
        .all()
    )
    type_year_counts = {
        (str(year), label): count
        for year, label, count in type_year_rows
    }
    doc_type_trend_labels = type_labels[:6]
    doc_type_trend_datasets = [
        {
            "label": label,
            "data": [type_year_counts.get((year, label), 0) for year in sorted_years],
            "borderColor": _chart_color(index),
            "backgroundColor": _chart_color(index),
            "tension": 0.25,
            "fill": False,
        }
        for index, label in enumerate(doc_type_trend_labels)
    ]

    oa_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            OpenAlexWorkMetadata.is_oa,
            func.count(OpenAlexWorkMetadata.id),
        )
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year, OpenAlexWorkMetadata.is_oa)
        .all()
    )
    oa_year_counts = {
        (str(year), bool(is_oa)): count
        for year, is_oa, count in oa_year_rows
    }

    country_label_expr = func.coalesce(OpenAlexWorkInstitution.country_code, _("Unknown country"))
    country_count_expr = func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized))
    country_rows = (
        db.session.query(
            country_label_expr,
            country_count_expr,
        )
        .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
        .group_by(country_label_expr)
        .order_by(country_count_expr.desc())
        .limit(12)
        .all()
    )

    institution_rows = (
        db.session.query(
            OpenAlexWorkInstitution.institution_name,
            OpenAlexWorkInstitution.ror_id,
            OpenAlexWorkInstitution.country_code,
            func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).label("works_count"),
            func.coalesce(func.sum(OpenAlexWorkInstitution.author_count), 0).label("author_links"),
        )
        .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
        .group_by(
            OpenAlexWorkInstitution.institution_name,
            OpenAlexWorkInstitution.ror_id,
            OpenAlexWorkInstitution.country_code,
        )
        .order_by(func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).desc())
        .limit(12)
        .all()
    )

    chile_institution_rows = (
        db.session.query(
            OpenAlexWorkInstitution.institution_name,
            OpenAlexWorkInstitution.ror_id,
            func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).label("works_count"),
        )
        .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code == "CL")
        .group_by(OpenAlexWorkInstitution.institution_name, OpenAlexWorkInstitution.ror_id)
        .order_by(func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).desc())
        .limit(10)
        .all()
    )

    author_rows = (
        db.session.query(
            OpenAlexWorkAuthor.author_name,
            OpenAlexWorkAuthor.author_id,
            OpenAlexWorkAuthor.orcid,
            OpenAlexWorkAuthor.has_chile_affiliation,
            func.count(func.distinct(OpenAlexWorkAuthor.doi_normalized)).label("works_count"),
        )
        .join(doi_subquery, OpenAlexWorkAuthor.doi_normalized == doi_subquery.c.doi_normalized)
        .group_by(
            OpenAlexWorkAuthor.author_name,
            OpenAlexWorkAuthor.author_id,
            OpenAlexWorkAuthor.orcid,
            OpenAlexWorkAuthor.has_chile_affiliation,
        )
        .order_by(func.count(func.distinct(OpenAlexWorkAuthor.doi_normalized)).desc())
        .limit(12)
        .all()
    )

    selected_inst_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            func.count(func.distinct(OpenAlexWorkMetadata.doi_normalized)),
        )
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .join(OpenAlexWorkInstitution, OpenAlexWorkInstitution.doi_normalized == OpenAlexWorkMetadata.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .filter(OpenAlexWorkInstitution.ror_id == ror_id)
        .group_by(OpenAlexWorkMetadata.publication_year)
        .order_by(OpenAlexWorkMetadata.publication_year.asc())
        .all()
    )
    selected_inst_year_counts = {str(year): count for year, count in selected_inst_year_rows}

    chile_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            func.count(func.distinct(OpenAlexWorkMetadata.doi_normalized)),
        )
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .join(OpenAlexWorkInstitution, OpenAlexWorkInstitution.doi_normalized == OpenAlexWorkMetadata.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .filter(OpenAlexWorkInstitution.country_code == "CL")
        .group_by(OpenAlexWorkMetadata.publication_year)
        .order_by(OpenAlexWorkMetadata.publication_year.asc())
        .all()
    )
    chile_year_counts = {str(year): count for year, count in chile_year_rows}

    top_cited = (
        OpenAlexWorkMetadata.query
        .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        .order_by(OpenAlexWorkMetadata.cited_by_count.desc())
        .limit(10)
        .all()
    )

    coverage_counts = {
        _("Matched"): summary["matched_dois"],
        _("Not found"): summary["not_found_dois"],
        _("Errors"): summary["error_dois"],
        _("Pending"): summary["pending_dois"],
    }
    coverage_counts = {label: count for label, count in coverage_counts.items() if count}

    affiliation_counts = {
        _("Selected institution"): selected_institution_count,
        _("Other Chile affiliation"): chile_without_selected_count,
        _("No Chile affiliation"): no_chile_count,
    }
    affiliation_counts = {label: count for label, count in affiliation_counts.items() if count}
    default_metrics = [
        "filtered_articles",
        "selected_percent",
        "chile_percent",
        "open_access_percent",
        "total_citations",
        "average_fwci",
    ]
    metric_ids = [
        "filtered_articles",
        "selected_percent",
        "chile_percent",
        "selected_international_percent",
        "open_access_percent",
        "total_citations",
        "selected_articles",
        "chile_articles",
        "international_articles",
        "average_citations",
        "average_fwci",
        "openalex_institutions",
    ]
    selected_metrics = _selected_values(_list_filter(filters.get("metrics")), metric_ids, default_metrics)

    return {
        "summary": {
            "candidate_dois": summary["candidate_dois"],
            "enriched_dois": enriched_count,
            "total_enriched_dois": summary["matched_openalex_keys"],
            "processed_dois": summary["processed_dois"],
            "open_access_count": open_access_count,
            "open_access_percent": round((open_access_count / enriched_count * 100), 1) if enriched_count else 0,
            "total_citations": total_citations,
            "average_citations": round((total_citations / enriched_count), 1) if enriched_count else 0,
            "average_fwci": round(float(average_fwci), 2) if average_fwci is not None else None,
            "selected_institution_count": selected_institution_count,
            "selected_institution_percent": round((selected_institution_count / enriched_count * 100), 1) if enriched_count else 0,
            "chile_affiliation_count": chile_affiliation_count,
            "chile_affiliation_percent": round((chile_affiliation_count / enriched_count * 100), 1) if enriched_count else 0,
            "international_collaboration_count": international_collaboration_count,
            "international_collaboration_percent": round((international_collaboration_count / chile_affiliation_count * 100), 1) if chile_affiliation_count else 0,
            "selected_international_count": selected_international_count,
            "selected_international_percent": round((selected_international_count / selected_institution_count * 100), 1) if selected_institution_count else 0,
            "unique_authors": unique_authors,
            "unique_institutions": unique_institutions,
            "last_run": summary["last_run"],
        },
        "filters": {
            "year_from": year_from,
            "year_to": year_to,
            "type": selected_types,
            "oa_status": selected_oa_statuses,
            "affiliation": selected_affiliations,
            "metrics": selected_metrics,
        },
        "metric_cards": [
            {"id": "filtered_articles", "label": _("Filtered OpenAlex Articles"), "value": enriched_count, "icon": "fas fa-check-circle", "color": "bg-success"},
            {"id": "selected_percent", "label": _("Associated with Selected Institution"), "value": f"{round((selected_institution_count / enriched_count * 100), 1) if enriched_count else 0}%", "icon": "fas fa-university", "color": "bg-primary"},
            {"id": "chile_percent", "label": _("Associated with Chile"), "value": f"{round((chile_affiliation_count / enriched_count * 100), 1) if enriched_count else 0}%", "icon": "fas fa-flag", "color": "bg-info"},
            {"id": "selected_international_percent", "label": _("Selected Institution + International"), "value": f"{round((selected_international_count / selected_institution_count * 100), 1) if selected_institution_count else 0}%", "icon": "fas fa-globe-americas", "color": "bg-secondary"},
            {"id": "open_access_percent", "label": _("Open Access"), "value": f"{round((open_access_count / enriched_count * 100), 1) if enriched_count else 0}%", "icon": "fas fa-unlock-alt", "color": "bg-warning"},
            {"id": "total_citations", "label": _("Total Citations"), "value": total_citations, "icon": "fas fa-quote-right", "color": "bg-danger"},
            {"id": "selected_articles", "label": _("Selected Institution Articles"), "value": selected_institution_count, "icon": "fas fa-building", "color": "bg-primary"},
            {"id": "chile_articles", "label": _("Chile-Affiliated Articles"), "value": chile_affiliation_count, "icon": "fas fa-map-marker-alt", "color": "bg-info"},
            {"id": "international_articles", "label": _("International Collaboration"), "value": international_collaboration_count, "icon": "fas fa-globe", "color": "bg-dark"},
            {"id": "average_citations", "label": _("Avg. Citations"), "value": round((total_citations / enriched_count), 1) if enriched_count else 0, "icon": "fas fa-chart-line", "color": "bg-success"},
            {"id": "average_fwci", "label": _("Avg. FWCI"), "value": round(float(average_fwci), 2) if average_fwci is not None else "N/A", "icon": "fas fa-balance-scale", "color": "bg-secondary"},
            {"id": "openalex_institutions", "label": _("OpenAlex Institutions"), "value": unique_institutions, "icon": "fas fa-project-diagram", "color": "bg-indigo"},
        ],
        "filter_options": {
            "years": option_years,
            "types": option_types,
            "oa_statuses": option_oa_statuses,
            "affiliations": [
                {"value": "selected", "label": _("Selected institution")},
                {"value": "chile", "label": _("Any Chile affiliation")},
                {"value": "international", "label": _("International collaboration")},
                {"value": "not_selected", "label": _("Not associated with selected institution")},
            ],
        },
        "charts": {
            "years": sorted_years,
            "works_by_year": [row[1] for row in year_rows],
            "citations_by_year": [int(row[2] or 0) for row in year_rows],
            "selected_by_year": [selected_inst_year_counts.get(year, 0) for year in sorted_years],
            "chile_by_year": [chile_year_counts.get(year, 0) for year in sorted_years],
            "coverage_labels": list(coverage_counts.keys()),
            "coverage_values": list(coverage_counts.values()),
            "affiliation_labels": list(affiliation_counts.keys()),
            "affiliation_values": list(affiliation_counts.values()),
            "type_labels": type_labels,
            "type_values": type_values,
            "doc_type_trend_labels": sorted_years,
            "doc_type_trend_datasets": doc_type_trend_datasets,
            "open_access_year_values": [oa_year_counts.get((year, True), 0) for year in sorted_years],
            "closed_access_year_values": [oa_year_counts.get((year, False), 0) for year in sorted_years],
            "oa_labels": oa_labels,
            "oa_values": oa_values,
            "field_labels": field_labels,
            "field_values": field_values,
            "domain_labels": domain_labels,
            "domain_values": domain_values,
            "source_labels": source_labels,
            "source_values": source_values,
            "language_labels": language_labels,
            "language_values": language_values,
            "country_labels": [row[0] for row in country_rows],
            "country_values": [row[1] for row in country_rows],
            "institution_labels": [
                f"{row.institution_name or _('Unknown institution')} ({row.ror_id or row.country_code or 'OA'})"
                for row in institution_rows
            ],
            "institution_values": [row.works_count for row in institution_rows],
            "chile_institution_labels": [
                f"{row.institution_name or _('Unknown institution')} ({row.ror_id or 'CL'})"
                for row in chile_institution_rows
            ],
            "chile_institution_values": [row.works_count for row in chile_institution_rows],
            "author_labels": [
                row.author_name or row.orcid or row.author_id or _("Unknown author")
                for row in author_rows
            ],
            "author_values": [row.works_count for row in author_rows],
        },
        "top_cited": top_cited,
        "top_institutions": institution_rows,
        "top_authors": author_rows,
    }


def _openalex_global_analytics(filters: dict | None = None) -> dict:
    """Build a staff-only cross-institution OpenAlex comparison."""
    from ..models import (
        OpenAlexWorkAuthor,
        OpenAlexWorkInstitution,
        OpenAlexWorkMetadata,
        WorkCache,
    )

    filters = filters or {}
    year_from = _int_filter(filters.get("year_from"))
    year_to = _int_filter(filters.get("year_to"))
    selected_types = _list_filter(filters.get("type"))
    selected_oa_statuses = _list_filter(filters.get("oa_status"))
    active_tab = (filters.get("tab") or "overview").strip().lower()
    valid_tabs = {"overview", "universities", "production", "institution_authors", "articles"}
    if active_tab not in valid_tabs:
        active_tab = "overview"

    university_sort, university_dir = _table_sort_params(
        "university",
        {"university", "orcid_articles", "openalex", "coverage", "own", "chile", "international", "open_access", "citations", "fwci"},
        "openalex",
    )
    author_sort, author_dir = _table_sort_params(
        "author",
        {"author", "works", "citations", "average_citations", "fwci", "chile"},
        "works",
    )
    institution_sort, institution_dir = _table_sort_params(
        "institution",
        {"institution", "country", "works", "author_links", "corresponding", "citations", "average_citations", "fwci"},
        "works",
    )
    institution_author_sort, institution_author_dir = _table_sort_params(
        "institution_author",
        {"university", "author", "works", "citations", "average_citations", "fwci", "latest_year", "chile"},
        "works",
    )
    author_page, author_per_page = _table_page_params("author", default_per_page=25)
    institution_page, institution_per_page = _table_page_params("institution", default_per_page=25)
    institution_author_page, institution_author_per_page = _table_page_params("institution_author", default_per_page=50, max_per_page=250)
    selected_institution_author_rors = _list_filter(filters.get("institution_author_ror"))

    cache_key = _openalex_cache_key_expr(WorkCache).label("doi_normalized")
    local_pairs = (
        db.session.query(
            WorkCache.ror_id.label("ror_id"),
            cache_key,
        )
        .filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.type == "journal-article",
        )
        .distinct()
        .subquery()
    )

    option_years = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.publication_year)
            .join(local_pairs, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
            .distinct()
            .order_by(OpenAlexWorkMetadata.publication_year.desc())
            .all()
        )
    ]
    option_types = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.type)
            .join(local_pairs, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.type.isnot(None), OpenAlexWorkMetadata.type != "")
            .distinct()
            .order_by(OpenAlexWorkMetadata.type.asc())
            .all()
        )
    ]
    option_oa_statuses = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.oa_status)
            .join(local_pairs, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.oa_status.isnot(None), OpenAlexWorkMetadata.oa_status != "")
            .distinct()
            .order_by(OpenAlexWorkMetadata.oa_status.asc())
            .all()
        )
    ]

    filtered_pair_query = (
        db.session.query(
            local_pairs.c.ror_id,
            OpenAlexWorkMetadata.doi_normalized.label("doi_normalized"),
        )
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
    )
    if year_from is not None:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.publication_year >= year_from)
    if year_to is not None:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.publication_year <= year_to)
    if selected_types:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.type.in_(selected_types))
    if selected_oa_statuses:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.oa_status.in_(selected_oa_statuses))

    filtered_pairs = filtered_pair_query.distinct().subquery()
    global_filtered_dois = (
        db.session.query(filtered_pairs.c.doi_normalized.label("doi_normalized"))
        .select_from(filtered_pairs)
        .distinct()
        .subquery()
    )

    global_year_rows = (
        db.session.query(OpenAlexWorkMetadata.publication_year)
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .distinct()
        .order_by(OpenAlexWorkMetadata.publication_year.asc())
        .all()
    )
    global_years = [str(row[0]) for row in global_year_rows]

    global_type_label_expr = func.coalesce(OpenAlexWorkMetadata.type, _("Unknown type"))
    global_type_labels = [
        row[0]
        for row in (
            db.session.query(global_type_label_expr, func.count())
            .select_from(filtered_pairs)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
            .group_by(global_type_label_expr)
            .order_by(func.count().desc())
            .limit(6)
            .all()
        )
    ]
    global_type_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            global_type_label_expr,
            func.count(),
        )
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year, global_type_label_expr)
        .all()
    )
    global_type_year_counts = {
        (str(year), label): count
        for year, label, count in global_type_year_rows
    }
    global_doc_type_trend_datasets = [
        {
            "label": label,
            "data": [global_type_year_counts.get((year, label), 0) for year in global_years],
            "borderColor": _chart_color(index),
            "backgroundColor": _chart_color(index),
            "tension": 0.25,
            "fill": False,
        }
        for index, label in enumerate(global_type_labels)
    ]

    global_oa_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            OpenAlexWorkMetadata.is_oa,
            func.count(),
        )
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year, OpenAlexWorkMetadata.is_oa)
        .all()
    )
    global_oa_year_counts = {
        (str(year), bool(is_oa)): count
        for year, is_oa, count in global_oa_year_rows
    }

    institutions = _institution_lookup()

    def _ordered(expression, direction: str):
        return expression.asc() if direction == "asc" else expression.desc()

    def _author_key_expr():
        return func.coalesce(
            OpenAlexWorkAuthor.author_id,
            OpenAlexWorkAuthor.orcid,
            OpenAlexWorkAuthor.author_name,
            literal("unknown"),
        )

    def _institution_key_expr():
        return func.coalesce(
            OpenAlexWorkInstitution.ror_id,
            OpenAlexWorkInstitution.institution_id,
            OpenAlexWorkInstitution.institution_name,
            literal("unknown"),
        )

    def _author_institution_associations(author_keys: list[str], ror_scope: list[str] | None = None) -> dict:
        if not author_keys:
            return {}

        author_key = _author_key_expr().label("author_key")
        query = (
            db.session.query(
                author_key,
                OpenAlexWorkAuthor.institution_names,
            )
            .select_from(OpenAlexWorkAuthor)
            .join(global_filtered_dois, OpenAlexWorkAuthor.doi_normalized == global_filtered_dois.c.doi_normalized)
        )
        if ror_scope:
            query = query.join(filtered_pairs, filtered_pairs.c.doi_normalized == OpenAlexWorkAuthor.doi_normalized)
            query = query.filter(filtered_pairs.c.ror_id.in_(ror_scope))

        counters: dict[str, dict[str, int]] = {key: {} for key in author_keys}
        for author_key_chunk in _chunks(author_keys, 5000):
            chunk_query = query.filter(author_key.in_(author_key_chunk))
            for row in chunk_query.all():
                counter = counters.setdefault(row.author_key, {})
                for name in _json_list(row.institution_names):
                    counter[name] = counter.get(name, 0) + 1

        return {key: _association_summary(counter) for key, counter in counters.items()}

    def _institution_author_associations(institution_keys: list[str]) -> dict:
        if not institution_keys:
            return {}

        institution_key = _institution_key_expr().label("institution_key")
        query = (
            db.session.query(
                institution_key,
                OpenAlexWorkInstitution.ror_id,
                OpenAlexWorkInstitution.institution_name,
                OpenAlexWorkAuthor.author_name,
                OpenAlexWorkAuthor.author_id,
                OpenAlexWorkAuthor.orcid,
                OpenAlexWorkAuthor.institution_rors,
                OpenAlexWorkAuthor.institution_names,
            )
            .select_from(OpenAlexWorkInstitution)
            .join(global_filtered_dois, OpenAlexWorkInstitution.doi_normalized == global_filtered_dois.c.doi_normalized)
            .join(OpenAlexWorkAuthor, OpenAlexWorkAuthor.doi_normalized == OpenAlexWorkInstitution.doi_normalized)
        )

        counters: dict[str, dict[str, int]] = {key: {} for key in institution_keys}
        for institution_key_chunk in _chunks(institution_keys, 5000):
            chunk_query = query.filter(institution_key.in_(institution_key_chunk))
            for row in chunk_query.all():
                author_rors = set(_json_list(row.institution_rors))
                author_institutions = set(_json_list(row.institution_names))
                if row.ror_id and row.ror_id not in author_rors:
                    continue
                if not row.ror_id and row.institution_name and row.institution_name not in author_institutions:
                    continue
                label = row.author_name or row.orcid or row.author_id or _("Unknown author")
                counter = counters.setdefault(row.institution_key, {})
                counter[label] = counter.get(label, 0) + 1

        return {key: _association_summary(counter) for key, counter in counters.items()}

    def _institution_author_row_associations(pairs: list[tuple[str, str]]) -> dict:
        if not pairs:
            return {}

        ror_scope = sorted({ror_id for ror_id, _ in pairs})
        author_keys = sorted({author_key for _, author_key in pairs})
        pair_set = set(pairs)
        author_key = _author_key_expr().label("author_key")
        query = (
            db.session.query(
                filtered_pairs.c.ror_id,
                author_key,
                OpenAlexWorkAuthor.institution_names,
            )
            .select_from(filtered_pairs)
            .join(OpenAlexWorkAuthor, OpenAlexWorkAuthor.doi_normalized == filtered_pairs.c.doi_normalized)
            .filter(filtered_pairs.c.ror_id.in_(ror_scope))
        )

        counters: dict[tuple[str, str], dict[str, int]] = {pair: {} for pair in pairs}
        for author_key_chunk in _chunks(author_keys, 5000):
            chunk_query = query.filter(author_key.in_(author_key_chunk))
            for row in chunk_query.all():
                pair = (row.ror_id, row.author_key)
                if pair not in pair_set:
                    continue
                counter = counters.setdefault(pair, {})
                for name in _json_list(row.institution_names):
                    counter[name] = counter.get(name, 0) + 1

        return {pair: _association_summary(counter) for pair, counter in counters.items()}

    top_authors = []
    top_institutions = []
    institution_author_rows = []
    author_pagination = _pagination_dict(author_page, author_per_page, 0)
    institution_pagination = _pagination_dict(institution_page, institution_per_page, 0)
    institution_author_pagination = _pagination_dict(institution_author_page, institution_author_per_page, 0)
    include_associations = not _is_export_request()

    if active_tab == "production":
        author_key = _author_key_expr().label("author_key")
        author_work_base = (
            db.session.query(
                author_key,
                OpenAlexWorkAuthor.author_name.label("author_name"),
                OpenAlexWorkAuthor.author_id.label("author_id"),
                OpenAlexWorkAuthor.orcid.label("orcid"),
                OpenAlexWorkAuthor.has_chile_affiliation.label("has_chile_affiliation"),
                OpenAlexWorkAuthor.doi_normalized.label("doi_normalized"),
                OpenAlexWorkMetadata.cited_by_count.label("cited_by_count"),
                OpenAlexWorkMetadata.fwci.label("fwci"),
            )
            .select_from(OpenAlexWorkAuthor)
            .join(global_filtered_dois, OpenAlexWorkAuthor.doi_normalized == global_filtered_dois.c.doi_normalized)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkAuthor.doi_normalized)
            .distinct()
            .subquery()
        )
        author_grouped = (
            db.session.query(
                author_work_base.c.author_key,
                func.max(author_work_base.c.author_name).label("author_name"),
                func.max(author_work_base.c.author_id).label("author_id"),
                func.max(author_work_base.c.orcid).label("orcid"),
                func.max(case((author_work_base.c.has_chile_affiliation.is_(True), 1), else_=0)).label("has_chile_affiliation"),
                func.count(func.distinct(author_work_base.c.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(author_work_base.c.cited_by_count), 0).label("total_citations"),
                func.avg(author_work_base.c.cited_by_count).label("average_citations"),
                func.avg(author_work_base.c.fwci).label("average_fwci"),
            )
            .select_from(author_work_base)
            .group_by(author_work_base.c.author_key)
            .subquery()
        )
        author_sort_columns = {
            "author": author_grouped.c.author_name,
            "works": author_grouped.c.works_count,
            "citations": author_grouped.c.total_citations,
            "average_citations": author_grouped.c.average_citations,
            "fwci": author_grouped.c.average_fwci,
            "chile": author_grouped.c.has_chile_affiliation,
        }
        author_total_rows = db.session.query(func.count()).select_from(author_grouped).scalar() or 0
        author_pagination = _pagination_dict(author_page, author_per_page, author_total_rows)
        author_rows = (
            db.session.query(author_grouped)
            .order_by(
                _ordered(author_sort_columns[author_sort], author_dir),
                author_grouped.c.works_count.desc(),
                author_grouped.c.author_name.asc(),
            )
            .offset((author_pagination["page"] - 1) * author_pagination["per_page"])
            .limit(author_pagination["per_page"])
            .all()
        )
        author_associations = _author_institution_associations([row.author_key for row in author_rows]) if include_associations else {}
        top_authors = [
            {
                "author_key": row.author_key,
                "author": row.author_name or row.orcid or row.author_id or _("Unknown author"),
                "author_id": row.author_id,
                "orcid": row.orcid,
                "has_chile_affiliation": bool(row.has_chile_affiliation),
                "works_count": int(row.works_count or 0),
                "total_citations": int(row.total_citations or 0),
                "average_citations": round(float(row.average_citations or 0), 1),
                "average_fwci": round(float(row.average_fwci), 2) if row.average_fwci is not None else None,
                "associated_institutions": author_associations.get(row.author_key, _association_summary({})),
            }
            for row in author_rows
        ]

        institution_key = _institution_key_expr().label("institution_key")
        institution_work_base = (
            db.session.query(
                institution_key,
                OpenAlexWorkInstitution.institution_name.label("institution_name"),
                OpenAlexWorkInstitution.institution_id.label("institution_id"),
                OpenAlexWorkInstitution.ror_id.label("ror_id"),
                OpenAlexWorkInstitution.country_code.label("country_code"),
                OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"),
                OpenAlexWorkInstitution.author_count.label("author_count"),
                OpenAlexWorkInstitution.has_corresponding_author.label("has_corresponding_author"),
                OpenAlexWorkMetadata.cited_by_count.label("cited_by_count"),
                OpenAlexWorkMetadata.fwci.label("fwci"),
            )
            .select_from(OpenAlexWorkInstitution)
            .join(global_filtered_dois, OpenAlexWorkInstitution.doi_normalized == global_filtered_dois.c.doi_normalized)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkInstitution.doi_normalized)
            .distinct()
            .subquery()
        )
        institution_grouped = (
            db.session.query(
                institution_work_base.c.institution_key,
                func.max(institution_work_base.c.institution_name).label("institution_name"),
                func.max(institution_work_base.c.institution_id).label("institution_id"),
                func.max(institution_work_base.c.ror_id).label("ror_id"),
                func.max(institution_work_base.c.country_code).label("country_code"),
                func.count(func.distinct(institution_work_base.c.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(institution_work_base.c.author_count), 0).label("author_links"),
                func.count(func.distinct(case(
                    (institution_work_base.c.has_corresponding_author.is_(True), institution_work_base.c.doi_normalized),
                ))).label("corresponding_works"),
                func.coalesce(func.sum(institution_work_base.c.cited_by_count), 0).label("total_citations"),
                func.avg(institution_work_base.c.cited_by_count).label("average_citations"),
                func.avg(institution_work_base.c.fwci).label("average_fwci"),
            )
            .select_from(institution_work_base)
            .group_by(institution_work_base.c.institution_key)
            .subquery()
        )
        institution_sort_columns = {
            "institution": institution_grouped.c.institution_name,
            "country": institution_grouped.c.country_code,
            "works": institution_grouped.c.works_count,
            "author_links": institution_grouped.c.author_links,
            "corresponding": institution_grouped.c.corresponding_works,
            "citations": institution_grouped.c.total_citations,
            "average_citations": institution_grouped.c.average_citations,
            "fwci": institution_grouped.c.average_fwci,
        }
        institution_total_rows = db.session.query(func.count()).select_from(institution_grouped).scalar() or 0
        institution_pagination = _pagination_dict(institution_page, institution_per_page, institution_total_rows)
        institution_rows = (
            db.session.query(institution_grouped)
            .order_by(
                _ordered(institution_sort_columns[institution_sort], institution_dir),
                institution_grouped.c.works_count.desc(),
                institution_grouped.c.institution_name.asc(),
            )
            .offset((institution_pagination["page"] - 1) * institution_pagination["per_page"])
            .limit(institution_pagination["per_page"])
            .all()
        )
        institution_associations = _institution_author_associations([row.institution_key for row in institution_rows]) if include_associations else {}
        top_institutions = [
            {
                "institution_key": row.institution_key,
                "institution": row.institution_name or row.ror_id or row.institution_id or _("Unknown institution"),
                "institution_id": row.institution_id,
                "ror_id": row.ror_id,
                "country_code": row.country_code or _("Unknown"),
                "works_count": int(row.works_count or 0),
                "author_links": int(row.author_links or 0),
                "corresponding_works": int(row.corresponding_works or 0),
                "total_citations": int(row.total_citations or 0),
                "average_citations": round(float(row.average_citations or 0), 1),
                "average_fwci": round(float(row.average_fwci), 2) if row.average_fwci is not None else None,
                "associated_authors": institution_associations.get(row.institution_key, _association_summary({})),
            }
            for row in institution_rows
        ]

    if active_tab == "institution_authors":
        author_key = _author_key_expr().label("author_key")
        institution_author_base_query = (
            db.session.query(
                filtered_pairs.c.ror_id.label("ror_id"),
                author_key,
                OpenAlexWorkAuthor.author_name.label("author_name"),
                OpenAlexWorkAuthor.author_id.label("author_id"),
                OpenAlexWorkAuthor.orcid.label("orcid"),
                OpenAlexWorkAuthor.has_chile_affiliation.label("has_chile_affiliation"),
                OpenAlexWorkAuthor.doi_normalized.label("doi_normalized"),
                OpenAlexWorkMetadata.cited_by_count.label("cited_by_count"),
                OpenAlexWorkMetadata.fwci.label("fwci"),
                OpenAlexWorkMetadata.publication_year.label("publication_year"),
            )
            .select_from(filtered_pairs)
            .join(OpenAlexWorkAuthor, OpenAlexWorkAuthor.doi_normalized == filtered_pairs.c.doi_normalized)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        )
        if selected_institution_author_rors:
            institution_author_base_query = institution_author_base_query.filter(filtered_pairs.c.ror_id.in_(selected_institution_author_rors))
        institution_author_base = institution_author_base_query.distinct().subquery()
        institution_author_grouped = (
            db.session.query(
                institution_author_base.c.ror_id,
                institution_author_base.c.author_key,
                func.max(institution_author_base.c.author_name).label("author_name"),
                func.max(institution_author_base.c.author_id).label("author_id"),
                func.max(institution_author_base.c.orcid).label("orcid"),
                func.max(case((institution_author_base.c.has_chile_affiliation.is_(True), 1), else_=0)).label("has_chile_affiliation"),
                func.count(func.distinct(institution_author_base.c.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(institution_author_base.c.cited_by_count), 0).label("total_citations"),
                func.avg(institution_author_base.c.cited_by_count).label("average_citations"),
                func.avg(institution_author_base.c.fwci).label("average_fwci"),
                func.max(institution_author_base.c.publication_year).label("latest_year"),
            )
            .select_from(institution_author_base)
            .group_by(institution_author_base.c.ror_id, institution_author_base.c.author_key)
            .subquery()
        )
        institution_author_sort_columns = {
            "university": institution_author_grouped.c.ror_id,
            "author": institution_author_grouped.c.author_name,
            "works": institution_author_grouped.c.works_count,
            "citations": institution_author_grouped.c.total_citations,
            "average_citations": institution_author_grouped.c.average_citations,
            "fwci": institution_author_grouped.c.average_fwci,
            "latest_year": institution_author_grouped.c.latest_year,
            "chile": institution_author_grouped.c.has_chile_affiliation,
        }
        institution_author_total_rows = db.session.query(func.count()).select_from(institution_author_grouped).scalar() or 0
        institution_author_pagination = _pagination_dict(institution_author_page, institution_author_per_page, institution_author_total_rows)
        paged_institution_author_rows = (
            db.session.query(institution_author_grouped)
            .order_by(
                _ordered(institution_author_sort_columns[institution_author_sort], institution_author_dir),
                institution_author_grouped.c.works_count.desc(),
                institution_author_grouped.c.author_name.asc(),
            )
            .offset((institution_author_pagination["page"] - 1) * institution_author_pagination["per_page"])
            .limit(institution_author_pagination["per_page"])
            .all()
        )
        pair_associations = _institution_author_row_associations([
            (row.ror_id, row.author_key)
            for row in paged_institution_author_rows
        ]) if include_associations else {}
        institution_author_rows = [
            {
                "ror_id": row.ror_id,
                "university": institutions.get(row.ror_id) or row.ror_id,
                "author_key": row.author_key,
                "author": row.author_name or row.orcid or row.author_id or _("Unknown author"),
                "author_id": row.author_id,
                "orcid": row.orcid,
                "has_chile_affiliation": bool(row.has_chile_affiliation),
                "works_count": int(row.works_count or 0),
                "total_citations": int(row.total_citations or 0),
                "average_citations": round(float(row.average_citations or 0), 1),
                "average_fwci": round(float(row.average_fwci), 2) if row.average_fwci is not None else None,
                "latest_year": row.latest_year,
                "associated_institutions": pair_associations.get((row.ror_id, row.author_key), _association_summary({})),
            }
            for row in paged_institution_author_rows
        ]

    local_article_counts = {
        ror_id: count
        for ror_id, count in (
            db.session.query(WorkCache.ror_id, func.count(WorkCache.id))
            .filter(
                WorkCache.ror_id.isnot(None),
                WorkCache.ror_id != "",
                WorkCache.type == "journal-article",
            )
            .group_by(WorkCache.ror_id)
            .all()
        )
    }
    local_key_counts = {
        ror_id: count
        for ror_id, count in (
            db.session.query(local_pairs.c.ror_id, func.count())
            .select_from(local_pairs)
            .group_by(local_pairs.c.ror_id)
            .all()
        )
    }
    matched_key_counts = {
        ror_id: count
        for ror_id, count in (
            db.session.query(
                local_pairs.c.ror_id,
                func.count(func.distinct(local_pairs.c.doi_normalized)),
            )
            .select_from(local_pairs)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .group_by(local_pairs.c.ror_id)
            .all()
        )
    }

    selected_pairs = (
        db.session.query(
            filtered_pairs.c.ror_id.label("ror_id"),
            filtered_pairs.c.doi_normalized.label("doi_normalized"),
        )
        .join(
            OpenAlexWorkInstitution,
            and_(
                OpenAlexWorkInstitution.doi_normalized == filtered_pairs.c.doi_normalized,
                OpenAlexWorkInstitution.ror_id == filtered_pairs.c.ror_id,
            ),
        )
        .distinct()
        .subquery()
    )
    chile_dois = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
        .filter(OpenAlexWorkInstitution.country_code == "CL")
        .distinct()
        .subquery()
    )
    non_chile_dois = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
        .filter(OpenAlexWorkInstitution.country_code.isnot(None))
        .filter(OpenAlexWorkInstitution.country_code != "CL")
        .distinct()
        .subquery()
    )

    metric_rows = (
        db.session.query(
            filtered_pairs.c.ror_id,
            func.count(func.distinct(filtered_pairs.c.doi_normalized)).label("enriched_count"),
            func.count(func.distinct(case(
                (OpenAlexWorkMetadata.is_oa.is_(True), filtered_pairs.c.doi_normalized),
            ))).label("open_access_count"),
            func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0).label("total_citations"),
            func.avg(OpenAlexWorkMetadata.fwci).label("average_fwci"),
            func.count(func.distinct(case(
                (selected_pairs.c.doi_normalized.isnot(None), filtered_pairs.c.doi_normalized),
            ))).label("selected_count"),
            func.count(func.distinct(case(
                (chile_dois.c.doi_normalized.isnot(None), filtered_pairs.c.doi_normalized),
            ))).label("chile_count"),
            func.count(func.distinct(case(
                (
                    and_(
                        chile_dois.c.doi_normalized.isnot(None),
                        non_chile_dois.c.doi_normalized.isnot(None),
                    ),
                    filtered_pairs.c.doi_normalized,
                ),
            ))).label("international_count"),
        )
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .outerjoin(
            selected_pairs,
            and_(
                selected_pairs.c.ror_id == filtered_pairs.c.ror_id,
                selected_pairs.c.doi_normalized == filtered_pairs.c.doi_normalized,
            ),
        )
        .outerjoin(chile_dois, chile_dois.c.doi_normalized == filtered_pairs.c.doi_normalized)
        .outerjoin(non_chile_dois, non_chile_dois.c.doi_normalized == filtered_pairs.c.doi_normalized)
        .group_by(filtered_pairs.c.ror_id)
        .all()
    )

    metric_by_ror = {row.ror_id: row for row in metric_rows}
    all_ror_ids = sorted(set(local_article_counts) | set(local_key_counts) | set(metric_by_ror))
    rows = []
    for ror_id in all_ror_ids:
        metric = metric_by_ror.get(ror_id)
        enriched_count = int(getattr(metric, "enriched_count", 0) or 0)
        local_key_count = int(local_key_counts.get(ror_id, 0) or 0)
        matched_key_count = int(matched_key_counts.get(ror_id, 0) or 0)
        selected_count = int(getattr(metric, "selected_count", 0) or 0)
        chile_count = int(getattr(metric, "chile_count", 0) or 0)
        international_count = int(getattr(metric, "international_count", 0) or 0)
        open_access_count = int(getattr(metric, "open_access_count", 0) or 0)
        total_citations = int(getattr(metric, "total_citations", 0) or 0)
        average_fwci = getattr(metric, "average_fwci", None)
        rows.append({
            "ror_id": ror_id,
            "institution": institutions.get(ror_id) or ror_id,
            "article_works": int(local_article_counts.get(ror_id, 0) or 0),
            "openalex_candidates": local_key_count,
            "matched_keys_total": matched_key_count,
            "enriched_count": enriched_count,
            "coverage_percent": round((matched_key_count / local_key_count * 100), 1) if local_key_count else 0,
            "selected_count": selected_count,
            "selected_percent": round((selected_count / enriched_count * 100), 1) if enriched_count else 0,
            "chile_count": chile_count,
            "chile_percent": round((chile_count / enriched_count * 100), 1) if enriched_count else 0,
            "international_count": international_count,
            "international_percent": round((international_count / chile_count * 100), 1) if chile_count else 0,
            "open_access_count": open_access_count,
            "open_access_percent": round((open_access_count / enriched_count * 100), 1) if enriched_count else 0,
            "total_citations": total_citations,
            "average_citations": round((total_citations / enriched_count), 1) if enriched_count else 0,
            "average_fwci": round(float(average_fwci), 2) if average_fwci is not None else None,
        })

    chart_rows = sorted(rows, key=lambda row: (-row["enriched_count"], row["institution"]))[:12]
    university_sort_map = {
        "university": ("institution", "text"),
        "orcid_articles": ("article_works", "number"),
        "openalex": ("enriched_count", "number"),
        "coverage": ("coverage_percent", "number"),
        "own": ("selected_percent", "number"),
        "chile": ("chile_percent", "number"),
        "international": ("international_percent", "number"),
        "open_access": ("open_access_percent", "number"),
        "citations": ("total_citations", "number"),
        "fwci": ("average_fwci", "number"),
    }
    row_key, row_kind = university_sort_map[university_sort]
    if row_kind == "text":
        rows.sort(key=lambda row: (row.get(row_key) or "").lower(), reverse=university_dir == "desc")
    else:
        rows.sort(key=lambda row: row.get(row_key) if row.get(row_key) is not None else -1, reverse=university_dir == "desc")

    total_enriched = sum(row["enriched_count"] for row in rows)
    total_selected = sum(row["selected_count"] for row in rows)
    total_chile = sum(row["chile_count"] for row in rows)
    total_international = sum(row["international_count"] for row in rows)
    total_open_access = sum(row["open_access_count"] for row in rows)
    total_citations = sum(row["total_citations"] for row in rows)
    total_candidates = sum(row["openalex_candidates"] for row in rows)
    total_matched_keys = sum(row["matched_keys_total"] for row in rows)

    return {
        "active_tab": active_tab,
        "summary": {
            "institutions_count": len([row for row in rows if row["article_works"] or row["enriched_count"]]),
            "article_works": sum(row["article_works"] for row in rows),
            "openalex_candidates": total_candidates,
            "enriched_count": total_enriched,
            "matched_keys_total": total_matched_keys,
            "coverage_percent": round((total_matched_keys / total_candidates * 100), 1) if total_candidates else 0,
            "selected_count": total_selected,
            "selected_percent": round((total_selected / total_enriched * 100), 1) if total_enriched else 0,
            "chile_count": total_chile,
            "chile_percent": round((total_chile / total_enriched * 100), 1) if total_enriched else 0,
            "international_count": total_international,
            "international_percent": round((total_international / total_chile * 100), 1) if total_chile else 0,
            "open_access_count": total_open_access,
            "open_access_percent": round((total_open_access / total_enriched * 100), 1) if total_enriched else 0,
            "total_citations": total_citations,
            "average_citations": round((total_citations / total_enriched), 1) if total_enriched else 0,
        },
        "filters": {
            "year_from": year_from,
            "year_to": year_to,
            "type": selected_types,
            "oa_status": selected_oa_statuses,
            "tab": active_tab,
            "institution_author_ror": selected_institution_author_rors,
        },
        "filter_options": {
            "years": option_years,
            "types": option_types,
            "oa_statuses": option_oa_statuses,
            "institutions": [
                {"ror_id": row["ror_id"], "name": row["institution"]}
                for row in sorted(rows, key=lambda row: row["institution"])
                if row["enriched_count"]
            ],
        },
        "charts": {
            "institution_labels": [row["institution"] for row in chart_rows],
            "enriched_values": [row["enriched_count"] for row in chart_rows],
            "coverage_values": [row["coverage_percent"] for row in chart_rows],
            "selected_values": [row["selected_percent"] for row in chart_rows],
            "chile_values": [row["chile_percent"] for row in chart_rows],
            "international_values": [row["international_percent"] for row in chart_rows],
            "citation_values": [row["total_citations"] for row in chart_rows],
            "trend_years": global_years,
            "doc_type_trend_datasets": global_doc_type_trend_datasets,
            "open_access_year_values": [global_oa_year_counts.get((year, True), 0) for year in global_years],
            "closed_access_year_values": [global_oa_year_counts.get((year, False), 0) for year in global_years],
            "oa_labels": [_("Open Access"), _("Closed")],
            "oa_values": [total_open_access, max(total_enriched - total_open_access, 0)],
        },
        "rows": rows,
        "top_authors": top_authors,
        "top_institutions": top_institutions,
        "institution_author_rows": institution_author_rows,
        "tables": {
            "universities": {
                "sort": university_sort,
                "dir": university_dir,
            },
            "authors": {
                "sort": author_sort,
                "dir": author_dir,
                "pagination": author_pagination,
                "per_page_options": [25, 50, 100],
            },
            "institutions": {
                "sort": institution_sort,
                "dir": institution_dir,
                "pagination": institution_pagination,
                "per_page_options": [25, 50, 100],
            },
            "institution_authors": {
                "sort": institution_author_sort,
                "dir": institution_author_dir,
                "pagination": institution_author_pagination,
                "per_page_options": [25, 50, 100, 250],
            },
        },
    }


def _has_cache_works(ror_id: str) -> bool:
    """Checks if any Works records exist for the given ROR."""
    from ..models import WorkCache
    return db.session.query(WorkCache.id).filter_by(ror_id=ror_id).first() is not None

def _last_cache_run_works(ror_id: str):
    """Retrieves the last successful Works synchronization log."""
    from ..models import WorkCacheRun
    return (
        WorkCacheRun.query
        .filter_by(ror_id=ror_id, status='success')
        .order_by(WorkCacheRun.finished_at.desc())
        .first()
    )

def _has_cache_fundings(ror_id: str) -> bool:
    """Checks if any Funding records exist for the given ROR."""
    from ..models import FundingCache
    return db.session.query(FundingCache.id).filter_by(ror_id=ror_id).first() is not None

def _last_cache_run_fundings(ror_id: str):
    """Retrieves the last successful Fundings synchronization log."""
    from ..models import FundingCacheRun
    return (
        FundingCacheRun.query
        .filter_by(ror_id=ror_id, status='success')
        .order_by(FundingCacheRun.finished_at.desc())
        .first()
    )


def _run_full_sync_for_ror(ror_id: str, base_url: str, headers: dict) -> dict:
    """Run works, fundings, and profile-name synchronization for one ROR."""
    from ..models import WorkCacheRun, FundingCacheRun
    from ..services.cache_service import (
        build_works_cache_for_ror,
        build_fundings_cache_for_ror,
        build_researcher_names_cache,
    )

    result = {"ror_id": ror_id, "works": 0, "fundings": 0, "profiles": 0, "errors": []}

    run_w = WorkCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run_w)
    db.session.commit()
    try:
        result["works"] = build_works_cache_for_ror(ror_id, base_url, headers)
        run_w.status = 'success'
        run_w.rows_count = result["works"]
    except Exception as exc:
        db.session.rollback()
        logger.exception("Works sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Works")
        run_w.status = 'failed'
        run_w.error = str(exc)
    finally:
        run_w.finished_at = dt.utcnow()
        db.session.add(run_w)
        db.session.commit()

    run_f = FundingCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run_f)
    db.session.commit()
    try:
        result["fundings"] = build_fundings_cache_for_ror(ror_id, base_url, headers)
        run_f.status = 'success'
        run_f.rows_count = result["fundings"]
    except Exception as exc:
        db.session.rollback()
        logger.exception("Fundings sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Fundings")
        run_f.status = 'failed'
        run_f.error = str(exc)
    finally:
        run_f.finished_at = dt.utcnow()
        db.session.add(run_f)
        db.session.commit()

    try:
        result["profiles"] = build_researcher_names_cache(ror_id)
    except Exception as exc:
        db.session.rollback()
        logger.exception("Profiles sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Profiles")

    return result


@bp_works.route('/cache/full/build', methods=['POST'])
@login_required
def cache_full_build():
    """
    Executes the complete synchronization sequence for an institution.
    
    Sequence:
    1. Works (Publications)
    2. Fundings (Grants)
    3. Researcher Profiles (Names/Bio) - Optimized Multithreaded
    
    This ensures data consistency across all related tables in a single operation.
    """
    if not (session.get('is_admin') or session.get('is_manager')):
        flash_err(_("You do not have sufficient permissions to perform this action."))
        return redirect(url_for('main.index'))

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('works.cache_works_status'))

    # Setup API Context (Defaulting to Public API)
    base_url = current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/')
    headers = {'Accept': 'application/json'}

    result = _run_full_sync_for_ror(ror_id, base_url, headers)

    if not result["errors"]:
        flash_ok(_('Full synchronization complete: %(w)s works, %(f)s grants, and %(p)s profiles updated.', 
                   w=result["works"], f=result["fundings"], p=result["profiles"]))
    else:
        flash_err(_('Sync completed with errors in: %(err)s. Check logs.', err=", ".join(result["errors"])))

    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/cache/full/build-all', methods=['POST'])
@login_required
def cache_full_build_all():
    """Run the full metadata cache synchronization for every known institution."""
    if not session.get('is_admin'):
        flash_err(_("Access restricted to administrators."))
        return redirect(url_for('works.cache_works_status'))

    from ..services.institution_registry_service import get_institution_options

    institutions = get_institution_options()
    if not institutions:
        flash_err(_("No institutions available for synchronization."))
        return redirect(url_for('works.cache_works_status'))

    base_url = current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/')
    headers = {'Accept': 'application/json'}

    totals = {"works": 0, "fundings": 0, "profiles": 0, "failed": 0}
    for institution in institutions:
        ror_id = institution.get("ror_id")
        if not ror_id:
            continue

        result = _run_full_sync_for_ror(ror_id, base_url, headers)
        totals["works"] += result["works"]
        totals["fundings"] += result["fundings"]
        totals["profiles"] += result["profiles"]
        if result["errors"]:
            totals["failed"] += 1

    if totals["failed"]:
        flash_err(_(
            "All-institution synchronization completed with %(failed)s institution errors. Totals: %(w)s works, %(f)s fundings, %(p)s profiles.",
            failed=totals["failed"], w=totals["works"], f=totals["fundings"], p=totals["profiles"],
        ))
    else:
        flash_ok(_(
            "All-institution synchronization complete: %(count)s institutions, %(w)s works, %(f)s fundings, %(p)s profiles.",
            count=len(institutions), w=totals["works"], f=totals["fundings"], p=totals["profiles"],
        ))

    return redirect(url_for('works.cache_works_status'))
# INDIVIDUAL OPERATIONS (Legacy / Specific)

@bp_works.route('/cache/works/build', methods=['POST'])
@login_required
def cache_works_build():
    """Wrapper to trigger the full build from legacy UI buttons."""
    return cache_full_build() 

@bp_works.route('/cache/fundings/build', methods=['POST'])
@login_required
def cache_fundings_build():
    """
    Isolated Funding Sync. 
    Useful if the user specifically wants to update grants without waiting for publications.
    """
    from ..models import FundingCacheRun
    from ..services.cache_service import build_fundings_cache_for_ror
    ror_id = get_active_ror_id()
    
    run = FundingCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run)
    db.session.commit()
    try:
        base_url = current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/')
        rows = build_fundings_cache_for_ror(ror_id, base_url, {'Accept': 'application/json'})
        run.status = 'success'
        run.rows_count = rows
        flash_ok(_('Funding cache updated.'))
    except Exception as e:
        db.session.rollback()
        run.status = 'failed'
        run.error = str(e)
        flash_err(_('Error updating fundings.'))
    finally:
        run.finished_at = dt.utcnow()
        db.session.commit()
    return redirect(url_for('works.cache_works_status'))

@bp_works.route('/cache/profiles/build', methods=['POST'])
@login_required
def cache_profiles_build():
    """Isolated Profile Sync."""
    from ..services.cache_service import build_researcher_names_cache
    ror_id = get_active_ror_id()
    try:
        c = build_researcher_names_cache(ror_id)
        flash_ok(_('Profiles updated: %s', c))
    except Exception:
        flash_err(_('Error updating profiles'))
    return redirect(url_for('works.cache_works_status'))
@bp_works.route('/cache/works/status')
@login_required
def cache_works_status():
    """
    Renders the Data Management Dashboard.
    Displays last run times, record counts, and action buttons for sync/export.
    """
    from ..models import FundingCache, OpenAlexWorkRawCache, ResearcherCache, WorkCache

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    # Gather Statistics
    last_run_works = _last_cache_run_works(ror_id)
    w_count = db.session.query(WorkCache.id).filter_by(ror_id=ror_id).count()

    last_run_fundings = _last_cache_run_fundings(ror_id)
    f_count = db.session.query(FundingCache.id).filter_by(ror_id=ror_id).count()
    
    # Global profile count (Approximate)
    p_count = db.session.query(ResearcherCache.orcid).count()
    admin_researcher_count = _researcher_count() if session.get('is_admin') else 0
    admin_works_count = db.session.query(WorkCache.id).count() if session.get('is_admin') else 0
    admin_fundings_count = db.session.query(FundingCache.id).count() if session.get('is_admin') else 0
    admin_openalex_count = db.session.query(OpenAlexWorkRawCache.id).count() if session.get('is_admin') else 0
    openalex_summary = _openalex_cache_summary(ror_id)

    return render_template(
        'works/cache_status.html',
        has_cache_works=(w_count > 0),
        last_run_works=last_run_works,
        count_works=w_count,
        has_cache_fundings=(f_count > 0),
        last_run_fundings=last_run_fundings,
        count_fundings=f_count,
        count_profiles=p_count,
        admin_researcher_count=admin_researcher_count,
        admin_works_count=admin_works_count,
        admin_fundings_count=admin_fundings_count,
        admin_openalex_count=admin_openalex_count,
        openalex_summary=openalex_summary,
    )


@bp_works.route('/openalex/sync', methods=['POST'])
@login_required
def openalex_sync():
    """Synchronize OpenAlex metadata for DOI-backed journal articles."""
    if not (session.get('is_admin') or session.get('is_manager')):
        flash_err(_("You do not have sufficient permissions to perform this action."))
        return redirect(url_for('works.cache_works_status'))

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('works.cache_works_status'))

    mode = (request.form.get("mode") or "missing").strip().lower()
    if mode not in {"missing", "all", "title"}:
        flash_err(_('Invalid OpenAlex synchronization mode.'))
        return redirect(url_for('works.cache_works_status'))

    from ..services.openalex_service import sync_openalex_title_matches, sync_openalex_works

    force_refresh = mode == "all"
    if mode == "title":
        result = sync_openalex_title_matches(
            ror_id=ror_id,
            stale_days=0,
            articles_only=True,
        )
    else:
        result = sync_openalex_works(
            ror_id=ror_id,
            force_refresh=force_refresh,
            stale_days=0,
            articles_only=True,
        )

    if result["status"] == "failed":
        flash_err(_('OpenAlex synchronization failed: %(error)s', error=result.get("error") or _("Check logs.")))
    elif mode == "title":
        flash_ok(_(
            'OpenAlex title matching complete: %(fetched)s searched, %(matched)s matched, %(not_found)s not found, %(skipped)s skipped, %(errors)s errors.',
            fetched=result["fetched_count"],
            matched=result["matched_count"],
            not_found=result["not_found_count"],
            skipped=result["skipped_count"],
            errors=result["error_count"],
        ))
    else:
        flash_ok(_(
            'OpenAlex synchronization complete: %(fetched)s fetched, %(matched)s matched, %(not_found)s not found, %(skipped)s skipped, %(errors)s errors.',
            fetched=result["fetched_count"],
            matched=result["matched_count"],
            not_found=result["not_found_count"],
            skipped=result["skipped_count"],
            errors=result["error_count"],
        ))

    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/openalex/sync-system', methods=['POST'])
@admin_required
def openalex_sync_system():
    """Synchronize OpenAlex metadata for DOI-backed journal articles system-wide."""
    mode = (request.form.get("mode") or "missing").strip().lower()
    if mode not in {"missing", "all", "title"}:
        flash_err(_('Invalid OpenAlex synchronization mode.'))
        return redirect(url_for('works.cache_works_status'))

    from ..services.openalex_service import sync_openalex_title_matches, sync_openalex_works

    force_refresh = mode == "all"
    if mode == "title":
        result = sync_openalex_title_matches(
            ror_id=None,
            stale_days=0,
            articles_only=True,
        )
    else:
        result = sync_openalex_works(
            ror_id=None,
            force_refresh=force_refresh,
            stale_days=0,
            articles_only=True,
        )

    if result["status"] == "failed":
        flash_err(_('OpenAlex synchronization failed: %(error)s', error=result.get("error") or _("Check logs.")))
    elif mode == "title":
        flash_ok(_(
            'System-wide OpenAlex title matching complete: %(fetched)s searched, %(matched)s matched, %(not_found)s not found, %(skipped)s skipped, %(errors)s errors.',
            fetched=result["fetched_count"],
            matched=result["matched_count"],
            not_found=result["not_found_count"],
            skipped=result["skipped_count"],
            errors=result["error_count"],
        ))
    else:
        flash_ok(_(
            'System-wide OpenAlex synchronization complete: %(fetched)s fetched, %(matched)s matched, %(not_found)s not found, %(skipped)s skipped, %(errors)s errors.',
            fetched=result["fetched_count"],
            matched=result["matched_count"],
            not_found=result["not_found_count"],
            skipped=result["skipped_count"],
            errors=result["error_count"],
        ))

    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/openalex/works')
@login_required
def openalex_works():
    """Render OpenAlex enrichment coverage for DOI-backed works."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    coverage = request.args.get("coverage", "all")
    if coverage not in {"all", "enriched", "missing"}:
        coverage = "all"

    page, per_page = _page_params()
    rows, summary, pagination = _openalex_work_rows(
        ror_id,
        coverage=coverage,
        page=page,
        per_page=per_page,
    )
    return render_template(
        'works/openalex_works.html',
        rows=rows,
        summary=summary,
        pagination=pagination,
        coverage=coverage,
        ror_id=ror_id,
    )


@bp_works.route('/openalex/works/export')
@login_required
def openalex_works_export():
    """Export OpenAlex enrichment detail for the active institution."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    coverage = request.args.get("coverage", "all")
    if coverage not in {"all", "enriched", "missing"}:
        coverage = "all"

    rows, _summary, _pagination = _openalex_work_rows(ror_id, coverage=coverage, page=1, per_page=100000)
    data_frame = pd.DataFrame([{
        "title": row["title"],
        "orcid": row["orcid"],
        "type": row["type"],
        "publication_year": row["pub_year"],
        "journal": row["journal_title"],
        "doi": row["doi"],
        "openalex_cache_key": row["openalex_cache_key"],
        "matched": row["matched"],
        "raw_status": row["raw_status"],
        "raw_error": row["raw_error"],
        "openalex_id": row["openalex_id"],
        "openalex_url": row["openalex_url"],
        "citations": row["cited_by_count"],
        "is_open_access": row["is_oa"],
        "oa_status": row["oa_status"],
        "source": row["source_name"],
        "issn_l": row["source_issn_l"],
        "topic_field": row["primary_topic_field"],
        "topic_domain": row["primary_topic_domain"],
    } for row in rows])
    return _send_dataframe_export(data_frame, f"openalex_works_{ror_id}_{coverage}", "OpenAlex works")


@bp_works.route('/openalex/analytics')
@login_required
def openalex_analytics():
    """Render charts and trends from OpenAlex-enriched journal articles."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "affiliation": _request_list_arg("affiliation"),
        "metrics": _request_list_arg("metric"),
    }
    analytics = _openalex_analytics(ror_id, analytics_filters)

    def export_url(table_key: str, export_format: str):
        params = request.args.to_dict(flat=False)
        params["format"] = export_format
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_analytics_export", table_key=table_key, **params)

    return render_template(
        'works/openalex_analytics.html',
        analytics=analytics,
        ror_id=ror_id,
        export_url=export_url,
    )


@bp_works.route('/openalex/analytics/export/<table_key>')
@login_required
def openalex_analytics_export(table_key: str):
    """Export table-like OpenAlex analytics for the active institution."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "affiliation": _request_list_arg("affiliation"),
    }
    analytics = _openalex_analytics(ror_id, analytics_filters)

    if table_key == "top_cited":
        rows = [{
            "title": row.title,
            "openalex_id": row.openalex_id,
            "publication_year": row.publication_year,
            "type": row.type,
            "source": row.source_name,
            "citations": row.cited_by_count,
            "fwci": row.fwci,
            "is_open_access": row.is_oa,
            "oa_status": row.oa_status,
            "topic_field": row.primary_topic_field,
            "topic_domain": row.primary_topic_domain,
        } for row in analytics["top_cited"]]
        sheet_name = "Top cited"
    elif table_key == "authors":
        rows = [{
            "author": row.author_name,
            "author_id": row.author_id,
            "orcid": row.orcid,
            "has_chile_affiliation": row.has_chile_affiliation,
            "articles": row.works_count,
        } for row in analytics["top_authors"]]
        sheet_name = "Authors"
    elif table_key == "institutions":
        rows = [{
            "institution": row.institution_name,
            "ror_id": row.ror_id,
            "country": row.country_code,
            "articles": row.works_count,
            "author_links": row.author_links,
        } for row in analytics["top_institutions"]]
        sheet_name = "Institutions"
    else:
        flash_err(_('Invalid export table.'))
        return redirect(url_for('works.openalex_analytics', **request.args.to_dict(flat=False)))

    return _send_dataframe_export(pd.DataFrame(rows), f"openalex_analytics_{ror_id}_{table_key}", sheet_name)


@bp_works.route('/openalex/global')
@staff_required
def openalex_global():
    """Render a staff-only OpenAlex comparison across institutions."""
    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "tab": request.args.get("tab"),
        "institution_author_ror": _request_list_arg("institution_author_ror"),
    }

    def query_url(**updates):
        params = request.args.to_dict(flat=False)
        params.update({key: value for key, value in updates.items() if value is not None})
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_global", **params)

    table_prefixes = {
        "universities": "university",
        "authors": "author",
        "institutions": "institution",
        "institution_authors": "institution_author",
    }

    def table_url(table_key: str, **updates):
        params = request.args.to_dict(flat=False)
        prefix = table_prefixes.get(table_key, table_key)
        for key, value in updates.items():
            if key in {"sort", "dir", "page", "per_page"}:
                params[f"{prefix}_{key}"] = value
            else:
                params[key] = value
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_global", **params)

    def export_url(table_key: str, export_format: str):
        params = request.args.to_dict(flat=False)
        params["format"] = export_format
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_global_export", table_key=table_key, **params)

    analytics = _openalex_global_analytics_with_cache(analytics_filters)
    return render_template(
        'works/openalex_global.html',
        analytics=analytics,
        query_url=query_url,
        table_url=table_url,
        export_url=export_url,
    )


@bp_works.route('/openalex/global/export/<table_key>')
@staff_required
def openalex_global_export(table_key: str):
    """Export global OpenAlex analytics tables with current filters and sort."""
    table_tabs = {
        "universities": "universities",
        "authors": "production",
        "institutions": "production",
        "institution_authors": "institution_authors",
    }
    if table_key not in table_tabs:
        flash_err(_('Invalid export table.'))
        return redirect(url_for('works.openalex_global', **request.args.to_dict(flat=False)))

    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "tab": table_tabs[table_key],
        "institution_author_ror": _request_list_arg("institution_author_ror"),
    }
    analytics = _openalex_global_analytics(analytics_filters)

    if table_key == "universities":
        rows = [{
            "university": row["institution"],
            "ror_id": row["ror_id"],
            "orcid_articles": row["article_works"],
            "openalex_articles": row["enriched_count"],
            "overall_coverage_percent": row["coverage_percent"],
            "own_institution_percent": row["selected_percent"],
            "own_institution_articles": row["selected_count"],
            "chile_percent": row["chile_percent"],
            "chile_articles": row["chile_count"],
            "international_percent": row["international_percent"],
            "international_articles": row["international_count"],
            "open_access_percent": row["open_access_percent"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
        } for row in analytics["rows"]]
        sheet_name = "Universities"
    elif table_key == "authors":
        rows = [{
            "author": row["author"],
            "author_id": row["author_id"],
            "orcid": row["orcid"],
            "associated_institutions": _summary_labels(row["associated_institutions"]),
            "articles": row["works_count"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
            "has_chile_affiliation": row["has_chile_affiliation"],
        } for row in analytics["top_authors"]]
        sheet_name = "Authors"
    elif table_key == "institutions":
        rows = [{
            "institution": row["institution"],
            "institution_id": row["institution_id"],
            "ror_id": row["ror_id"],
            "country": row["country_code"],
            "associated_authors": _summary_labels(row["associated_authors"]),
            "articles": row["works_count"],
            "author_links": row["author_links"],
            "corresponding_articles": row["corresponding_works"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
        } for row in analytics["top_institutions"]]
        sheet_name = "Institutions"
    else:
        rows = [{
            "university": row["university"],
            "ror_id": row["ror_id"],
            "author": row["author"],
            "author_id": row["author_id"],
            "orcid": row["orcid"],
            "associated_institutions": _summary_labels(row["associated_institutions"]),
            "articles": row["works_count"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
            "latest_year": row["latest_year"],
            "has_chile_affiliation": row["has_chile_affiliation"],
        } for row in analytics["institution_author_rows"]]
        sheet_name = "Institution authors"

    return _send_dataframe_export(pd.DataFrame(rows), f"openalex_global_{table_key}", sheet_name)


@bp_works.route('/download/all-works/cache')
@login_required
def download_all_works_cache():
    """
    Exports the complete Works cache for the current institution.
    """
    from ..models import WorkCache
    ror_id = get_active_ror_id()
    
    records = WorkCache.query.filter_by(ror_id=ror_id).all()
    if not records:
        flash_err(_('The publication cache is currently empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_frame = _build_works_dataframe(records)
        file_base_name = f"orcid_works_cache_{ror_id}"
        return _send_dataframe_export(data_frame, file_base_name, 'Works')
    except Exception as exc:
        logger.exception("EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/all-fundings/cache')
@login_required
def download_all_fundings_cache():
    """
    Exports the complete Funding cache for the current institution.
    """
    from ..models import FundingCache
    ror_id = get_active_ror_id()
    
    records = FundingCache.query.filter_by(ror_id=ror_id).all()
    if not records:
        flash_err(_('The funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_frame = _build_fundings_dataframe(records)
        file_base_name = f"orcid_fundings_cache_{ror_id}"
        return _send_dataframe_export(data_frame, file_base_name, 'Fundings')
    except Exception as exc:
        logger.exception("EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/researchers/cache')
@admin_required
def download_all_researchers_admin():
    """Export every institution-researcher pair known in local caches."""
    data_frame = _build_researchers_dataframe()
    if data_frame.empty:
        flash_err(_('No researcher cache data available.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        return _send_dataframe_export(
            data_frame,
            'orcid_researchers_all_institutions',
            'Researchers',
        )
    except Exception as exc:
        logger.exception("ADMIN RESEARCHERS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-works/cache')
@admin_required
def download_all_works_admin():
    """Export cached works for all institutions."""
    from ..models import WorkCache

    records = WorkCache.query.order_by(WorkCache.ror_id, WorkCache.orcid, WorkCache.id).all()
    if not records:
        flash_err(_('The publication cache is currently empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_frame = _build_works_dataframe(records)
        return _send_dataframe_export(
            data_frame,
            'orcid_works_all_institutions',
            'Works',
        )
    except Exception as exc:
        logger.exception("ADMIN WORKS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-fundings/cache')
@admin_required
def download_all_fundings_admin():
    """Export cached fundings for all institutions."""
    from ..models import FundingCache

    records = FundingCache.query.order_by(FundingCache.ror_id, FundingCache.orcid, FundingCache.id).all()
    if not records:
        flash_err(_('The funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_frame = _build_fundings_dataframe(records)
        return _send_dataframe_export(
            data_frame,
            'orcid_fundings_all_institutions',
            'Fundings',
        )
    except Exception as exc:
        logger.exception("ADMIN FUNDINGS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/openalex/cache')
@admin_required
def download_openalex_admin():
    """Export cached OpenAlex work metadata for all institutions."""
    from ..models import OpenAlexWorkMetadata, OpenAlexWorkRawCache
    from sqlalchemy.orm import load_only

    records_query = (
        db.session.query(OpenAlexWorkRawCache, OpenAlexWorkMetadata)
        .outerjoin(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkRawCache.doi_normalized)
        .order_by(OpenAlexWorkRawCache.doi_normalized.asc())
    )
    if (request.args.get('format') or '').lower() == 'excel':
        records_query = records_query.options(
            load_only(
                OpenAlexWorkRawCache.doi_normalized,
                OpenAlexWorkRawCache.source_doi,
                OpenAlexWorkRawCache.openalex_id,
                OpenAlexWorkRawCache.status,
                OpenAlexWorkRawCache.http_status,
                OpenAlexWorkRawCache.error,
                OpenAlexWorkRawCache.fetched_at,
                OpenAlexWorkRawCache.created_at,
                OpenAlexWorkRawCache.oa_updated_date,
            ),
            load_only(
                OpenAlexWorkMetadata.openalex_id,
                OpenAlexWorkMetadata.title,
                OpenAlexWorkMetadata.publication_year,
                OpenAlexWorkMetadata.publication_date,
                OpenAlexWorkMetadata.type,
                OpenAlexWorkMetadata.language,
                OpenAlexWorkMetadata.cited_by_count,
                OpenAlexWorkMetadata.fwci,
                OpenAlexWorkMetadata.is_retracted,
                OpenAlexWorkMetadata.is_oa,
                OpenAlexWorkMetadata.oa_status,
                OpenAlexWorkMetadata.oa_url,
                OpenAlexWorkMetadata.best_pdf_url,
                OpenAlexWorkMetadata.source_name,
                OpenAlexWorkMetadata.source_issn_l,
                OpenAlexWorkMetadata.source_type,
                OpenAlexWorkMetadata.source_is_in_doaj,
                OpenAlexWorkMetadata.primary_topic_name,
                OpenAlexWorkMetadata.primary_topic_field,
                OpenAlexWorkMetadata.primary_topic_domain,
                OpenAlexWorkMetadata.fetched_at,
                OpenAlexWorkMetadata.updated_at,
            ),
        )
    if not db.session.query(OpenAlexWorkRawCache.id).first():
        flash_err(_('The OpenAlex cache is empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        return _send_openalex_export(records_query)
    except Exception as exc:
        logger.exception("ADMIN OPENALEX EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))
