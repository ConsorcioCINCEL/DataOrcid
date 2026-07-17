"""Work/funding cache management views and exports."""

import copy
import csv
import hashlib
import json
import logging
import math
import os
import tempfile
import time
from collections import OrderedDict
from datetime import datetime as dt, timedelta, timezone
from io import BytesIO, StringIO
from pathlib import Path
from threading import RLock

import pandas as pd
from flask import (
    Blueprint, g, request, redirect, url_for,
    Response, send_file, current_app, render_template, session, stream_with_context
)
from flask_babel import _
from sqlalchemy import String, and_, case, cast, func, literal, or_, select

from .. import db, plain_text
from ..decorators import admin_required, login_required, normalize_ror_id, staff_required
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

bp_works = Blueprint("works", __name__)
logger = logging.getLogger(__name__)

_OPENALEX_GLOBAL_ANALYTICS_CACHE = OrderedDict()
_OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK = RLock()
_OPENALEX_INSTITUTION_ANALYTICS_CACHE = OrderedDict()
_OPENALEX_INSTITUTION_ANALYTICS_CACHE_LOCK = RLock()
_OPENALEX_ANALYTICS_CACHE_MAX_SIZE = 40
_OPENALEX_ANALYTICS_CACHE_TTL = 86400
_OPENALEX_PERSISTENT_CACHE_VERSION = 1
_OPENALEX_PERSISTENT_CACHE_MAX_FILES = 120


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
    """Build a DOI normalization expression supported by PostgreSQL and SQLite."""
    trimmed = func.lower(func.trim(model.doi))
    without_url = func.replace(
        func.replace(
            func.replace(
                func.replace(trimmed, "https://dx.doi.org/", ""),
                "http://dx.doi.org/",
                "",
            ),
            "https://doi.org/",
            "",
        ),
        "http://doi.org/",
        "",
    )
    without_prefix = func.replace(without_url, "doi:", "")
    return func.rtrim(func.trim(without_prefix), ".")


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

    return page, min(max(per_page, 10), 250)


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


def _openalex_analytics_cache_ttl() -> int:
    try:
        return max(int(current_app.config.get("OPENALEX_ANALYTICS_CACHE_TTL", _OPENALEX_ANALYTICS_CACHE_TTL)), 0)
    except (TypeError, ValueError):
        return _OPENALEX_ANALYTICS_CACHE_TTL


def _signature_row(model, timestamp_column) -> dict:
    count, latest = db.session.query(func.count(model.id), func.max(timestamp_column)).one()
    return {
        "count": int(count or 0),
        "latest": latest.isoformat() if latest else "",
    }


def _openalex_data_signature(ror_id: str | None = None) -> dict:
    from ..models import OpenAlexWorkAuthor, OpenAlexWorkInstitution, OpenAlexWorkMetadata, WorkCache

    work_query = db.session.query(func.count(WorkCache.id), func.max(WorkCache.created_at))
    if ror_id:
        work_query = work_query.filter(WorkCache.ror_id == ror_id)
    work_count, work_latest = work_query.one()

    return {
        "works": {
            "count": int(work_count or 0),
            "latest": work_latest.isoformat() if work_latest else "",
        },
        "metadata": _signature_row(OpenAlexWorkMetadata, OpenAlexWorkMetadata.updated_at),
        "authors": _signature_row(OpenAlexWorkAuthor, OpenAlexWorkAuthor.created_at),
        "institutions": _signature_row(OpenAlexWorkInstitution, OpenAlexWorkInstitution.created_at),
    }


def _openalex_analytics_request_cache_key(namespace: str, filters: dict, ror_id: str | None = None) -> str:
    request_args = {
        key: request.args.getlist(key)
        for key in sorted(request.args.keys())
        if key not in {"lang", "refresh_cache", "section", "tab"}
    }
    payload = {
        "namespace": namespace,
        "ror_id": ror_id,
        "filters": filters,
        "request_args": request_args,
        "locale": session.get("locale") or current_app.config.get("BABEL_DEFAULT_LOCALE", "en"),
        "data_signature": _openalex_data_signature(ror_id),
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _openalex_persistent_cache_path(namespace: str, cache_key: str) -> Path:
    cache_dir = Path(current_app.instance_path) / "openalex-analytics-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{namespace}-{cache_key}.json"


def _read_openalex_persistent_cache(namespace: str, cache_key: str, ttl: int) -> dict | None:
    path = _openalex_persistent_cache_path(namespace, cache_key)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        stored_at = float(payload.get("stored_at", 0))
        if payload.get("version") != _OPENALEX_PERSISTENT_CACHE_VERSION:
            return None
        if time.time() - stored_at > ttl:
            return None
        analytics = payload.get("analytics")
        if not isinstance(analytics, dict):
            return None
        return {
            "analytics": analytics,
            "stored_at": stored_at,
            "generated_at": payload.get("generated_at") or dt.now(timezone.utc).isoformat(),
        }
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _prune_openalex_persistent_cache(cache_dir: Path) -> None:
    try:
        cache_files = sorted(
            cache_dir.glob("*.json"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        for path in cache_files[_OPENALEX_PERSISTENT_CACHE_MAX_FILES:]:
            path.unlink(missing_ok=True)
    except OSError:
        logger.debug("Could not prune the persistent OpenAlex analytics cache.", exc_info=True)


def _write_openalex_persistent_cache(
    namespace: str,
    cache_key: str,
    analytics: dict,
    generated_at: str,
) -> None:
    path = _openalex_persistent_cache_path(namespace, cache_key)
    payload = {
        "version": _OPENALEX_PERSISTENT_CACHE_VERSION,
        "stored_at": time.time(),
        "generated_at": generated_at,
        "analytics": analytics,
    }
    temporary_path = None
    try:
        descriptor, temporary_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        os.replace(temporary_path, path)
        _prune_openalex_persistent_cache(path.parent)
    except (OSError, TypeError, ValueError):
        logger.warning("Could not persist the OpenAlex analytics cache.", exc_info=True)
        if temporary_path:
            try:
                Path(temporary_path).unlink(missing_ok=True)
            except OSError:
                pass


def _openalex_analytics_with_cache(
    namespace: str,
    filters: dict,
    builder,
    memory_cache: OrderedDict,
    cache_lock: RLock,
    ror_id: str | None = None,
) -> dict:
    ttl = _openalex_analytics_cache_ttl()
    refresh_cache = request.args.get("refresh_cache") == "1"
    now = time.monotonic()
    cache_key = _openalex_analytics_request_cache_key(namespace, filters, ror_id) if ttl > 0 else None

    if cache_key and not refresh_cache:
        with cache_lock:
            cached = memory_cache.get(cache_key)
            if cached and now - cached["stored_at"] <= ttl:
                memory_cache.move_to_end(cache_key)
                analytics = copy.deepcopy(cached["analytics"])
                analytics["cache"] = {
                    "hit": True,
                    "layer": "memory",
                    "generated_at": cached["generated_at"],
                    "ttl_seconds": ttl,
                }
                return analytics
            if cached:
                memory_cache.pop(cache_key, None)

        persistent = _read_openalex_persistent_cache(namespace, cache_key, ttl)
        if persistent:
            with cache_lock:
                memory_cache[cache_key] = {
                    "analytics": copy.deepcopy(persistent["analytics"]),
                    "stored_at": now,
                    "generated_at": persistent["generated_at"],
                }
                while len(memory_cache) > _OPENALEX_ANALYTICS_CACHE_MAX_SIZE:
                    memory_cache.popitem(last=False)
            analytics = persistent["analytics"]
            analytics["cache"] = {
                "hit": True,
                "layer": "persistent",
                "generated_at": persistent["generated_at"],
                "ttl_seconds": ttl,
            }
            return analytics

    analytics = builder(filters)
    generated_at = dt.now(timezone.utc).isoformat()

    if cache_key:
        cached_analytics = copy.deepcopy(analytics)
        cached_analytics.pop("cache", None)
        with cache_lock:
            memory_cache[cache_key] = {
                "analytics": cached_analytics,
                "stored_at": now,
                "generated_at": generated_at,
            }
            while len(memory_cache) > _OPENALEX_ANALYTICS_CACHE_MAX_SIZE:
                memory_cache.popitem(last=False)
        _write_openalex_persistent_cache(namespace, cache_key, cached_analytics, generated_at)

    analytics["cache"] = {
        "hit": False,
        "layer": "database",
        "generated_at": generated_at,
        "ttl_seconds": ttl,
    }
    return analytics


def _openalex_institution_analytics_with_cache(ror_id: str, filters: dict) -> dict:
    return _openalex_analytics_with_cache(
        "institution",
        filters,
        lambda current_filters: _openalex_analytics(ror_id, current_filters),
        _OPENALEX_INSTITUTION_ANALYTICS_CACHE,
        _OPENALEX_INSTITUTION_ANALYTICS_CACHE_LOCK,
        ror_id=ror_id,
    )


def _openalex_global_analytics_with_cache(filters: dict) -> dict:
    requested_tab = (filters.get("tab") or "overview").strip().lower()
    if requested_tab not in {"overview", "universities", "production", "institution_authors", "articles"}:
        requested_tab = "overview"

    # Overview, university, and article tabs use the same global aggregate.
    # Cache them as one dataset and apply the selected presentation tab after
    # retrieval so navigating between those sections does not rerun the query.
    cache_filters = dict(filters)
    cache_filters["tab"] = (
        requested_tab
        if requested_tab in {"production", "institution_authors"}
        else "overview"
    )
    analytics = _openalex_analytics_with_cache(
        "global",
        cache_filters,
        _openalex_global_analytics,
        _OPENALEX_GLOBAL_ANALYTICS_CACHE,
        _OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK,
    )
    analytics["active_tab"] = requested_tab
    analytics.setdefault("filters", {})["tab"] = requested_tab
    return analytics


def _sorted_counter_rows(counter: dict, limit: int = 10) -> tuple[list[str], list[int]]:
    rows = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [row[0] for row in rows], [row[1] for row in rows]


def _summary_labels(summary: dict | None) -> str:
    if not summary:
        return ""
    return "; ".join(summary.get("labels") or [])


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
            WorkCache.doi.isnot(None),
            WorkCache.doi != "",
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
        "unmatched_dois": max(candidate_dois - matched - max(candidate_dois - processed, 0) - errors, 0),
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
    per_page: int = 25,
    search: str = "",
    sort: str = "citations",
    direction: str = "desc",
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
        query = query.filter(
            WorkCache.doi.isnot(None),
            func.trim(WorkCache.doi) != "",
            OpenAlexWorkRawCache.id.is_(None),
        )
    elif coverage == "not_found":
        query = query.filter(OpenAlexWorkRawCache.status.in_(("not_found", "error")))
    elif coverage == "no_doi":
        query = query.filter(or_(WorkCache.doi.is_(None), func.trim(WorkCache.doi) == ""))

    search = (search or "").strip()
    if search:
        pattern = f"%{search}%"
        query = query.filter(or_(
            WorkCache.title.ilike(pattern),
            WorkCache.doi.ilike(pattern),
            WorkCache.orcid.ilike(pattern),
            WorkCache.journal_title.ilike(pattern),
            OpenAlexWorkMetadata.source_name.ilike(pattern),
            OpenAlexWorkMetadata.primary_topic_field.ilike(pattern),
            OpenAlexWorkMetadata.primary_topic_domain.ilike(pattern),
            OpenAlexWorkMetadata.openalex_id.ilike(pattern),
        ))

    total_rows = query.count()
    pagination = _pagination_dict(page, per_page, total_rows)
    page = pagination["page"]

    status_sort = case(
        (OpenAlexWorkMetadata.id.isnot(None), 0),
        (OpenAlexWorkRawCache.status == "error", 3),
        (OpenAlexWorkRawCache.status == "not_found", 2),
        else_=1,
    )
    sort_columns = {
        "title": WorkCache.title,
        "year": WorkCache.pub_year,
        "citations": OpenAlexWorkMetadata.cited_by_count,
        "open_access": OpenAlexWorkMetadata.is_oa,
        "source": OpenAlexWorkMetadata.source_name,
        "status": status_sort,
    }
    sort = sort if sort in sort_columns else "citations"
    direction = direction if direction in {"asc", "desc"} else "desc"
    sort_column = sort_columns[sort]
    null_rank = case((sort_column.is_(None), 1), else_=0)
    primary_order = sort_column.asc() if direction == "asc" else sort_column.desc()
    query = query.order_by(null_rank.asc(), primary_order, WorkCache.title.asc(), WorkCache.id.asc())
    result_rows = query.offset((page - 1) * per_page).limit(per_page).all()

    rows = []
    for row in result_rows:
        raw_status = row.raw_status or "pending"
        openalex_id = row.openalex_id
        has_doi = bool(row.doi and row.doi.strip())
        if openalex_id:
            status_key = "matched"
        elif raw_status == "not_found":
            status_key = "not_found"
        elif raw_status == "error":
            status_key = "error"
        elif not has_doi:
            status_key = "no_doi"
        else:
            status_key = "pending"
        rows.append({
            "work_cache_id": row.work_cache_id,
            "title": plain_text(row.title),
            "orcid": row.orcid,
            "type": row.type or "",
            "pub_year": row.pub_year or "",
            "journal_title": row.journal_title or "",
            "doi": row.doi or "",
            "doi_normalized": row.doi_normalized,
            "openalex_cache_key": row.openalex_cache_key,
            "has_doi": has_doi,
            "matched": bool(openalex_id),
            "status_key": status_key,
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

    has_doi_condition = and_(WorkCache.doi.isnot(None), func.trim(WorkCache.doi) != "")
    coverage_row = (
        db.session.query(
            func.count(WorkCache.id),
            func.coalesce(func.sum(case((OpenAlexWorkMetadata.id.isnot(None), 1), else_=0)), 0),
            func.coalesce(func.sum(case((and_(has_doi_condition, OpenAlexWorkRawCache.id.is_(None)), 1), else_=0)), 0),
            func.coalesce(func.sum(case((OpenAlexWorkRawCache.status.in_(("not_found", "error")), 1), else_=0)), 0),
            func.coalesce(func.sum(case((~has_doi_condition, 1), else_=0)), 0),
        )
        .select_from(WorkCache)
        .outerjoin(OpenAlexWorkRawCache, OpenAlexWorkRawCache.doi_normalized == _openalex_cache_key_expr(WorkCache))
        .outerjoin(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == _openalex_cache_key_expr(WorkCache))
        .filter(WorkCache.ror_id == ror_id, WorkCache.type == "journal-article")
        .one()
    )

    summary.update({
        "total_article_works": summary["article_works"],
        "total_doi_works": summary["article_doi_works"],
        "unique_dois": summary["candidate_dois"],
        "enriched_unique_dois": summary["matched_openalex_keys"],
        "processed_unique_dois": summary["processed_dois"],
        "coverage_percent": round((coverage_row[1] / coverage_row[0] * 100), 1) if coverage_row[0] else 0,
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
        "coverage_counts": {
            "all": int(coverage_row[0] or 0),
            "enriched": int(coverage_row[1] or 0),
            "missing": int(coverage_row[2] or 0),
            "not_found": int(coverage_row[3] or 0),
            "no_doi": int(coverage_row[4] or 0),
        },
    })

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
    top_cited = [
        {
            "title": row.title,
            "openalex_id": row.openalex_id,
            "publication_year": row.publication_year,
            "type": row.type,
            "source_name": row.source_name,
            "cited_by_count": row.cited_by_count,
            "fwci": row.fwci,
            "is_oa": row.is_oa,
            "oa_status": row.oa_status,
            "primary_topic_field": row.primary_topic_field,
            "primary_topic_domain": row.primary_topic_domain,
        }
        for row in top_cited
    ]
    top_institutions = [
        {
            "institution_name": row.institution_name,
            "ror_id": row.ror_id,
            "country_code": row.country_code,
            "works_count": row.works_count,
            "author_links": row.author_links,
        }
        for row in institution_rows
    ]
    top_authors = [
        {
            "author_name": row.author_name,
            "author_id": row.author_id,
            "orcid": row.orcid,
            "has_chile_affiliation": row.has_chile_affiliation,
            "works_count": row.works_count,
        }
        for row in author_rows
    ]

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
        "top_institutions": top_institutions,
        "top_authors": top_authors,
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
    from ..models import FundingCacheRun, utc_now
    return (
        FundingCacheRun.query
        .filter_by(ror_id=ror_id, status='success')
        .order_by(FundingCacheRun.finished_at.desc())
        .first()
    )


def _recent_sync_runs(
    ror_id: str,
    limit: int = 6,
    include_system: bool = False,
) -> list[dict]:
    """Return recent institutional and optional system runs in one shape."""
    from ..models import FundingCacheRun, OpenAlexSyncRun, SyncJob, WorkCacheRun

    openalex_query = OpenAlexSyncRun.query
    job_query = SyncJob.query
    if include_system:
        openalex_query = openalex_query.filter(
            or_(OpenAlexSyncRun.ror_id == ror_id, OpenAlexSyncRun.ror_id.is_(None))
        )
        job_query = job_query.filter(
            or_(SyncJob.ror_id == ror_id, SyncJob.ror_id.is_(None))
        )
    else:
        openalex_query = openalex_query.filter_by(ror_id=ror_id)
        job_query = job_query.filter_by(ror_id=ror_id)

    run_groups = (
        (
            "works",
            WorkCacheRun.query.filter_by(ror_id=ror_id)
            .order_by(WorkCacheRun.started_at.desc())
            .limit(limit)
            .all(),
        ),
        (
            "fundings",
            FundingCacheRun.query.filter_by(ror_id=ror_id)
            .order_by(FundingCacheRun.started_at.desc())
            .limit(limit)
            .all(),
        ),
        (
            "openalex",
            openalex_query
            .order_by(OpenAlexSyncRun.started_at.desc())
            .limit(limit)
            .all(),
        ),
    )

    runs = []
    for kind, rows in run_groups:
        for row in rows:
            started_at = row.started_at
            finished_at = row.finished_at
            duration_seconds = None
            if started_at and finished_at:
                duration_seconds = max(int((finished_at - started_at).total_seconds()), 0)

            if kind == "openalex":
                records = row.matched_count or row.fetched_count or row.works_seen or 0
                errors = row.error_count or 0
            else:
                records = row.rows_count or 0
                errors = 1 if row.error else 0

            runs.append({
                "id": row.id,
                "kind": kind,
                "status": (row.status or "pending").lower(),
                "started_at": started_at,
                "finished_at": finished_at,
                "timestamp": finished_at or started_at,
                "duration_seconds": duration_seconds,
                "records": int(records),
                "errors": int(errors),
            })

    for job in job_query.order_by(SyncJob.created_at.desc()).limit(limit).all():
        is_openalex_job = (job.job_type or "").startswith("openalex_")
        result = job.result_json or {}
        openalex_result = (result.get("openalex") or {}) if isinstance(result, dict) else {}
        result_errors = (result.get("errors") or []) if isinstance(result, dict) else []
        runs.append({
            "id": job.id,
            "kind": "openalex" if is_openalex_job else "full",
            "status": (job.status or "queued").lower(),
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "timestamp": job.finished_at or job.started_at or job.created_at,
            "duration_seconds": (
                max(int((job.finished_at - job.started_at).total_seconds()), 0)
                if job.started_at and job.finished_at
                else None
            ),
            "records": int(
                openalex_result.get("matched_count", 0)
                if is_openalex_job
                else job.progress_current or 0
            ),
            "records_total": int(job.progress_total or 0),
            "errors": len(result_errors) if result_errors else (1 if job.error else 0),
        })

    runs.sort(key=lambda item: item["timestamp"] or dt.min, reverse=True)
    return runs[:limit]


def _run_full_sync_for_ror(
    ror_id: str,
    base_url: str,
    headers: dict,
    job_id: str | None = None,
) -> dict:
    """Refresh ORCID data and every eligible OpenAlex article for one institution."""
    from ..models import WorkCacheRun, FundingCacheRun
    from ..services.cache_service import build_full_cache_for_ror
    from ..services.openalex_service import sync_openalex_works
    from ..services.background_jobs import update_job_progress, update_job_step

    result = {
        "ror_id": ror_id,
        "researchers": 0,
        "works": 0,
        "fundings": 0,
        "profiles": 0,
        "openalex": {
            "works_seen": 0,
            "fetched_count": 0,
            "matched_count": 0,
            "not_found_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "status": "pending",
            "error": None,
        },
        "errors": [],
    }

    started_at = dt.now(timezone.utc).replace(tzinfo=None)
    run_w = WorkCacheRun(ror_id=ror_id, status='running', started_at=started_at)
    run_f = FundingCacheRun(ror_id=ror_id, status='running', started_at=started_at)
    db.session.add(run_w)
    db.session.add(run_f)
    db.session.commit()
    for step_name in ("researchers", "profiles", "works", "fundings", "canonical_works"):
        update_job_step(job_id, step_name, "running" if step_name == "researchers" else "pending")
    try:
        cache_result = build_full_cache_for_ror(ror_id, base_url, headers)
        result.update(cache_result)
        run_w.status = 'success'
        run_w.rows_count = result["works"]
        run_f.status = 'success'
        run_f.rows_count = result["fundings"]
        update_job_step(job_id, "researchers", "success", records_count=result["researchers"])
        update_job_step(job_id, "profiles", "success", records_count=result["profiles"])
        update_job_step(job_id, "works", "success", records_count=result["works"])
        update_job_step(job_id, "fundings", "success", records_count=result["fundings"])
        update_job_step(
            job_id,
            "canonical_works",
            "success",
            records_count=result.get("unique_works", 0),
        )
    except Exception as exc:
        db.session.rollback()
        logger.exception("Full metadata sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Metadata")
        run_w.status = 'failed'
        run_w.error = str(exc)
        run_f.status = 'failed'
        run_f.error = str(exc)
        for step_name in ("researchers", "profiles", "works", "fundings", "canonical_works"):
            update_job_step(job_id, step_name, "failed", error=str(exc))
    finally:
        finished_at = dt.now(timezone.utc).replace(tzinfo=None)
        run_w.finished_at = finished_at
        run_f.finished_at = finished_at
        db.session.add(run_w)
        db.session.add(run_f)
        db.session.commit()

    update_job_step(job_id, "openalex", "running")
    try:
        sync_kwargs = {
            "ror_id": ror_id,
            "force_refresh": True,
            "stale_days": 0,
            "articles_only": True,
        }
        if job_id:
            sync_kwargs["progress"] = lambda summary: update_job_progress(
                job_id,
                summary["fetched_count"] + summary["skipped_count"],
                summary["dois_found"],
                "candidates",
                message=(
                    f"OpenAlex: {summary['fetched_count'] + summary['skipped_count']} "
                    f"of {summary['dois_found']} candidates processed."
                ),
            )
        openalex_result = sync_openalex_works(
            **sync_kwargs,
        )
        result["openalex"].update(openalex_result)
        if openalex_result.get("status") in {"failed", "partial"}:
            result["errors"].append("OpenAlex")
            update_job_step(
                job_id,
                "openalex",
                "failed",
                records_count=openalex_result.get("matched_count", 0),
                error=openalex_result.get("error") or "Some OpenAlex records could not be synchronized.",
            )
        else:
            update_job_step(
                job_id,
                "openalex",
                "success",
                records_count=openalex_result.get("matched_count", 0),
            )
    except Exception as exc:
        db.session.rollback()
        logger.exception("OpenAlex sync failed during full refresh for ROR %s: %s", ror_id, exc)
        result["openalex"].update({"status": "failed", "error": str(exc)})
        result["errors"].append("OpenAlex")
        update_job_step(job_id, "openalex", "failed", error=str(exc))

    return result


@bp_works.route('/cache/full/build', methods=['POST'])
@login_required
def cache_full_build():
    """
    Discover researchers through every verified institution identifier and use
    one profile download pass to rebuild researcher, work, funding, and status
    metadata before refreshing every eligible OpenAlex article.
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

    from ..services.background_jobs import submit_background_job

    job_id = submit_background_job(
        current_app._get_current_object(),
        f"full-cache-{ror_id}",
        _run_full_sync_for_ror,
        ror_id,
        base_url,
        headers,
        job_type="full_institution_sync",
        ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
        steps=["researchers", "profiles", "works", "fundings", "canonical_works", "openalex"],
    )
    flash_ok(_(
        'Full synchronization started in the background. Job ID: %(job)s',
        job=job_id,
    ))

    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/cache/full/build-all', methods=['POST'])
@login_required
def cache_full_build_all():
    """Queue complete ORCID and OpenAlex synchronization for every institution."""
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

    from ..services.background_jobs import submit_background_job

    steps = [
        f"institution:{institution['ror_id']}"
        for institution in institutions
        if institution.get("ror_id")
    ]
    job_id = submit_background_job(
        current_app._get_current_object(),
        "full-cache-all-institutions",
        _run_all_institution_sync,
        institutions,
        base_url,
        headers,
        job_type="full_system_sync",
        requested_by_user_id=session.get("user_id"),
        steps=steps,
    )
    flash_ok(_(
        "All-institution synchronization started in the background for %(count)s institutions. Job ID: %(job)s",
        count=len(steps),
        job=job_id,
    ))

    return redirect(url_for('works.cache_works_status'))


def _run_all_institution_sync(
    institutions: list[dict],
    base_url: str,
    headers: dict,
    job_id: str | None = None,
) -> dict:
    """Run every institution sequentially while persisting per-scope progress."""
    from ..services.background_jobs import update_job_progress, update_job_step

    totals = {
        "institutions": 0,
        "researchers": 0,
        "works": 0,
        "fundings": 0,
        "profiles": 0,
        "openalex": 0,
        "failed": 0,
    }
    valid_institutions = [institution for institution in institutions if institution.get("ror_id")]
    update_job_progress(job_id, 0, len(valid_institutions), "institutions")
    for index, institution in enumerate(valid_institutions, start=1):
        ror_id = institution.get("ror_id")
        step_name = f"institution:{ror_id}"
        update_job_step(job_id, step_name, "running")
        try:
            result = _run_full_sync_for_ror(ror_id, base_url, headers)
            totals["institutions"] += 1
            totals["researchers"] += result["researchers"]
            totals["works"] += result["works"]
            totals["fundings"] += result["fundings"]
            totals["profiles"] += result["profiles"]
            totals["openalex"] += result["openalex"]["matched_count"]
            if result["errors"]:
                totals["failed"] += 1
                update_job_step(
                    job_id,
                    step_name,
                    "failed",
                    error=", ".join(result["errors"]),
                )
            else:
                update_job_step(
                    job_id,
                    step_name,
                    "success",
                    records_count=result["researchers"],
                )
        except Exception as exc:
            db.session.rollback()
            totals["failed"] += 1
            update_job_step(job_id, step_name, "failed", error=str(exc))
        finally:
            update_job_progress(
                job_id,
                index,
                len(valid_institutions),
                "institutions",
                message=f"Processed institution {index} of {len(valid_institutions)}.",
            )
    return totals


@bp_works.route('/cache/staff/institution/<ror_id>/build', methods=['POST'])
@staff_required
def cache_staff_institution_build(ror_id: str):
    """Queue a full cache refresh for one institution from the staff overview."""
    from ..services.background_jobs import submit_background_job
    from ..services.institution_registry_service import get_institution_by_ror

    ror_id = normalize_ror_id(ror_id)
    institution = get_institution_by_ror(ror_id) if ror_id else None
    if not institution:
        flash_err(_('Institution not found.'))
        return redirect(url_for('works.cache_works_status', scope='system'))

    base_url = current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/')
    headers = {'Accept': 'application/json'}
    job_id = submit_background_job(
        current_app._get_current_object(),
        f"full-cache-{ror_id}",
        _run_full_sync_for_ror,
        ror_id,
        base_url,
        headers,
        job_type="full_institution_sync",
        ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
        steps=["researchers", "profiles", "works", "fundings", "canonical_works", "openalex"],
    )
    flash_ok(_(
        'Full cache refresh started for %(institution)s. Researchers, profiles, works, funding, and OpenAlex will be synchronized in the background. Job ID: %(job)s',
        institution=institution.get('name') or ror_id,
        job=job_id,
    ))
    return redirect(url_for('works.cache_works_status', scope='system'))


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
    
    run = FundingCacheRun(ror_id=ror_id, status='running', started_at=utc_now())
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
        run.finished_at = utc_now()
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
    from ..models import FundingCache, WorkCache

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    is_admin = bool(session.get('is_admin'))
    is_manager = bool(session.get('is_manager'))
    is_staff = is_admin or is_manager
    cache_scope = "system" if is_staff and request.args.get("scope") == "system" else "institution"
    institution_summaries = []
    institution_pagination = _pagination_dict(1, 10, 0)
    institution_query = (request.args.get("institution_q") or "").strip()
    institution_sort = (request.args.get("institution_sort") or "name").strip().lower()
    institution_direction = (request.args.get("institution_dir") or "asc").strip().lower()
    if institution_sort not in {"name", "researchers", "works", "fundings", "openalex", "updated"}:
        institution_sort = "name"
    if institution_direction not in {"asc", "desc"}:
        institution_direction = "asc"

    current_works_count = db.session.query(func.count(WorkCache.id)).filter_by(ror_id=ror_id).scalar() or 0
    current_fundings_count = db.session.query(func.count(FundingCache.id)).filter_by(ror_id=ror_id).scalar() or 0
    p_count = _researcher_count(ror_id)

    if cache_scope == "system":
        from ..services.institution_registry_service import get_institution_options

        institution_options = get_institution_options()
        if is_admin:
            # Reuse the exact option list rendered by the top institution selector.
            g.institution_options = institution_options
        all_summaries = _institution_cache_summaries(institution_options)
        if institution_query:
            normalized_query = institution_query.casefold()
            all_summaries = [
                item for item in all_summaries
                if normalized_query in f"{item['name']} {item['ror_id']}".casefold()
            ]

        sort_keys = {
            "name": lambda item: item["name"].casefold(),
            "researchers": lambda item: item["researchers"],
            "works": lambda item: item["works"],
            "fundings": lambda item: item["fundings"],
            "openalex": lambda item: item["openalex_percent"],
            "updated": lambda item: item["last_update"] or dt.min,
        }
        all_summaries.sort(
            key=sort_keys[institution_sort],
            reverse=institution_direction == "desc",
        )
        try:
            institution_page = max(int(request.args.get("institution_page", 1)), 1)
        except (TypeError, ValueError):
            institution_page = 1
        try:
            institution_per_page = int(request.args.get("institution_per_page", 10))
        except (TypeError, ValueError):
            institution_per_page = 10
        if institution_per_page not in {10, 25, 50}:
            institution_per_page = 10
        institution_pagination = _pagination_dict(
            institution_page,
            institution_per_page,
            len(all_summaries),
        )
        start = (institution_pagination["page"] - 1) * institution_pagination["per_page"]
        end = start + institution_pagination["per_page"]
        institution_summaries = all_summaries[start:end]

    w_count = int(current_works_count or 0)
    f_count = int(current_fundings_count or 0)
    last_run_works = _last_cache_run_works(ror_id)
    last_run_fundings = _last_cache_run_fundings(ror_id)
    admin_works_count = (
        db.session.query(func.count(WorkCache.id)).scalar() or 0
        if is_admin
        else 0
    )
    openalex_summary = _openalex_cache_summary(ror_id)
    recent_runs = _recent_sync_runs(
        ror_id,
        include_system=bool(is_admin and cache_scope == "system"),
    )

    from ..services.canonical_work_service import canonical_work_counts
    from ..services.data_health_service import institution_data_health

    cache_health = institution_data_health(ror_id)
    canonical_summary = canonical_work_counts(ror_id)
    from ..services.institution_registry_service import get_institution_by_ror

    active_institution = get_institution_by_ror(ror_id)
    active_institution_name = (
        (active_institution or {}).get("name")
        or session.get("institution_name")
        or ror_id
    )

    def cache_status_url(**updates):
        params = request.args.to_dict(flat=False)
        for key, value in updates.items():
            if value is None or value == "" or value == []:
                params.pop(key, None)
            else:
                params[key] = value
        return url_for("works.cache_works_status", **params)

    return render_template(
        'works/cache_status.html',
        has_cache_works=(w_count > 0),
        last_run_works=last_run_works,
        count_works=w_count,
        has_cache_fundings=(f_count > 0),
        last_run_fundings=last_run_fundings,
        count_fundings=f_count,
        count_profiles=p_count,
        admin_works_count=admin_works_count,
        openalex_summary=openalex_summary,
        recent_runs=recent_runs,
        institution_summaries=institution_summaries,
        institution_pagination=institution_pagination,
        institution_query=institution_query,
        institution_sort=institution_sort,
        institution_direction=institution_direction,
        cache_scope=cache_scope,
        cache_status_url=cache_status_url,
        cache_health=cache_health,
        canonical_summary=canonical_summary,
        active_ror_id=ror_id,
        active_institution_name=active_institution_name,
    )


@bp_works.route('/data-quality')
@login_required
def data_quality():
    """Show provenance, completeness, canonical output, and safe cross-module signals."""
    from ..services.data_quality_service import institution_quality_report

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))
    section = (request.args.get("section") or "overview").strip().lower()
    if section not in {"overview", "researchers", "funding", "integrity"}:
        section = "overview"
    return render_template(
        'works/data_quality.html',
        report=institution_quality_report(ror_id),
        section=section,
        can_manage=bool(session.get('is_admin') or session.get('is_manager')),
    )


@bp_works.route('/data-quality/backfill-associations', methods=['POST'])
@staff_required
def data_quality_backfill_associations():
    """Queue a traceable cache-derived institution/researcher backfill."""
    from ..services.background_jobs import submit_background_job
    from ..services.data_trust_service import backfill_inferred_associations

    ror_id = get_active_ror_id()
    scope_ror = None if session.get('is_admin') and request.form.get('scope') == 'all' else ror_id
    job_id = submit_background_job(
        current_app._get_current_object(),
        f"association-backfill-{scope_ror or 'all'}",
        backfill_inferred_associations,
        scope_ror,
        job_type="association_backfill",
        ror_id=scope_ror,
        requested_by_user_id=session.get("user_id"),
        steps=["association_backfill"],
    )
    flash_ok(_('Researcher relationship backfill started. Job ID: %(job)s', job=job_id))
    return redirect(url_for('works.data_quality'))


@bp_works.route('/data-quality/rebuild-canonical-works', methods=['POST'])
@staff_required
def data_quality_rebuild_canonical_works():
    """Queue canonical work reconstruction for the active or global scope."""
    from ..services.background_jobs import submit_background_job
    from ..services.canonical_work_service import rebuild_canonical_works

    ror_id = get_active_ror_id()
    scope_ror = None if session.get('is_admin') and request.form.get('scope') == 'all' else ror_id
    job_id = submit_background_job(
        current_app._get_current_object(),
        f"canonical-work-rebuild-{scope_ror or 'all'}",
        rebuild_canonical_works,
        scope_ror,
        job_type="canonical_work_rebuild",
        ror_id=scope_ror,
        requested_by_user_id=session.get("user_id"),
        steps=["canonical_works"],
    )
    flash_ok(_('Canonical work rebuild started. Job ID: %(job)s', job=job_id))
    return redirect(url_for('works.data_quality'))


def _run_openalex_sync(
    ror_id: str | None,
    mode: str,
    job_id: str | None = None,
) -> dict:
    """Run one durable OpenAlex synchronization job and report partial results."""
    from ..services.background_jobs import update_job_progress, update_job_step
    from ..services.openalex_service import sync_openalex_title_matches, sync_openalex_works

    update_job_step(job_id, "openalex", "running")

    def progress(summary: dict) -> None:
        if not job_id:
            return
        processed = summary["fetched_count"] + summary["skipped_count"]
        update_job_progress(
            job_id,
            processed,
            summary["dois_found"],
            "candidates",
            message=f"OpenAlex: {processed} of {summary['dois_found']} candidates processed.",
        )

    if mode == "title":
        result = sync_openalex_title_matches(
            ror_id=ror_id,
            stale_days=0,
            articles_only=True,
            progress=progress,
        )
    else:
        result = sync_openalex_works(
            ror_id=ror_id,
            force_refresh=mode == "all",
            stale_days=0,
            articles_only=True,
            progress=progress,
        )

    records_count = result.get("matched_count", 0)
    if result.get("status") == "failed":
        error = result.get("error") or "OpenAlex synchronization failed."
        update_job_step(job_id, "openalex", "failed", records_count=records_count, error=error)
        raise RuntimeError(error)
    if result.get("status") == "partial":
        error = result.get("error") or "Some OpenAlex records could not be synchronized."
        update_job_step(job_id, "openalex", "failed", records_count=records_count, error=error)
        return {"openalex": result, "errors": [error]}

    update_job_step(job_id, "openalex", "success", records_count=records_count)
    return {"openalex": result, "errors": []}


def _queue_openalex_sync(ror_id: str | None, mode: str) -> str:
    """Queue or reuse the same active OpenAlex synchronization scope."""
    from ..services.background_jobs import submit_background_job

    scope = ror_id or "system"
    return submit_background_job(
        current_app._get_current_object(),
        f"openalex-{scope}-{mode}",
        _run_openalex_sync,
        ror_id,
        mode,
        job_type="openalex_system_sync" if ror_id is None else "openalex_institution_sync",
        ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
        steps=["openalex"],
        deduplicate=True,
    )


@bp_works.route('/openalex/sync', methods=['POST'])
@login_required
def openalex_sync():
    """Queue OpenAlex metadata synchronization for the active institution."""
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

    job_id = _queue_openalex_sync(ror_id, mode)
    flash_ok(_(
        'OpenAlex synchronization was queued or is already running. Job ID: %(job)s',
        job=job_id,
    ))

    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/openalex/sync-system', methods=['POST'])
@admin_required
def openalex_sync_system():
    """Queue system-wide OpenAlex metadata synchronization."""
    mode = (request.form.get("mode") or "missing").strip().lower()
    if mode not in {"missing", "all", "title"}:
        flash_err(_('Invalid OpenAlex synchronization mode.'))
        return redirect(url_for('works.cache_works_status'))

    job_id = _queue_openalex_sync(None, mode)
    flash_ok(_(
        'System-wide OpenAlex synchronization was queued or is already running. Job ID: %(job)s',
        job=job_id,
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
    if coverage not in {"all", "enriched", "missing", "not_found", "no_doi"}:
        coverage = "all"

    page, per_page = _page_params(default_per_page=10)
    search = request.args.get("q", "").strip()
    sort = request.args.get("sort", "citations")
    if sort not in {"title", "year", "citations", "open_access", "source", "status"}:
        sort = "citations"
    direction = request.args.get("dir", "desc").lower()
    if direction not in {"asc", "desc"}:
        direction = "desc"
    rows, summary, pagination = _openalex_work_rows(
        ror_id,
        coverage=coverage,
        page=page,
        per_page=per_page,
        search=search,
        sort=sort,
        direction=direction,
    )
    return render_template(
        'works/openalex_works.html',
        rows=rows,
        summary=summary,
        pagination=pagination,
        coverage=coverage,
        query=search,
        sort=sort,
        direction=direction,
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
    if coverage not in {"all", "enriched", "missing", "not_found", "no_doi"}:
        coverage = "all"

    search = request.args.get("q", "").strip()
    sort = request.args.get("sort", "citations")
    direction = request.args.get("dir", "desc").lower()
    rows, _summary, _pagination = _openalex_work_rows(
        ror_id,
        coverage=coverage,
        page=1,
        per_page=100000,
        search=search,
        sort=sort,
        direction=direction,
    )
    data_frame = pd.DataFrame([{
        "title": plain_text(row["title"]),
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
    section = request.args.get("section", "overview")
    if section not in {"overview", "collaboration", "topics", "impact"}:
        section = "overview"
    analytics = _openalex_institution_analytics_with_cache(ror_id, analytics_filters)

    def query_url(**updates):
        params = request.args.to_dict(flat=False)
        params.update({key: value for key, value in updates.items() if value is not None})
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_analytics", **params)

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
        section=section,
        query_url=query_url,
        has_active_filters=bool(
            analytics_filters["year_from"] or analytics_filters["year_to"]
            or analytics_filters["type"] or analytics_filters["oa_status"]
            or analytics_filters["affiliation"]
        ),
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
            "title": plain_text(row["title"]),
            "openalex_id": row["openalex_id"],
            "publication_year": row["publication_year"],
            "type": row["type"],
            "source": row["source_name"],
            "citations": row["cited_by_count"],
            "fwci": row["fwci"],
            "is_open_access": row["is_oa"],
            "oa_status": row["oa_status"],
            "topic_field": row["primary_topic_field"],
            "topic_domain": row["primary_topic_domain"],
        } for row in analytics["top_cited"]]
        sheet_name = "Top cited"
    elif table_key == "authors":
        rows = [{
            "author": row["author_name"],
            "author_id": row["author_id"],
            "orcid": row["orcid"],
            "has_chile_affiliation": row["has_chile_affiliation"],
            "articles": row["works_count"],
        } for row in analytics["top_authors"]]
        sheet_name = "Authors"
    elif table_key == "institutions":
        rows = [{
            "institution": row["institution_name"],
            "ror_id": row["ror_id"],
            "country": row["country_code"],
            "articles": row["works_count"],
            "author_links": row["author_links"],
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
        has_active_filters=bool(
            analytics_filters["year_from"] or analytics_filters["year_to"]
            or analytics_filters["type"] or analytics_filters["oa_status"]
        ),
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


@bp_works.route('/download/researchers/cache')
@login_required
def download_researchers_cache():
    """Export all active researcher associations for the current institution."""
    ror_id = get_active_ror_id()
    data_frame = _build_researchers_dataframe(ror_id)
    if data_frame.empty:
        flash_err(_('No researcher association data available.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        return _send_dataframe_export(
            data_frame,
            f'orcid_researchers_{ror_id}',
            'Researchers',
        )
    except Exception as exc:
        logger.exception("RESEARCHERS EXPORT ERROR FOR ROR %s: %s", ror_id, exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/staff/institutions/cache-summary')
@staff_required
def download_institution_cache_summary():
    """Export the staff-facing cache summary for every known institution."""
    summaries = _institution_cache_summaries()
    if not summaries:
        flash_err(_('No institutional cache summary is available.'))
        return redirect(url_for('works.cache_works_status'))

    rows = [{
        "institution": item["name"],
        "ror_id": item["ror_id"],
        "researchers": item["researchers"],
        "works": item["works"],
        "fundings": item["fundings"],
        "openalex_matched": item["openalex_matched"],
        "openalex_candidates": item["openalex_candidates"],
        "openalex_coverage_percent": item["openalex_percent"],
        "last_update": _format_datetime(item["last_update"]),
        "cache_status": item["health"],
    } for item in summaries]
    return _send_dataframe_export(
        pd.DataFrame(rows),
        'institution_cache_summary',
        'Institution summary',
    )


@bp_works.route('/download/staff/institution/<ror_id>/<dataset_key>')
@staff_required
def download_staff_institution_cache(ror_id: str, dataset_key: str):
    """Export one institutional dataset without changing the active context."""
    from ..models import (
        FundingCache,
        OpenAlexWorkMetadata,
        OpenAlexWorkRawCache,
        WorkCache,
    )
    from ..services.institution_registry_service import get_institution_by_ror

    ror_id = normalize_ror_id(ror_id)
    if not ror_id or not get_institution_by_ror(ror_id):
        flash_err(_('Institution not found.'))
        return redirect(url_for('works.cache_works_status'))

    if dataset_key == 'works':
        records = WorkCache.query.filter_by(ror_id=ror_id).all()
        if not records:
            flash_err(_('The publication cache is currently empty.'))
            return redirect(url_for('works.cache_works_status'))
        return _send_dataframe_export(
            _build_works_dataframe(records),
            f'orcid_works_cache_{ror_id}',
            'Works',
        )

    if dataset_key == 'fundings':
        records = FundingCache.query.filter_by(ror_id=ror_id).all()
        if not records:
            flash_err(_('The funding cache is empty.'))
            return redirect(url_for('works.cache_works_status'))
        return _send_dataframe_export(
            _build_fundings_dataframe(records),
            f'orcid_fundings_cache_{ror_id}',
            'Fundings',
        )

    if dataset_key == 'researchers':
        data_frame = _build_researchers_dataframe(ror_id)
        if data_frame.empty:
            flash_err(_('No researcher association data available.'))
            return redirect(url_for('works.cache_works_status'))
        return _send_dataframe_export(
            data_frame,
            f'orcid_researchers_{ror_id}',
            'Researchers',
        )

    if dataset_key == 'openalex':
        cache_key = _openalex_cache_key_expr(WorkCache).label('openalex_cache_key')
        cache_keys = (
            db.session.query(cache_key)
            .filter(
                WorkCache.ror_id == ror_id,
                WorkCache.type == 'journal-article',
            )
            .distinct()
            .subquery()
        )
        records_query = (
            db.session.query(OpenAlexWorkRawCache, OpenAlexWorkMetadata)
            .join(
                cache_keys,
                OpenAlexWorkRawCache.doi_normalized == cache_keys.c.openalex_cache_key,
            )
            .outerjoin(
                OpenAlexWorkMetadata,
                OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkRawCache.doi_normalized,
            )
            .order_by(OpenAlexWorkRawCache.doi_normalized.asc())
        )
        if records_query.first() is None:
            flash_err(_('No OpenAlex cache data available.'))
            return redirect(url_for('works.cache_works_status'))
        return _send_openalex_export(
            records_query,
            f'openalex_articles_{ror_id}',
        )

    flash_err(_('Invalid dataset type.'))
    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/researchers/cache')
@staff_required
def download_all_researchers_admin():
    """Export every institution-researcher pair for staff users."""
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
        logger.exception("STAFF RESEARCHERS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-works/cache')
@staff_required
def download_all_works_admin():
    """Export cached works for all institutions to staff users."""
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
        logger.exception("STAFF WORKS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-fundings/cache')
@staff_required
def download_all_fundings_admin():
    """Export cached fundings for all institutions to staff users."""
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
        logger.exception("STAFF FUNDINGS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/openalex/cache')
@staff_required
def download_openalex_admin():
    """Export cached OpenAlex work metadata for all institutions to staff users."""
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
        logger.exception("STAFF OPENALEX EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))
