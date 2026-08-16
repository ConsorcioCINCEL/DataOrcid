"""Work/funding cache management views and exports."""

import copy
import hashlib
import json
import logging
import os
import tempfile
import time
from collections import OrderedDict
from datetime import datetime as dt, timezone
from pathlib import Path
from threading import RLock

from flask import current_app, request, session
from sqlalchemy import func

from .. import db
logger = logging.getLogger(__name__)
from .works_state import (
    _OPENALEX_ANALYTICS_CACHE_MAX_SIZE,
    _OPENALEX_ANALYTICS_CACHE_TTL,
    _OPENALEX_GLOBAL_ANALYTICS_CACHE,
    _OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK,
    _OPENALEX_INSTITUTION_ANALYTICS_CACHE,
    _OPENALEX_INSTITUTION_ANALYTICS_CACHE_LOCK,
    _OPENALEX_PERSISTENT_CACHE_MAX_FILES,
    _OPENALEX_PERSISTENT_CACHE_VERSION,
)
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
    from ..services.analytics_service import get_analytics_data_version

    request_prefixes = ()
    if namespace == "institution" and filters.get("section") == "open_access":
        request_prefixes = ("priority_source_", "priority_article_")
    elif namespace == "global":
        requested_tab = (request.args.get("tab") or "overview").strip().lower()
        request_prefixes = {
            "open_access": (
                "priority_source_",
                "priority_article_",
                "priority_institution_",
            ),
            "universities": ("university_",),
            "production": ("author_", "institution_"),
            "institution_authors": ("institution_author_",),
            "articles": ("priority_article_",),
        }.get(requested_tab, ())
    request_args = {
        key: request.args.getlist(key)
        for key in sorted(request.args.keys())
        if request_prefixes and key.startswith(request_prefixes)
    }
    data_version = get_analytics_data_version(ror_id)
    payload = {
        "namespace": namespace,
        "ror_id": ror_id,
        "filters": filters,
        "request_args": request_args,
        "locale": session.get("locale") or current_app.config.get("BABEL_DEFAULT_LOCALE", "en"),
        "data_signature": data_version or _openalex_data_signature(ror_id),
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
    from .works_institution_analytics import _openalex_analytics

    return _openalex_analytics_with_cache(
        "institution",
        filters,
        lambda current_filters: _openalex_analytics(ror_id, current_filters),
        _OPENALEX_INSTITUTION_ANALYTICS_CACHE,
        _OPENALEX_INSTITUTION_ANALYTICS_CACHE_LOCK,
        ror_id=ror_id,
    )


def _openalex_global_analytics_with_cache(filters: dict) -> dict:
    from .works_global_analytics import _openalex_global_analytics

    requested_tab = (filters.get("tab") or "overview").strip().lower()
    if requested_tab not in {
        "overview",
        "open_access",
        "universities",
        "production",
        "institution_authors",
        "articles",
    }:
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
