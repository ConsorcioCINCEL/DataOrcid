"""OpenAlex API client and DOI-based enrichment cache helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import re
from urllib.parse import quote

import requests
from flask import current_app
from sqlalchemy import func

from .. import db
from ..models import (
    OpenAlexSyncRun,
    OpenAlexWorkMetadata,
    OpenAlexWorkRawCache,
    WorkCache,
)

logger = logging.getLogger(__name__)


class OpenAlexConfigError(RuntimeError):
    """Raised when OpenAlex is not configured for API calls."""


def normalize_doi(value: str | None) -> str:
    """Normalize DOI values so ORCID and OpenAlex rows can be joined safely."""
    doi = (value or "").strip().lower()
    if not doi:
        return ""

    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi)
    return doi.strip().rstrip(".")


def _parse_openalex_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


class OpenAlexClient:
    """Small OpenAlex client focused on work lookups by DOI."""

    def __init__(
        self,
        api_key: str | None,
        base_url: str = "https://api.openalex.org",
        timeout: int = 20,
        mailto: str | None = None,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.base_url = (base_url or "https://api.openalex.org").rstrip("/")
        self.timeout = int(timeout or 20)
        self.mailto = (mailto or "").strip() or None

        if not self.api_key or self.api_key == "REPLACE_OR_USE_ENV":
            raise OpenAlexConfigError("OPENALEX_API_KEY is not configured.")

    @classmethod
    def from_current_app(cls) -> "OpenAlexClient":
        return cls(
            api_key=current_app.config.get("OPENALEX_API_KEY"),
            base_url=current_app.config.get("OPENALEX_BASE_URL", "https://api.openalex.org"),
            timeout=current_app.config.get("OPENALEX_TIMEOUT", 20),
            mailto=current_app.config.get("OPENALEX_MAILTO"),
        )

    def fetch_work_by_doi(self, doi: str) -> dict:
        """Fetch one OpenAlex work by DOI and return status metadata plus JSON."""
        normalized_doi = normalize_doi(doi)
        if not normalized_doi:
            return {
                "status": "error",
                "http_status": None,
                "payload": None,
                "error": "Missing DOI.",
            }

        work_id = quote(f"doi:{normalized_doi}", safe=":")
        params = {"api_key": self.api_key}
        if self.mailto:
            params["mailto"] = self.mailto

        try:
            response = requests.get(
                f"{self.base_url}/works/{work_id}",
                params=params,
                timeout=self.timeout,
                headers={"Accept": "application/json"},
            )
        except requests.RequestException as exc:
            return {
                "status": "error",
                "http_status": None,
                "payload": None,
                "error": str(exc),
            }

        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                return {
                    "status": "error",
                    "http_status": response.status_code,
                    "payload": None,
                    "error": f"Invalid OpenAlex JSON response: {exc}",
                }
            return {
                "status": "found",
                "http_status": response.status_code,
                "payload": payload,
                "error": None,
            }
        if response.status_code == 404:
            return {
                "status": "not_found",
                "http_status": response.status_code,
                "payload": None,
                "error": None,
            }

        return {
            "status": "error",
            "http_status": response.status_code,
            "payload": None,
            "error": _response_error(response),
        }


def _response_error(response: requests.Response) -> str:
    if response.status_code == 429:
        return "OpenAlex rate limit reached."
    body = (response.text or "").strip()
    return body[:500] if body else f"OpenAlex HTTP {response.status_code}"


def extract_work_metadata(payload: dict, doi: str) -> dict:
    """Map a raw OpenAlex work payload to the local queryable metadata shape."""
    primary_location = payload.get("primary_location") or {}
    best_oa_location = payload.get("best_oa_location") or {}
    open_access = payload.get("open_access") or {}
    source = (primary_location.get("source") or best_oa_location.get("source") or {})
    primary_topic = payload.get("primary_topic") or {}
    field = primary_topic.get("field") or {}
    domain = primary_topic.get("domain") or {}

    return {
        "doi_normalized": normalize_doi(doi),
        "openalex_id": _openalex_short_id(payload.get("id")),
        "title": payload.get("title") or payload.get("display_name"),
        "publication_year": payload.get("publication_year"),
        "publication_date": payload.get("publication_date"),
        "type": payload.get("type"),
        "language": payload.get("language"),
        "cited_by_count": int(payload.get("cited_by_count") or 0),
        "fwci": payload.get("fwci"),
        "is_retracted": bool(payload.get("is_retracted")),
        "is_oa": bool(open_access.get("is_oa")),
        "oa_status": open_access.get("oa_status"),
        "oa_url": open_access.get("oa_url"),
        "best_pdf_url": best_oa_location.get("pdf_url"),
        "source_name": source.get("display_name"),
        "source_issn_l": source.get("issn_l"),
        "source_type": source.get("type"),
        "source_is_in_doaj": source.get("is_in_doaj"),
        "primary_topic_name": primary_topic.get("display_name"),
        "primary_topic_field": field.get("display_name"),
        "primary_topic_domain": domain.get("display_name"),
        "raw_updated_date": _parse_openalex_datetime(payload.get("updated_date")),
        "fetched_at": datetime.utcnow(),
    }


def _openalex_short_id(value: str | None) -> str | None:
    if not value:
        return None
    return str(value).rstrip("/").split("/")[-1]


def should_refresh_raw(raw_row: OpenAlexWorkRawCache | None, stale_days: int, force_refresh: bool = False) -> bool:
    if force_refresh or not raw_row:
        return True
    if stale_days <= 0:
        return False
    if not raw_row.fetched_at:
        return True
    return raw_row.fetched_at < datetime.utcnow() - timedelta(days=stale_days)


def sync_work_by_doi(
    doi: str,
    client: OpenAlexClient | None = None,
    force_refresh: bool = False,
    stale_days: int | None = None,
) -> dict:
    """Fetch, persist raw OpenAlex data, and refresh derived metadata for one DOI."""
    normalized_doi = normalize_doi(doi)
    if not normalized_doi:
        return {"status": "error", "doi": doi, "error": "Missing DOI."}

    stale_days = current_app.config.get("OPENALEX_STALE_DAYS", 30) if stale_days is None else stale_days
    raw_row = OpenAlexWorkRawCache.query.filter_by(doi_normalized=normalized_doi).first()
    if not should_refresh_raw(raw_row, int(stale_days or 0), force_refresh):
        return {"status": "skipped", "doi": normalized_doi, "raw_status": raw_row.status}

    client = client or OpenAlexClient.from_current_app()
    result = client.fetch_work_by_doi(normalized_doi)
    now = datetime.utcnow()

    if not raw_row:
        raw_row = OpenAlexWorkRawCache(doi_normalized=normalized_doi)
        db.session.add(raw_row)

    raw_row.source_doi = doi
    raw_row.status = result["status"]
    raw_row.http_status = result.get("http_status")
    raw_row.raw_json = result.get("payload")
    raw_row.error = result.get("error")
    raw_row.fetched_at = now

    payload = result.get("payload")
    if payload:
        raw_row.openalex_id = _openalex_short_id(payload.get("id"))
        raw_row.oa_updated_date = _parse_openalex_datetime(payload.get("updated_date"))
        _upsert_metadata(extract_work_metadata(payload, normalized_doi))
    elif result["status"] == "not_found":
        OpenAlexWorkMetadata.query.filter_by(doi_normalized=normalized_doi).delete(synchronize_session=False)

    db.session.commit()
    return {"status": result["status"], "doi": normalized_doi, "error": result.get("error")}


def _upsert_metadata(values: dict) -> OpenAlexWorkMetadata:
    metadata = OpenAlexWorkMetadata.query.filter_by(doi_normalized=values["doi_normalized"]).first()
    if not metadata:
        metadata = OpenAlexWorkMetadata(doi_normalized=values["doi_normalized"])
        db.session.add(metadata)

    for key, value in values.items():
        setattr(metadata, key, value)
    return metadata


def collect_work_dois(ror_id: str | None = None, articles_only: bool = True) -> tuple[int, list[dict]]:
    """Return candidate DOI values from the local ORCID works cache."""
    base_query = WorkCache.query
    if ror_id:
        base_query = base_query.filter(WorkCache.ror_id == ror_id)
    if articles_only:
        base_query = base_query.filter(func.lower(WorkCache.type) == "journal-article")

    works_seen = base_query.count()
    doi_rows = (
        base_query
        .with_entities(WorkCache.doi)
        .filter(WorkCache.doi.isnot(None), WorkCache.doi != "")
        .distinct()
        .all()
    )

    by_normalized = {}
    for (source_doi,) in doi_rows:
        normalized = normalize_doi(source_doi)
        if normalized and normalized not in by_normalized:
            by_normalized[normalized] = {
                "doi": normalized,
                "source_doi": source_doi,
            }

    return works_seen, sorted(by_normalized.values(), key=lambda item: item["doi"])


def sync_openalex_works(
    ror_id: str | None = None,
    limit: int | None = None,
    force_refresh: bool = False,
    stale_days: int | None = None,
    articles_only: bool = True,
    dry_run: bool = False,
) -> dict:
    """Synchronize OpenAlex metadata for DOI-backed works in the local ORCID cache."""
    works_seen, candidates = collect_work_dois(ror_id=ror_id, articles_only=articles_only)
    if limit:
        candidates = candidates[:limit]

    summary = {
        "ror_id": ror_id,
        "works_seen": works_seen,
        "dois_found": len(candidates),
        "fetched_count": 0,
        "matched_count": 0,
        "not_found_count": 0,
        "error_count": 0,
        "skipped_count": 0,
        "status": "dry_run" if dry_run else "success",
        "error": None,
    }
    if dry_run:
        return summary

    run = OpenAlexSyncRun(ror_id=ror_id, works_seen=works_seen, dois_found=len(candidates))
    db.session.add(run)
    db.session.commit()

    try:
        client = OpenAlexClient.from_current_app()
        for candidate in candidates:
            result = sync_work_by_doi(
                candidate["source_doi"],
                client=client,
                force_refresh=force_refresh,
                stale_days=stale_days,
            )
            if result["status"] == "skipped":
                summary["skipped_count"] += 1
            else:
                summary["fetched_count"] += 1

            if result["status"] == "found":
                summary["matched_count"] += 1
            elif result["status"] == "not_found":
                summary["not_found_count"] += 1
            elif result["status"] == "error":
                summary["error_count"] += 1

        summary["status"] = "failed" if summary["error_count"] else "success"
    except Exception as exc:
        db.session.rollback()
        logger.exception("OpenAlex synchronization failed: %s", exc)
        summary["status"] = "failed"
        summary["error"] = str(exc)
    finally:
        run.status = summary["status"]
        run.fetched_count = summary["fetched_count"]
        run.matched_count = summary["matched_count"]
        run.not_found_count = summary["not_found_count"]
        run.error_count = summary["error_count"]
        run.skipped_count = summary["skipped_count"]
        run.error = summary["error"]
        run.finished_at = datetime.utcnow()
        db.session.add(run)
        db.session.commit()

    return summary
