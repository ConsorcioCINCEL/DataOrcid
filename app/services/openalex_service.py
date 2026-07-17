"""OpenAlex API client and DOI-based enrichment cache helpers."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import logging
import re
from time import sleep
import unicodedata
from urllib.parse import quote

import requests
from flask import current_app
from sqlalchemy import func

from .. import db
from ..models import (
    OpenAlexSyncRun,
    OpenAlexWorkAuthor,
    OpenAlexWorkInstitution,
    OpenAlexWorkMetadata,
    OpenAlexWorkRawCache,
    WorkCache,
    utc_now,
)

logger = logging.getLogger(__name__)
DEFAULT_OPENALEX_WORKERS = 4
DEFAULT_OPENALEX_TITLE_WORKERS = 2
OPENALEX_RETRY_STATUSES = {429, 500, 502, 503, 504}
OPENALEX_MAX_RETRIES = 2
OPENALEX_DB_MAX_RETRIES = 3
OPENALEX_PROGRESS_INTERVAL = 500
TITLE_MATCH_NOT_FOUND_ERROR = "No confident OpenAlex title match."
TITLE_MATCH_MIN_SCORE_WITH_YEAR = 0.88
TITLE_MATCH_MIN_SCORE_WITHOUT_YEAR = 0.94


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


def normalize_title(value: str | None) -> str:
    """Normalize a title for conservative local/OpenAlex comparison."""
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _openalex_search_filter_value(value: str) -> str:
    """Quote OpenAlex filter values so punctuation inside titles is not parsed as filters."""
    text = re.sub(r"\s+", " ", (value or "").strip())
    text = text.replace('"', " ")
    text = re.sub(r"[*?]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return f'"{text}"'


def _work_cache_key(work: WorkCache) -> str:
    normalized_doi = normalize_doi(work.doi)
    return normalized_doi or f"work:{work.id}"


def _parse_year(value) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"\d{4}", text)
    return int(match.group(0)) if match else None


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


def _short_openalex_id(value: str | None) -> str | None:
    if not value:
        return None
    return str(value).rstrip("/").split("/")[-1]


def _normalize_ror(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip().rstrip("/").lower()
    suffix = text.split("/")[-1]
    return suffix or None


def _normalize_orcid(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip().rstrip("/")
    suffix = text.split("/")[-1]
    return suffix or None


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

    def _get(self, path: str, params: dict) -> tuple[requests.Response | None, str | None]:
        """Run an OpenAlex GET with short retries for transient failures."""
        url = f"{self.base_url}{path}"
        for attempt in range(OPENALEX_MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                    headers={"Accept": "application/json"},
                )
            except requests.RequestException as exc:
                if attempt >= OPENALEX_MAX_RETRIES:
                    return None, str(exc)
                sleep(min(2 ** attempt, 5))
                continue

            if response.status_code not in OPENALEX_RETRY_STATUSES or attempt >= OPENALEX_MAX_RETRIES:
                return response, None

            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else min(2 ** attempt, 5)
            except ValueError:
                delay = min(2 ** attempt, 5)
            sleep(delay)

        return None, "OpenAlex request failed."

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

        response, error = self._get(f"/works/{work_id}", params=params)
        if error or response is None:
            return {
                "status": "error",
                "http_status": None,
                "payload": None,
                "error": error,
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

    def search_works_by_title(self, title: str, per_page: int = 5) -> dict:
        """Search OpenAlex works by title and return a small result set."""
        search_title = (title or "").strip()
        if not search_title:
            return {
                "status": "error",
                "http_status": None,
                "payload": None,
                "error": "Missing title.",
            }

        params = {
            "api_key": self.api_key,
            "filter": f"title.search:{_openalex_search_filter_value(search_title)}",
            "per-page": max(min(int(per_page or 5), 10), 1),
        }
        if self.mailto:
            params["mailto"] = self.mailto

        response, error = self._get("/works", params=params)
        if error or response is None:
            return {
                "status": "error",
                "http_status": None,
                "payload": None,
                "error": error,
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
        "openalex_id": _short_openalex_id(payload.get("id")),
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
        "fetched_at": utc_now(),
    }


def _openalex_work_title(payload: dict) -> str:
    return payload.get("title") or payload.get("display_name") or ""


def _type_compatible(local_type: str | None, openalex_type: str | None) -> bool:
    local = (local_type or "").strip().lower()
    remote = (openalex_type or "").strip().lower()
    if not local or not remote:
        return True
    if local == remote:
        return True
    if local == "journal-article" and remote in {"article", "journal-article"}:
        return True
    return False


def _select_best_title_match(candidate: dict, results: list[dict]) -> tuple[dict | None, dict]:
    source_title = candidate.get("title") or ""
    source_title_norm = normalize_title(source_title)
    source_year = _parse_year(candidate.get("pub_year"))
    source_doi = normalize_doi(candidate.get("source_doi"))

    best_payload = None
    best_info = {
        "method": "title",
        "source_title": source_title,
        "source_year": source_year,
        "score": 0.0,
        "accepted": False,
        "reason": "no_results",
    }

    for payload in results:
        result_title = _openalex_work_title(payload)
        result_title_norm = normalize_title(result_title)
        if not source_title_norm or not result_title_norm:
            continue

        result_doi = normalize_doi(payload.get("doi"))
        score = SequenceMatcher(None, source_title_norm, result_title_norm).ratio()
        result_year = _parse_year(payload.get("publication_year") or payload.get("publication_date"))
        year_delta = abs(source_year - result_year) if source_year and result_year else None
        year_compatible = year_delta is None or year_delta <= 1
        type_compatible = _type_compatible(candidate.get("type"), payload.get("type"))
        doi_match = bool(source_doi and result_doi and source_doi == result_doi)

        current_info = {
            "method": "title",
            "source_title": source_title,
            "source_year": source_year,
            "openalex_title": result_title,
            "openalex_year": result_year,
            "openalex_type": payload.get("type"),
            "openalex_doi": result_doi,
            "score": round(score, 4),
            "year_compatible": year_compatible,
            "type_compatible": type_compatible,
            "doi_match": doi_match,
        }

        if score > best_info["score"]:
            best_payload = payload
            best_info = {**current_info, "accepted": False, "reason": "below_threshold"}

        if doi_match:
            return payload, {**current_info, "score": 1.0, "accepted": True, "reason": "doi_match"}

        if not type_compatible or not year_compatible:
            continue

        threshold = TITLE_MATCH_MIN_SCORE_WITH_YEAR if source_year and result_year else TITLE_MATCH_MIN_SCORE_WITHOUT_YEAR
        if score >= threshold:
            return payload, {**current_info, "accepted": True, "reason": "title_year_type"}

    return None, best_info


def should_refresh_raw(raw_row: OpenAlexWorkRawCache | None, stale_days: int, force_refresh: bool = False) -> bool:
    if force_refresh or not raw_row:
        return True
    if raw_row.status in {"error", "pending"}:
        return True
    if stale_days <= 0:
        return False
    if not raw_row.fetched_at:
        return True
    return raw_row.fetched_at < utc_now() - timedelta(days=stale_days)


def _openalex_worker_count(workers: int | None = None) -> int:
    configured_workers = current_app.config.get("OPENALEX_WORKERS", DEFAULT_OPENALEX_WORKERS)
    value = configured_workers if workers is None else workers
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return DEFAULT_OPENALEX_WORKERS


def _openalex_title_worker_count(workers: int | None = None) -> int:
    configured_workers = current_app.config.get("OPENALEX_TITLE_WORKERS", DEFAULT_OPENALEX_TITLE_WORKERS)
    value = configured_workers if workers is None else workers
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return DEFAULT_OPENALEX_TITLE_WORKERS


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
    is_new_raw_row = raw_row is None

    client = client or OpenAlexClient.from_current_app()
    result = client.fetch_work_by_doi(normalized_doi)
    now = utc_now()

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
        raw_row.openalex_id = _short_openalex_id(payload.get("id"))
        raw_row.oa_updated_date = _parse_openalex_datetime(payload.get("updated_date"))
        _upsert_metadata(extract_work_metadata(payload, normalized_doi))
        _replace_work_dimensions(normalized_doi, payload, delete_existing=not is_new_raw_row)
    elif result["status"] == "not_found":
        OpenAlexWorkMetadata.query.filter_by(doi_normalized=normalized_doi).delete(synchronize_session=False)
        if not is_new_raw_row:
            _delete_work_dimensions(normalized_doi)

    db.session.commit()
    return {"status": result["status"], "doi": normalized_doi, "error": result.get("error")}


def sync_work_by_title(
    candidate: dict,
    client: OpenAlexClient | None = None,
    force_refresh: bool = False,
    stale_days: int | None = None,
) -> dict:
    """Search OpenAlex by title for one local work candidate and persist a confident match."""
    cache_key = candidate.get("cache_key") or ""
    title = (candidate.get("title") or "").strip()
    if not cache_key or not title:
        return {"status": "error", "doi": cache_key, "error": "Missing local work key or title."}

    stale_days = current_app.config.get("OPENALEX_STALE_DAYS", 30) if stale_days is None else stale_days
    raw_row = OpenAlexWorkRawCache.query.filter_by(doi_normalized=cache_key).first()
    metadata_row = OpenAlexWorkMetadata.query.filter_by(doi_normalized=cache_key).first()
    is_new_raw_row = raw_row is None
    title_search_already_failed = bool(raw_row and raw_row.error == TITLE_MATCH_NOT_FOUND_ERROR)

    if metadata_row and not should_refresh_raw(raw_row, int(stale_days or 0), force_refresh):
        return {"status": "skipped", "doi": cache_key, "raw_status": raw_row.status if raw_row else "found"}
    if title_search_already_failed and not should_refresh_raw(raw_row, int(stale_days or 0), force_refresh):
        return {"status": "skipped", "doi": cache_key, "raw_status": raw_row.status}

    client = client or OpenAlexClient.from_current_app()
    result = client.search_works_by_title(title)
    now = utc_now()

    if not raw_row:
        raw_row = OpenAlexWorkRawCache(doi_normalized=cache_key)
        db.session.add(raw_row)

    raw_row.source_doi = candidate.get("source_doi")
    raw_row.http_status = result.get("http_status")
    raw_row.fetched_at = now

    if result["status"] == "error":
        raw_row.status = "not_found" if candidate.get("source_doi") else "error"
        raw_row.raw_json = None
        raw_row.error = result.get("error")
        db.session.commit()
        return {"status": "error", "doi": cache_key, "error": result.get("error")}

    payloads = (result.get("payload") or {}).get("results") or []
    payload, match_info = _select_best_title_match(candidate, payloads)
    if payload:
        payload.setdefault("_local_match", match_info)
        raw_row.status = "found"
        raw_row.raw_json = payload
        raw_row.error = None
        raw_row.openalex_id = _short_openalex_id(payload.get("id"))
        raw_row.oa_updated_date = _parse_openalex_datetime(payload.get("updated_date"))
        _upsert_metadata(extract_work_metadata(payload, cache_key))
        _replace_work_dimensions(cache_key, payload, delete_existing=not is_new_raw_row)
        db.session.commit()
        return {
            "status": "found",
            "doi": cache_key,
            "error": None,
            "match_score": match_info.get("score"),
        }

    raw_row.status = "not_found"
    raw_row.raw_json = {"_local_match": match_info}
    raw_row.error = TITLE_MATCH_NOT_FOUND_ERROR
    raw_row.openalex_id = None
    raw_row.oa_updated_date = None
    OpenAlexWorkMetadata.query.filter_by(doi_normalized=cache_key).delete(synchronize_session=False)
    if not is_new_raw_row:
        _delete_work_dimensions(cache_key)
    db.session.commit()
    return {"status": "not_found", "doi": cache_key, "error": TITLE_MATCH_NOT_FOUND_ERROR}


def _is_retryable_database_error(exc: Exception) -> bool:
    """Return whether an exception represents transient database contention."""
    original = getattr(exc, "orig", exc)
    args = getattr(original, "args", ())
    mysql_code = args[0] if args else None
    sqlstate = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
    return mysql_code in {1205, 1213} or sqlstate in {"40001", "40P01", "55P03"}


def _bounded_executor_results(executor, submit, candidates, max_pending: int):
    """Yield executor results without retaining a Future for every candidate."""
    iterator = iter(candidates)
    pending = set()

    for _ in range(max(max_pending, 1)):
        try:
            pending.add(submit(executor, next(iterator)))
        except StopIteration:
            break

    while pending:
        completed, pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in completed:
            yield future.result()
            try:
                pending.add(submit(executor, next(iterator)))
            except StopIteration:
                pass


def _sync_candidate_in_worker(app, candidate: dict, force_refresh: bool, stale_days: int | None) -> dict:
    """Synchronize one DOI inside a worker-owned app context and DB session."""
    with app.app_context():
        for attempt in range(OPENALEX_DB_MAX_RETRIES + 1):
            try:
                return sync_work_by_doi(
                    candidate["source_doi"],
                    force_refresh=force_refresh,
                    stale_days=stale_days,
                )
            except Exception as exc:
                db.session.rollback()
                if _is_retryable_database_error(exc) and attempt < OPENALEX_DB_MAX_RETRIES:
                    delay = 0.25 * (2 ** attempt)
                    logger.warning(
                        "Retrying OpenAlex DOI %s after database contention (attempt %s/%s)",
                        candidate.get("doi"),
                        attempt + 1,
                        OPENALEX_DB_MAX_RETRIES,
                    )
                    sleep(delay)
                    continue

                logger.exception("OpenAlex DOI sync failed for %s: %s", candidate.get("doi"), exc)
                return {
                    "status": "error",
                    "doi": candidate.get("doi"),
                    "error": str(exc),
                }
            finally:
                db.session.remove()

    return {"status": "error", "doi": candidate.get("doi"), "error": "Retry limit exceeded."}


def _sync_title_candidate_in_worker(app, candidate: dict, force_refresh: bool, stale_days: int | None) -> dict:
    """Synchronize one title candidate inside a worker-owned app context and DB session."""
    with app.app_context():
        for attempt in range(OPENALEX_DB_MAX_RETRIES + 1):
            try:
                return sync_work_by_title(
                    candidate,
                    force_refresh=force_refresh,
                    stale_days=stale_days,
                )
            except Exception as exc:
                db.session.rollback()
                if _is_retryable_database_error(exc) and attempt < OPENALEX_DB_MAX_RETRIES:
                    delay = 0.25 * (2 ** attempt)
                    logger.warning(
                        "Retrying OpenAlex title %s after database contention (attempt %s/%s)",
                        candidate.get("cache_key"),
                        attempt + 1,
                        OPENALEX_DB_MAX_RETRIES,
                    )
                    sleep(delay)
                    continue

                logger.exception("OpenAlex title sync failed for %s: %s", candidate.get("cache_key"), exc)
                return {
                    "status": "error",
                    "doi": candidate.get("cache_key"),
                    "error": str(exc),
                }
            finally:
                db.session.remove()

    return {"status": "error", "doi": candidate.get("cache_key"), "error": "Retry limit exceeded."}


def _apply_sync_result(summary: dict, result: dict) -> None:
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
        if not summary.get("error"):
            summary["error"] = result.get("error")


def _checkpoint_sync_run(run: OpenAlexSyncRun, summary: dict) -> None:
    """Persist in-flight counters so interrupted runs retain useful progress."""
    run.fetched_count = summary["fetched_count"]
    run.matched_count = summary["matched_count"]
    run.not_found_count = summary["not_found_count"]
    run.error_count = summary["error_count"]
    run.skipped_count = summary["skipped_count"]
    run.error = summary["error"]
    db.session.add(run)
    db.session.commit()


def _checkpoint_sync_run_if_due(run: OpenAlexSyncRun, summary: dict) -> None:
    processed = summary["fetched_count"] + summary["skipped_count"]
    if processed and processed % OPENALEX_PROGRESS_INTERVAL == 0:
        _checkpoint_sync_run(run, summary)


def _upsert_metadata(values: dict) -> OpenAlexWorkMetadata:
    metadata = OpenAlexWorkMetadata.query.filter_by(doi_normalized=values["doi_normalized"]).first()
    if not metadata:
        metadata = OpenAlexWorkMetadata(doi_normalized=values["doi_normalized"])
        db.session.add(metadata)

    for key, value in values.items():
        setattr(metadata, key, value)
    return metadata


def _delete_work_dimensions(doi_normalized: str) -> None:
    OpenAlexWorkAuthor.query.filter_by(doi_normalized=doi_normalized).delete(synchronize_session=False)
    OpenAlexWorkInstitution.query.filter_by(doi_normalized=doi_normalized).delete(synchronize_session=False)


def _replace_work_dimensions(
    doi_normalized: str,
    payload: dict,
    delete_existing: bool = True,
) -> tuple[int, int]:
    """Replace author and institution dimension rows for one OpenAlex work."""
    if delete_existing:
        _delete_work_dimensions(doi_normalized)

    openalex_id = _short_openalex_id(payload.get("id"))
    institution_map = {}
    author_rows = []
    author_keys = set()
    now = utc_now()

    for authorship in payload.get("authorships") or []:
        author = authorship.get("author") or {}
        institutions = authorship.get("institutions") or []
        countries = sorted({
            country
            for country in (authorship.get("countries") or [])
            if country
        } | {
            institution.get("country_code")
            for institution in institutions
            if institution.get("country_code")
        })
        institution_rors = sorted({
            ror
            for ror in (_normalize_ror(institution.get("ror")) for institution in institutions)
            if ror
        })
        institution_names = sorted({
            institution.get("display_name")
            for institution in institutions
            if institution.get("display_name")
        })
        has_chile_affiliation = "CL" in countries

        if has_chile_affiliation:
            author_id = _short_openalex_id(author.get("id"))
            author_orcid = _normalize_orcid(author.get("orcid") or authorship.get("raw_orcid"))
            author_key = author_id or author_orcid or (
                (author.get("display_name") or authorship.get("raw_author_name") or "").strip().lower(),
                authorship.get("author_position"),
            )
            if author_key not in author_keys:
                author_keys.add(author_key)
                author_rows.append(OpenAlexWorkAuthor(
                    doi_normalized=doi_normalized,
                    openalex_id=openalex_id,
                    author_id=author_id,
                    author_name=author.get("display_name"),
                    orcid=author_orcid,
                    raw_author_name=authorship.get("raw_author_name"),
                    author_position=authorship.get("author_position"),
                    is_corresponding=bool(authorship.get("is_corresponding")),
                    has_chile_affiliation=has_chile_affiliation,
                    countries=countries or None,
                    institution_rors=institution_rors or None,
                    institution_names=institution_names or None,
                    created_at=now,
                ))

        for institution in institutions:
            institution_id = _short_openalex_id(institution.get("id"))
            ror_id = _normalize_ror(institution.get("ror"))
            key = institution_id or ror_id or institution.get("display_name")
            if not key:
                continue

            row = institution_map.setdefault(key, {
                "doi_normalized": doi_normalized,
                "openalex_id": openalex_id,
                "institution_id": institution_id,
                "institution_name": institution.get("display_name"),
                "ror_id": ror_id,
                "country_code": institution.get("country_code"),
                "institution_type": institution.get("type"),
                "author_count": 0,
                "has_corresponding_author": False,
            })
            row["author_count"] += 1
            row["has_corresponding_author"] = row["has_corresponding_author"] or bool(authorship.get("is_corresponding"))

    institution_rows = [
        OpenAlexWorkInstitution(**values, created_at=now)
        for values in institution_map.values()
    ]

    if author_rows:
        db.session.bulk_save_objects(author_rows)
    if institution_rows:
        db.session.bulk_save_objects(institution_rows)

    return len(author_rows), len(institution_rows)


def rebuild_openalex_dimensions(
    limit: int | None = None,
    batch_size: int = 50,
    missing_only: bool = False,
    reset: bool = False,
    progress=None,
) -> dict:
    """Backfill author and institution dimensions from stored raw OpenAlex JSON."""
    if reset:
        OpenAlexWorkAuthor.query.delete(synchronize_session=False)
        OpenAlexWorkInstitution.query.delete(synchronize_session=False)
        db.session.commit()

    processed = 0
    author_rows = 0
    institution_rows = 0
    last_id = 0
    batch_size = max(int(batch_size or 50), 1)

    while True:
        remaining = None if limit is None else limit - processed
        if remaining is not None and remaining <= 0:
            break

        current_batch_size = min(batch_size, remaining) if remaining is not None else batch_size
        query = (
            OpenAlexWorkRawCache.query
            .filter(OpenAlexWorkRawCache.id > last_id)
            .filter(OpenAlexWorkRawCache.status == "found")
            .filter(OpenAlexWorkRawCache.raw_json.isnot(None))
        )
        if missing_only:
            processed_subquery = db.session.query(OpenAlexWorkAuthor.doi_normalized)
            query = query.filter(~OpenAlexWorkRawCache.doi_normalized.in_(processed_subquery))

        rows = query.order_by(OpenAlexWorkRawCache.id.asc()).limit(current_batch_size).all()
        if not rows:
            break

        for raw_row in rows:
            authors, institutions = _replace_work_dimensions(raw_row.doi_normalized, raw_row.raw_json or {})
            processed += 1
            author_rows += authors
            institution_rows += institutions
            last_id = raw_row.id

        db.session.commit()
        if progress:
            progress(processed, author_rows, institution_rows, last_id)

    db.session.commit()
    integrity = repair_openalex_integrity()
    return {
        "processed": processed,
        "author_rows": author_rows,
        "institution_rows": institution_rows,
        "integrity": integrity,
    }


def repair_openalex_integrity() -> dict:
    """Remove orphaned and duplicate OpenAlex dimension rows."""
    orphan_authors = OpenAlexWorkAuthor.query.filter(
        ~db.session.query(OpenAlexWorkMetadata.id)
        .filter(OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkAuthor.doi_normalized)
        .exists()
    ).delete(synchronize_session=False)
    orphan_institutions = OpenAlexWorkInstitution.query.filter(
        ~db.session.query(OpenAlexWorkMetadata.id)
        .filter(OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkInstitution.doi_normalized)
        .exists()
    ).delete(synchronize_session=False)

    duplicate_authors = _delete_duplicate_dimension_rows(
        OpenAlexWorkAuthor,
        (OpenAlexWorkAuthor.doi_normalized, OpenAlexWorkAuthor.author_id),
        OpenAlexWorkAuthor.author_id.isnot(None),
    )
    duplicate_institutions = _delete_duplicate_dimension_rows(
        OpenAlexWorkInstitution,
        (OpenAlexWorkInstitution.doi_normalized, OpenAlexWorkInstitution.institution_id),
        OpenAlexWorkInstitution.institution_id.isnot(None),
    )
    db.session.commit()
    return {
        "orphan_authors_removed": int(orphan_authors or 0),
        "orphan_institutions_removed": int(orphan_institutions or 0),
        "duplicate_authors_removed": duplicate_authors,
        "duplicate_institutions_removed": duplicate_institutions,
    }


def _delete_duplicate_dimension_rows(model, group_columns, eligibility) -> int:
    duplicate_groups = (
        db.session.query(*group_columns, func.min(model.id).label("keep_id"))
        .filter(eligibility)
        .group_by(*group_columns)
        .having(func.count(model.id) > 1)
        .all()
    )
    removed = 0
    for group in duplicate_groups:
        filters = [column == value for column, value in zip(group_columns, group[:-1])]
        removed += model.query.filter(*filters, model.id != group.keep_id).delete(
            synchronize_session=False
        )
    return removed


def collect_work_dois(ror_id: str | None = None, articles_only: bool = True) -> tuple[int, list[dict]]:
    """Return candidate DOI values from the local ORCID works cache."""
    base_query = WorkCache.query
    if ror_id:
        base_query = base_query.filter(WorkCache.ror_id == ror_id)
    if articles_only:
        base_query = base_query.filter(WorkCache.type == "journal-article")

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


def collect_title_match_candidates(ror_id: str | None = None, articles_only: bool = True) -> tuple[int, list[dict]]:
    """Return works eligible for title-only OpenAlex matching.

    Candidates are local works with a title where either no DOI exists locally, or
    the local DOI was already tried against OpenAlex and returned not_found.
    """
    base_query = WorkCache.query
    if ror_id:
        base_query = base_query.filter(WorkCache.ror_id == ror_id)
    if articles_only:
        base_query = base_query.filter(WorkCache.type == "journal-article")

    works_seen = base_query.count()
    work_rows = (
        base_query
        .filter(WorkCache.title.isnot(None), WorkCache.title != "")
        .order_by(WorkCache.id.asc())
        .all()
    )

    doi_keys = sorted({
        normalize_doi(work.doi)
        for work in work_rows
        if normalize_doi(work.doi)
    })
    no_doi_keys = [
        _work_cache_key(work)
        for work in work_rows
        if not normalize_doi(work.doi)
    ]
    raw_keys = doi_keys + no_doi_keys
    raw_by_key = {
        row.doi_normalized: row
        for row in OpenAlexWorkRawCache.query
        .filter(OpenAlexWorkRawCache.doi_normalized.in_(raw_keys))
        .all()
    } if raw_keys else {}
    metadata_keys = {
        row.doi_normalized
        for row in OpenAlexWorkMetadata.query
        .filter(OpenAlexWorkMetadata.doi_normalized.in_(raw_keys))
        .all()
    } if raw_keys else set()

    candidates = {}
    for work in work_rows:
        normalized_doi = normalize_doi(work.doi)
        if normalized_doi:
            raw_row = raw_by_key.get(normalized_doi)
            if normalized_doi in metadata_keys or not raw_row or raw_row.status != "not_found":
                continue
            if raw_row.error == TITLE_MATCH_NOT_FOUND_ERROR:
                continue
            cache_key = normalized_doi
        else:
            cache_key = _work_cache_key(work)
            raw_row = raw_by_key.get(cache_key)
            if cache_key in metadata_keys or (raw_row and raw_row.status == "found"):
                continue
            if raw_row and raw_row.error == TITLE_MATCH_NOT_FOUND_ERROR:
                continue

        if cache_key not in candidates:
            candidates[cache_key] = {
                "cache_key": cache_key,
                "work_cache_id": work.id,
                "title": work.title,
                "source_doi": work.doi,
                "pub_year": work.pub_year,
                "type": work.type,
                "orcid": work.orcid,
                "ror_id": work.ror_id,
            }

    return works_seen, sorted(candidates.values(), key=lambda item: (normalize_title(item["title"]), item["cache_key"]))


def sync_openalex_works(
    ror_id: str | None = None,
    limit: int | None = None,
    force_refresh: bool = False,
    stale_days: int | None = None,
    articles_only: bool = True,
    dry_run: bool = False,
    workers: int | None = None,
) -> dict:
    """Synchronize OpenAlex metadata for DOI-backed works in the local ORCID cache."""
    works_seen, candidates = collect_work_dois(ror_id=ror_id, articles_only=articles_only)
    if limit:
        candidates = candidates[:limit]
    worker_count = _openalex_worker_count(workers)

    summary = {
        "ror_id": ror_id,
        "works_seen": works_seen,
        "dois_found": len(candidates),
        "workers": worker_count,
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
        if worker_count == 1 or len(candidates) <= 1:
            for candidate in candidates:
                result = sync_work_by_doi(
                    candidate["source_doi"],
                    client=client,
                    force_refresh=force_refresh,
                    stale_days=stale_days,
                )
                _apply_sync_result(summary, result)
                _checkpoint_sync_run_if_due(run, summary)
        else:
            app = current_app._get_current_object()
            max_workers = min(worker_count, len(candidates))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                def submit(pool, candidate):
                    return pool.submit(
                        _sync_candidate_in_worker,
                        app,
                        candidate,
                        force_refresh,
                        stale_days,
                    )

                results = _bounded_executor_results(
                    executor,
                    submit,
                    candidates,
                    max_pending=max_workers * 2,
                )
                for result in results:
                    _apply_sync_result(summary, result)
                    _checkpoint_sync_run_if_due(run, summary)

        summary["status"] = "failed" if summary["error_count"] else "success"
    except Exception as exc:
        db.session.rollback()
        logger.exception("OpenAlex synchronization failed: %s", exc)
        summary["status"] = "failed"
        summary["error"] = str(exc)
    finally:
        run.status = summary["status"]
        _checkpoint_sync_run(run, summary)
        run.finished_at = utc_now()
        db.session.add(run)
        db.session.commit()

    return summary


def sync_openalex_title_matches(
    ror_id: str | None = None,
    limit: int | None = None,
    force_refresh: bool = False,
    stale_days: int | None = None,
    articles_only: bool = True,
    dry_run: bool = False,
    workers: int | None = None,
) -> dict:
    """Synchronize OpenAlex metadata by title for DOI misses and works without DOI."""
    works_seen, candidates = collect_title_match_candidates(ror_id=ror_id, articles_only=articles_only)
    if limit:
        candidates = candidates[:limit]
    worker_count = _openalex_title_worker_count(workers)

    summary = {
        "ror_id": ror_id,
        "works_seen": works_seen,
        "dois_found": len(candidates),
        "title_candidates_found": len(candidates),
        "workers": worker_count,
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
        if worker_count == 1 or len(candidates) <= 1:
            for candidate in candidates:
                result = sync_work_by_title(
                    candidate,
                    client=client,
                    force_refresh=force_refresh,
                    stale_days=stale_days,
                )
                _apply_sync_result(summary, result)
                _checkpoint_sync_run_if_due(run, summary)
        else:
            app = current_app._get_current_object()
            max_workers = min(worker_count, len(candidates))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                def submit(pool, candidate):
                    return pool.submit(
                        _sync_title_candidate_in_worker,
                        app,
                        candidate,
                        force_refresh,
                        stale_days,
                    )

                results = _bounded_executor_results(
                    executor,
                    submit,
                    candidates,
                    max_pending=max_workers * 2,
                )
                for result in results:
                    _apply_sync_result(summary, result)
                    _checkpoint_sync_run_if_due(run, summary)

        summary["status"] = "failed" if summary["error_count"] else "success"
    except Exception as exc:
        db.session.rollback()
        logger.exception("OpenAlex title synchronization failed: %s", exc)
        summary["status"] = "failed"
        summary["error"] = str(exc)
    finally:
        run.status = summary["status"]
        _checkpoint_sync_run(run, summary)
        run.finished_at = utc_now()
        db.session.add(run)
        db.session.commit()

    return summary
