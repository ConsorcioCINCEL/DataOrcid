"""Provide one shared freshness and completeness state for every product view."""

from __future__ import annotations

from datetime import timedelta

from flask import current_app
from sqlalchemy import false, func

from .. import db
from ..models import (
    FundingCache,
    FundingCacheRun,
    InstitutionRegistry,
    InstitutionResearcher,
    OpenAlexSyncRun,
    OpenAlexWorkMetadata,
    SyncJob,
    WorkCache,
    WorkCacheRun,
    utc_now,
)


def institution_data_health(ror_id: str | None) -> dict:
    """Return a stable current/stale/running/partial/failed/empty state."""
    stale_days = max(int(current_app.config.get("CACHE_STALE_DAYS", 30)), 1)
    if not ror_id:
        return _empty_health(stale_days)

    works_count = WorkCache.query.filter_by(ror_id=ror_id).count()
    fundings_count = FundingCache.query.filter_by(ror_id=ror_id).count()
    institution = InstitutionRegistry.query.filter_by(ror_id=ror_id).first()
    association_query = InstitutionResearcher.query.filter_by(is_active=True)
    if institution:
        association_query = association_query.filter_by(institution_id=institution.id)
    else:
        association_query = association_query.filter(false())
    association_count = association_query.count()
    verified_associations = association_query.filter_by(is_verified=True).count()
    inferred_associations = association_query.filter_by(is_verified=False).count()

    work_success = _latest_run(WorkCacheRun, ror_id, "success")
    funding_success = _latest_run(FundingCacheRun, ror_id, "success")
    openalex_success = _latest_run(OpenAlexSyncRun, ror_id, "success")
    work_latest = _latest_run(WorkCacheRun, ror_id)
    funding_latest = _latest_run(FundingCacheRun, ror_id)
    openalex_latest = _latest_run(OpenAlexSyncRun, ror_id)
    metadata_freshness = db.session.query(func.max(OpenAlexWorkMetadata.fetched_at)).scalar()

    active_job = (
        SyncJob.query.filter(
            SyncJob.ror_id == ror_id,
            SyncJob.status.in_({"queued", "running"}),
            SyncJob.heartbeat_at >= utc_now() - timedelta(minutes=30),
        )
        .order_by(SyncJob.created_at.desc())
        .first()
    )

    successful_updates = [
        value
        for value in (
            _run_timestamp(work_success),
            _run_timestamp(funding_success),
            _association_timestamp(association_query),
            metadata_freshness,
        )
        if value
    ]
    latest_update = max(successful_updates) if successful_updates else None
    oldest_update = min(successful_updates) if successful_updates else None
    stale_before = utc_now() - timedelta(days=stale_days)

    latest_failure = max(
        [
            timestamp
            for run in (work_latest, funding_latest, openalex_latest)
            if run and (run.status or "").lower() == "failed"
            for timestamp in [_run_timestamp(run)]
            if timestamp
        ],
        default=None,
    )
    has_data = bool(works_count or fundings_count or association_count)
    missing_components = []
    if not work_success:
        missing_components.append("works")
    if not funding_success:
        missing_components.append("fundings")
    if association_count == 0:
        missing_components.append("researchers")
    if works_count and not (openalex_success or metadata_freshness):
        missing_components.append("openalex")
    if association_count and verified_associations == 0:
        missing_components.append("verified_affiliations")

    if active_job:
        state = "running"
    elif not has_data:
        state = "empty"
    elif latest_failure and (not latest_update or latest_failure > latest_update):
        state = "failed"
    elif oldest_update and oldest_update < stale_before:
        state = "stale"
    elif missing_components:
        state = "partial"
    else:
        state = "current"

    return {
        "state": state,
        "has_data": has_data,
        "latest_update": latest_update,
        "oldest_update": oldest_update,
        "stale_days": stale_days,
        "missing_components": missing_components,
        "active_job": active_job,
        "components": {
            "researchers": {
                "records": association_count,
                "verified": verified_associations,
                "inferred": inferred_associations,
                "last_update": _association_timestamp(association_query),
            },
            "works": {
                "records": works_count,
                "last_update": _run_timestamp(work_success),
                "status": (work_latest.status if work_latest else "pending"),
            },
            "fundings": {
                "records": fundings_count,
                "last_update": _run_timestamp(funding_success),
                "status": (funding_latest.status if funding_latest else "pending"),
            },
            "openalex": {
                "last_scope_update": _run_timestamp(openalex_success),
                "last_metadata_update": metadata_freshness,
                "status": (openalex_latest.status if openalex_latest else "pending"),
            },
        },
    }


def health_presentation(state: str) -> dict:
    """Return shared non-localized icon and CSS semantics for one state."""
    return {
        "current": {"css": "is-ready", "icon": "far fa-check-circle"},
        "stale": {"css": "is-stale", "icon": "far fa-clock"},
        "running": {"css": "is-running", "icon": "fas fa-sync-alt fa-spin"},
        "partial": {"css": "is-attention", "icon": "fas fa-exclamation-circle"},
        "failed": {"css": "is-error", "icon": "fas fa-times-circle"},
        "empty": {"css": "is-pending", "icon": "far fa-clock"},
    }.get(state, {"css": "is-pending", "icon": "far fa-clock"})


def _latest_run(model, ror_id: str, status: str | None = None):
    query = model.query.filter_by(ror_id=ror_id)
    if status:
        query = query.filter_by(status=status)
    return query.order_by(model.started_at.desc()).first()


def _run_timestamp(run):
    return (run.finished_at or run.started_at) if run else None


def _association_timestamp(query):
    return query.with_entities(func.max(InstitutionResearcher.last_seen_at)).scalar()


def _empty_health(stale_days: int) -> dict:
    return {
        "state": "empty",
        "has_data": False,
        "latest_update": None,
        "oldest_update": None,
        "stale_days": stale_days,
        "missing_components": ["researchers", "works", "fundings", "openalex"],
        "active_job": None,
        "components": {},
    }
