"""Small background job runner with durable database-backed status."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import inspect
import logging
import uuid
from typing import Any, Callable

from sqlalchemy import func

from .. import db
from ..models import SyncJob, SyncJobStep, utc_now

logger = logging.getLogger(__name__)
_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="orcid-job")


def submit_background_job(
    app,
    name: str,
    func: Callable[..., Any],
    *args,
    job_type: str = "generic",
    ror_id: str | None = None,
    requested_by_user_id: int | None = None,
    steps: list[str] | None = None,
    **kwargs,
) -> str:
    """Persist and submit a callable to run under an application context."""
    job_id = str(uuid.uuid4())
    job = SyncJob(
        id=job_id,
        name=name,
        job_type=job_type,
        ror_id=ror_id,
        requested_by_user_id=requested_by_user_id,
        status="queued",
        progress_total=len(steps or []),
        heartbeat_at=utc_now(),
    )
    db.session.add(job)
    for position, step_name in enumerate(steps or [], start=1):
        db.session.add(
            SyncJobStep(
                sync_job_id=job_id,
                name=step_name,
                position=position,
                status="pending",
            )
        )
    db.session.commit()

    _EXECUTOR.submit(_run_job, app, job_id, func, args, kwargs)
    return job_id


def get_background_job(job_id: str) -> SyncJob | None:
    """Return the durable status row for a background job."""
    return db.session.get(SyncJob, job_id)


def update_background_job(job_id: str, **values) -> None:
    """Update a job heartbeat, status, progress, message, or result."""
    job = db.session.get(SyncJob, job_id)
    if not job:
        return
    for key, value in values.items():
        if hasattr(job, key):
            setattr(job, key, value)
    job.heartbeat_at = utc_now()
    db.session.commit()


def update_job_step(
    job_id: str | None,
    name: str,
    status: str,
    *,
    records_count: int | None = None,
    error: str | None = None,
) -> None:
    """Update one durable synchronization step and aggregate job progress."""
    if not job_id:
        return
    step = SyncJobStep.query.filter_by(sync_job_id=job_id, name=name).first()
    if not step:
        next_position = (
            db.session.query(func.max(SyncJobStep.position))
            .filter_by(sync_job_id=job_id)
            .scalar()
            or 0
        ) + 1
        step = SyncJobStep(
            sync_job_id=job_id,
            name=name,
            position=next_position,
        )
        db.session.add(step)

    now = utc_now()
    step.status = status
    if status == "running" and not step.started_at:
        step.started_at = now
    if status in {"success", "failed", "skipped"}:
        step.finished_at = now
    if records_count is not None:
        step.records_count = int(records_count)
    step.error = error

    job = db.session.get(SyncJob, job_id)
    if job:
        completed = SyncJobStep.query.filter(
            SyncJobStep.sync_job_id == job_id,
            SyncJobStep.status.in_({"success", "failed", "skipped"}),
        ).count()
        total = SyncJobStep.query.filter_by(sync_job_id=job_id).count()
        job.progress_current = completed
        job.progress_total = total
        job.heartbeat_at = now
    db.session.commit()


def recover_interrupted_jobs(stale_minutes: int = 30) -> int:
    """Mark abandoned jobs as interrupted after their heartbeat becomes stale."""
    cutoff = utc_now() - timedelta(minutes=max(stale_minutes, 1))
    rows = SyncJob.query.filter(
        SyncJob.status.in_({"queued", "running"}),
        SyncJob.heartbeat_at.isnot(None),
        SyncJob.heartbeat_at < cutoff,
    ).all()
    for job in rows:
        job.status = "interrupted"
        job.error = "The application stopped receiving job heartbeats."
        job.finished_at = utc_now()
    if rows:
        db.session.commit()
    return len(rows)


def _run_job(app, job_id: str, func: Callable[..., Any], args: tuple, kwargs: dict) -> None:
    with app.app_context():
        try:
            update_background_job(
                job_id,
                status="running",
                started_at=utc_now(),
                message="Background job started.",
            )
            call_kwargs = dict(kwargs)
            if "job_id" in inspect.signature(func).parameters and "job_id" not in call_kwargs:
                call_kwargs["job_id"] = job_id
            result = func(*args, **call_kwargs)
            open_steps = SyncJobStep.query.filter(
                SyncJobStep.sync_job_id == job_id,
                SyncJobStep.status.in_({"pending", "running"}),
            ).all()
            for step in open_steps:
                step.status = "success"
                step.finished_at = utc_now()
            if open_steps:
                job = db.session.get(SyncJob, job_id)
                if job:
                    job.progress_current = SyncJobStep.query.filter_by(sync_job_id=job_id).count()
                db.session.commit()
            result_has_errors = bool(
                isinstance(result, dict)
                and (result.get("errors") or result.get("failed"))
            )
            update_background_job(
                job_id,
                status="partial" if result_has_errors else "success",
                result_json=result if isinstance(result, (dict, list)) else None,
                message=(
                    "Background job completed with errors."
                    if result_has_errors
                    else "Background job completed."
                ),
                finished_at=utc_now(),
            )
        except Exception as exc:
            db.session.rollback()
            logger.exception("Background job %s failed: %s", job_id, exc)
            update_background_job(
                job_id,
                status="failed",
                error=str(exc),
                message="Background job failed.",
                finished_at=utc_now(),
            )
        finally:
            db.session.remove()
