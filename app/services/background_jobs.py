"""Small background job runner with durable database-backed status."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from datetime import timedelta
import inspect
import logging
from threading import Lock
import uuid
from typing import Any, Callable

from sqlalchemy import func, text

from .. import db
from ..models import SyncJob, SyncJobStep, utc_now

logger = logging.getLogger(__name__)
_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="orcid-job")
_SUBMISSION_LOCK = Lock()


@contextmanager
def _deduplicated_submission(name: str):
    """Serialize active-job checks locally and across PostgreSQL processes."""
    with _SUBMISSION_LOCK:
        if db.session.get_bind().dialect.name == "postgresql":
            db.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 1))"),
                {"name": name},
            )
        yield


def submit_background_job(
    app,
    name: str,
    func: Callable[..., Any],
    *args,
    job_type: str = "generic",
    ror_id: str | None = None,
    requested_by_user_id: int | None = None,
    steps: list[str] | None = None,
    deduplicate: bool = False,
    **kwargs,
) -> str:
    """Persist and submit a callable to run under an application context."""
    submission_context = _deduplicated_submission(name) if deduplicate else nullcontext()
    with submission_context:
        if deduplicate:
            active_job = (
                SyncJob.query
                .filter_by(name=name)
                .filter(SyncJob.status.in_({"queued", "running"}))
                .order_by(SyncJob.created_at.desc())
                .first()
            )
            if active_job:
                db.session.commit()
                return active_job.id

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


def update_job_progress(
    job_id: str | None,
    current: int,
    total: int,
    unit: str,
    *,
    message: str | None = None,
) -> None:
    """Persist item-level progress independently from completed job steps."""
    if not job_id:
        return
    job = db.session.get(SyncJob, job_id)
    if not job:
        return

    total = max(int(total or 0), 0)
    current = max(int(current or 0), 0)
    job.items_total = total
    job.items_current = min(current, total) if total else current
    job.progress_unit = (unit or "items")[:32]
    if message is not None:
        job.message = message
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
    terminal_statuses = {"success", "failed", "skipped", "interrupted"}
    if status in terminal_statuses:
        step.finished_at = now
    if records_count is not None:
        step.records_count = int(records_count)
    step.error = error

    job = db.session.get(SyncJob, job_id)
    if job:
        completed = SyncJobStep.query.filter(
            SyncJobStep.sync_job_id == job_id,
            SyncJobStep.status.in_(terminal_statuses),
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
    interrupted_at = utc_now()
    for job in rows:
        job.status = "interrupted"
        job.error = "The application stopped receiving job heartbeats."
        job.finished_at = interrupted_at
        steps = SyncJobStep.query.filter(
            SyncJobStep.sync_job_id == job.id,
            SyncJobStep.status.in_({"pending", "running"}),
        ).all()
        for step in steps:
            if step.status == "running":
                step.status = "interrupted"
                step.error = job.error
            else:
                step.status = "skipped"
            step.finished_at = interrupted_at
        if steps:
            job.progress_current = SyncJobStep.query.filter_by(sync_job_id=job.id).count()
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
            first_step = (
                SyncJobStep.query
                .filter_by(sync_job_id=job_id, status="pending")
                .order_by(SyncJobStep.position.asc())
                .first()
            )
            if first_step:
                first_step.status = "running"
                first_step.started_at = utc_now()
                db.session.commit()
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
            failed_at = utc_now()
            open_steps = SyncJobStep.query.filter(
                SyncJobStep.sync_job_id == job_id,
                SyncJobStep.status.in_({"pending", "running"}),
            ).all()
            for step in open_steps:
                if step.status == "running":
                    step.status = "failed"
                    step.error = str(exc)
                else:
                    step.status = "skipped"
                step.finished_at = failed_at
            if open_steps:
                job = db.session.get(SyncJob, job_id)
                if job:
                    job.progress_current = SyncJobStep.query.filter_by(sync_job_id=job_id).count()
                db.session.commit()
            update_background_job(
                job_id,
                status="failed",
                error=str(exc),
                message="Background job failed.",
                finished_at=utc_now(),
            )
        finally:
            db.session.remove()
