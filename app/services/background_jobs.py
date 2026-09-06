"""Small background job runner with durable database-backed status."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from datetime import timedelta
import importlib
import inspect
import json
import logging
import socket
from threading import Event, Lock, Thread
import uuid
from typing import Any, Callable

from sqlalchemy import func, text

from .. import db
from ..models import SyncJob, SyncJobStep, User, utc_now
from .institution_lock import institution_write_lock

logger = logging.getLogger(__name__)
_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="orcid-job")
_SUBMISSION_LOCK = Lock()
_PROCESS_ID = uuid.uuid4().hex[:12]


def _callable_path(func: Callable[..., Any]) -> str:
    """Return an importable path for a top-level background-job handler."""
    qualname = getattr(func, "__qualname__", "")
    if not qualname or "<locals>" in qualname:
        raise TypeError("Background job handlers must be importable top-level callables.")
    return f"{func.__module__}:{qualname}"


def _serialized_payload(args: tuple, kwargs: dict) -> dict:
    """Validate and normalize a durable JSON job payload."""
    payload = {"args": list(args), "kwargs": dict(kwargs)}
    try:
        return json.loads(json.dumps(payload))
    except (TypeError, ValueError) as exc:
        raise TypeError("Background job arguments must be JSON serializable.") from exc


def _load_callable(path: str) -> Callable[..., Any]:
    """Import a persisted handler without evaluating arbitrary expressions."""
    module_name, separator, qualname = (path or "").partition(":")
    if not separator or not module_name.startswith("app."):
        raise RuntimeError(f"Invalid background job handler: {path!r}")
    target: Any = importlib.import_module(module_name)
    for attribute in qualname.split("."):
        if not attribute or "__" in attribute:
            raise RuntimeError(f"Invalid background job handler: {path!r}")
        target = getattr(target, attribute)
    if not callable(target):
        raise RuntimeError(f"Background job handler is not callable: {path!r}")
    return target


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
    deduplicate: bool = True,
    **kwargs,
) -> str:
    """Persist and submit a callable to run under an application context."""
    execution_mode = app.config.get("JOB_EXECUTION_MODE", "thread")
    if deduplicate:
        # Make an abandoned durable job eligible for reuse before deciding
        # whether another job with the same logical name is active.
        recover_interrupted_jobs(app.config.get("JOB_STALE_MINUTES", 30))

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
                should_submit = bool(
                    execution_mode == "thread"
                    and active_job.status == "queued"
                    and active_job.claimed_by is None
                )
                if should_submit:
                    active_job.claimed_by = f"thread-queued:{_PROCESS_ID}"
                    active_job.claimed_at = utc_now()
                    active_job.heartbeat_at = utc_now()
                db.session.commit()
                if should_submit:
                    _EXECUTOR.submit(
                        _run_job,
                        app,
                        active_job.id,
                        func,
                        args,
                        kwargs,
                        already_claimed=False,
                    )
                return active_job.id

        handler = _callable_path(func)
        payload = _serialized_payload(args, kwargs)
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
            handler=handler,
            payload_json=payload,
            max_attempts=max(int(app.config.get("JOB_MAX_ATTEMPTS", 3)), 1),
            claimed_by=(f"thread-queued:{_PROCESS_ID}" if execution_mode == "thread" else None),
            claimed_at=(utc_now() if execution_mode == "thread" else None),
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

    if execution_mode == "thread":
        _EXECUTOR.submit(
            _run_job,
            app,
            job_id,
            func,
            args,
            kwargs,
            already_claimed=False,
        )
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
    """Persist item-level progress without committing the handler session.

    Export handlers stream large PostgreSQL result sets through server-side
    cursors.  Committing the Flask-scoped ORM session while one of those
    cursors is being consumed closes the cursor, so progress writes use their
    own short transaction instead.
    """
    if not job_id:
        return

    total = max(int(total or 0), 0)
    current = max(int(current or 0), 0)
    values = {
        "items_total": total,
        "items_current": min(current, total) if total else current,
        "progress_unit": (unit or "items")[:32],
        "heartbeat_at": utc_now(),
    }
    if message is not None:
        values["message"] = message
    with db.engine.begin() as connection:
        connection.execute(
            SyncJob.__table__.update()
            .where(SyncJob.id == job_id)
            .values(**values)
        )


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
    terminal_statuses = {"success", "partial", "failed", "skipped", "interrupted"}
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
    """Requeue recoverable stale jobs and interrupt legacy exhausted jobs."""
    cutoff = utc_now() - timedelta(minutes=max(stale_minutes, 1))
    lease_timestamp = func.coalesce(
        SyncJob.heartbeat_at,
        SyncJob.claimed_at,
        SyncJob.started_at,
        SyncJob.created_at,
    )
    query = SyncJob.query.filter(
        SyncJob.status == "running",
        lease_timestamp < cutoff,
    )
    if db.session.get_bind().dialect.name == "postgresql":
        # A concurrent heartbeat update and recovery must not both win. Locked
        # rows are either inspected after the heartbeat commits or skipped
        # until the next recovery pass.
        query = query.with_for_update(skip_locked=True)
    rows = query.all()
    interrupted_at = utc_now()
    for job in rows:
        recoverable = bool(
            job.handler
            and job.payload_json is not None
            and int(job.attempt_count or 0) < int(job.max_attempts or 1)
        )
        job.status = "queued" if recoverable else "interrupted"
        job.error = (
            "The previous worker stopped; the job was queued for recovery."
            if recoverable
            else "The application stopped receiving job heartbeats."
        )
        job.finished_at = None if recoverable else interrupted_at
        job.claimed_by = None
        job.claimed_at = None
        job.heartbeat_at = interrupted_at
        steps = SyncJobStep.query.filter(
            SyncJobStep.sync_job_id == job.id,
            SyncJobStep.status.in_({"pending", "running"}),
        ).all()
        for step in steps:
            if recoverable:
                step.status = "pending"
                step.error = None
                step.started_at = None
                step.finished_at = None
            elif step.status == "running":
                step.status = "interrupted"
                step.error = job.error
            else:
                step.status = "skipped"
            if not recoverable:
                step.finished_at = interrupted_at
        if steps:
            job.progress_current = (
                0
                if recoverable
                else SyncJobStep.query.filter_by(sync_job_id=job.id).count()
            )
    if rows:
        db.session.commit()
    return len(rows)


def _claim_next_job(worker_id: str) -> str | None:
    """Atomically claim the oldest queued job for one persistent worker."""
    query = SyncJob.query.filter_by(status="queued").order_by(SyncJob.created_at.asc())
    if db.session.get_bind().dialect.name == "postgresql":
        query = query.with_for_update(skip_locked=True)
    else:
        query = query.with_for_update()
    job = query.first()
    if not job:
        db.session.rollback()
        return None
    job.status = "running"
    job.claimed_by = worker_id[:80]
    job.claimed_at = utc_now()
    job.heartbeat_at = utc_now()
    db.session.commit()
    return job.id


def run_queued_job(app, worker_id: str | None = None) -> str | None:
    """Claim and execute one persisted job, returning its ID when found."""
    worker_id = worker_id or f"{socket.gethostname()}:{uuid.uuid4().hex[:12]}"
    with app.app_context():
        recover_interrupted_jobs(app.config.get("JOB_STALE_MINUTES", 30))
        job_id = _claim_next_job(worker_id)
        if not job_id:
            return None
        job = db.session.get(SyncJob, job_id)
        try:
            handler = _load_callable(job.handler)
            payload = job.payload_json or {}
            args = tuple(payload.get("args") or [])
            kwargs = dict(payload.get("kwargs") or {})
        except Exception as exc:
            logger.exception("Could not load handler for background job %s", job_id)
            update_background_job(
                job_id,
                status="failed",
                error=str(exc),
                message="Background job handler could not be loaded.",
                finished_at=utc_now(),
                claimed_by=None,
                claimed_at=None,
            )
            return job_id

    _run_job(app, job_id, handler, args, kwargs, already_claimed=True, expected_claim=worker_id[:80])
    return job_id


@contextmanager
def _job_heartbeat(app, job_id: str, claim_token: str):
    """Refresh a running job lease while a handler performs long blocking work."""
    interval = max(int(app.config.get("JOB_HEARTBEAT_SECONDS", 30)), 1)
    stopped = Event()

    def refresh_lease() -> None:
        while not stopped.wait(interval):
            with app.app_context():
                try:
                    SyncJob.query.filter_by(id=job_id, status="running", claimed_by=claim_token).update(
                        {SyncJob.heartbeat_at: utc_now()},
                        synchronize_session=False,
                    )
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                    logger.warning(
                        "Could not refresh heartbeat for background job %s.",
                        job_id,
                        exc_info=True,
                    )
                finally:
                    db.session.remove()

    heartbeat_thread = Thread(
        target=refresh_lease,
        name=f"job-heartbeat-{job_id[:8]}",
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        yield
    finally:
        stopped.set()
        heartbeat_thread.join(timeout=min(interval, 2))


def _run_job(
    app,
    job_id: str,
    func: Callable[..., Any],
    args: tuple,
    kwargs: dict,
    *,
    already_claimed: bool = False,
    expected_claim: str | None = None,
) -> None:
    with app.app_context():
        error_context = {"source": "background_job", "job_id": job_id}
        claim_token = None
        try:
            job = db.session.get(SyncJob, job_id)
            if not job:
                return
            if not already_claimed:
                claim_token = f"thread:{uuid.uuid4().hex}"
                claimed = SyncJob.query.filter_by(id=job_id, status="queued").update(
                    {SyncJob.status: "running", SyncJob.claimed_by: claim_token,
                     SyncJob.claimed_at: utc_now()}, synchronize_session=False,
                )
                db.session.commit()
                if not claimed:
                    return
                db.session.refresh(job)
            elif job.status != "running" or (expected_claim and job.claimed_by != expected_claim):
                return
            claim_token = job.claimed_by
            if job:
                error_context.update({
                    "user_id": job.requested_by_user_id,
                    "institution_ror": job.ror_id,
                    "job_name": job.name,
                    "handler": job.handler,
                })
                if job.requested_by_user_id:
                    requester = db.session.get(User, job.requested_by_user_id)
                    if requester:
                        error_context["username"] = requester.username
            attempt_count = int(job.attempt_count or 0) + 1 if job else 1
            update_background_job(
                job_id,
                status="running",
                started_at=(job.started_at if job and job.started_at else utc_now()),
                attempt_count=attempt_count,
                claimed_by=claim_token,
                claimed_at=(job.claimed_at if already_claimed and job else utc_now()),
                message="Background job started.",
                error=None,
                result_json=None,
                finished_at=None,
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
            write_job = not job.job_type.startswith("export") and job.job_type != "generic"
            scope_lock = institution_write_lock(job.ror_id) if write_job else nullcontext()
            with _job_heartbeat(app, job_id, claim_token), scope_lock:
                db.session.refresh(job)
                if job.status != "running" or job.claimed_by != claim_token:
                    return
                result = func(*args, **call_kwargs)
            db.session.refresh(job)
            if job.claimed_by != claim_token:
                return
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
                claimed_by=None,
                claimed_at=None,
                error=None,
            )
        except Exception as exc:
            db.session.rollback()
            current_job = db.session.get(SyncJob, job_id)
            if claim_token and current_job and current_job.claimed_by != claim_token:
                return
            logger.exception(
                "Background job %s failed: %s",
                job_id,
                exc,
                extra={"system_error_context": error_context},
            )
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
                claimed_by=None,
                claimed_at=None,
            )
        finally:
            db.session.remove()
