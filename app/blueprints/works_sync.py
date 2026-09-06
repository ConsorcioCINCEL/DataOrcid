"""Work/funding cache management views and exports."""

import logging
from datetime import datetime as dt, timezone

from flask import current_app, g, redirect, render_template, request, session, url_for
from flask_babel import _
from sqlalchemy import func, or_

from .. import db
from ..decorators import admin_required, login_required, normalize_ror_id, staff_required
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

from .works_blueprint import bp_works
from .works_openalex_data import _openalex_cache_summary
from .works_shared import _institution_cache_summaries, _pagination_dict, _researcher_count
logger = logging.getLogger(__name__)
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
        run_w.status = 'partial' if result.get('failed_profiles') else 'success'
        run_w.rows_count = result["works"]
        run_f.status = 'partial' if result.get('failed_profiles') else 'success'
        run_f.rows_count = result["fundings"]
        update_job_step(job_id, "researchers", "success", records_count=result["researchers"])
        update_job_step(job_id, "profiles", "partial" if result.get("failed_profiles") else "success", records_count=result["profiles"])
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


@bp_works.route('/cache/profiles/retry', methods=['POST'])
@staff_required
def cache_retry_failed_profiles():
    from ..services.background_jobs import submit_background_job
    from ..services.cache_service import retry_failed_profiles_for_ror

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('works.cache_works_status'))
    job_id = submit_background_job(
        current_app._get_current_object(), f"retry-profiles-{ror_id}",
        retry_failed_profiles_for_ror, ror_id,
        current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/'),
        {'Accept': 'application/json'}, job_type="failed_profile_retry", ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
    )
    flash_ok(_('Failed profile retry queued. Job ID: %(job)s', job=job_id))
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


def _run_fundings_sync_for_ror(
    ror_id: str,
    base_url: str,
    job_id: str | None = None,
) -> dict:
    """Rebuild one institutional funding cache as a durable job."""
    from ..models import FundingCacheRun, utc_now
    from ..services.background_jobs import update_job_progress, update_job_step
    from ..services.cache_service import build_fundings_cache_for_ror

    run = FundingCacheRun(ror_id=ror_id, status="running", started_at=utc_now())
    db.session.add(run)
    db.session.commit()
    update_job_step(job_id, "fundings", "running")
    try:
        result = build_fundings_cache_for_ror(
            ror_id,
            base_url,
            {"Accept": "application/json"},
            return_result=True,
        )
        rows = result["fundings"]
        run.status = "partial" if result.get("errors") else "success"
        run.rows_count = rows
        update_job_progress(job_id, rows, rows, "records")
        update_job_step(job_id, "fundings", run.status, records_count=rows)
        return dict(result, ror_id=ror_id)
    except Exception as exc:
        db.session.rollback()
        run.status = "failed"
        run.error = str(exc)
        update_job_step(job_id, "fundings", "failed", error=str(exc))
        raise
    finally:
        run.finished_at = utc_now()
        db.session.add(run)
        db.session.commit()


def _run_profiles_sync_for_ror(ror_id: str, job_id: str | None = None) -> dict:
    """Rebuild researcher display names as a durable job."""
    from ..services.background_jobs import update_job_progress, update_job_step
    from ..services.cache_service import build_researcher_names_cache

    update_job_step(job_id, "profiles", "running")
    try:
        result = build_researcher_names_cache(ror_id, return_result=True)
        records = result["profiles"]
        update_job_progress(job_id, records, records, "records")
        update_job_step(job_id, "profiles", "partial" if result.get("errors") else "success", records_count=records)
        return dict(result, ror_id=ror_id)
    except Exception as exc:
        db.session.rollback()
        update_job_step(job_id, "profiles", "failed", error=str(exc))
        raise

@bp_works.route('/cache/works/build', methods=['POST'])
@login_required
def cache_works_build():
    """Wrapper to trigger the full build from legacy UI buttons."""
    return cache_full_build()

@bp_works.route('/cache/fundings/build', methods=['POST'])
@staff_required
def cache_fundings_build():
    """
    Isolated Funding Sync.
    Useful if the user specifically wants to update grants without waiting for publications.
    """
    from ..services.background_jobs import submit_background_job

    ror_id = get_active_ror_id()
    base_url = current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/')
    job_id = submit_background_job(
        current_app._get_current_object(),
        f"fundings-cache-{ror_id}",
        _run_fundings_sync_for_ror,
        ror_id,
        base_url,
        job_type="fundings_sync",
        ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
        steps=["fundings"],
        deduplicate=True,
    )
    flash_ok(_('Funding cache update started in the background. Job ID: %(job)s', job=job_id))
    return redirect(url_for('works.cache_works_status'))

@bp_works.route('/cache/profiles/build', methods=['POST'])
@staff_required
def cache_profiles_build():
    """Isolated Profile Sync."""
    from ..services.background_jobs import submit_background_job

    ror_id = get_active_ror_id()
    job_id = submit_background_job(
        current_app._get_current_object(),
        f"profiles-cache-{ror_id}",
        _run_profiles_sync_for_ror,
        ror_id,
        job_type="profiles_sync",
        ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
        steps=["profiles"],
        deduplicate=True,
    )
    flash_ok(_('Profile update started in the background. Job ID: %(job)s', job=job_id))
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

    from ..models import InstitutionSyncVersion
    latest_version = InstitutionSyncVersion.query.filter_by(ror_id=ror_id).order_by(InstitutionSyncVersion.created_at.desc()).first()
    return render_template(
        'works/cache_status.html',
        latest_version=latest_version,
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
    from ..services.canonical_work_service import canonical_review_groups
    review_page = max(request.args.get("page", 1, type=int), 1)
    return render_template(
        'works/data_quality.html',
        review_groups=canonical_review_groups(ror_id, page=review_page) if section == "integrity" else [],
        review_page=review_page,
        report=institution_quality_report(ror_id),
        section=section,
        can_manage=bool(session.get('is_admin') or session.get('is_manager')),
    )


@bp_works.route('/data-quality/review-canonical', methods=['POST'])
@staff_required
def data_quality_review_canonical():
    from ..services.canonical_work_service import review_canonical_records
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('works.data_quality'))
    try:
        action = request.form.get("action")
        if action not in {"merge", "split"}:
            raise ValueError("Invalid review action.")
        review_canonical_records(
            ror_id, [int(value) for value in request.form.getlist("record_ids")],
            merge=action == "merge", reason=request.form.get("reason", ""), user_id=session.get("user_id"),
        )
    except ValueError:
        db.session.rollback()
        flash_err(_('Select valid records from this institution and provide a review reason. Records with different DOIs cannot be merged.'))
    else:
        flash_ok(_('Publication grouping updated. The review decision will be preserved during synchronization.'))
    return redirect(url_for('works.data_quality', section='integrity'))


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
