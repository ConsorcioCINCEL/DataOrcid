"""Admin web endpoints for Member API cache rebuilds."""

import logging
from flask import Blueprint, redirect, url_for, session, current_app, request
from flask_babel import _

from .. import db
from ..decorators import login_required
from ..models import FundingCacheRun, WorkCacheRun, utc_now
from ..services.cache_service import build_works_cache_for_ror, build_fundings_cache_for_ror
from ..services.background_jobs import submit_background_job
from ..services.orcid_service import get_client_credentials_token
from ..utils.session_helpers import get_active_ror_id
from ..utils.flashes import flash_err, flash_success

bp_cache = Blueprint("cache_control", __name__, url_prefix="/cache-control")
logger = logging.getLogger(__name__)
def _get_api_context():
    """
    Retrieves the required security credentials for the ORCID Member API.
    
    This helper ensures that the Access Token is correctly formatted as a Bearer token
    and determines the correct API Endpoint (Sandbox vs Production) based on configuration.
    
    Returns:
        tuple: (base_url, headers) if successful, otherwise (None, None).
    """
    # Fetch a fresh token using Client Credentials Flow
    token = get_client_credentials_token()
    
    if token:
        # Standardize Authorization header format
        if not token.startswith('Bearer '):
            token = f"Bearer {token}"
            
        # Determine target API URL (Member API features higher rate limits)
        base_url = current_app.config.get('ORCID_MEMBER_URL')
        if not base_url:
            # Fallback to default production endpoint if config is missing
            base_url = 'https://api.orcid.org/v3.0/'
            
        headers = {
            'Accept': 'application/json',
            'Authorization': token
        }
        return base_url, headers
    
    return None, None


def _log_run_web(model_class, ror_id, status, count, error_msg=None):
    """
    Persists the result of a cache rebuild execution into the database.
    Used for the 'System Usage Logs' dashboard.
    
    Args:
        model_class: The SQLAlchemy model to write to (WorkCacheRun or FundingCacheRun).
        ror_id (str): The ROR ID of the institution being processed.
        status (str): Execution outcome ('success' or 'failed').
        count (int): Number of records processed/cached.
        error_msg (str, optional): Detailed error message trace if failed.
    """
    try:
        execution_time = utc_now()
        run = model_class(
            ror_id=ror_id,
            status=status,
            rows_count=count,
            error=error_msg,
            started_at=execution_time,
            finished_at=execution_time
        )
        db.session.add(run)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        logger.error("Failed to save execution log for ROR %s: %s", ror_id, exc)


def _run_member_cache_rebuild(target: str, ror_id: str, base_url: str, headers: dict) -> None:
    """Run a member API cache rebuild outside the request lifecycle."""
    try:
        if target == 'works':
            count = build_works_cache_for_ror(ror_id, base_url, headers)
            _log_run_web(WorkCacheRun, ror_id, 'success', count)
        elif target == 'fundings':
            count = build_fundings_cache_for_ror(ror_id, base_url, headers)
            _log_run_web(FundingCacheRun, ror_id, 'success', count)
    except Exception as exc:
        db.session.rollback()
        logger.exception("Cache rebuild failed for target %s and ROR %s", target, ror_id)
        log_model = WorkCacheRun if target == 'works' else FundingCacheRun
        _log_run_web(log_model, ror_id, 'failed', 0, str(exc))
        raise


@bp_cache.route('/rebuild/<target>', methods=['POST'])
@login_required
def rebuild_cache(target):
    """
    Route: /cache-control/rebuild/<target>
    Method: POST
    
    Trigger a complete rebuild of the local cache for the active institution.
    
    Args:
        target (str): The entity to rebuild. Options: 'works' or 'fundings'.
    """
    if not (session.get('is_admin') or session.get('is_manager')):
        flash_err(_("You do not have sufficient permissions to perform this action."))
        return redirect(url_for('main.index'))
    # Admins can select a different ROR than their own for management purposes.
    ror_id = get_active_ror_id()
    
    if not ror_id:
        flash_err(_("No active institution (ROR ID) selected. Please select one first."))
        return redirect(url_for('main.index'))
    base_url, headers = _get_api_context()
    
    logger.info("Starting cache rebuild. Target: %s | ROR ID: %s | API: %s", 
                target, ror_id, base_url)

    if not base_url or not headers:
        flash_err(_("Authentication failed: Could not obtain a valid ORCID Member Token."))
        return redirect(request.referrer or url_for('main.index'))
    if target not in {'works', 'fundings'}:
        flash_err(_("Invalid cache target specified: '%(target)s'.", target=target))
        return redirect(request.referrer or url_for('main.index'))

    app_obj = current_app._get_current_object()
    job_id = submit_background_job(
        app_obj,
        f"member-cache-{target}-{ror_id}",
        _run_member_cache_rebuild,
        target,
        ror_id,
        base_url,
        headers,
        job_type=f"member_{target}_sync",
        ror_id=ror_id,
        requested_by_user_id=session.get("user_id"),
        steps=[target],
    )
    flash_success(_("Cache rebuild started in the background. Job ID: %(job)s", job=job_id))

    return redirect(request.referrer or url_for('main.index'))
