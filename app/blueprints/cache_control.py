"""
Module: cache_control.py
Author: Gastón Olivares
Project: DataOrcid-Chile
License: MIT
Description:
    Blueprint responsible for triggering on-demand cache rebuilds via the Web UI.
    
    It acts as a secure controller that:
    1. Validates user permissions (Admin/Manager role required).
    2. Retrieves valid ORCID Member API credentials (Client Credentials flow).
    3. Delegates the data fetching logic to the service layer.
    4. Logs the execution results (success/failure) for system auditing.
"""

import logging
import datetime as dt
from flask import Blueprint, redirect, url_for, session, current_app, request
from flask_babel import _

from .. import db
from ..decorators import login_required
from ..models import WorkCacheRun, FundingCacheRun
from ..services.cache_service import build_works_cache_for_ror, build_fundings_cache_for_ror
from ..services.orcid_service import get_client_credentials_token
from ..utils.flashes import flash_err, flash_success

# --- Blueprint Configuration ---
bp_cache = Blueprint("cache_control", __name__, url_prefix="/cache-control")
logger = logging.getLogger(__name__)


# ============================================================
# INTERNAL HELPERS
# ============================================================

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
        base_url = current_app.config.get('ORCID_BASE_URL_MEMBER')
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
        execution_time = dt.datetime.utcnow()
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


# ============================================================
# ADMINISTRATIVE ROUTES
# ============================================================

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
    # 1. Access Control (RBAC)
    if not (session.get('is_admin') or session.get('is_manager')):
        flash_err(_("You do not have sufficient permissions to perform this action."))
        return redirect(url_for('main.index'))

    # 2. Context Resolution
    # Admins can select a different ROR than their own for management purposes.
    ror_id = session.get('admin_selected_ror') or session.get('ror_id')
    
    if not ror_id:
        flash_err(_("No active institution (ROR ID) selected. Please select one first."))
        return redirect(url_for('main.index'))

    # 3. API Preparation
    base_url, headers = _get_api_context()
    
    logger.info("Starting cache rebuild. Target: %s | ROR ID: %s | API: %s", 
                target, ror_id, base_url)

    if not base_url or not headers:
        flash_err(_("Authentication failed: Could not obtain a valid ORCID Member Token."))
        return redirect(request.referrer or url_for('main.index'))

    # 4. Execution
    count = 0
    try:
        if target == 'works':
            # Calls the optimized multithreaded service
            count = build_works_cache_for_ror(ror_id, base_url, headers)
            _log_run_web(WorkCacheRun, ror_id, 'success', count)
            flash_success(_("Works cache successfully rebuilt: %(count)s records updated.", count=count))
            
        elif target == 'fundings':
            # Calls the optimized multithreaded service
            count = build_fundings_cache_for_ror(ror_id, base_url, headers)
            _log_run_web(FundingCacheRun, ror_id, 'success', count)
            flash_success(_("Funding cache successfully rebuilt: %(count)s records updated.", count=count))
            
        else:
            flash_err(_("Invalid cache target specified: '%(target)s'.", target=target))

    except Exception as exc:
        # 5. Error Handling & Auditing
        logger.exception("Cache rebuild failed for target %s and ROR %s", target, ror_id)
        
        error_str = str(exc)
        flash_err(_("Critical error during cache rebuild: %(error)s", error=error_str))
        
        # Log failure state to DB for admin review
        log_model = WorkCacheRun if target == 'works' else FundingCacheRun
        _log_run_web(log_model, ror_id, 'failed', 0, error_str)

    return redirect(request.referrer or url_for('main.index'))