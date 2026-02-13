"""
Module: fundings.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Funding Cache Management Blueprint.
    
    This module handles the lifecycle of funding/grant data associated with an institution (ROR).
    It provides endpoints to:
    1. Trigger a rebuild of the local funding cache (fetching fresh data from ORCID).
    2. Check the status of cache operations.
    3. Export the cached funding data to Excel (.xlsx) or CSV formats for analysis.
"""

from io import BytesIO
import pandas as pd
import logging
from datetime import datetime as dt
from flask import (
    Blueprint, request, redirect, url_for,
    send_file, current_app, render_template
)
from flask_babel import _

from .. import db
from ..decorators import login_required
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

# --- Blueprint Configuration ---
bp_fund = Blueprint("fundings", __name__)
logger = logging.getLogger(__name__)


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _has_cache_fundings(ror_id: str) -> bool:
    """
    Checks if there is any funding data cached for the specified ROR ID.
    
    Args:
        ror_id (str): The Research Organization Registry identifier.
        
    Returns:
        bool: True if records exist, False otherwise.
    """
    # Lazy import to avoid circular dependencies during app initialization
    from ..models import FundingCache
    return db.session.query(FundingCache.id).filter_by(ror_id=ror_id).first() is not None


def _last_cache_run_fundings(ror_id: str):
    """
    Retrieves the metadata of the last successful cache synchronization run.
    Useful for displaying "Last Updated" timestamps in the UI.
    
    Args:
        ror_id (str): The target ROR ID.
        
    Returns:
        FundingCacheRun: The database model instance or None.
    """
    from ..models import FundingCacheRun
    return (
        FundingCacheRun.query
        .filter_by(ror_id=ror_id, status='success')
        .order_by(FundingCacheRun.finished_at.desc())
        .first()
    )


# ============================================================
# CACHE BUILDER ENDPOINTS
# ============================================================

@bp_fund.route('/cache/fundings/build', methods=['POST'])
@login_required
def cache_fundings_build():
    """
    Triggers the process to build/rebuild the funding cache for the active institution.
    
    Process:
    1. Creates a 'running' entry in the FundingCacheRun audit log.
    2. Calls the service layer to fetch data from the ORCID Public API.
    3. Updates the log with the result (success/failure) and row count.
    
    Note: 
    This uses the Public API by default (`ORCID_BASE_URL_PUBLIC`). 
    For higher rate limits, consider using the Member API via the `cache_control` blueprint.
    """
    from ..models import FundingCacheRun
    from ..services.cache_service import build_fundings_cache_for_ror

    # 1. Context Resolution
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No ROR ID found in session.'))
        return redirect(url_for('main.index'))

    # 2. Audit Log Initialization
    run = FundingCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run)
    db.session.commit()

    try:
        # 3. Execution
        # We default to the Public API for this general user route
        base_url = current_app.config.get('ORCID_BASE_URL_PUBLIC', 'https://pub.orcid.org/v3.0/')
        headers = {'Accept': 'application/json'}

        # The service layer handles logic for ROR->GRID resolution and fetching
        rows_count = build_fundings_cache_for_ror(ror_id, base_url, headers)
        
        # 4. Success Handling
        run.status = 'success'
        run.rows_count = rows_count
        flash_ok(_('Funding cache created successfully: %(count)s records.', count=rows_count))

    except Exception as e:
        # 5. Error Handling
        logger.exception("Failed building funding cache for ROR %s", ror_id)
        run.status = 'failed'
        run.error = str(e)
        flash_err(_('Error building funding cache. Check logs.'))
    finally:
        # 6. Finalization
        run.finished_at = dt.utcnow()
        db.session.commit()

    # Redirect back to the status dashboard (usually shared with Works status)
    return redirect(url_for('works.cache_works_status'))


# ============================================================
# EXPORT ENDPOINTS
# ============================================================

@bp_fund.route('/download/all-fundings/cache')
@login_required
def download_all_fundings_cache():
    """
    Generates a downloadable report of all cached funding records for the active ROR.
    
    Supported Formats:
    - CSV (default): Best for data interoperability.
    - Excel (.xlsx): Best for human analysis.
    
    Returns:
        File Response: The generated file stream.
    """
    from ..models import FundingCache

    # 1. Validation
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No ROR ID found in session.'))
        return redirect(url_for('works.cache_works_status'))

    # Check if data exists before attempting generation
    q = FundingCache.query.filter_by(ror_id=ror_id)
    if not db.session.query(q.exists()).scalar():
        flash_err(_('No funding cache available. Please run "Download from scratch".'))
        return redirect(url_for('works.cache_works_status'))

    rows = q.all()
    if not rows:
        flash_err(_('Funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        # 2. Data Transformation (SQLAlchemy -> Pandas DataFrame)
        # We explicitly map fields to ensure clean column names in the export
        df = pd.DataFrame([{
            'orcid': r.orcid,
            'title': r.title,
            'type': r.type,
            'org_name': r.org_name,
            'city': r.city,
            'country': r.country,
            'start_y': r.start_y,
            'start_m': r.start_m,
            'start_d': r.start_d,
            'end_y': r.end_y,
            'end_m': r.end_m,
            'end_d': r.end_d,
            'grant_number': r.grant_number,
            'currency': r.currency,
            'amount': r.amount,
            'source': r.source,
            'url': r.url,
            'visibility': r.visibility,
            'created_at': (
                r.created_at.isoformat() if getattr(r, 'created_at', None) else None
            )
        } for r in rows])

        # 3. File Generation
        export_format = (request.args.get('format') or '').lower()
        base_name = f"all_fundings_cache_{ror_id}"

        # Option A: Excel Export
        if export_format == 'excel':
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Fundings', index=False)
            output.seek(0)
            
            return send_file(
                output,
                as_attachment=True,
                download_name=f"{base_name}.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        # Option B: CSV Export (Default)
        # using 'utf-8-sig' ensures Excel opens the CSV with correct character encoding
        output = BytesIO(df.to_csv(index=False).encode('utf-8-sig'))
        output.seek(0)
        
        return send_file(
            output,
            as_attachment=True,
            download_name=f"{base_name}.csv",
            mimetype='text/csv'
        )

    except Exception as e:
        logger.exception("Error exporting funding cache for ROR %s: %s", ror_id, e)
        flash_err(_("An error occurred generating the export file."))
        return redirect(url_for('works.cache_works_status'))