"""
Module: works.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Works and Fundings Management Blueprint.
    
    This module orchestrates the core data synchronization tasks. It handles:
    1. The 'Master Sync' process (Works -> Fundings -> Profiles).
    2. Monitoring the status of cache updates.
    3. Exporting cached data to CSV/Excel for external analysis.
"""

import logging
from datetime import datetime as dt
from io import BytesIO

import pandas as pd
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
bp_works = Blueprint("works", __name__)
logger = logging.getLogger(__name__)


# ============================================================
# INTERNAL HELPERS
# ============================================================

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
    from ..models import FundingCacheRun
    return (
        FundingCacheRun.query
        .filter_by(ror_id=ror_id, status='success')
        .order_by(FundingCacheRun.finished_at.desc())
        .first()
    )

# ============================================================
# MASTER SYNC OPERATION
# ============================================================

@bp_works.route('/cache/full/build', methods=['POST'])
@login_required
def cache_full_build():
    """
    Executes the complete synchronization sequence for an institution.
    
    Sequence:
    1. Works (Publications)
    2. Fundings (Grants)
    3. Researcher Profiles (Names/Bio) - Optimized Multithreaded
    
    This ensures data consistency across all related tables in a single operation.
    """
    from ..models import WorkCacheRun, FundingCacheRun
    from ..services.cache_service import (
        build_works_cache_for_ror, 
        build_fundings_cache_for_ror, 
        build_researcher_names_cache
    )

    # 1. Validation
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('works.cache_works_status'))

    # Setup API Context (Defaulting to Public API)
    base_url = current_app.config.get('ORCID_BASE_URL_PUBLIC', 'https://pub.orcid.org/v3.0/')
    headers = {'Accept': 'application/json'}
    
    total_w, total_f, total_p = 0, 0, 0
    errors = []

    # ---------------------------------------------------------
    # STEP 1: SYNC WORKS
    # ---------------------------------------------------------
    try:
        run_w = WorkCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
        db.session.add(run_w)
        db.session.commit()
        
        total_w = build_works_cache_for_ror(ror_id, base_url, headers)
        
        run_w.status = 'success'
        run_w.rows_count = total_w
    except Exception as e:
        logger.error(f"Works Sync Failed: {e}")
        errors.append("Works")
    finally:
        # Safely close the run log even if an error occurred
        if 'run_w' in locals():
            run_w.finished_at = dt.utcnow()
            db.session.commit()

    # ---------------------------------------------------------
    # STEP 2: SYNC FUNDINGS
    # ---------------------------------------------------------
    try:
        run_f = FundingCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
        db.session.add(run_f)
        db.session.commit()
        
        total_f = build_fundings_cache_for_ror(ror_id, base_url, headers)
        
        run_f.status = 'success'
        run_f.rows_count = total_f
    except Exception as e:
        logger.error(f"Fundings Sync Failed: {e}")
        errors.append("Fundings")
    finally:
        if 'run_f' in locals():
            run_f.finished_at = dt.utcnow()
            db.session.commit()

    # ---------------------------------------------------------
    # STEP 3: SYNC PROFILES
    # ---------------------------------------------------------
    try:
        # Updates names/bios for all ORCIDs found in steps 1 and 2
        total_p = build_researcher_names_cache(ror_id)
    except Exception as e:
        logger.error(f"Profiles Sync Failed: {e}")
        errors.append("Profiles")

    # Feedback to User
    if not errors:
        flash_ok(_('Full synchronization complete: %(w)s works, %(f)s grants, and %(p)s profiles updated.', 
                   w=total_w, f=total_f, p=total_p))
    else:
        flash_err(_('Sync completed with errors in: %(err)s. Check logs.', err=", ".join(errors)))

    return redirect(url_for('works.cache_works_status'))


# ============================================================
# INDIVIDUAL OPERATIONS (Legacy / Specific)
# ============================================================

@bp_works.route('/cache/works/build', methods=['POST'])
@login_required
def cache_works_build():
    """Wrapper to trigger the full build from legacy UI buttons."""
    return cache_full_build() 

@bp_works.route('/cache/fundings/build', methods=['POST'])
@login_required
def cache_fundings_build():
    """
    Isolated Funding Sync. 
    Useful if the user specifically wants to update grants without waiting for publications.
    """
    from ..models import FundingCacheRun
    from ..services.cache_service import build_fundings_cache_for_ror
    ror_id = get_active_ror_id()
    
    run = FundingCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run)
    db.session.commit()
    try:
        base_url = current_app.config.get('ORCID_BASE_URL_PUBLIC', 'https://pub.orcid.org/v3.0/')
        rows = build_fundings_cache_for_ror(ror_id, base_url, {'Accept': 'application/json'})
        run.status = 'success'
        run.rows_count = rows
        flash_ok(_('Funding cache updated.'))
    except Exception as e:
        run.status = 'failed'
        run.error = str(e)
        flash_err(_('Error updating fundings.'))
    finally:
        run.finished_at = dt.utcnow()
        db.session.commit()
    return redirect(url_for('works.cache_works_status'))

@bp_works.route('/cache/profiles/build', methods=['POST'])
@login_required
def cache_profiles_build():
    """Isolated Profile Sync."""
    from ..services.cache_service import build_researcher_names_cache
    ror_id = get_active_ror_id()
    try:
        c = build_researcher_names_cache(ror_id)
        flash_ok(_('Profiles updated: %s', c))
    except Exception:
        flash_err(_('Error updating profiles'))
    return redirect(url_for('works.cache_works_status'))


# ============================================================
# MONITORING VIEW
# ============================================================

@bp_works.route('/cache/works/status')
@login_required
def cache_works_status():
    """
    Renders the Data Management Dashboard.
    Displays last run times, record counts, and action buttons for sync/export.
    """
    from ..models import WorkCache, FundingCache, ResearcherCache

    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    # Gather Statistics
    last_run_works = _last_cache_run_works(ror_id)
    w_count = db.session.query(WorkCache.id).filter_by(ror_id=ror_id).count()

    last_run_fundings = _last_cache_run_fundings(ror_id)
    f_count = db.session.query(FundingCache.id).filter_by(ror_id=ror_id).count()
    
    # Global profile count (Approximate)
    p_count = db.session.query(ResearcherCache.orcid).count() 

    return render_template(
        'works/cache_status.html',
        has_cache_works=(w_count > 0),
        last_run_works=last_run_works,
        count_works=w_count,
        has_cache_fundings=(f_count > 0),
        last_run_fundings=last_run_fundings,
        count_fundings=f_count,
        count_profiles=p_count
    )


# ============================================================
# EXPORT OPERATIONS
# ============================================================

@bp_works.route('/download/all-works/cache')
@login_required
def download_all_works_cache():
    """
    Exports the complete Works cache for the current institution.
    """
    from ..models import WorkCache
    ror_id = get_active_ror_id()
    
    records = WorkCache.query.filter_by(ror_id=ror_id).all()
    if not records:
        flash_err(_('The publication cache is currently empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        # Flatten data for export
        data_frame = pd.DataFrame([{
            'orcid': r.orcid, 'title': r.title, 'type': r.type,
            'journal': r.journal_title, 'pub_year': r.pub_year,
            'doi': r.doi, 'url': r.url, 'source': r.source
        } for r in records])

        export_format = (request.args.get('format') or '').lower()
        file_base_name = f"orcid_works_cache_{ror_id}"

        # Excel Export
        if export_format == 'excel':
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                data_frame.to_excel(writer, sheet_name='Works', index=False)
            output.seek(0)
            return send_file(output, as_attachment=True, download_name=f"{file_base_name}.xlsx")

        # CSV Export (Default)
        output = BytesIO(data_frame.to_csv(index=False).encode('utf-8-sig'))
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{file_base_name}.csv", mimetype='text/csv')
    except Exception as exc:
        logger.exception("EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/all-fundings/cache')
@login_required
def download_all_fundings_cache():
    """
    Exports the complete Funding cache for the current institution.
    """
    from ..models import FundingCache
    ror_id = get_active_ror_id()
    
    records = FundingCache.query.filter_by(ror_id=ror_id).all()
    if not records:
        flash_err(_('The funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_list = []
        for r in records:
            # Robust retrieval of organization name (handles schema variations)
            org = getattr(r, 'organization', getattr(r, 'organization_name', getattr(r, 'org_name', '—')))
            data_list.append({
                'orcid': r.orcid,
                'title': r.title,
                'type': r.type,
                'organization': org,
                'amount': getattr(r, 'amount', '—'),
                'currency': getattr(r, 'currency', '—'),
                'start_year': getattr(r, 'start_year', getattr(r, 'start_y', '—')),
                'source': r.source
            })
        
        data_frame = pd.DataFrame(data_list)
        export_format = (request.args.get('format') or '').lower()
        file_base_name = f"orcid_fundings_cache_{ror_id}"

        if export_format == 'excel':
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                data_frame.to_excel(writer, sheet_name='Fundings', index=False)
            output.seek(0)
            return send_file(output, as_attachment=True, download_name=f"{file_base_name}.xlsx")

        output = BytesIO(data_frame.to_csv(index=False).encode('utf-8-sig'))
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{file_base_name}.csv", mimetype='text/csv')
    except Exception as exc:
        logger.exception("EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))