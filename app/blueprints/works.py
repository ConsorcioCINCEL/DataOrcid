"""Work/funding cache management views and exports."""

import logging
from datetime import datetime as dt
from io import BytesIO

import pandas as pd
from flask import (
    Blueprint, request, redirect, url_for,
    send_file, current_app, render_template, session
)
from flask_babel import _
from sqlalchemy import func

from .. import db
from ..decorators import admin_required, login_required
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

bp_works = Blueprint("works", __name__)
logger = logging.getLogger(__name__)


def _format_datetime(value):
    """Return a stable string for spreadsheet exports."""
    return value.isoformat() if value else None


def _send_dataframe_export(data_frame: pd.DataFrame, base_name: str, sheet_name: str):
    """Send a dataframe as CSV by default or Excel when requested."""
    export_format = (request.args.get('format') or '').lower()

    if export_format == 'excel':
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            data_frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        output.seek(0)
        return send_file(
            output,
            as_attachment=True,
            download_name=f"{base_name}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    output = BytesIO(data_frame.to_csv(index=False).encode('utf-8-sig'))
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{base_name}.csv",
        mimetype='text/csv',
    )


def _institution_lookup() -> dict:
    """Map ROR IDs to readable institution names."""
    from ..services.institution_registry_service import get_institution_options

    return {
        item["ror_id"]: item.get("name") or item["ror_id"]
        for item in get_institution_options()
        if item.get("ror_id")
    }


def _institution_name(ror_id: str, institutions: dict) -> str:
    return institutions.get(ror_id) or ror_id or ""


def _chunks(items: list, size: int = 500):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _researcher_pairs():
    """Return unique (ror_id, orcid) pairs known by any local cache."""
    from ..models import FundingCache, ResearcherStatus, WorkCache

    pairs = set()
    sources = (
        db.session.query(WorkCache.ror_id, WorkCache.orcid).filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.orcid.isnot(None),
            WorkCache.orcid != "",
        ),
        db.session.query(FundingCache.ror_id, FundingCache.orcid).filter(
            FundingCache.ror_id.isnot(None),
            FundingCache.ror_id != "",
            FundingCache.orcid.isnot(None),
            FundingCache.orcid != "",
        ),
        db.session.query(ResearcherStatus.ror_id, ResearcherStatus.orcid).filter(
            ResearcherStatus.ror_id.isnot(None),
            ResearcherStatus.ror_id != "",
            ResearcherStatus.orcid.isnot(None),
            ResearcherStatus.orcid != "",
        ),
    )
    for query in sources:
        pairs.update((ror_id, orcid) for ror_id, orcid in query.distinct().all())
    return sorted(pairs)


def _researcher_count() -> int:
    return len(_researcher_pairs())


def _build_researchers_dataframe() -> pd.DataFrame:
    from ..models import FundingCache, ResearcherCache, ResearcherStatus, WorkCache

    institutions = _institution_lookup()
    pairs = _researcher_pairs()
    orcids = sorted({orcid for _, orcid in pairs})

    metadata = {}
    if orcids:
        for chunk in _chunks(orcids):
            rows = ResearcherCache.query.filter(ResearcherCache.orcid.in_(chunk)).all()
            metadata.update({row.orcid: row for row in rows})

    status_rows = []
    for chunk in _chunks(orcids):
        status_rows.extend(ResearcherStatus.query.filter(
            ResearcherStatus.orcid.in_(chunk)
        ).all())
    status_map = {
        (row.ror_id, row.orcid): bool(row.is_managed_by_am)
        for row in status_rows
    }

    works_counts = {
        (ror_id, orcid): count
        for ror_id, orcid, count in db.session.query(
            WorkCache.ror_id, WorkCache.orcid, func.count(WorkCache.id)
        ).filter(
            WorkCache.ror_id.isnot(None),
            WorkCache.ror_id != "",
            WorkCache.orcid.isnot(None),
            WorkCache.orcid != "",
        ).group_by(WorkCache.ror_id, WorkCache.orcid).all()
    }
    funding_counts = {
        (ror_id, orcid): count
        for ror_id, orcid, count in db.session.query(
            FundingCache.ror_id, FundingCache.orcid, func.count(FundingCache.id)
        ).filter(
            FundingCache.ror_id.isnot(None),
            FundingCache.ror_id != "",
            FundingCache.orcid.isnot(None),
            FundingCache.orcid != "",
        ).group_by(FundingCache.ror_id, FundingCache.orcid).all()
    }

    return pd.DataFrame([{
        'institution': _institution_name(ror_id, institutions),
        'ror_id': ror_id,
        'orcid': orcid,
        'given_names': getattr(metadata.get(orcid), 'given_names', None),
        'family_name': getattr(metadata.get(orcid), 'family_name', None),
        'credit_name': getattr(metadata.get(orcid), 'credit_name', None),
        'email': getattr(metadata.get(orcid), 'email', None),
        'is_managed_by_am': status_map.get((ror_id, orcid), False),
        'works_count': works_counts.get((ror_id, orcid), 0),
        'fundings_count': funding_counts.get((ror_id, orcid), 0),
        'profile_updated_at': _format_datetime(getattr(metadata.get(orcid), 'updated_at', None)),
    } for ror_id, orcid in pairs])


def _build_works_dataframe(records, institutions: dict | None = None) -> pd.DataFrame:
    institutions = institutions or _institution_lookup()
    return pd.DataFrame([{
        'institution': _institution_name(r.ror_id, institutions),
        'ror_id': r.ror_id,
        'orcid': r.orcid,
        'title': r.title,
        'type': r.type,
        'put_code': r.put_code,
        'journal_title': r.journal_title,
        'pub_year': r.pub_year,
        'pub_month': r.pub_month,
        'pub_day': r.pub_day,
        'doi': r.doi,
        'issn': r.issn,
        'other_external_ids': r.other_external_ids,
        'source': r.source,
        'url': r.url,
        'visibility': r.visibility,
        'created_at': _format_datetime(r.created_at),
    } for r in records])


def _build_fundings_dataframe(records, institutions: dict | None = None) -> pd.DataFrame:
    institutions = institutions or _institution_lookup()
    return pd.DataFrame([{
        'institution': _institution_name(r.ror_id, institutions),
        'ror_id': r.ror_id,
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
        'created_at': _format_datetime(r.created_at),
    } for r in records])


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


def _run_full_sync_for_ror(ror_id: str, base_url: str, headers: dict) -> dict:
    """Run works, fundings, and profile-name synchronization for one ROR."""
    from ..models import WorkCacheRun, FundingCacheRun
    from ..services.cache_service import (
        build_works_cache_for_ror,
        build_fundings_cache_for_ror,
        build_researcher_names_cache,
    )

    result = {"ror_id": ror_id, "works": 0, "fundings": 0, "profiles": 0, "errors": []}

    run_w = WorkCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run_w)
    db.session.commit()
    try:
        result["works"] = build_works_cache_for_ror(ror_id, base_url, headers)
        run_w.status = 'success'
        run_w.rows_count = result["works"]
    except Exception as exc:
        db.session.rollback()
        logger.exception("Works sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Works")
        run_w.status = 'failed'
        run_w.error = str(exc)
    finally:
        run_w.finished_at = dt.utcnow()
        db.session.add(run_w)
        db.session.commit()

    run_f = FundingCacheRun(ror_id=ror_id, status='running', started_at=dt.utcnow())
    db.session.add(run_f)
    db.session.commit()
    try:
        result["fundings"] = build_fundings_cache_for_ror(ror_id, base_url, headers)
        run_f.status = 'success'
        run_f.rows_count = result["fundings"]
    except Exception as exc:
        db.session.rollback()
        logger.exception("Fundings sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Fundings")
        run_f.status = 'failed'
        run_f.error = str(exc)
    finally:
        run_f.finished_at = dt.utcnow()
        db.session.add(run_f)
        db.session.commit()

    try:
        result["profiles"] = build_researcher_names_cache(ror_id)
    except Exception as exc:
        db.session.rollback()
        logger.exception("Profiles sync failed for ROR %s: %s", ror_id, exc)
        result["errors"].append("Profiles")

    return result


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

    result = _run_full_sync_for_ror(ror_id, base_url, headers)

    if not result["errors"]:
        flash_ok(_('Full synchronization complete: %(w)s works, %(f)s grants, and %(p)s profiles updated.', 
                   w=result["works"], f=result["fundings"], p=result["profiles"]))
    else:
        flash_err(_('Sync completed with errors in: %(err)s. Check logs.', err=", ".join(result["errors"])))

    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/cache/full/build-all', methods=['POST'])
@login_required
def cache_full_build_all():
    """Run the full metadata cache synchronization for every known institution."""
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

    totals = {"works": 0, "fundings": 0, "profiles": 0, "failed": 0}
    for institution in institutions:
        ror_id = institution.get("ror_id")
        if not ror_id:
            continue

        result = _run_full_sync_for_ror(ror_id, base_url, headers)
        totals["works"] += result["works"]
        totals["fundings"] += result["fundings"]
        totals["profiles"] += result["profiles"]
        if result["errors"]:
            totals["failed"] += 1

    if totals["failed"]:
        flash_err(_(
            "All-institution synchronization completed with %(failed)s institution errors. Totals: %(w)s works, %(f)s fundings, %(p)s profiles.",
            failed=totals["failed"], w=totals["works"], f=totals["fundings"], p=totals["profiles"],
        ))
    else:
        flash_ok(_(
            "All-institution synchronization complete: %(count)s institutions, %(w)s works, %(f)s fundings, %(p)s profiles.",
            count=len(institutions), w=totals["works"], f=totals["fundings"], p=totals["profiles"],
        ))

    return redirect(url_for('works.cache_works_status'))
# INDIVIDUAL OPERATIONS (Legacy / Specific)

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
        base_url = current_app.config.get('ORCID_SEARCH_URL', 'https://pub.orcid.org/v3.0/')
        rows = build_fundings_cache_for_ror(ror_id, base_url, {'Accept': 'application/json'})
        run.status = 'success'
        run.rows_count = rows
        flash_ok(_('Funding cache updated.'))
    except Exception as e:
        db.session.rollback()
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
    admin_researcher_count = _researcher_count() if session.get('is_admin') else 0
    admin_works_count = db.session.query(WorkCache.id).count() if session.get('is_admin') else 0
    admin_fundings_count = db.session.query(FundingCache.id).count() if session.get('is_admin') else 0

    return render_template(
        'works/cache_status.html',
        has_cache_works=(w_count > 0),
        last_run_works=last_run_works,
        count_works=w_count,
        has_cache_fundings=(f_count > 0),
        last_run_fundings=last_run_fundings,
        count_fundings=f_count,
        count_profiles=p_count,
        admin_researcher_count=admin_researcher_count,
        admin_works_count=admin_works_count,
        admin_fundings_count=admin_fundings_count,
    )
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
        data_frame = _build_works_dataframe(records)
        file_base_name = f"orcid_works_cache_{ror_id}"
        return _send_dataframe_export(data_frame, file_base_name, 'Works')
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
        data_frame = _build_fundings_dataframe(records)
        file_base_name = f"orcid_fundings_cache_{ror_id}"
        return _send_dataframe_export(data_frame, file_base_name, 'Fundings')
    except Exception as exc:
        logger.exception("EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/researchers/cache')
@admin_required
def download_all_researchers_admin():
    """Export every institution-researcher pair known in local caches."""
    data_frame = _build_researchers_dataframe()
    if data_frame.empty:
        flash_err(_('No researcher cache data available.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        return _send_dataframe_export(
            data_frame,
            'orcid_researchers_all_institutions',
            'Researchers',
        )
    except Exception as exc:
        logger.exception("ADMIN RESEARCHERS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-works/cache')
@admin_required
def download_all_works_admin():
    """Export cached works for all institutions."""
    from ..models import WorkCache

    records = WorkCache.query.order_by(WorkCache.ror_id, WorkCache.orcid, WorkCache.id).all()
    if not records:
        flash_err(_('The publication cache is currently empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_frame = _build_works_dataframe(records)
        return _send_dataframe_export(
            data_frame,
            'orcid_works_all_institutions',
            'Works',
        )
    except Exception as exc:
        logger.exception("ADMIN WORKS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-fundings/cache')
@admin_required
def download_all_fundings_admin():
    """Export cached fundings for all institutions."""
    from ..models import FundingCache

    records = FundingCache.query.order_by(FundingCache.ror_id, FundingCache.orcid, FundingCache.id).all()
    if not records:
        flash_err(_('The funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        data_frame = _build_fundings_dataframe(records)
        return _send_dataframe_export(
            data_frame,
            'orcid_fundings_all_institutions',
            'Fundings',
        )
    except Exception as exc:
        logger.exception("ADMIN FUNDINGS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))
