"""
Module: main.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Core System Blueprint.
    
    This module manages the primary user interface and dashboard logic.
    It handles:
    1. The landing page (Index) with high-level stats.
    2. The Researcher Directory (fetching live data from ORCID).
    3. The Analytics Panel (charts, trends, and top performers).
    4. Integration guides and static resource serving.
"""

import os
import io
import datetime as dt
import logging
import pandas as pd
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    session, send_from_directory, current_app, send_file
)
from flask_babel import _
from sqlalchemy import func, inspect, desc

from .. import db
from ..models import (
    WorkCache, FundingCache, WorkCacheRun, 
    FundingCacheRun, ResearcherStatus, User, ResearcherCache
)
from ..decorators import login_required
from ..utils.flashes import flash_err
from ..utils.session_helpers import get_active_ror_id
from ..services.orcid_service import (
    get_full_orcid_profile, 
    list_orcids_for_institution, 
    get_client_credentials_token
)
from ..services.cache_service import ensure_and_heal_grid_for_ror

# --- Blueprint Configuration ---
bp_main = Blueprint("main", __name__)
logger = logging.getLogger(__name__)


# ============================================================
# CONTEXT PROCESSORS (GLOBAL VARIABLES)
# ============================================================

@bp_main.app_context_processor
def inject_global_vars():
    """
    Injects variables available to all Jinja2 templates (e.g., base.html).
    
    Functionality:
    - Calculates the 'last_works_update' timestamp for the active institution.
    - This allows the UI to show "Last updated: X mins ago" in the footer/header.
    """
    ror_id = get_active_ror_id()
    last_update = None
    
    if ror_id:
        try:
            # Prioritize Works cache timestamp, fall back to Fundings if necessary
            last_run = WorkCacheRun.query.filter_by(ror_id=ror_id, status='success').order_by(WorkCacheRun.finished_at.desc()).first()
            if not last_run:
                last_run = FundingCacheRun.query.filter_by(ror_id=ror_id, status='success').order_by(FundingCacheRun.finished_at.desc()).first()
            
            if last_run and last_run.finished_at:
                last_update = last_run.finished_at.strftime("%Y-%m-%d %H:%M")
        except Exception:
            # Fail silently to avoid breaking the entire page render
            pass 
            
    return dict(last_works_update=last_update)


# ============================================================
# SYSTEM DASHBOARD & NAVIGATION
# ============================================================

@bp_main.route('/')
@login_required
def index():
    """
    Main landing view (Dashboard Home).
    
    Logic:
    1. Determines the active ROR context.
    2. Checks the local database for cached records (Works/Fundings).
    3. If no local cache exists, attempts to fetch a *live count* of researchers
       from ORCID to show the user what *could* be imported.
    """
    ror_id = get_active_ror_id()
    researcher_count = None
    affiliation_hist = None
    
    # Initialize counters
    works_count = 0
    fundings_count = 0

    if ror_id:
        # Fetch local DB stats
        works_count = db.session.query(WorkCache.id).filter_by(ror_id=ror_id).count()
        fundings_count = db.session.query(FundingCache.id).filter_by(ror_id=ror_id).count()

        # Count unique researchers in local cache
        rc = db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id).distinct().count()
        if rc == 0:
            rc = db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id).distinct().count()

        # If local cache is empty, query the ORCID Search API for a live estimate
        if rc == 0:
            try:
                # Ensure we have a GRID ID (ORCID often requires GRID for affiliation search)
                grid_id = ensure_and_heal_grid_for_ror(ror_id)
                token = get_client_credentials_token()
                
                # Determine API endpoint (Member vs Public) based on config
                base_url = current_app.config.get('ORCID_MEMBER_URL') or current_app.config.get('ORCID_SEARCH_URL')
                headers = {'Accept': 'application/json'}
                if token: headers['Authorization'] = f'Bearer {token}'
                
                researchers = list_orcids_for_institution(ror_id, grid_id, base_url, headers) or []
                rc = len(researchers)
                affiliation_hist = {1: rc} if rc else {}
            except Exception as exc:
                logger.warning("Failed to query live ORCID count: %s", exc)
        
        researcher_count = rc
        
        # Fallback to session history if live query fails
        if affiliation_hist is None:
            pre = session.get('affiliation_hist')
            affiliation_hist = pre if (isinstance(pre, dict) and pre) else ({1: rc} if rc else {})

    has_any_cache = bool(works_count > 0 or fundings_count > 0)
    
    return render_template(
        'main/index.html',
        researcher_count=researcher_count,
        affiliation_hist=affiliation_hist,
        works_count=works_count,
        fundings_count=fundings_count,
        has_any_cache=has_any_cache
    )


@bp_main.route('/researcher-list')
@login_required
def researcher_list():
    """
    Retrieves and displays the directory of researchers.
    
    This view combines:
    1. Live search results from ORCID (for the most up-to-date list).
    2. Local status flags (e.g., if a researcher is 'Managed' by Affiliation Manager).
    """
    ror_id = session.get('admin_selected_ror') or session.get('ror_id')
    if not ror_id:
        flash_err(_('No active ROR ID found.'))
        return redirect(url_for('main.index'))

    # Prepare API Request
    grid_id = ensure_and_heal_grid_for_ror(ror_id)
    token = get_client_credentials_token()
    base_url = current_app.config.get('ORCID_SEARCH_URL')
    headers = {'Accept': 'application/json'}
    if token: headers['Authorization'] = f'Bearer {token}'

    try:
        # Fetch list from ORCID
        all_researchers = list_orcids_for_institution(ror_id, grid_id, base_url, headers)
        
        if all_researchers:
            # Augment list with local 'Managed' status
            orcid_list = [r.get('orcid-id') for r in all_researchers if r.get('orcid-id')]
            status_map = {}
            if orcid_list:
                rows = ResearcherStatus.query.filter(ResearcherStatus.ror_id == ror_id, ResearcherStatus.orcid.in_(orcid_list)).all()
                status_map = {row.orcid: row.is_managed_by_am for row in rows}
            
            for r in all_researchers:
                r['is_managed'] = status_map.get(r.get('orcid-id'), False)

    except Exception as exc:
        logger.exception("CRITICAL: ORCID API error: %s", exc)
        flash_err(_('Could not communicate with ORCID services.'))
        return redirect(url_for('main.index'))

    session['researcher_count'] = len(all_researchers)
    return render_template('main/researcher_list.html', researchers=all_researchers)


@bp_main.route('/works-fundings')
@login_required
def cache_dashboard():
    """Shortcut redirect to the detailed data download page."""
    return redirect(url_for('works.cache_works_status'))


# ============================================================
# DATA MANAGEMENT & ANALYTICS
# ============================================================

def _resolve_name(orcid):
    """
    Internal Helper: Fetches a researcher's name from the live ORCID API.
    Used as a fallback when local name cache is missing.
    """
    try:
        profile = get_full_orcid_profile(orcid)
        if not profile: return _('Unknown Researcher')
        name_node = profile.get('person', {}).get('name')
        if not name_node: return _('Unknown Researcher')
        
        credit = name_node.get('credit-name')
        if credit and credit.get('value'): return credit['value']
        
        given = name_node.get('given-names', {}).get('value', '')
        family = name_node.get('family-name', {}).get('value', '')
        full = f"{given} {family}".strip()
        
        return full if full else _('Unknown Researcher')
    except Exception:
        return _('Unknown Researcher')


@bp_main.route('/metrics-panel')
@login_required
def metrics_panel():
    """
    Aggregates and renders key performance indicators (KPIs) and charts.
    
    Data provided:
    - Total Works/Fundings counts.
    - Trends over years (Works & Fundings).
    - Top Journals & Funding Organizations.
    - Top Researchers (by volume of works/grants).
    
    Note:
    Top researcher names are retrieved from the local `ResearcherCache` table
    to avoid N+1 API calls during page load.
    """
    ror_id = get_active_ror_id()
    
    metrics = {
        'total_works': 0, 'total_fundings': 0, 'active_researchers': 0, 'last_update': 'N/A',
        'chart_years': [], 'chart_counts': [],
        'chart_types_labels': [], 'chart_types_values': [],
        'chart_journals_labels': [], 'chart_journals_values': [],
        'top_researchers_works': [],
        'chart_funding_years': [], 'chart_funding_counts': [],
        'chart_funding_types_labels': [], 'chart_funding_types_values': [],
        'chart_funding_orgs_labels': [], 'chart_funding_orgs_values': [],
        'top_researchers_fundings': []
    }

    if not ror_id:
        return render_template('main/metrics.html', stats=metrics)

    # 1. Basic KPI Counts
    try:
        metrics['total_works'] = WorkCache.query.filter_by(ror_id=ror_id).count()
        metrics['total_fundings'] = FundingCache.query.filter_by(ror_id=ror_id).count()
        
        w_orcids = db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id)
        f_orcids = db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id)
        metrics['active_researchers'] = w_orcids.union(f_orcids).distinct().count()
        
        last_run = WorkCacheRun.query.filter_by(ror_id=ror_id, status='success').order_by(WorkCacheRun.finished_at.desc()).first()
        if last_run and last_run.finished_at:
            metrics['last_update'] = last_run.finished_at.strftime("%Y-%m-%d %H:%M")
    except Exception as e: logger.error(f"Error in basic counts: {e}")

    # 2. Chart: Works Trend (by Year)
    try:
        year_stats = db.session.query(WorkCache.pub_year, func.count(WorkCache.id)).filter_by(ror_id=ror_id).group_by(WorkCache.pub_year).order_by(WorkCache.pub_year).all()
        metrics['chart_years'] = [str(row[0]) for row in year_stats if row[0] and str(row[0]).isdigit()]
        metrics['chart_counts'] = [row[1] for row in year_stats if row[0] and str(row[0]).isdigit()]
    except Exception as e: logger.error(f"Error in Works Trend: {e}")

    # 3. Chart: Works Types (Article, Conference, etc.)
    try:
        type_stats = db.session.query(WorkCache.type, func.count(WorkCache.id)).filter_by(ror_id=ror_id).group_by(WorkCache.type).all()
        metrics['chart_types_labels'] = [str(row[0]).replace('_', ' ').title() for row in type_stats]
        metrics['chart_types_values'] = [row[1] for row in type_stats]
    except Exception as e: logger.error(f"Error in Work Types: {e}")

    # 4. Chart: Funding Trend (by Start Year)
    try:
        f_year_stats = db.session.query(FundingCache.start_y, func.count(FundingCache.id)).filter_by(ror_id=ror_id).group_by(FundingCache.start_y).order_by(FundingCache.start_y).all()
        metrics['chart_funding_years'] = [str(row[0]) for row in f_year_stats if row[0] and str(row[0]).isdigit()]
        metrics['chart_funding_counts'] = [row[1] for row in f_year_stats if row[0] and str(row[0]).isdigit()]
    except Exception as e: logger.error(f"Error in Funding Trend: {e}")

    # 5. Chart: Funding Types
    try:
        f_type_stats = db.session.query(FundingCache.type, func.count(FundingCache.id)).filter_by(ror_id=ror_id).group_by(FundingCache.type).all()
        metrics['chart_funding_types_labels'] = [str(row[0]).replace('_', ' ').title() for row in f_type_stats]
        metrics['chart_funding_types_values'] = [row[1] for row in f_type_stats]
    except Exception as e: logger.error(f"Error in Funding Types: {e}")

    # 6. Chart: Top Funding Organizations
    try:
        # Dynamic column check for compatibility
        columns = [c.key for c in inspect(FundingCache).mapper.column_attrs]
        org_col = FundingCache.org_name if 'org_name' in columns else (FundingCache.organization_name if 'organization_name' in columns else FundingCache.title)
        
        fund_stats = db.session.query(org_col, func.count(FundingCache.id)).filter_by(ror_id=ror_id).group_by(org_col).order_by(func.count(FundingCache.id).desc()).limit(10).all()
        metrics['chart_funding_orgs_labels'] = [str(row[0])[:30] for row in fund_stats]
        metrics['chart_funding_orgs_values'] = [row[1] for row in fund_stats]
    except Exception as e: logger.error(f"Error in Funding Orgs: {e}")

    # 7. Chart: Top Journals
    try:
        journal_stats = db.session.query(WorkCache.journal_title, func.count(WorkCache.id)).filter_by(ror_id=ror_id).filter(WorkCache.journal_title != None).group_by(WorkCache.journal_title).order_by(func.count(WorkCache.id).desc()).limit(5).all()
        metrics['chart_journals_labels'] = [str(row[0])[:30] + '...' for row in journal_stats]
        metrics['chart_journals_values'] = [row[1] for row in journal_stats]
    except Exception as e: logger.error(f"Error in Top Journals: {e}")

    # 8. Table: Top Contributors (Works)
    # Join WorkCache with ResearcherCache to display names efficiently
    try:
        raw_works = db.session.query(
            WorkCache.orcid, 
            func.count(WorkCache.id).label('count'),
            ResearcherCache.given_names,
            ResearcherCache.family_name,
            ResearcherCache.credit_name
        ).outerjoin(ResearcherCache, WorkCache.orcid == ResearcherCache.orcid)\
         .filter(WorkCache.ror_id == ror_id)\
         .group_by(WorkCache.orcid, ResearcherCache.given_names, ResearcherCache.family_name, ResearcherCache.credit_name)\
         .order_by(func.count(WorkCache.id).desc()).limit(10).all()
        
        top_works = []
        for r in raw_works:
            orcid, count, given, family, credit = r
            if credit: name = credit
            elif given or family: name = f"{given or ''} {family or ''}".strip()
            else: name = _('Unknown Researcher')
            top_works.append((orcid, count, name))
        metrics['top_researchers_works'] = top_works
    except Exception as e: logger.error(f"Error in Top Researchers Works: {e}")

    # 9. Table: Top Contributors (Fundings)
    try:
        raw_funds = db.session.query(
            FundingCache.orcid, 
            func.count(FundingCache.id).label('count'),
            ResearcherCache.given_names,
            ResearcherCache.family_name,
            ResearcherCache.credit_name
        ).outerjoin(ResearcherCache, FundingCache.orcid == ResearcherCache.orcid)\
         .filter(FundingCache.ror_id == ror_id)\
         .group_by(FundingCache.orcid, ResearcherCache.given_names, ResearcherCache.family_name, ResearcherCache.credit_name)\
         .order_by(func.count(FundingCache.id).desc()).limit(10).all()
        
        top_funds = []
        for r in raw_funds:
            orcid, count, given, family, credit = r
            if credit: name = credit
            elif given or family: name = f"{given or ''} {family or ''}".strip()
            else: name = _('Unknown Researcher')
            top_funds.append((orcid, count, name))
        metrics['top_researchers_fundings'] = top_funds
    except Exception as e: logger.error(f"Error in Top Researchers Fundings: {e}")

    return render_template('main/metrics.html', stats=metrics)


@bp_main.route('/download/metrics/<string:chart_type>')
@login_required
def download_metrics_data(chart_type):
    """
    Export endpoint for Chart Data.
    Allows users to download the underlying data of any chart in CSV or Excel format.
    
    Args:
        chart_type (str): The identifier of the chart (e.g., 'trend_works', 'top_researchers_works').
    """
    ror_id = get_active_ror_id()
    fmt = request.args.get('format', 'csv')
    
    if not ror_id:
        return redirect(url_for('main.metrics_panel'))

    data = []
    filename = f"chart_{chart_type}"

    try:
        # Generate data based on requested chart type
        if chart_type == 'trend_works':
            query = db.session.query(WorkCache.pub_year.label('Year'), func.count(WorkCache.id).label('Count')).filter_by(ror_id=ror_id).group_by(WorkCache.pub_year).order_by(WorkCache.pub_year).all()
            data = [{'Year': r[0], 'Works': r[1]} for r in query if r[0]]
        
        elif chart_type == 'trend_fundings':
            query = db.session.query(FundingCache.start_y.label('Year'), func.count(FundingCache.id).label('Count')).filter_by(ror_id=ror_id).group_by(FundingCache.start_y).order_by(FundingCache.start_y).all()
            data = [{'Year': r[0], 'Fundings': r[1]} for r in query if r[0]]

        elif chart_type == 'types_works':
            query = db.session.query(WorkCache.type.label('Type'), func.count(WorkCache.id).label('Count')).filter_by(ror_id=ror_id).group_by(WorkCache.type).all()
            data = [{'Type': str(r[0]).replace('_', ' ').title(), 'Count': r[1]} for r in query]

        elif chart_type == 'types_fundings':
            query = db.session.query(FundingCache.type.label('Type'), func.count(FundingCache.id).label('Count')).filter_by(ror_id=ror_id).group_by(FundingCache.type).all()
            data = [{'Type': str(r[0]).replace('_', ' ').title(), 'Count': r[1]} for r in query]

        elif chart_type == 'journals':
            query = db.session.query(WorkCache.journal_title.label('Journal'), func.count(WorkCache.id).label('Count')).filter_by(ror_id=ror_id).filter(WorkCache.journal_title != None).group_by(WorkCache.journal_title).order_by(func.count(WorkCache.id).desc()).limit(50).all()
            data = [{'Journal': r[0], 'Count': r[1]} for r in query]

        elif chart_type == 'funding_orgs':
            columns = [c.key for c in inspect(FundingCache).mapper.column_attrs]
            org_col = FundingCache.org_name if 'org_name' in columns else (FundingCache.organization_name if 'organization_name' in columns else FundingCache.title)
            query = db.session.query(org_col.label('Organization'), func.count(FundingCache.id).label('Count')).filter_by(ror_id=ror_id).group_by(org_col).order_by(func.count(FundingCache.id).desc()).limit(50).all()
            data = [{'Organization': r[0], 'Count': r[1]} for r in query]

        elif chart_type == 'top_researchers_works':
            # Joined query for proper name export
            query = db.session.query(
                WorkCache.orcid, 
                func.count(WorkCache.id),
                ResearcherCache.given_names,
                ResearcherCache.family_name,
                ResearcherCache.credit_name
            ).outerjoin(ResearcherCache, WorkCache.orcid == ResearcherCache.orcid)\
             .filter(WorkCache.ror_id == ror_id)\
             .group_by(WorkCache.orcid, ResearcherCache.given_names, ResearcherCache.family_name, ResearcherCache.credit_name)\
             .order_by(func.count(WorkCache.id).desc()).limit(100).all()
            
            data = []
            for r in query:
                orcid, count, given, family, credit = r
                if credit: name = credit
                elif given or family: name = f"{given or ''} {family or ''}".strip()
                else: name = 'Unknown'
                data.append({'ORCID': orcid, 'Name': name, 'Works Count': count})

        elif chart_type == 'top_researchers_fundings':
            query = db.session.query(
                FundingCache.orcid, 
                func.count(FundingCache.id),
                ResearcherCache.given_names,
                ResearcherCache.family_name,
                ResearcherCache.credit_name
            ).outerjoin(ResearcherCache, FundingCache.orcid == ResearcherCache.orcid)\
             .filter(FundingCache.ror_id == ror_id)\
             .group_by(FundingCache.orcid, ResearcherCache.given_names, ResearcherCache.family_name, ResearcherCache.credit_name)\
             .order_by(func.count(FundingCache.id).desc()).limit(100).all()
            
            data = []
            for r in query:
                orcid, count, given, family, credit = r
                if credit: name = credit
                elif given or family: name = f"{given or ''} {family or ''}".strip()
                else: name = 'Unknown'
                data.append({'ORCID': orcid, 'Name': name, 'Fundings Count': count})

        if not data:
            flash_err(_("No data available to download for this chart."))
            return redirect(url_for('main.metrics_panel'))

        # File Generation
        df = pd.DataFrame(data)
        output = io.BytesIO()

        if fmt == 'excel':
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name=chart_type)
            output.seek(0)
            return send_file(output, as_attachment=True, download_name=f"{filename}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            # UTF-8 BOM for Excel compatibility with CSV
            output.write(df.to_csv(index=False).encode('utf-8-sig'))
            output.seek(0)
            return send_file(output, as_attachment=True, download_name=f"{filename}.csv", mimetype='text/csv')

    except Exception as exc:
        logger.error(f"Export error: {exc}")
        flash_err(_("An error occurred while exporting data."))
        return redirect(url_for('main.metrics_panel'))


# ============================================================
# STATIC PAGES & RESOURCES
# ============================================================

@bp_main.route('/resources')
@login_required
def resources():
    """Renders the Knowledge Base / Help page."""
    return render_template('main/resources.html')


@bp_main.route('/integration-orcid/pull')
@login_required
def integration_pull():
    """Renders the API Pull Integration documentation page."""
    return render_template('main/integration_pull.html')


@bp_main.route('/orcid-profile/<orcid_id>')
@login_required
def orcid_profile(orcid_id: str):
    """
    Renders a live view of a specific ORCID profile.
    This fetches fresh data from ORCID to display in the UI modal.
    """
    data = get_full_orcid_profile(orcid_id)
    if not data:
        flash_err(_("The requested ORCID profile could not be loaded."))
        return redirect(url_for('main.researcher_list'))
    return render_template('main/orcid_profile.html', data=data)


@bp_main.route('/integration_guide', defaults={'filename': None})
@bp_main.route('/integration_guide/<path:filename>')
@login_required
def integration_guide(filename: str | None):
    """
    Serves downloadable datasets or technical guides stored in the /datasets folder.
    Securely checks for file existence before serving.
    """
    datasets_dir = current_app.config.get('DATASETS_DIR', os.path.join(current_app.root_path, 'datasets'))
    os.makedirs(datasets_dir, exist_ok=True)
    from werkzeug.utils import secure_filename
    
    # Download specific file
    if filename:
        safe_name = secure_filename(filename)
        full_path = os.path.join(datasets_dir, safe_name)
        if not os.path.isfile(full_path):
            flash_err(_("The requested file was not found on the server."))
            return redirect(url_for('main.integration_guide'))
        return send_from_directory(directory=datasets_dir, path=safe_name, as_attachment=True)
    
    # List available files
    datasets = []
    try:
        for entry in os.listdir(datasets_dir):
            full_path = os.path.join(datasets_dir, entry)
            if os.path.isfile(full_path):
                stat = os.stat(full_path)
                datasets.append({
                    "name": os.path.splitext(entry)[0],
                    "filename": entry,
                    "size_bytes": stat.st_size,
                    "updated_at": dt.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
                })
    except Exception as exc:
        logger.exception("Error scanning datasets directory: %s", exc)
        flash_err(_("Could not retrieve the list of available files."))
        
    return render_template('main/integration_guide.html', datasets=datasets)