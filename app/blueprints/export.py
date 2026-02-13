"""
Module: export.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Export Management Blueprint.
    
    This module provides the functionality to generate and download reports 
    of ORCID profiles. It supports:
    1. Full Profile Export (Multi-sheet Excel).
    2. Section-specific Export (Excel or CSV).
    
    Key Features:
    - In-memory file generation (no temporary files on disk).
    - Bilingual headers (English/Spanish) based on user locale.
    - specialized handling for UTF-8 BOM in CSVs for Excel compatibility.
"""

import logging
from io import BytesIO
import pandas as pd
from flask import Blueprint, request, send_file, abort
from flask_babel import _

from ..decorators import login_required

# --- Blueprint Configuration ---
bp_export = Blueprint("export", __name__)
logger = logging.getLogger(__name__)


# ============================================================
# ADMINISTRATIVE EXPORT ENDPOINTS
# ============================================================

@bp_export.route('/download/excel/<string:orcid_id>')
@login_required
def download_excel(orcid_id: str):
    """
    Generates a comprehensive, multi-sheet Excel report for a specific ORCID ID.
    
    This endpoint fetches live data from the ORCID API (Person + Activities) 
    and organizes it into separate sheets (Identity, Biography, Works, Fundings).
    
    Args:
        orcid_id (str): The ORCID identifier to export.
        
    Returns:
        File Response: A downloadable .xlsx file.
    """
    try:
        # Lazy import to avoid circular dependencies
        from ..orcid_queries import fetch_person, fetch_activities

        # Fetch live data from ORCID API
        # These functions handle the API calls and return dictionaries
        person = fetch_person(orcid_id) or {}
        activities = fetch_activities(orcid_id) or {}

        # Use BytesIO to generate the file in RAM, avoiding disk I/O
        output = BytesIO()

        # Generate multi-sheet Excel file using the openpyxl engine
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            _write_personal_info(writer, orcid_id, person)
            _write_activities(writer, activities)

        # Reset pointer to the beginning of the stream before sending
        output.seek(0)
        
        # Localize the filename based on current language
        filename = f"{_('ORCID_Full_Report')}_{orcid_id}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as exc:
        logger.exception("CRITICAL: Failed full Excel export for ORCID %s: %s", orcid_id, exc)
        abort(500, description=_("Could not generate the requested Excel file."))


@bp_export.route('/download/section/<section>')
@login_required
def download_section_excel(section: str):
    """
    Exports a specific subset of ORCID data (e.g., only 'works' or 'fundings').
    
    This endpoint supports format conversion. It first generates an Excel object 
    via the helper function and, if CSV is requested, converts that dataframe 
    on the fly.
    
    Args:
        section (str): The data section to export.
    
    Query Params:
        orcid_id (str): The target ORCID ID (required).
        format (str): 'xlsx' (default) or 'csv'.
    """
    orcid_id = request.args.get('orcid_id')
    fmt = request.args.get('format', 'xlsx').lower()
    
    if not orcid_id:
        abort(400, description="Missing required parameter: orcid_id")

    from ..helpers import build_excel_for_section

    try:
        # Retrieve the data as an Excel BytesIO object from the helper logic
        bio, filename = build_excel_for_section(orcid_id, section)
        
        if fmt == 'csv':
            # Conversion Logic: Excel -> DataFrame -> CSV
            # This allows us to reuse the complex formatting logic in 'build_excel_for_section'
            bio.seek(0)
            df = pd.read_excel(bio)
            
            output = BytesIO()
            # 'utf-8-sig' includes the BOM, which forces Excel to read special characters correctly
            output.write(df.to_csv(index=False).encode('utf-8-sig'))
            output.seek(0)
            
            return send_file(
                output,
                as_attachment=True,
                download_name=filename.replace('.xlsx', '.csv'),
                mimetype='text/csv'
            )

        # Default Case: Return the original Excel file
        return send_file(
            bio,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as exc:
        logger.exception("Failed section export for %s: %s", section, exc)
        abort(500, description=_("Could not generate the requested file."))


# ============================================================
# INTERNAL DATA WRITERS (BILINGUAL)
# ============================================================

def _write_personal_info(writer: pd.ExcelWriter, orcid_id: str, person: dict) -> None:
    """
    Helper function to parse personal metadata and write it to Excel sheets.
    Translates column headers using Flask-Babel's `_()`.
    """
    try:
        # --- Sheet 1: Identity ---
        name_node = person.get('name') or {}
        personal_df = pd.DataFrame([{
            'ORCID iD': orcid_id,
            _('Given Names'): (name_node.get('given-names') or {}).get('value'),
            _('Family Names'): (name_node.get('family-name') or {}).get('value'),
            _('Credit Name'): (name_node.get('credit-name') or {}).get('value'),
            _('Created'): (name_node.get('created-date') or {}).get('value'),
            _('Visibility'): name_node.get('visibility')
        }])
        personal_df.to_excel(writer, sheet_name=_('Identity'), index=False)

        # --- Sheet 2: Biography (Optional) ---
        bio_node = person.get('biography') or {}
        if bio_node.get('content'):
            pd.DataFrame([{
                _('Biography'): bio_node.get('content'),
                _('Visibility'): bio_node.get('visibility')
            }]).to_excel(writer, sheet_name=_('Biography'), index=False)

        # --- Sheet 3: External Identifiers ---
        ext_ids = (person.get('external-identifiers') or {}).get('external-identifier') or []
        if ext_ids:
            # List comprehension to flatten the nested JSON structure
            data = [{_('Type'): x.get('external-id-type'), _('Value'): x.get('external-id-value')} for x in ext_ids]
            pd.DataFrame(data).to_excel(writer, sheet_name=_('ExternalIDs'), index=False)

    except Exception as exc:
        logger.error("Error writing personal info: %s", exc)


def _write_activities(writer: pd.ExcelWriter, activities: dict) -> None:
    """
    Helper function to parse scholarly activities (Works, Funding) and write them to Excel sheets.
    Handles deep nesting in the ORCID JSON schema.
    """
    try:
        # --- Sheet 4: Fundings (Grants) ---
        funding_list = []
        # Navigate: activities-summary -> fundings -> group -> funding-summary
        for group in (activities.get('fundings') or {}).get('group') or []:
            for summary in group.get('funding-summary') or []:
                funding_list.append({
                    _('Title'): (summary.get('title') or {}).get('title', {}).get('value'),
                    _('Agency'): (summary.get('organization') or {}).get('name'),
                    _('Type'): (summary.get('type') or '').replace('_', ' ').title(),
                    _('Start Year'): (summary.get('start-date') or {}).get('year', {}).get('value'),
                    _('Source'): (summary.get('source', {}).get('source-name') or {}).get('value')
                })
        
        if funding_list:
            pd.DataFrame(funding_list).to_excel(writer, sheet_name=_('Fundings'), index=False)

        # --- Sheet 5: Works (Publications) ---
        works_list = []
        # Navigate: activities-summary -> works -> group -> work-summary
        for group in (activities.get('works') or {}).get('group') or []:
            for work in group.get('work-summary') or []:
                # Extract DOI specifically from external-ids list
                ext_ids = (work.get('external-ids') or {}).get('external-id') or []
                doi = next((e.get('external-id-value') for e in ext_ids if e.get('external-id-type') == 'doi'), None)
                
                works_list.append({
                    _('Title'): (work.get('title') or {}).get('title', {}).get('value'),
                    _('Type'): (work.get('type') or '').replace('_', ' ').title(),
                    _('Year'): (work.get('publication-date') or {}).get('year', {}).get('value'),
                    _('DOI'): doi,
                    _('Journal/Publisher'): (work.get('journal-title') or {}).get('value'),
                    _('Source'): (work.get('source', {}).get('source-name') or {}).get('value')
                })
        
        if works_list:
            pd.DataFrame(works_list).to_excel(writer, sheet_name=_('Works'), index=False)

    except Exception as exc:
        logger.error("Error writing activities: %s", exc)