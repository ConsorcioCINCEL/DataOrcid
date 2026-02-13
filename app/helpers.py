"""
Module: helpers.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Data Processing and Transformation Utilities.
    
    This module serves as the ETL (Extract, Transform, Load) layer for the application.
    It provides:
    1. Robust JSON navigation utilities (`get_nested`) to handle missing data safely.
    2. Normalization functions that convert complex ORCID JSON schemas (Person, Works, Fundings)
       into flat Pandas DataFrames.
    3. Institutional metric calculations for dashboard visualizations.
"""

from __future__ import annotations
import logging
import requests
import urllib.parse
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime
from collections import Counter

import pandas as pd
from flask import current_app, session
from flask_babel import _
from requests.exceptions import RequestException, Timeout

# Internal dependencies (Legacy queries module)
from .orcid_queries import fetch_person, fetch_activities

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


# ============================================================
# GENERIC UTILITIES & TRANSFORMERS
# ============================================================

def timestamp_to_date(ms_timestamp: Any) -> str:
    """
    Converts a Unix millisecond timestamp to a standard 'YYYY-MM-DD' string.
    
    Args:
        ms_timestamp (Any): The timestamp value (int, float, or string).
        
    Returns:
        str: Formatted date string, or an empty string if conversion fails.
    """
    try:
        seconds = int(ms_timestamp) / 1000
        dt_obj = datetime.utcfromtimestamp(seconds)
        return dt_obj.strftime('%Y-%m-%d')
    except (ValueError, TypeError, Exception):
        return ''


def get_nested(obj: Optional[Dict[str, Any]], path: Iterable[Any], default: Any = None) -> Any:
    """
    Safely retrieves a value from a deeply nested dictionary structure.
    
    This utility prevents `KeyError` or `TypeError` (NoneType subscripting) when 
    navigating erratic JSON responses from external APIs.
    
    Args:
        obj (Optional[Dict]): The root dictionary.
        path (Iterable): A list of keys to traverse (e.g., ['title', 'value']).
        default (Any): The fallback value to return if the path is invalid.
        
    Returns:
        Any: The target value or the default fallback.
    """
    current = obj
    try:
        for key in path:
            if current is None or not isinstance(current, dict):
                return default
            current = current.get(key)
        return default if current is None else current
    except Exception:
        return default


def df_from_rows(rows: List[Dict[str, Any]], fallback_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Robustly creates a Pandas DataFrame from a list of dictionaries.
    
    Args:
        rows (List[Dict]): The list of data records.
        fallback_cols (List[str], optional): A list of column names to instantiate 
                                             if the input list is empty.
                                             
    Returns:
        pd.DataFrame: A normalized DataFrame ready for export or analysis.
    """
    if not rows:
        return pd.DataFrame(columns=fallback_cols or [])
    try:
        # json_normalize handles nested dicts better than the default constructor
        return pd.json_normalize(rows)
    except Exception:
        return pd.DataFrame(rows)


# ============================================================
# PERSON SECTION NORMALIZERS
# ============================================================
# These functions transform specific sections of the ORCID 'Person' schema.

def norm_biography(person: Dict[str, Any]) -> pd.DataFrame:
    """Extracts and normalizes the biography section."""
    bio = person.get('biography') or {}
    row = {
        _('Biography'): bio.get('content'),
        _('Created Date'): get_nested(bio, ['created-date', 'value']),
        _('Last Modified'): get_nested(bio, ['last-modified-date', 'value']),
        _('Visibility'): bio.get('visibility'),
    }
    return df_from_rows([row])


def norm_other_names(person: Dict[str, Any]) -> pd.DataFrame:
    """Extracts alternative names (aliases)."""
    items = get_nested(person, ['other-names', 'other-name'], []) or []
    rows = [{_('Name'): n.get('content'), _('Visibility'): n.get('visibility')} for n in items if n]
    return df_from_rows(rows, [_('Name'), _('Visibility')])


def norm_emails(person: Dict[str, Any]) -> pd.DataFrame:
    """Extracts email addresses."""
    items = get_nested(person, ['emails', 'email'], []) or []
    rows = []
    for e in items:
        rows.append({
            _('Email'): e.get('email'),
            _('Verified'): e.get('verified'),
            _('Primary'): e.get('primary'),
            _('Visibility'): e.get('visibility'),
        })
    return df_from_rows(rows, [_('Email'), _('Verified')])


def norm_addresses(person: Dict[str, Any]) -> pd.DataFrame:
    """Extracts geographical locations (Country)."""
    items = get_nested(person, ['addresses', 'address'], []) or []
    rows = [{_('Country'): get_nested(a, ['country', 'value']), _('Visibility'): a.get('visibility')} for a in items]
    return df_from_rows(rows, [_('Country')])


def norm_keywords(person: Dict[str, Any]) -> pd.DataFrame:
    """Extracts research keywords."""
    items = get_nested(person, ['keywords', 'keyword'], []) or []
    rows = [{_('Keyword'): k.get('content'), _('Visibility'): k.get('visibility')} for k in items]
    return df_from_rows(rows, [_('Keyword')])


def norm_external_ids(person: Dict[str, Any]) -> pd.DataFrame:
    """Extracts external person identifiers (Scopus, ResearcherID, etc.)."""
    items = get_nested(person, ['external-identifiers', 'external-identifier'], []) or []
    rows = []
    for x in items:
        rows.append({
            _('Type'): x.get('external-id-type'),
            _('Value'): x.get('external-id-value'),
            _('URL'): get_nested(x, ['external-id-url', 'value']),
            _('Visibility'): x.get('visibility'),
        })
    return df_from_rows(rows)


# ============================================================
# ACTIVITIES SECTION NORMALIZERS
# ============================================================
# These functions transform sections of the ORCID 'Activities' schema.

def norm_distinctions(activities: Dict[str, Any]) -> pd.DataFrame:
    """Normalizes Distinctions (Awards/Honors)."""
    groups = get_nested(activities, ['distinctions', 'affiliation-group'], []) or []
    rows = []
    for g in groups:
        for s in g.get('summaries', []) or []:
            d = s.get('distinction-summary') or {}
            org = d.get('organization') or {}
            sd = d.get('start-date') or {}
            ed = d.get('end-date') or {}
            
            rows.append({
                _('Title'): d.get('role-title'),
                _('Organization'): org.get('name'),
                _('Start Year'): get_nested(sd, ['year', 'value']),
                _('End Year'): get_nested(ed, ['year', 'value']),
                _('Source'): get_nested(d, ['source', 'source-name', 'value']),
            })
    return df_from_rows(rows)


def norm_educations(activities: Dict[str, Any]) -> pd.DataFrame:
    """Normalizes Education history."""
    items = get_nested(activities, ['educations', 'education-summary'], []) or []
    rows = []
    for ed in items:
        sd = ed.get('start-date') or {}
        rows.append({
            _('Degree/Title'): ed.get('role-title'),
            _('Institution'): get_nested(ed, ['organization', 'name']),
            _('Start Year'): get_nested(sd, ['year', 'value']),
            _('End Year'): get_nested(ed, ['end-date', 'year', 'value']),
            _('Source'): get_nested(ed, ['source', 'source-name', 'value']),
        })
    return df_from_rows(rows)


def norm_employments(activities: Dict[str, Any]) -> pd.DataFrame:
    """Normalizes Employment history."""
    items = get_nested(activities, ['employments', 'employment-summary'], []) or []
    rows = []
    for emp in items:
        sd = emp.get('start-date') or {}
        rows.append({
            _('Position'): emp.get('role-title'),
            _('Organization'): get_nested(emp, ['organization', 'name']),
            _('Start Year'): get_nested(sd, ['year', 'value']),
            _('End Year'): get_nested(emp, ['end-date', 'year', 'value']),
            _('Source'): get_nested(emp, ['source', 'source-name', 'value']),
        })
    return df_from_rows(rows)


def norm_fundings(activities: Dict[str, Any]) -> pd.DataFrame:
    """Normalizes Funding/Grant history."""
    groups = get_nested(activities, ['fundings', 'group'], []) or []
    rows = []
    for g in groups:
        for s in g.get('funding-summary', []) or []:
            sd = s.get('start-date') or {}
            eids = get_nested(s, ['external-ids', 'external-id'], []) or []
            rows.append({
                _('Title'): get_nested(s, ['title', 'title', 'value']),
                _('Funder'): get_nested(s, ['organization', 'name']),
                _('Grant ID'): (eids[0].get('external-id-value') if eids else None),
                _('Type'): s.get('type'),
                _('Year'): get_nested(sd, ['year', 'value']),
                _('Source'): get_nested(s, ['source', 'source-name', 'value']),
            })
    return df_from_rows(rows)


def norm_peer_reviews(activities: Dict[str, Any]) -> pd.DataFrame:
    """Normalizes Peer Review activities."""
    groups = get_nested(activities, ['peer-reviews', 'group'], []) or []
    rows = []
    for g in groups:
        for pr_group in g.get('peer-review-group', []) or []:
            for pr in pr_group.get('peer-review-summary', []) or []:
                cd = pr.get('completion-date') or {}
                rows.append({
                    _('Organization'): get_nested(pr, ['convening-organization', 'name']),
                    _('Role'): pr.get('reviewer-role'),
                    _('Completion Year'): get_nested(cd, ['year', 'value']),
                    _('Source'): get_nested(pr, ['source', 'source-name', 'value']),
                })
    return df_from_rows(rows)


def norm_works(activities: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalizes Works (Publications).
    Handles DOI and ISSN extraction logic.
    """
    groups = get_nested(activities, ['works', 'group'], []) or []
    rows = []
    for g in groups:
        for w in g.get('work-summary', []) or []:
            pub_date = w.get('publication-date') or {}
            ext_ids = get_nested(w, ['external-ids', 'external-id'], []) or []
            
            # Smart extraction: Find specific ID types in the list
            doi = next((e.get('external-id-value') for e in ext_ids if (e.get('external-id-type') or '').lower() == 'doi'), None)
            issn = next((e.get('external-id-value') for e in ext_ids if (e.get('external-id-type') or '').lower() == 'issn'), None)

            rows.append({
                _('Title'): get_nested(w, ['title', 'title', 'value']),
                _('Type'): w.get('type'),
                _('Year'): get_nested(pub_date, ['year', 'value']),
                _('DOI'): doi,
                _('ISSN'): issn,
            })
    return df_from_rows(rows)


# ============================================================
# ORCHESTRATION & EXPORT LOGIC
# ============================================================

SECTION_MAP = {
    'biography': norm_biography, 'other-names': norm_other_names,
    'emails': norm_emails, 'addresses': norm_addresses,
    'keywords': norm_keywords, 'external-ids': norm_external_ids,
    'distinctions': norm_distinctions, 'educations': norm_educations,
    'employments': norm_employments, 'fundings': norm_fundings,
    'peer-reviews': norm_peer_reviews, 'works': norm_works
}

def build_section_df(orcid_id: str, section: str) -> pd.DataFrame:
    """
    Orchestrator: Fetches data for an ORCID ID and applies the correct normalizer.
    
    Args:
        orcid_id (str): The target ORCID identifier.
        section (str): The section name (e.g., 'works', 'educations').
        
    Returns:
        pd.DataFrame: Normalized data.
    """
    section = (section or '').strip().lower()
    if section not in SECTION_MAP:
        raise ValueError(f"Unsupported section: {section}")

    # Fetch fresh data (These functions are imported from orcid_queries)
    person = fetch_person(orcid_id) or {}
    activities = fetch_activities(orcid_id) or {}

    # Route to the correct normalizer based on section type
    if section in ['distinctions', 'educations', 'employments', 'fundings', 'peer-reviews', 'works']:
        return SECTION_MAP[section](activities)
    return SECTION_MAP[section](person)


def build_excel_for_section(orcid_id: str, section: str) -> Tuple[BytesIO, str]:
    """
    Generates an Excel file (in-memory) for a specific profile section.
    
    Returns:
        Tuple[BytesIO, str]: The file stream and the suggested filename.
    """
    df = build_section_df(orcid_id, section)
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        safe_df = df if not df.empty else pd.DataFrame({'status': [_('No data available')]})
        safe_df.to_excel(writer, sheet_name=section[:31], index=False)
    
    output.seek(0)
    filename = f"orcid_{orcid_id}_{section}.xlsx"
    return output, filename


# ============================================================
# INSTITUTIONAL METRICS
# ============================================================

def get_researchers_by_ror(ror_id: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Queries ORCID's 'Expanded Search' API to find all researchers linked to a ROR.
    
    Note: This is used for live metrics calculation on the dashboard.
    
    Args:
        ror_id (str): The Research Organization Registry ID.
        
    Returns:
        Tuple: (List of researcher dicts, Error message string or None).
    """
    # Use Public API for broad search queries
    base_url = current_app.config.get('ORCID_SEARCH_URL') or current_app.config.get('ORCID_BASE_URL_PUB', 'https://pub.orcid.org/v3.0/')
    start, rows = 0, 1000
    all_results = []
    
    try:
        while True:
            # Construct Lucene query for ROR ID
            query = f'ror-org-id:"https://ror.org/{ror_id}"'
            url = f"{base_url.rstrip('/')}/expanded-search/?q={urllib.parse.quote(query)}&start={start}&rows={rows}"
            
            resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=20)
            if resp.status_code != 200:
                logger.error("ORCID Search API failed [%d]: %s", resp.status_code, resp.text)
                return None, _("ORCID service communication error.")

            data = resp.json()
            researchers = data.get('expanded-result', []) or []
            all_results.extend(researchers)

            # Pagination check
            if len(researchers) < rows:
                break
            start += rows

        return all_results, None

    except (RequestException, Timeout) as exc:
        logger.error("ORCID Network error for ROR %s: %s", ror_id, exc)
        return None, _("Connection timed out. Please try again.")


def calculate_home_metrics_from_list(researchers: List[Dict]) -> Tuple[int, Dict]:
    """
    Computes summary metrics from a raw list of researchers.
    
    Args:
        researchers (List[Dict]): Raw search results.
        
    Returns:
        Tuple[int, Dict]: (Total count, Histogram of affiliation counts).
    """
    total = len(researchers)
    inst_counts = []
    
    for r in researchers:
        names = r.get('institution-name', []) or []
        if isinstance(names, str):
            names = [names]
        # Count unique, non-empty affiliation names for histogram analysis
        inst_counts.append(len(set(filter(None, names))))

    return total, dict(Counter(inst_counts))


def load_home_metrics_if_missing(ror_id: str) -> Tuple[bool, Optional[str]]:
    """
    Ensures that institutional metrics are cached in the user session.
    If missing, it triggers a live fetch from ORCID.
    """
    if session.get('researcher_count') is not None:
        return True, None

    data, err = get_researchers_by_ror(ror_id)
    if err:
        return False, err

    rc, hist = calculate_home_metrics_from_list(data)
    
    # Cache results in session to avoid repeated API calls
    session['researcher_count'] = rc
    session['affiliation_hist'] = hist
    return True, None