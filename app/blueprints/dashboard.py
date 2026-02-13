"""
Module: dashboard.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Data Visualization Blueprint.
    
    This module is responsible for aggregating and rendering statistical dashboards 
    based on the local cache of ORCID data. It provides insights into:
    - Publication metrics (Works per year, top journals, DOI coverage).
    - Funding metrics (Grants per agency, currency distribution).
    
    The dashboard context is scoped to the active ROR ID (Multi-tenancy support).
"""

from collections import Counter, defaultdict
import logging

from flask import Blueprint, render_template, redirect, url_for, session
from flask_babel import _
from ..decorators import login_required
from ..utils.flashes import flash_err
from ..models import WorkCache

# Attempt to import FundingCache, handle gracefully if the model is missing
try:
    from ..models import FundingCache
except ImportError:
    FundingCache = None

# --- Blueprint Configuration ---
bp_dash = Blueprint("dashboard", __name__)
logger = logging.getLogger(__name__)


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _get_active_ror_id():
    """
    Retrieves the currently active ROR ID from the user session.
    Prioritizes the admin-selected context ('impersonation') over the user's native ROR.
    
    Returns:
        str: The active ROR ID or None.
    """
    return session.get('admin_selected_ror') or session.get('ror_id')


# ============================================================
# DASHBOARD ROUTES
# ============================================================

@bp_dash.route('/cache/dashboard')
@login_required
def cache_dashboard():
    """
    Renders the main analytics dashboard for the active institution.

    Data Aggregation Strategy:
    - Fetches all records for the active ROR from the database.
    - Uses Python's 'collections.Counter' for high-speed in-memory aggregation.
    - Prepares structured JSON-compatible dictionaries for Chart.js rendering in the frontend.

    Returns:
        Template: 'dashboard/index.html' with aggregated statistics objects.
    """
    # 1. Context Resolution
    ror_id = _get_active_ror_id()
    if not ror_id:
        flash_err(_("No active ROR context found. Please log in again or select an institution."))
        return redirect(url_for('main.index'))

    # ---------------------------------------------------------
    # PART A: WORKS (PUBLICATIONS) STATISTICS
    # ---------------------------------------------------------
    works_rows = WorkCache.query.filter_by(ror_id=ror_id).all()
    has_works = bool(works_rows)

    # Initialize data structure for Chart.js
    works_stats = {
        "total": len(works_rows),
        "by_type": {"labels": [], "values": []},
        "by_year": {"labels": [], "values": []},
        "top_authors": {"labels": [], "values": []},
        "top_journals": {"labels": [], "values": []},
        "doi_coverage": {"labels": [_("With DOI"), _("Without DOI")], "values": [0, 0]},
    }

    if has_works:
        try:
            # Metric 1: Distribution by Work Type (Journal Article, Conference, etc.)
            # Unknown types are grouped under "—"
            c_type = Counter((w.type or "—") for w in works_rows)
            if c_type:
                # Sort by count descending
                labels, values = zip(*sorted(c_type.items(), key=lambda x: (-x[1], x[0])))
                works_stats["by_type"] = {"labels": list(labels), "values": list(values)}

            # Metric 2: Timeline (Works per Year)
            # Custom sorting key to handle non-integer years gracefully
            c_year = Counter(((str(w.pub_year) if w.pub_year else "—").strip() or "—") for w in works_rows)

            def _year_sort_key(item):
                key, _ = item
                try:
                    return (0, int(key)) # Valid years first, sorted numerically
                except ValueError:
                    return (1, 9999)     # "—" or invalid years last

            if c_year:
                # Sort chronologically
                year_items = sorted(c_year.items(), key=_year_sort_key)
                labels, values = zip(*year_items)
                works_stats["by_year"] = {"labels": list(labels), "values": list(values)}

            # Metric 3: Top Contributors (by ORCID iD)
            c_auth = Counter((w.orcid or "—") for w in works_rows)
            top_auth = c_auth.most_common(10)
            if top_auth:
                labels, values = zip(*top_auth)
                works_stats["top_authors"] = {"labels": list(labels), "values": list(values)}

            # Metric 4: Top Journals/Venues
            c_journal = Counter(
                (w.journal_title or "—") for w in works_rows if (w.journal_title or "").strip()
            )
            top_j = c_journal.most_common(10)
            if top_j:
                labels, values = zip(*top_j)
                works_stats["top_journals"] = {"labels": list(labels), "values": list(values)}

            # Metric 5: Data Quality (DOI Presence)
            doi_yes = sum(1 for w in works_rows if (w.doi or "").strip())
            doi_no = works_stats["total"] - doi_yes
            works_stats["doi_coverage"]["values"] = [doi_yes, doi_no]

        except Exception as exc:
            logger.exception("Error processing Works statistics for ROR %s: %s", ror_id, exc)
            flash_err(_("An error occurred while generating publication statistics."))

    # ---------------------------------------------------------
    # PART B: FUNDING (GRANTS) STATISTICS
    # ---------------------------------------------------------
    has_fundings = False
    funding_stats = {
        "total": 0,
        "by_org": {"labels": [], "values": []},
        "by_type": {"labels": [], "values": []},
        "by_currency_sum": {"labels": [], "values": []},
        "top_authors": {"labels": [], "values": []},
    }

    if FundingCache:
        try:
            funds_rows = FundingCache.query.filter_by(ror_id=ror_id).all()
            has_fundings = bool(funds_rows)

            if has_fundings:
                funding_stats["total"] = len(funds_rows)

                # Metric 1: Top Funding Agencies
                c_org = Counter((f.org_name or "—") for f in funds_rows if (f.org_name or "").strip())
                top_org = c_org.most_common(10)
                if top_org:
                    labels, values = zip(*top_org)
                    funding_stats["by_org"] = {"labels": list(labels), "values": list(values)}

                # Metric 2: Funding Type (Grant, Contract, Award, etc.)
                c_ftype = Counter((f.type or "—") for f in funds_rows)
                if c_ftype:
                    labels, values = zip(*sorted(c_ftype.items(), key=lambda x: (-x[1], x[0])))
                    funding_stats["by_type"] = {"labels": list(labels), "values": list(values)}

                # Metric 3: Financial Volume by Currency
                # Sums up the monetary value per currency code (e.g., USD, EUR, CLP)
                sums = defaultdict(float)
                for f in funds_rows:
                    currency = (f.currency or "—").strip() or "—"
                    try:
                        amount = float(f.amount) if f.amount not in (None, "", "None") else 0.0
                    except ValueError:
                        amount = 0.0
                    sums[currency] += amount

                if sums:
                    # Sort by total amount descending
                    items = sorted(sums.items(), key=lambda x: (-x[1], x[0]))
                    labels, values = zip(*items)
                    funding_stats["by_currency_sum"] = {"labels": list(labels), "values": list(values)}

                # Metric 4: Top Funded Researchers
                c_fauth = Counter((f.orcid or "—") for f in funds_rows)
                top_fauth = c_fauth.most_common(10)
                if top_fauth:
                    labels, values = zip(*top_fauth)
                    funding_stats["top_authors"] = {"labels": list(labels), "values": list(values)}

        except Exception as exc:
            logger.exception("Error processing Funding statistics for ROR %s: %s", ror_id, exc)
            # We don't flash an error here to allow partial dashboard rendering (Works only)
            has_fundings = False

    # ---------------------------------------------------------
    # RENDER TEMPLATE
    # ---------------------------------------------------------
    return render_template(
        'dashboard/index.html',
        has_works=has_works,
        works=works_stats,
        has_fundings=has_fundings,
        fundings=funding_stats,
    )