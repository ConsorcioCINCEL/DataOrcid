"""Main application views, metrics, profile pages, and dataset downloads."""

import os
import io
import datetime as dt
import calendar
import logging
import math
import pandas as pd
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    session, send_from_directory, current_app, send_file
)
from flask_babel import _
from sqlalchemy import String, and_, case, cast, func, literal, or_

from .. import db, datetimeformat
from ..models import (
    WorkCache, FundingCache, WorkCacheRun, 
    FundingCacheRun, InstitutionRegistry, InstitutionResearcher,
    ResearcherAffiliationEvidence, ResearcherStatus, User, ResearcherCache,
    WorkRecordLink, OpenAlexWorkMetadata,
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
from ..services.canonical_work_service import canonical_work_counts
from ..services.data_health_service import health_presentation, institution_data_health
from ..services.duplicate_profile_service import build_duplicate_report

bp_main = Blueprint("main", __name__)
logger = logging.getLogger(__name__)


def _five_year_trend(model, year_column, ror_id):
    """Return the five most recent valid aggregate years in chronological order."""
    rows = (
        db.session.query(year_column, func.count(model.id))
        .filter(model.ror_id == ror_id)
        .group_by(year_column)
        .all()
    )
    valid_rows = sorted(
        [
            (str(year), count)
            for year, count in rows
            if year and str(year).isdigit()
        ],
        key=lambda item: int(item[0]),
        reverse=True,
    )[:5]
    valid_rows.reverse()
    if not valid_rows:
        return [], []
    labels, values = zip(*valid_rows)
    return list(labels), list(values)


def _safe_date_part(value):
    """Return a numeric date part or None when cached metadata is incomplete."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pagination_page_numbers(
    page: int,
    pages: int,
    radius: int = 2,
    full_range_limit: int = 50,
) -> list[int | None]:
    """Return every page for normal lists and a compact range for very large ones."""
    if pages <= full_range_limit:
        return list(range(1, pages + 1))

    visible_pages = {1, pages}
    visible_pages.update(range(max(page - radius, 1), min(page + radius, pages) + 1))

    page_numbers = []
    previous_page = None
    for page_number in sorted(visible_pages):
        if previous_page is not None and page_number - previous_page > 1:
            page_numbers.append(None)
        page_numbers.append(page_number)
        previous_page = page_number
    return page_numbers


def _is_within_comparison_period(month, day, cutoff):
    """Return whether a cached month/day falls within the comparison cutoff."""
    month_value = _safe_date_part(month)
    if month_value is None or not 1 <= month_value <= 12:
        return False
    if month_value < cutoff.month:
        return True
    if month_value > cutoff.month:
        return False

    day_value = _safe_date_part(day)
    if cutoff.day == calendar.monthrange(cutoff.year, cutoff.month)[1]:
        return day_value is None or 1 <= day_value <= cutoff.day
    return day_value is not None and 1 <= day_value <= cutoff.day


def _year_to_date_comparison(
    model,
    year_column,
    month_column,
    day_column,
    ror_id,
    today=None,
):
    """Compare equivalent year-to-date periods using records with usable months."""
    today = today or dt.date.today()
    if today.month == 1:
        cutoff = today
    else:
        cutoff = dt.date(today.year, today.month, 1) - dt.timedelta(days=1)

    previous_year = cutoff.year - 1
    previous_day = min(
        cutoff.day,
        calendar.monthrange(previous_year, cutoff.month)[1],
    )
    previous_cutoff = dt.date(previous_year, cutoff.month, previous_day)
    years = {str(cutoff.year), str(previous_year)}
    rows = (
        db.session.query(year_column, month_column, day_column)
        .filter(model.ror_id == ror_id, year_column.in_(years))
        .all()
    )

    current_count = 0
    previous_count = 0
    for year, month, day in rows:
        year_value = str(year or "")
        if year_value == str(cutoff.year) and _is_within_comparison_period(month, day, cutoff):
            current_count += 1
        elif year_value == str(previous_year) and _is_within_comparison_period(
            month,
            day,
            previous_cutoff,
        ):
            previous_count += 1

    change = None
    if previous_count:
        change = round(((current_count - previous_count) / previous_count) * 100)

    return {
        "change": change,
        "current_count": current_count,
        "previous_count": previous_count,
        "cutoff_date": cutoff,
        "previous_year": previous_year,
    }


def _dashboard_quality_summary(ror_id, affiliated_count):
    """Build lightweight coverage and duplicate-profile indicators for the home view."""
    affiliated_rows = (
        db.session.query(InstitutionResearcher.orcid)
        .join(
            InstitutionRegistry,
            InstitutionRegistry.id == InstitutionResearcher.institution_id,
        )
        .filter(
            InstitutionRegistry.ror_id == ror_id,
            InstitutionRegistry.is_active.is_(True),
            InstitutionResearcher.is_active.is_(True),
            InstitutionResearcher.is_verified.is_(True),
        )
        .distinct()
        .all()
    )
    affiliated_orcids = {row[0] for row in affiliated_rows if row[0]}
    cached_orcids = {
        row[0]
        for row in db.session.query(WorkCache.orcid)
        .filter_by(ror_id=ror_id)
        .distinct()
        .all()
        if row[0]
    }
    cached_orcids.update(
        row[0]
        for row in db.session.query(FundingCache.orcid)
        .filter_by(ror_id=ror_id)
        .distinct()
        .all()
        if row[0]
    )
    known_orcids = affiliated_orcids | cached_orcids
    affiliation_coverage = (
        round((len(affiliated_orcids) / len(known_orcids)) * 100)
        if known_orcids
        else 0
    )

    complete_profiles = 0
    if known_orcids:
        complete_profiles = (
            db.session.query(func.count(ResearcherCache.orcid))
            .filter(
                ResearcherCache.orcid.in_(known_orcids),
                or_(
                    func.trim(func.coalesce(ResearcherCache.credit_name, "")) != "",
                    func.trim(func.coalesce(ResearcherCache.given_names, "")) != "",
                    func.trim(func.coalesce(ResearcherCache.family_name, "")) != "",
                ),
            )
            .scalar()
            or 0
        )
    denominator = len(known_orcids) or affiliated_count or 0
    profile_completion = (
        round((complete_profiles / denominator) * 100)
        if denominator
        else 0
    )

    duplicate_groups = 0
    try:
        duplicate_report = build_duplicate_report(ror_ids=[ror_id])
        duplicate_groups = duplicate_report.get("summary", {}).get("candidate_groups", 0)
    except Exception as exc:
        logger.warning("Duplicate-profile summary unavailable for %s: %s", ror_id, exc)

    latest_profile_update = (
        db.session.query(func.max(ResearcherCache.updated_at))
        .filter(ResearcherCache.orcid.in_(known_orcids))
        .scalar()
        if known_orcids
        else None
    )

    return {
        "affiliation_coverage": affiliation_coverage,
        "verified_associations": len(affiliated_orcids),
        "inferred_associations": max(len(known_orcids) - len(affiliated_orcids), 0),
        "profile_completion": profile_completion,
        "duplicate_groups": duplicate_groups,
        "profile_update": latest_profile_update,
    }


def _dashboard_openalex_cache_key_expr():
    """Return the normalized OpenAlex cache key for an ORCID work row."""
    trimmed = func.lower(func.trim(WorkCache.doi))
    without_url = func.replace(
        func.replace(
            func.replace(
                func.replace(trimmed, "https://dx.doi.org/", ""),
                "http://dx.doi.org/",
                "",
            ),
            "https://doi.org/",
            "",
        ),
        "http://doi.org/",
        "",
    )
    normalized_doi = func.rtrim(func.trim(func.replace(without_url, "doi:", "")), ".")
    has_doi = and_(WorkCache.doi.isnot(None), func.trim(WorkCache.doi) != "")
    return case(
        (has_doi, normalized_doi),
        else_=literal("work:") + cast(WorkCache.id, String),
    )


def _dashboard_openalex_summary(ror_id: str) -> dict:
    """Build lightweight OpenAlex coverage and trend metrics for the home view."""
    cache_key = _dashboard_openalex_cache_key_expr().label("openalex_cache_key")
    eligible_keys = (
        db.session.query(cache_key)
        .filter(
            WorkCache.ror_id == ror_id,
            WorkCache.type == "journal-article",
        )
        .distinct()
        .subquery()
    )

    eligible = int(
        db.session.query(func.count()).select_from(eligible_keys).scalar() or 0
    )
    metrics = (
        db.session.query(
            func.count(OpenAlexWorkMetadata.id),
            func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0),
            func.coalesce(
                func.sum(
                    case(
                        (OpenAlexWorkMetadata.is_oa.is_(True), 1),
                        else_=0,
                    )
                ),
                0,
            ),
            func.max(OpenAlexWorkMetadata.updated_at),
        )
        .join(
            eligible_keys,
            OpenAlexWorkMetadata.doi_normalized
            == eligible_keys.c.openalex_cache_key,
        )
        .one()
    )
    matched = int(metrics[0] or 0)

    trend_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            func.count(OpenAlexWorkMetadata.id),
        )
        .join(
            eligible_keys,
            OpenAlexWorkMetadata.doi_normalized
            == eligible_keys.c.openalex_cache_key,
        )
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year)
        .order_by(OpenAlexWorkMetadata.publication_year.desc())
        .limit(5)
        .all()
    )
    trend_rows.reverse()

    return {
        "eligible": eligible,
        "matched": matched,
        "coverage": round((matched / eligible) * 100) if eligible else 0,
        "citations": int(metrics[1] or 0),
        "open_access": int(metrics[2] or 0),
        "last_update": metrics[3],
        "trend_years": [str(year) for year, _count in trend_rows],
        "trend_counts": [int(count or 0) for _year, count in trend_rows],
    }


@bp_main.app_context_processor
def inject_global_vars():
    """Inject the active institution's shared freshness state into the layout."""
    ror_id = get_active_ror_id()
    last_update = None
    data_health = None
    data_health_ui = health_presentation("empty")
    
    if ror_id:
        try:
            last_run = WorkCacheRun.query.filter_by(ror_id=ror_id, status='success').order_by(WorkCacheRun.finished_at.desc()).first()
            if not last_run:
                last_run = FundingCacheRun.query.filter_by(ror_id=ror_id, status='success').order_by(FundingCacheRun.finished_at.desc()).first()
            
            if last_run and last_run.finished_at:
                last_update = last_run.finished_at
            data_health = institution_data_health(ror_id)
            data_health_ui = health_presentation(data_health["state"])
        except Exception:
            # Layout status must never make an otherwise valid view unavailable.
            pass 
            
    return dict(
        last_works_update=last_update,
        global_data_health=data_health,
        global_data_health_ui=data_health_ui,
    )
@bp_main.route('/')
@login_required
def index():
    """
    Main landing view (Dashboard Home).
    
    Logic:
    1. Determines the active ROR context.
    2. Checks the local database for cached records (Works/Fundings).
    3. Calculates a 5-year production trend for the dashboard chart.
    4. If no local cache exists, attempts to fetch a *live count* of researchers
       from ORCID to show the user what *could* be imported.
    """
    ror_id = get_active_ror_id()
    researcher_count = None
    affiliation_hist = None
    
    # Initialize counters and trend data
    works_count = 0
    fundings_count = 0
    trend_years = []
    trend_counts = []
    funding_trend_years = []
    funding_trend_counts = []
    publication_comparison = {
        "change": None,
        "current_count": 0,
        "previous_count": 0,
        "cutoff_date": None,
        "previous_year": None,
    }
    funding_comparison = publication_comparison.copy()
    last_works_run = None
    last_fundings_run = None
    quality = {
        "affiliation_coverage": 0,
        "verified_associations": 0,
        "inferred_associations": 0,
        "profile_completion": 0,
        "duplicate_groups": 0,
        "profile_update": None,
    }
    data_health = institution_data_health(ror_id)
    unique_work_summary = {
        "source_records": 0,
        "unique_outputs": 0,
        "doi_outputs": 0,
        "fallback_outputs": 0,
        "excess_records": 0,
    }
    openalex_summary = {
        "eligible": 0,
        "matched": 0,
        "coverage": 0,
        "citations": 0,
        "open_access": 0,
        "last_update": None,
        "trend_years": [],
        "trend_counts": [],
    }

    if ror_id:
        # Fetch local DB stats
        works_count = db.session.query(WorkCache.id).filter_by(ror_id=ror_id).count()
        fundings_count = db.session.query(FundingCache.id).filter_by(ror_id=ror_id).count()
        try:
            unique_work_summary = canonical_work_counts(ror_id)
        except Exception as exc:
            logger.warning("Canonical work summary unavailable for %s: %s", ror_id, exc)

        # Prefer the complete search-backed association snapshot when available.
        rc = (
            db.session.query(InstitutionResearcher.orcid)
            .join(
                InstitutionRegistry,
                InstitutionRegistry.id == InstitutionResearcher.institution_id,
            )
            .filter(
                InstitutionRegistry.ror_id == ror_id,
                InstitutionRegistry.is_active.is_(True),
                InstitutionResearcher.is_active.is_(True),
            )
            .distinct()
            .count()
        )
        if rc == 0:
            rc = db.session.query(WorkCache.orcid).filter_by(ror_id=ror_id).distinct().count()
        if rc == 0:
            rc = db.session.query(FundingCache.orcid).filter_by(ror_id=ror_id).distinct().count()

        last_works_run = (
            WorkCacheRun.query.filter_by(ror_id=ror_id, status="success")
            .order_by(WorkCacheRun.finished_at.desc())
            .first()
        )
        last_fundings_run = (
            FundingCacheRun.query.filter_by(ror_id=ror_id, status="success")
            .order_by(FundingCacheRun.finished_at.desc())
            .first()
        )

        # Calculate five-year publication and funding trends from cached data.
        if works_count > 0:
            try:
                trend_years, trend_counts = _five_year_trend(
                    WorkCache,
                    WorkCache.pub_year,
                    ror_id,
                )
                publication_comparison = _year_to_date_comparison(
                    WorkCache,
                    WorkCache.pub_year,
                    WorkCache.pub_month,
                    WorkCache.pub_day,
                    ror_id,
                )
            except Exception as e:
                logger.error(f"Error fetching 5-year trend for index: {e}")

        if fundings_count > 0:
            try:
                funding_trend_years, funding_trend_counts = _five_year_trend(
                    FundingCache,
                    FundingCache.start_y,
                    ror_id,
                )
                funding_comparison = _year_to_date_comparison(
                    FundingCache,
                    FundingCache.start_y,
                    FundingCache.start_m,
                    FundingCache.start_d,
                    ror_id,
                )
            except Exception as exc:
                logger.error("Error fetching funding trend for index: %s", exc)

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

        try:
            quality = _dashboard_quality_summary(ror_id, researcher_count or 0)
        except Exception as exc:
            logger.warning("Dashboard quality metrics unavailable for %s: %s", ror_id, exc)

        try:
            openalex_summary = _dashboard_openalex_summary(ror_id)
        except Exception as exc:
            logger.warning("Dashboard OpenAlex metrics unavailable for %s: %s", ror_id, exc)
        
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
        has_any_cache=has_any_cache,
        trend_years=list(trend_years),
        trend_counts=list(trend_counts),
        trend_labels=[
            _("%(year)s YTD", year=year) if year == str(dt.date.today().year) else year
            for year in trend_years
        ],
        publication_comparison=publication_comparison,
        funding_trend_years=list(funding_trend_years),
        funding_trend_counts=list(funding_trend_counts),
        funding_trend_labels=[
            _("%(year)s YTD", year=year) if year == str(dt.date.today().year) else year
            for year in funding_trend_years
        ],
        funding_comparison=funding_comparison,
        quality=quality,
        data_health=data_health,
        data_health_ui=health_presentation(data_health["state"]),
        unique_work_summary=unique_work_summary,
        openalex_summary=openalex_summary,
        openalex_trend_labels=[
            _("%(year)s YTD", year=year)
            if year == str(dt.date.today().year)
            else year
            for year in openalex_summary["trend_years"]
        ],
        last_works_run=last_works_run,
        last_fundings_run=last_fundings_run,
        can_sync=bool(session.get("is_admin") or session.get("is_manager")),
    )


def _researcher_directory_rows(ror_id: str) -> list[dict]:
    """Build one normalized directory row per researcher in the active scope."""
    institution = InstitutionRegistry.query.filter_by(ror_id=ror_id).first()
    association_rows = []
    if institution:
        association_rows = InstitutionResearcher.query.filter_by(
            institution_id=institution.id,
            is_active=True,
        ).all()
    association_map = {row.orcid: row for row in association_rows}

    orcid_ids = set(association_map)
    for model in (WorkCache, FundingCache, ResearcherStatus):
        orcid_ids.update(
            orcid
            for (orcid,) in db.session.query(model.orcid).filter(
                model.ror_id == ror_id,
                model.orcid.isnot(None),
                model.orcid != '',
            ).distinct().all()
        )

    metadata = {}
    status_map = {}
    sorted_orcids = sorted(orcid_ids)
    for start in range(0, len(sorted_orcids), 500):
        chunk = sorted_orcids[start:start + 500]
        metadata.update({
            row.orcid: row
            for row in ResearcherCache.query.filter(ResearcherCache.orcid.in_(chunk)).all()
        })
        status_map.update({
            row.orcid: bool(row.is_managed_by_am)
            for row in ResearcherStatus.query.filter(
                ResearcherStatus.ror_id == ror_id,
                ResearcherStatus.orcid.in_(chunk),
            ).all()
        })

    work_counts = dict(
        db.session.query(WorkCache.orcid, func.count(WorkCache.id))
        .filter(WorkCache.ror_id == ror_id)
        .group_by(WorkCache.orcid)
        .all()
    )
    funding_counts = dict(
        db.session.query(FundingCache.orcid, func.count(FundingCache.id))
        .filter(FundingCache.ror_id == ror_id)
        .group_by(FundingCache.orcid)
        .all()
    )
    institution_name = institution.name if institution else session.get('institution_name')
    all_researchers = []
    for orcid in sorted_orcids:
        profile = metadata.get(orcid)
        association = association_map.get(orcid)
        matches = {
            'ror': bool(association and association.matched_by_ror),
            'grid': bool(association and association.matched_by_grid),
            'ringgold': bool(association and association.matched_by_ringgold),
        }
        given_names = profile.given_names if profile else None
        family_name = profile.family_name if profile else None
        credit_name = profile.credit_name if profile else None
        display_name = credit_name or f"{given_names or ''} {family_name or ''}".strip() or orcid
        matched_by = [key for key, matched in matches.items() if matched]
        all_researchers.append({
            'orcid-id': orcid,
            'given-names': given_names,
            'family-names': family_name,
            'credit-name': credit_name,
            'display-name': display_name,
            'email': profile.email if profile else None,
            'institution-name': institution_name,
            'matched_identifiers': matches,
            'matched_by': matched_by,
            'association_verified': bool(association and association.is_verified),
            'association_evidence': (
                association.evidence_type
                if association
                else 'cache_fallback'
            ),
            'association_sources': (
                association.evidence_sources or []
                if association
                else []
            ),
            'profile_complete': bool(credit_name or given_names or family_name),
            'is_managed': status_map.get(orcid, False),
            'works_count': int(work_counts.get(orcid, 0)),
            'fundings_count': int(funding_counts.get(orcid, 0)),
            'total_activity': int(work_counts.get(orcid, 0) + funding_counts.get(orcid, 0)),
        })

    return all_researchers


def _filter_researcher_directory(rows: list[dict]) -> tuple[list[dict], dict]:
    """Apply server-side directory filters and stable sorting."""
    query = (request.args.get('q') or '').strip()
    am_status = request.args.get('am', 'all')
    if am_status not in {'all', 'managed', 'unmanaged'}:
        am_status = 'all'
    match_source = request.args.get('match', 'all')
    if match_source not in {'all', 'verified', 'inferred', 'ror', 'grid', 'ringgold'}:
        match_source = 'all'
    sort = request.args.get('sort', 'name')
    if sort not in {
        'name', 'orcid', 'activity', 'managed', 'relationship', 'works', 'fundings'
    }:
        sort = 'name'
    direction = request.args.get('dir', 'asc').lower()
    if direction not in {'asc', 'desc'}:
        direction = 'asc'

    filtered = rows
    if query:
        needle = query.casefold()
        filtered = [
            row for row in filtered
            if needle in ' '.join([
                row.get('display-name') or '',
                row.get('orcid-id') or '',
                row.get('email') or '',
                row.get('institution-name') or '',
            ]).casefold()
        ]
    if am_status != 'all':
        expected = am_status == 'managed'
        filtered = [row for row in filtered if row['is_managed'] is expected]
    if match_source == 'verified':
        filtered = [row for row in filtered if row['association_verified']]
    elif match_source == 'inferred':
        filtered = [row for row in filtered if not row['association_verified']]
    elif match_source != 'all':
        filtered = [row for row in filtered if row['matched_identifiers'].get(match_source)]

    key_functions = {
        'name': lambda row: ((row.get('display-name') or '').casefold(), row['orcid-id']),
        'orcid': lambda row: row['orcid-id'],
        'activity': lambda row: row['total_activity'],
        'managed': lambda row: bool(row['is_managed']),
        'relationship': lambda row: bool(row['association_verified']),
        'works': lambda row: row['works_count'],
        'fundings': lambda row: row['fundings_count'],
    }
    if sort in {'name', 'orcid'}:
        filtered = sorted(
            filtered,
            key=key_functions[sort],
            reverse=direction == 'desc',
        )
    else:
        filtered = sorted(filtered, key=key_functions['name'])
        filtered = sorted(
            filtered,
            key=key_functions[sort],
            reverse=direction == 'desc',
        )
    return filtered, {
        'q': query,
        'am': am_status,
        'match': match_source,
        'sort': sort,
        'dir': direction,
    }


@bp_main.route('/researcher-list')
@login_required
def researcher_list():
    """Display a searchable, paginated directory for the active institution."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active ROR ID found.'))
        return redirect(url_for('main.index'))

    all_researchers = _researcher_directory_rows(ror_id)
    filtered_researchers, filters = _filter_researcher_directory(all_researchers)
    try:
        page = max(int(request.args.get('page', 1)), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        per_page = int(request.args.get('per_page', 10))
    except (TypeError, ValueError):
        per_page = 10
    per_page = per_page if per_page in {10, 25, 50} else 10
    total = len(filtered_researchers)
    pages = max(math.ceil(total / per_page), 1)
    page = min(page, pages)
    start = (page - 1) * per_page
    researchers = filtered_researchers[start:start + per_page]
    pagination = {
        'page': page,
        'pages': pages,
        'per_page': per_page,
        'total': total,
        'start': start + 1 if total else 0,
        'end': min(start + per_page, total),
        'has_prev': page > 1,
        'has_next': page < pages,
        'prev_page': max(page - 1, 1),
        'next_page': min(page + 1, pages),
        'page_numbers': _pagination_page_numbers(page, pages),
    }
    summary = {
        'total': len(all_researchers),
        'managed': sum(1 for row in all_researchers if row['is_managed']),
        'complete': sum(1 for row in all_researchers if row['profile_complete']),
        'associated': sum(1 for row in all_researchers if row['association_verified']),
        'inferred': sum(1 for row in all_researchers if not row['association_verified']),
    }

    session['researcher_count'] = len(all_researchers)
    return render_template(
        'main/researcher_list.html',
        researchers=researchers,
        filters=filters,
        pagination=pagination,
        summary=summary,
        has_active_filters=bool(
            filters['q']
            or filters['am'] != 'all'
            or filters['match'] != 'all'
        ),
    )


@bp_main.route('/researcher-list/export')
@login_required
def researcher_list_export():
    """Export all researcher rows matching the current directory filters."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active ROR ID found.'))
        return redirect(url_for('main.index'))

    rows, _filters = _filter_researcher_directory(_researcher_directory_rows(ror_id))
    data_frame = pd.DataFrame([{
        _('Name'): row['display-name'],
        _('ORCID iD'): row['orcid-id'],
        _('Email'): row['email'] or '',
        _('Institution'): row['institution-name'] or '',
        _('Affiliation Manager'): _('Yes') if row['is_managed'] else _('No'),
        _('Matched by'): ', '.join(value.upper() for value in row['matched_by']),
        _('Relationship evidence'): (
            _('Verified') if row['association_verified'] else _('Inferred from local caches')
        ),
        _('Works'): row['works_count'],
        _('Fundings'): row['fundings_count'],
    } for row in rows])
    output = io.BytesIO()
    export_format = request.args.get('format', 'csv').lower()
    if export_format == 'xlsx':
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            data_frame.to_excel(writer, index=False, sheet_name=_('Researchers')[:31])
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f'researchers_{ror_id}.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    output.write(data_frame.to_csv(index=False).encode('utf-8-sig'))
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f'researchers_{ror_id}.csv', mimetype='text/csv')


@bp_main.route('/works-fundings')
@login_required
def cache_dashboard():
    """Shortcut redirect to the detailed data download page."""
    return redirect(url_for('works.cache_works_status'))
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


def _optional_year(value) -> int | None:
    try:
        year = int(value)
    except (TypeError, ValueError):
        return None
    return year if 1000 <= year <= 9999 else None


def _metrics_filter_values() -> dict:
    """Normalize the shared filters used by ORCID analytics and its exports."""
    year_from = _optional_year(request.args.get('year_from'))
    year_to = _optional_year(request.args.get('year_to'))
    if year_from and year_to and year_from > year_to:
        year_from, year_to = year_to, year_from
    return {
        'year_from': year_from,
        'year_to': year_to,
        'work_type': [value for value in request.args.getlist('work_type') if value],
        'funding_type': [value for value in request.args.getlist('funding_type') if value],
        'researcher': (request.args.get('researcher') or '').strip(),
    }


def _filtered_work_query(query, ror_id: str, filters: dict):
    query = query.filter(WorkCache.ror_id == ror_id)
    if filters['year_from']:
        query = query.filter(WorkCache.pub_year >= str(filters['year_from']))
    if filters['year_to']:
        query = query.filter(WorkCache.pub_year <= str(filters['year_to']))
    if filters['work_type']:
        query = query.filter(WorkCache.type.in_(filters['work_type']))
    if filters['researcher']:
        query = query.filter(WorkCache.orcid == filters['researcher'])
    return query


def _filtered_funding_query(query, ror_id: str, filters: dict):
    query = query.filter(FundingCache.ror_id == ror_id)
    if filters['year_from']:
        query = query.filter(FundingCache.start_y >= str(filters['year_from']))
    if filters['year_to']:
        query = query.filter(FundingCache.start_y <= str(filters['year_to']))
    if filters['funding_type']:
        query = query.filter(FundingCache.type.in_(filters['funding_type']))
    if filters['researcher']:
        query = query.filter(FundingCache.orcid == filters['researcher'])
    return query


def _researcher_name(orcid: str, given: str | None, family: str | None, credit: str | None) -> str:
    return credit or f"{given or ''} {family or ''}".strip() or orcid or _('Unknown Researcher')


@bp_main.route('/metrics-panel')
@login_required
def metrics_panel():
    """Render filterable ORCID work and funding analytics in compact sections."""
    ror_id = get_active_ror_id()
    filters = _metrics_filter_values()
    section = request.args.get('section', 'overview')
    if section not in {'overview', 'publications', 'funding', 'researchers'}:
        section = 'overview'
    metrics = {
        'total_works': 0, 'total_fundings': 0, 'active_researchers': 0, 'last_update': _('Not available'),
        'chart_years': [], 'chart_counts': [], 'chart_types_labels': [], 'chart_types_values': [],
        'chart_journals_labels': [], 'chart_journals_values': [], 'top_researchers_works': [],
        'chart_funding_years': [], 'chart_funding_counts': [], 'chart_funding_types_labels': [],
        'chart_funding_types_values': [], 'chart_funding_orgs_labels': [], 'chart_funding_orgs_values': [],
        'top_researchers_fundings': [],
    }
    filter_options = {'years': [], 'work_types': [], 'funding_types': [], 'researchers': []}

    if ror_id:
        year_values = {
            str(value) for (value,) in db.session.query(WorkCache.pub_year).filter(WorkCache.ror_id == ror_id).distinct().all()
            if value and str(value).isdigit()
        }
        year_values.update(
            str(value) for (value,) in db.session.query(FundingCache.start_y).filter(FundingCache.ror_id == ror_id).distinct().all()
            if value and str(value).isdigit()
        )
        filter_options['years'] = sorted(year_values, key=int, reverse=True)
        filter_options['work_types'] = [
            value for (value,) in db.session.query(WorkCache.type).filter(
                WorkCache.ror_id == ror_id, WorkCache.type.isnot(None), WorkCache.type != ''
            ).distinct().order_by(WorkCache.type).all()
        ]
        filter_options['funding_types'] = [
            value for (value,) in db.session.query(FundingCache.type).filter(
                FundingCache.ror_id == ror_id, FundingCache.type.isnot(None), FundingCache.type != ''
            ).distinct().order_by(FundingCache.type).all()
        ]
        filter_options['researchers'] = [
            {'orcid': row['orcid-id'], 'name': row['display-name']}
            for row in _researcher_directory_rows(ror_id)
        ]

        work_count_query = _filtered_work_query(db.session.query(func.count(WorkCache.id)), ror_id, filters)
        funding_count_query = _filtered_funding_query(db.session.query(func.count(FundingCache.id)), ror_id, filters)
        metrics['total_works'] = int(work_count_query.scalar() or 0)
        metrics['total_fundings'] = int(funding_count_query.scalar() or 0)
        work_orcids = {
            row[0] for row in _filtered_work_query(db.session.query(WorkCache.orcid), ror_id, filters).distinct().all() if row[0]
        }
        funding_orcids = {
            row[0] for row in _filtered_funding_query(db.session.query(FundingCache.orcid), ror_id, filters).distinct().all() if row[0]
        }
        metrics['active_researchers'] = len(work_orcids | funding_orcids)
        last_run = WorkCacheRun.query.filter_by(ror_id=ror_id, status='success').order_by(WorkCacheRun.finished_at.desc()).first()
        if last_run and last_run.finished_at:
            metrics['last_update'] = datetimeformat(last_run.finished_at)

        year_stats = _filtered_work_query(
            db.session.query(WorkCache.pub_year, func.count(WorkCache.id)), ror_id, filters
        ).group_by(WorkCache.pub_year).order_by(WorkCache.pub_year).all()
        metrics['chart_years'] = [str(row[0]) for row in year_stats if row[0] and str(row[0]).isdigit()]
        metrics['chart_counts'] = [row[1] for row in year_stats if row[0] and str(row[0]).isdigit()]

        work_type_stats = _filtered_work_query(
            db.session.query(WorkCache.type, func.count(WorkCache.id)), ror_id, filters
        ).group_by(WorkCache.type).order_by(func.count(WorkCache.id).desc()).all()
        metrics['chart_types_labels'] = [str(row[0] or _('Unknown type')).replace('_', ' ').title() for row in work_type_stats]
        metrics['chart_types_values'] = [row[1] for row in work_type_stats]

        funding_year_stats = _filtered_funding_query(
            db.session.query(FundingCache.start_y, func.count(FundingCache.id)), ror_id, filters
        ).group_by(FundingCache.start_y).order_by(FundingCache.start_y).all()
        metrics['chart_funding_years'] = [str(row[0]) for row in funding_year_stats if row[0] and str(row[0]).isdigit()]
        metrics['chart_funding_counts'] = [row[1] for row in funding_year_stats if row[0] and str(row[0]).isdigit()]

        funding_type_stats = _filtered_funding_query(
            db.session.query(FundingCache.type, func.count(FundingCache.id)), ror_id, filters
        ).group_by(FundingCache.type).order_by(func.count(FundingCache.id).desc()).all()
        metrics['chart_funding_types_labels'] = [str(row[0] or _('Unknown type')).replace('_', ' ').title() for row in funding_type_stats]
        metrics['chart_funding_types_values'] = [row[1] for row in funding_type_stats]

        org_col = FundingCache.org_name
        funding_orgs = _filtered_funding_query(
            db.session.query(org_col, func.count(FundingCache.id)), ror_id, filters
        ).filter(org_col.isnot(None), org_col != '').group_by(org_col).order_by(func.count(FundingCache.id).desc()).limit(10).all()
        metrics['chart_funding_orgs_labels'] = [str(row[0])[:40] for row in funding_orgs]
        metrics['chart_funding_orgs_values'] = [row[1] for row in funding_orgs]

        journals = _filtered_work_query(
            db.session.query(WorkCache.journal_title, func.count(WorkCache.id)), ror_id, filters
        ).filter(WorkCache.journal_title.isnot(None), WorkCache.journal_title != '').group_by(WorkCache.journal_title).order_by(func.count(WorkCache.id).desc()).limit(10).all()
        metrics['chart_journals_labels'] = [str(row[0])[:40] for row in journals]
        metrics['chart_journals_values'] = [row[1] for row in journals]

        raw_works = _filtered_work_query(
            db.session.query(
                WorkCache.orcid, func.count(WorkCache.id), ResearcherCache.given_names,
                ResearcherCache.family_name, ResearcherCache.credit_name,
            ).outerjoin(ResearcherCache, WorkCache.orcid == ResearcherCache.orcid),
            ror_id, filters,
        ).group_by(WorkCache.orcid, ResearcherCache.given_names, ResearcherCache.family_name, ResearcherCache.credit_name).order_by(func.count(WorkCache.id).desc()).limit(10).all()
        metrics['top_researchers_works'] = [
            (orcid, count, _researcher_name(orcid, given, family, credit))
            for orcid, count, given, family, credit in raw_works
        ]

        raw_fundings = _filtered_funding_query(
            db.session.query(
                FundingCache.orcid, func.count(FundingCache.id), ResearcherCache.given_names,
                ResearcherCache.family_name, ResearcherCache.credit_name,
            ).outerjoin(ResearcherCache, FundingCache.orcid == ResearcherCache.orcid),
            ror_id, filters,
        ).group_by(FundingCache.orcid, ResearcherCache.given_names, ResearcherCache.family_name, ResearcherCache.credit_name).order_by(func.count(FundingCache.id).desc()).limit(10).all()
        metrics['top_researchers_fundings'] = [
            (orcid, count, _researcher_name(orcid, given, family, credit))
            for orcid, count, given, family, credit in raw_fundings
        ]

    def metrics_url(**updates):
        params = request.args.to_dict(flat=False)
        params.update({key: value for key, value in updates.items() if value is not None})
        for key, value in list(params.items()):
            if value is None or value == '' or value == []:
                params.pop(key)
        return url_for('main.metrics_panel', **params)

    def metric_export_url(chart_type: str, export_format: str):
        params = request.args.to_dict(flat=False)
        params.pop('section', None)
        params['format'] = export_format
        return url_for('main.download_metrics_data', chart_type=chart_type, **params)

    return render_template(
        'main/metrics.html', stats=metrics, filters=filters, filter_options=filter_options,
        section=section, metrics_url=metrics_url, metric_export_url=metric_export_url,
        has_active_filters=bool(filters['year_from'] or filters['year_to'] or filters['work_type'] or filters['funding_type'] or filters['researcher']),
    )


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
    filters = _metrics_filter_values()
    
    if not ror_id:
        return redirect(url_for('main.metrics_panel'))

    data = []
    filename = f"chart_{chart_type}"

    try:
        # Generate data based on requested chart type
        if chart_type == 'trend_works':
            query = _filtered_work_query(db.session.query(WorkCache.pub_year.label('Year'), func.count(WorkCache.id).label('Count')), ror_id, filters).group_by(WorkCache.pub_year).order_by(WorkCache.pub_year).all()
            data = [{'Year': r[0], 'Works': r[1]} for r in query if r[0]]
        
        elif chart_type == 'trend_fundings':
            query = _filtered_funding_query(db.session.query(FundingCache.start_y.label('Year'), func.count(FundingCache.id).label('Count')), ror_id, filters).group_by(FundingCache.start_y).order_by(FundingCache.start_y).all()
            data = [{'Year': r[0], 'Fundings': r[1]} for r in query if r[0]]

        elif chart_type == 'types_works':
            query = _filtered_work_query(db.session.query(WorkCache.type.label('Type'), func.count(WorkCache.id).label('Count')), ror_id, filters).group_by(WorkCache.type).all()
            data = [{'Type': str(r[0]).replace('_', ' ').title(), 'Count': r[1]} for r in query]

        elif chart_type == 'types_fundings':
            query = _filtered_funding_query(db.session.query(FundingCache.type.label('Type'), func.count(FundingCache.id).label('Count')), ror_id, filters).group_by(FundingCache.type).all()
            data = [{'Type': str(r[0]).replace('_', ' ').title(), 'Count': r[1]} for r in query]

        elif chart_type == 'journals':
            query = _filtered_work_query(db.session.query(WorkCache.journal_title.label('Journal'), func.count(WorkCache.id).label('Count')), ror_id, filters).filter(WorkCache.journal_title.isnot(None)).group_by(WorkCache.journal_title).order_by(func.count(WorkCache.id).desc()).limit(50).all()
            data = [{'Journal': r[0], 'Count': r[1]} for r in query]

        elif chart_type == 'funding_orgs':
            org_col = FundingCache.org_name
            query = _filtered_funding_query(db.session.query(org_col.label('Organization'), func.count(FundingCache.id).label('Count')), ror_id, filters).group_by(org_col).order_by(func.count(FundingCache.id).desc()).limit(50).all()
            data = [{'Organization': r[0], 'Count': r[1]} for r in query]

        elif chart_type == 'top_researchers_works':
            # Joined query for proper name export
            query = _filtered_work_query(db.session.query(
                WorkCache.orcid, 
                func.count(WorkCache.id),
                ResearcherCache.given_names,
                ResearcherCache.family_name,
                ResearcherCache.credit_name
            ).outerjoin(ResearcherCache, WorkCache.orcid == ResearcherCache.orcid), ror_id, filters)\
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
            query = _filtered_funding_query(db.session.query(
                FundingCache.orcid, 
                func.count(FundingCache.id),
                ResearcherCache.given_names,
                ResearcherCache.family_name,
                ResearcherCache.credit_name
            ).outerjoin(ResearcherCache, FundingCache.orcid == ResearcherCache.orcid), ror_id, filters)\
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
    """Render a cached public ORCID profile with optional explicit refresh."""
    force_refresh = request.args.get("refresh") == "1"
    data = get_full_orcid_profile(orcid_id, force_refresh=force_refresh)
    if not data:
        flash_err(_("The requested ORCID profile could not be loaded."))
        return redirect(url_for('main.researcher_list'))
    person = data.get("person") or {}
    biography = person.get("biography") or {}
    active_ror = get_active_ror_id()
    relationship_query = (
        db.session.query(InstitutionResearcher, InstitutionRegistry)
        .join(
            InstitutionRegistry,
            InstitutionRegistry.id == InstitutionResearcher.institution_id,
        )
        .filter(
            InstitutionResearcher.orcid == orcid_id,
            InstitutionResearcher.is_active.is_(True),
        )
    )
    if active_ror:
        relationship_query = relationship_query.filter(
            InstitutionRegistry.ror_id == active_ror
        )
    relationship_rows = relationship_query.order_by(InstitutionRegistry.name.asc()).all()
    institution_ids = [institution.id for _, institution in relationship_rows]
    evidence_rows = []
    if institution_ids:
        evidence_rows = (
            ResearcherAffiliationEvidence.query.filter(
                ResearcherAffiliationEvidence.orcid == orcid_id,
                ResearcherAffiliationEvidence.institution_id.in_(institution_ids),
            )
            .order_by(
                ResearcherAffiliationEvidence.is_current.desc(),
                ResearcherAffiliationEvidence.start_year.desc(),
            )
            .all()
        )
    work_query = WorkRecordLink.query.filter_by(orcid=orcid_id)
    funding_query = FundingCache.query.filter_by(orcid=orcid_id)
    source_work_query = WorkCache.query.filter_by(orcid=orcid_id)
    if active_ror:
        work_query = work_query.filter_by(ror_id=active_ror)
        funding_query = funding_query.filter_by(ror_id=active_ror)
        source_work_query = source_work_query.filter_by(ror_id=active_ror)
    local_context = {
        "relationships": [
            {
                "institution": institution.name,
                "ror_id": institution.ror_id,
                "verified": association.is_verified,
                "evidence_type": association.evidence_type,
                "sources": association.evidence_sources or [],
                "last_seen_at": association.last_seen_at,
            }
            for association, institution in relationship_rows
        ],
        "affiliations": evidence_rows,
        "unique_outputs": int(
            work_query.with_entities(
                func.count(func.distinct(WorkRecordLink.canonical_work_id))
            ).scalar()
            or 0
        ),
        "source_work_records": source_work_query.count(),
        "funding_records": funding_query.count(),
        "active_ror": active_ror,
    }
    return render_template(
        'main/orcid_profile.html',
        data=data,
        bio_text=biography.get("content") or biography.get("value") or "",
        profile_refreshed=force_refresh,
        local_context=local_context,
    )


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
