"""Work/funding cache management views and exports."""

import logging

import pandas as pd
from flask import g, redirect, render_template, request, url_for
from flask_babel import _

from .. import plain_text
from ..decorators import institution_required, login_required, staff_required
from ..utils.flashes import flash_err
from ..utils.session_helpers import get_active_ror_id

from .works_blueprint import bp_works
from .works_analytics_cache import (
    _openalex_global_analytics_with_cache,
    _openalex_institution_analytics_with_cache,
)
from .works_global_analytics import _openalex_global_analytics
from .works_institution_analytics import _openalex_analytics
from .works_openalex_data import (
    _openalex_institution_export_query,
    _openalex_work_rows,
    _request_list_arg,
    _send_openalex_institution_export,
)
from .works_shared import (
    _page_params,
    _priority_article_export_rows,
    _priority_institution_export_rows,
    _priority_source_export_rows,
    _send_dataframe_export,
    _summary_labels,
)
logger = logging.getLogger(__name__)
@bp_works.route('/openalex/works')
@login_required
def openalex_works():
    """Render OpenAlex enrichment coverage for DOI-backed works."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    coverage = request.args.get("coverage", "all")
    if coverage not in {"all", "enriched", "missing", "not_found", "no_doi"}:
        coverage = "all"

    page, per_page = _page_params(default_per_page=10)
    search = request.args.get("q", "").strip()
    sort = request.args.get("sort", "citations")
    if sort not in {"title", "year", "citations", "open_access", "source", "status"}:
        sort = "citations"
    direction = request.args.get("dir", "desc").lower()
    if direction not in {"asc", "desc"}:
        direction = "desc"
    rows, summary, pagination = _openalex_work_rows(
        ror_id,
        coverage=coverage,
        page=page,
        per_page=per_page,
        search=search,
        sort=sort,
        direction=direction,
    )
    return render_template(
        'works/openalex_works.html',
        rows=rows,
        summary=summary,
        pagination=pagination,
        coverage=coverage,
        query=search,
        sort=sort,
        direction=direction,
        ror_id=ror_id,
    )


@bp_works.route('/openalex/works/export')
@login_required
def openalex_works_export():
    """Export OpenAlex enrichment detail for the active institution."""
    ror_id = get_active_ror_id()
    if not ror_id:
        flash_err(_('No active institution context found.'))
        return redirect(url_for('main.index'))

    coverage = request.args.get("coverage", "all")
    if coverage not in {"all", "enriched", "missing", "not_found", "no_doi"}:
        coverage = "all"

    search = request.args.get("q", "").strip()
    sort = request.args.get("sort", "citations")
    direction = request.args.get("dir", "desc").lower()
    if request.args.get("background") == "1":
        from ..services.export_jobs import queue_export_response

        return queue_export_response(
            "institution_openalex",
            request.args.get("format") or "csv",
            {
                "ror_id": ror_id,
                "coverage": coverage,
                "search": search,
                "sort": sort,
                "direction": direction,
            },
            _("OpenAlex enrichment"),
        )
    records_query = _openalex_institution_export_query(
        ror_id,
        coverage=coverage,
        search=search,
        sort=sort,
        direction=direction,
    )
    return _send_openalex_institution_export(records_query, ror_id, coverage)


@bp_works.route('/openalex/analytics')
@institution_required
def openalex_analytics():
    """Render OpenAlex analytics for the account's authorized institution."""
    ror_id = g.institution_ror_id

    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "language": _request_list_arg("language"),
        "affiliation": _request_list_arg("affiliation"),
        "metrics": _request_list_arg("metric"),
    }
    section = request.args.get("section", "overview")
    if section not in {"overview", "open_access", "collaboration", "topics", "impact"}:
        section = "overview"
    analytics_filters["section"] = section
    analytics = _openalex_institution_analytics_with_cache(ror_id, analytics_filters)

    def query_url(**updates):
        params = request.args.to_dict(flat=False)
        params.update({key: value for key, value in updates.items() if value is not None})
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_analytics", **params)

    table_prefixes = {
        "priority_sources": "priority_source",
        "priority_articles": "priority_article",
    }

    def table_url(table_key: str, **updates):
        params = request.args.to_dict(flat=False)
        prefix = table_prefixes.get(table_key, table_key)
        for key, value in updates.items():
            if key in {"q", "sort", "dir", "page", "per_page"}:
                params[f"{prefix}_{key}"] = value
            else:
                params[key] = value
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_analytics", **params)

    def export_url(table_key: str, export_format: str):
        params = request.args.to_dict(flat=False)
        params["format"] = export_format
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_analytics_export", table_key=table_key, **params)

    return render_template(
        'works/openalex_analytics.html',
        analytics=analytics,
        section=section,
        query_url=query_url,
        has_active_filters=bool(
            analytics_filters["year_from"] or analytics_filters["year_to"]
            or analytics_filters["type"] or analytics_filters["oa_status"]
            or analytics_filters["language"]
            or analytics_filters["affiliation"]
        ),
        ror_id=ror_id,
        table_url=table_url,
        export_url=export_url,
    )


@bp_works.route('/openalex/analytics/export/<table_key>')
@institution_required
def openalex_analytics_export(table_key: str):
    """Export OpenAlex analytics for the account's authorized institution."""
    ror_id = g.institution_ror_id

    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "language": _request_list_arg("language"),
        "affiliation": _request_list_arg("affiliation"),
    }
    analytics = _openalex_analytics(ror_id, analytics_filters)

    if table_key == "top_cited":
        rows = [{
            "title": plain_text(row["title"]),
            "openalex_id": row["openalex_id"],
            "publication_year": row["publication_year"],
            "type": row["type"],
            "source": row["source_name"],
            "citations": row["cited_by_count"],
            "fwci": row["fwci"],
            "is_open_access": row["is_oa"],
            "oa_status": row["oa_status"],
            "topic_field": row["primary_topic_field"],
            "topic_domain": row["primary_topic_domain"],
        } for row in analytics["top_cited"]]
        sheet_name = "Top cited"
    elif table_key == "priority_sources":
        rows = _priority_source_export_rows(analytics["priority_open_access"])
        sheet_name = "Diamond Green sources"
    elif table_key == "priority_articles":
        rows = _priority_article_export_rows(analytics["priority_open_access"])
        sheet_name = "Diamond Green articles"
    elif table_key == "authors":
        rows = [{
            "author": row["author_name"],
            "author_id": row["author_id"],
            "orcid": row["orcid"],
            "has_chile_affiliation": row["has_chile_affiliation"],
            "articles": row["works_count"],
        } for row in analytics["top_authors"]]
        sheet_name = "Authors"
    elif table_key == "institutions":
        rows = [{
            "institution": row["institution_name"],
            "ror_id": row["ror_id"],
            "country": row["country_code"],
            "articles": row["works_count"],
            "author_links": row["author_links"],
        } for row in analytics["top_institutions"]]
        sheet_name = "Institutions"
    else:
        flash_err(_('Invalid export table.'))
        return redirect(url_for('works.openalex_analytics', **request.args.to_dict(flat=False)))

    return _send_dataframe_export(pd.DataFrame(rows), f"openalex_analytics_{ror_id}_{table_key}", sheet_name)


@bp_works.route('/openalex/global')
@staff_required
def openalex_global():
    """Render a staff-only OpenAlex comparison across institutions."""
    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "tab": request.args.get("tab"),
        "institution_author_ror": _request_list_arg("institution_author_ror"),
    }

    def query_url(**updates):
        params = request.args.to_dict(flat=False)
        params.update({key: value for key, value in updates.items() if value is not None})
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_global", **params)

    table_prefixes = {
        "universities": "university",
        "authors": "author",
        "institutions": "institution",
        "institution_authors": "institution_author",
        "priority_sources": "priority_source",
        "priority_articles": "priority_article",
        "priority_institutions": "priority_institution",
    }

    def table_url(table_key: str, **updates):
        params = request.args.to_dict(flat=False)
        prefix = table_prefixes.get(table_key, table_key)
        for key, value in updates.items():
            if key in {"q", "sort", "dir", "page", "per_page"}:
                params[f"{prefix}_{key}"] = value
            else:
                params[key] = value
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_global", **params)

    def export_url(table_key: str, export_format: str):
        params = request.args.to_dict(flat=False)
        params["format"] = export_format
        for key, value in list(params.items()):
            if value is None or value == "" or value == []:
                params.pop(key)
        return url_for("works.openalex_global_export", table_key=table_key, **params)

    analytics = _openalex_global_analytics_with_cache(analytics_filters)
    return render_template(
        'works/openalex_global.html',
        analytics=analytics,
        has_active_filters=bool(
            analytics_filters["year_from"] or analytics_filters["year_to"]
            or analytics_filters["type"] or analytics_filters["oa_status"]
        ),
        query_url=query_url,
        table_url=table_url,
        export_url=export_url,
    )


@bp_works.route('/openalex/global/export/<table_key>')
@staff_required
def openalex_global_export(table_key: str):
    """Export global OpenAlex analytics tables with current filters and sort."""
    table_tabs = {
        "universities": "universities",
        "authors": "production",
        "institutions": "production",
        "institution_authors": "institution_authors",
        "priority_sources": "open_access",
        "priority_articles": "open_access",
        "priority_universities": "open_access",
    }
    if table_key not in table_tabs:
        flash_err(_('Invalid export table.'))
        return redirect(url_for('works.openalex_global', **request.args.to_dict(flat=False)))

    analytics_filters = {
        "year_from": request.args.get("year_from"),
        "year_to": request.args.get("year_to"),
        "type": _request_list_arg("type"),
        "oa_status": _request_list_arg("oa_status"),
        "tab": table_tabs[table_key],
        "institution_author_ror": _request_list_arg("institution_author_ror"),
    }
    analytics = _openalex_global_analytics(analytics_filters)

    if table_key == "priority_sources":
        rows = _priority_source_export_rows(analytics["priority_open_access"])
        sheet_name = "Diamond Green sources"
    elif table_key == "priority_articles":
        rows = _priority_article_export_rows(analytics["priority_open_access"])
        sheet_name = "Diamond Green articles"
    elif table_key == "priority_universities":
        rows = _priority_institution_export_rows(analytics["priority_open_access"])
        sheet_name = "Diamond Green universities"
    elif table_key == "universities":
        rows = [{
            "university": row["institution"],
            "ror_id": row["ror_id"],
            "orcid_articles": row["article_works"],
            "openalex_articles": row["enriched_count"],
            "overall_coverage_percent": row["coverage_percent"],
            "own_institution_percent": row["selected_percent"],
            "own_institution_articles": row["selected_count"],
            "chile_percent": row["chile_percent"],
            "chile_articles": row["chile_count"],
            "international_percent": row["international_percent"],
            "international_articles": row["international_count"],
            "open_access_percent": row["open_access_percent"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
        } for row in analytics["rows"]]
        sheet_name = "Universities"
    elif table_key == "authors":
        rows = [{
            "author": row["author"],
            "author_id": row["author_id"],
            "orcid": row["orcid"],
            "associated_institutions": _summary_labels(row["associated_institutions"]),
            "articles": row["works_count"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
            "has_chile_affiliation": row["has_chile_affiliation"],
        } for row in analytics["top_authors"]]
        sheet_name = "Authors"
    elif table_key == "institutions":
        rows = [{
            "institution": row["institution"],
            "institution_id": row["institution_id"],
            "ror_id": row["ror_id"],
            "country": row["country_code"],
            "associated_authors": _summary_labels(row["associated_authors"]),
            "articles": row["works_count"],
            "author_links": row["author_links"],
            "corresponding_articles": row["corresponding_works"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
        } for row in analytics["top_institutions"]]
        sheet_name = "Institutions"
    else:
        rows = [{
            "university": row["university"],
            "ror_id": row["ror_id"],
            "author": row["author"],
            "author_id": row["author_id"],
            "orcid": row["orcid"],
            "associated_institutions": _summary_labels(row["associated_institutions"]),
            "articles": row["works_count"],
            "citations": row["total_citations"],
            "average_citations": row["average_citations"],
            "average_fwci": row["average_fwci"],
            "latest_year": row["latest_year"],
            "has_chile_affiliation": row["has_chile_affiliation"],
        } for row in analytics["institution_author_rows"]]
        sheet_name = "Institution authors"

    return _send_dataframe_export(pd.DataFrame(rows), f"openalex_global_{table_key}", sheet_name)
