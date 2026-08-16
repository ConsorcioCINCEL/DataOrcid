"""Work/funding cache management views and exports."""

import logging
from flask_babel import _
from sqlalchemy import and_, case, func, literal, or_, select

from .. import db
from .works_openalex_data import (
    _chart_color,
    _empty_priority_open_access,
    _int_filter,
    _list_filter,
    _openalex_cache_summary,
    _openalex_language_label,
    _openalex_oa_status_color,
    _openalex_oa_status_label,
    _priority_open_access_breakdown,
    _selected_values,
)
from .works_shared import _openalex_cache_key_expr
logger = logging.getLogger(__name__)
def _openalex_analytics(ror_id: str, filters: dict | None = None) -> dict:
    """Build chart-ready analytics from OpenAlex-enriched journal articles."""
    from ..models import (
        OpenAlexInstitutionWorkFact,
        OpenAlexWorkAuthor,
        OpenAlexWorkInstitution,
        OpenAlexWorkMetadata,
        WorkCache,
    )
    from ..services.analytics_service import openalex_fact_available

    filters = filters or {}
    year_from = _int_filter(filters.get("year_from"))
    year_to = _int_filter(filters.get("year_to"))
    selected_types = _list_filter(filters.get("type"))
    selected_oa_statuses = _list_filter(filters.get("oa_status"))
    selected_languages = _list_filter(filters.get("language"))
    active_section = (filters.get("section") or "all").strip().lower()
    if active_section not in {
        "all",
        "overview",
        "open_access",
        "collaboration",
        "topics",
        "impact",
    }:
        active_section = "all"
    selected_affiliations = [
        value.lower()
        for value in _list_filter(filters.get("affiliation"))
        if value.lower() in {"all", "selected", "chile", "international", "not_selected"}
    ]
    if not selected_affiliations or "all" in selected_affiliations:
        selected_affiliations = []

    summary = _openalex_cache_summary(ror_id)
    fact_enabled = openalex_fact_available(ror_id)
    if fact_enabled:
        local_key_subquery = (
            db.session.query(
                OpenAlexInstitutionWorkFact.openalex_cache_key.label("doi_normalized"),
                OpenAlexInstitutionWorkFact.has_selected_affiliation,
                OpenAlexInstitutionWorkFact.has_chile_affiliation,
                OpenAlexInstitutionWorkFact.has_non_chile_affiliation,
                OpenAlexInstitutionWorkFact.has_international_collaboration,
            )
            .filter(OpenAlexInstitutionWorkFact.ror_id == ror_id)
            .subquery()
        )
        base_metadata_query = OpenAlexInstitutionWorkFact.query.filter(
            OpenAlexInstitutionWorkFact.ror_id == ror_id,
            OpenAlexInstitutionWorkFact.openalex_id.isnot(None),
        )
        analytics_doi = OpenAlexInstitutionWorkFact.openalex_cache_key
        analytics_id = OpenAlexInstitutionWorkFact.id
        analytics_year = OpenAlexInstitutionWorkFact.publication_year
        analytics_type = OpenAlexInstitutionWorkFact.document_type
        analytics_oa_status = OpenAlexInstitutionWorkFact.oa_status
        analytics_language = OpenAlexInstitutionWorkFact.language
        analytics_citations = OpenAlexInstitutionWorkFact.cited_by_count
        analytics_fwci = OpenAlexInstitutionWorkFact.fwci
        analytics_is_oa = OpenAlexInstitutionWorkFact.is_oa
        analytics_source = OpenAlexInstitutionWorkFact.source_name
        analytics_field = OpenAlexInstitutionWorkFact.primary_topic_field
        analytics_domain = OpenAlexInstitutionWorkFact.primary_topic_domain
        analytics_openalex_id = OpenAlexInstitutionWorkFact.openalex_id
        analytics_title = OpenAlexInstitutionWorkFact.title
    else:
        cache_key = _openalex_cache_key_expr(WorkCache).label("doi_normalized")
        local_key_subquery = (
            db.session.query(cache_key)
            .filter(
                WorkCache.ror_id == ror_id,
                WorkCache.type == "journal-article",
            )
            .distinct()
            .subquery()
        )
        base_metadata_query = (
            OpenAlexWorkMetadata.query
            .join(
                local_key_subquery,
                OpenAlexWorkMetadata.doi_normalized
                == local_key_subquery.c.doi_normalized,
            )
        )
        analytics_doi = OpenAlexWorkMetadata.doi_normalized
        analytics_id = OpenAlexWorkMetadata.id
        analytics_year = OpenAlexWorkMetadata.publication_year
        analytics_type = OpenAlexWorkMetadata.type
        analytics_oa_status = OpenAlexWorkMetadata.oa_status
        analytics_language = OpenAlexWorkMetadata.language
        analytics_citations = OpenAlexWorkMetadata.cited_by_count
        analytics_fwci = OpenAlexWorkMetadata.fwci
        analytics_is_oa = OpenAlexWorkMetadata.is_oa
        analytics_source = OpenAlexWorkMetadata.source_name
        analytics_field = OpenAlexWorkMetadata.primary_topic_field
        analytics_domain = OpenAlexWorkMetadata.primary_topic_domain
        analytics_openalex_id = OpenAlexWorkMetadata.openalex_id
        analytics_title = OpenAlexWorkMetadata.title

    option_years = [
        row[0]
        for row in (
            base_metadata_query
            .with_entities(analytics_year)
            .filter(analytics_year.isnot(None))
            .distinct()
            .order_by(analytics_year.desc())
            .all()
        )
    ]
    option_types = [
        row[0]
        for row in (
            base_metadata_query
            .with_entities(analytics_type)
            .filter(analytics_type.isnot(None), analytics_type != "")
            .distinct()
            .order_by(analytics_type.asc())
            .all()
        )
    ]
    option_oa_statuses = [
        row[0]
        for row in (
            base_metadata_query
            .with_entities(analytics_oa_status)
            .filter(analytics_oa_status.isnot(None), analytics_oa_status != "")
            .distinct()
            .order_by(analytics_oa_status.asc())
            .all()
        )
    ]
    option_languages = [
        row[0]
        for row in (
            base_metadata_query
            .with_entities(analytics_language)
            .filter(analytics_language.isnot(None), analytics_language != "")
            .distinct()
            .order_by(analytics_language.asc())
            .all()
        )
    ]

    selected_inst_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized)
        .filter(OpenAlexWorkInstitution.ror_id == ror_id)
        .distinct()
        .subquery()
    )
    chile_inst_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code == "CL")
        .distinct()
        .subquery()
    )
    non_chile_inst_subquery = (
        db.session.query(OpenAlexWorkInstitution.doi_normalized)
        .filter(OpenAlexWorkInstitution.country_code.isnot(None))
        .filter(OpenAlexWorkInstitution.country_code != "CL")
        .distinct()
        .subquery()
    )
    selected_inst_select = select(selected_inst_subquery.c.doi_normalized)
    chile_inst_select = select(chile_inst_subquery.c.doi_normalized)
    non_chile_inst_select = select(non_chile_inst_subquery.c.doi_normalized)

    filtered_query = base_metadata_query
    if year_from is not None:
        filtered_query = filtered_query.filter(analytics_year >= year_from)
    if year_to is not None:
        filtered_query = filtered_query.filter(analytics_year <= year_to)
    if selected_types:
        filtered_query = filtered_query.filter(analytics_type.in_(selected_types))
    if selected_oa_statuses:
        filtered_query = filtered_query.filter(
            analytics_oa_status.in_(selected_oa_statuses)
        )
    if selected_languages:
        filtered_query = filtered_query.filter(
            analytics_language.in_(selected_languages)
        )
    affiliation_conditions = []
    if fact_enabled:
        if "selected" in selected_affiliations:
            affiliation_conditions.append(
                OpenAlexInstitutionWorkFact.has_selected_affiliation.is_(True)
            )
        if "chile" in selected_affiliations:
            affiliation_conditions.append(
                OpenAlexInstitutionWorkFact.has_chile_affiliation.is_(True)
            )
        if "international" in selected_affiliations:
            affiliation_conditions.append(
                OpenAlexInstitutionWorkFact.has_international_collaboration.is_(True)
            )
        if "not_selected" in selected_affiliations:
            affiliation_conditions.append(
                OpenAlexInstitutionWorkFact.has_selected_affiliation.is_(False)
            )
    else:
        if "selected" in selected_affiliations:
            affiliation_conditions.append(OpenAlexWorkMetadata.doi_normalized.in_(selected_inst_select))
        if "chile" in selected_affiliations:
            affiliation_conditions.append(OpenAlexWorkMetadata.doi_normalized.in_(chile_inst_select))
        if "international" in selected_affiliations:
            affiliation_conditions.append(and_(
                OpenAlexWorkMetadata.doi_normalized.in_(chile_inst_select),
                OpenAlexWorkMetadata.doi_normalized.in_(non_chile_inst_select),
            ))
        if "not_selected" in selected_affiliations:
            affiliation_conditions.append(~OpenAlexWorkMetadata.doi_normalized.in_(selected_inst_select))
    if affiliation_conditions:
        filtered_query = filtered_query.filter(or_(*affiliation_conditions))

    filtered_rows = (
        filtered_query
        .with_entities(
            analytics_doi.label("doi_normalized"),
            analytics_id.label("row_id"),
            analytics_year.label("publication_year"),
            analytics_type.label("type"),
            analytics_oa_status.label("oa_status"),
            analytics_language.label("language"),
            analytics_citations.label("cited_by_count"),
            analytics_fwci.label("fwci"),
            analytics_is_oa.label("is_oa"),
            analytics_source.label("source_name"),
            analytics_field.label("primary_topic_field"),
            analytics_domain.label("primary_topic_domain"),
            analytics_openalex_id.label("openalex_id"),
            analytics_title.label("title"),
            (
                OpenAlexInstitutionWorkFact.has_selected_affiliation
                if fact_enabled
                else literal(False)
            ).label("has_selected_affiliation"),
            (
                OpenAlexInstitutionWorkFact.has_chile_affiliation
                if fact_enabled
                else literal(False)
            ).label("has_chile_affiliation"),
            (
                OpenAlexInstitutionWorkFact.has_non_chile_affiliation
                if fact_enabled
                else literal(False)
            ).label("has_non_chile_affiliation"),
            (
                OpenAlexInstitutionWorkFact.has_international_collaboration
                if fact_enabled
                else literal(False)
            ).label("has_international_collaboration"),
        )
        .subquery()
    )
    doi_subquery = (
        db.session.query(filtered_rows.c.doi_normalized)
        .distinct()
        .subquery()
    )

    def _counter_query(column, fallback: str, limit: int = 10):
        label_expr = func.coalesce(func.nullif(column, ""), fallback)
        count_expr = func.count(filtered_rows.c.row_id)
        rows = (
            db.session.query(label_expr, count_expr)
            .select_from(filtered_rows)
            .group_by(label_expr)
            .order_by(count_expr.desc())
            .limit(limit)
            .all()
        )
        return [row[0] for row in rows], [row[1] for row in rows]

    aggregate_row = (
        db.session.query(
            func.count(filtered_rows.c.row_id),
            func.coalesce(func.sum(filtered_rows.c.cited_by_count), 0),
            func.coalesce(func.sum(case(
                (filtered_rows.c.is_oa.is_(True), 1),
                else_=0,
            )), 0),
            func.avg(filtered_rows.c.fwci),
        )
        .select_from(filtered_rows)
        .one()
    )
    enriched_count = int(aggregate_row[0] or 0)
    total_citations = int(aggregate_row[1] or 0)
    open_access_count = int(aggregate_row[2] or 0)
    average_fwci = aggregate_row[3]

    year_rows = []
    if active_section in {"all", "overview", "open_access"}:
        year_rows = (
            db.session.query(
                filtered_rows.c.publication_year,
                func.count(filtered_rows.c.row_id),
                func.coalesce(func.sum(filtered_rows.c.cited_by_count), 0),
            )
            .select_from(filtered_rows)
            .filter(filtered_rows.c.publication_year.isnot(None))
            .group_by(filtered_rows.c.publication_year)
            .order_by(filtered_rows.c.publication_year.asc())
            .all()
        )
    sorted_years = [str(row.publication_year) for row in year_rows]

    if fact_enabled:
        affiliation_row = (
            db.session.query(
                func.count(case((
                    filtered_rows.c.has_selected_affiliation.is_(True),
                    1,
                ))),
                func.count(case((
                    filtered_rows.c.has_chile_affiliation.is_(True),
                    1,
                ))),
                func.count(case((
                    filtered_rows.c.has_international_collaboration.is_(True),
                    1,
                ))),
                func.count(case((
                    and_(
                        filtered_rows.c.has_selected_affiliation.is_(True),
                        filtered_rows.c.has_non_chile_affiliation.is_(True),
                    ),
                    1,
                ))),
            )
            .select_from(filtered_rows)
            .one()
        )
        selected_institution_count = int(affiliation_row[0] or 0)
        chile_affiliation_count = int(affiliation_row[1] or 0)
        international_collaboration_count = int(affiliation_row[2] or 0)
        selected_international_count = int(affiliation_row[3] or 0)
    else:
        selected_institution_count = (
            db.session.query(func.count())
            .select_from(doi_subquery)
            .join(selected_inst_subquery, selected_inst_subquery.c.doi_normalized == doi_subquery.c.doi_normalized)
            .scalar()
            or 0
        )
        cl_doi_subquery = (
            db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
            .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
            .filter(OpenAlexWorkInstitution.country_code == "CL")
            .distinct()
            .subquery()
        )
        non_cl_doi_subquery = (
            db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
            .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
            .filter(OpenAlexWorkInstitution.country_code.isnot(None))
            .filter(OpenAlexWorkInstitution.country_code != "CL")
            .distinct()
            .subquery()
        )
        chile_affiliation_count = (
            db.session.query(func.count())
            .select_from(cl_doi_subquery)
            .scalar()
            or 0
        )
        international_collaboration_count = (
            db.session.query(func.count())
            .select_from(cl_doi_subquery)
            .join(non_cl_doi_subquery, cl_doi_subquery.c.doi_normalized == non_cl_doi_subquery.c.doi_normalized)
            .scalar()
            or 0
        )
        selected_international_count = (
            db.session.query(func.count())
            .select_from(doi_subquery)
            .join(selected_inst_subquery, selected_inst_subquery.c.doi_normalized == doi_subquery.c.doi_normalized)
            .join(non_chile_inst_subquery, non_chile_inst_subquery.c.doi_normalized == doi_subquery.c.doi_normalized)
            .scalar()
            or 0
        )
    chile_without_selected_count = max(chile_affiliation_count - selected_institution_count, 0)
    no_chile_count = max(enriched_count - chile_affiliation_count, 0)

    default_metrics = [
        "filtered_articles",
        "selected_percent",
        "chile_percent",
        "open_access_percent",
        "total_citations",
        "average_fwci",
    ]
    metric_ids = [
        "filtered_articles",
        "selected_percent",
        "chile_percent",
        "selected_international_percent",
        "open_access_percent",
        "total_citations",
        "selected_articles",
        "chile_articles",
        "international_articles",
        "average_citations",
        "average_fwci",
        "openalex_institutions",
    ]
    selected_metrics = _selected_values(
        _list_filter(filters.get("metrics")),
        metric_ids,
        default_metrics,
    )

    unique_authors = 0
    if active_section in {"all", "collaboration"}:
        unique_authors = (
            db.session.query(func.count(func.distinct(func.coalesce(OpenAlexWorkAuthor.author_id, OpenAlexWorkAuthor.author_name))))
            .join(doi_subquery, OpenAlexWorkAuthor.doi_normalized == doi_subquery.c.doi_normalized)
            .scalar()
            or 0
        )
    unique_institutions = 0
    if active_section in {"all", "collaboration"} or "openalex_institutions" in selected_metrics:
        unique_institutions = (
            db.session.query(func.count(func.distinct(func.coalesce(OpenAlexWorkInstitution.institution_id, OpenAlexWorkInstitution.ror_id, OpenAlexWorkInstitution.institution_name))))
            .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
            .scalar()
            or 0
        )

    type_labels, type_values = ([], [])
    if active_section in {"all", "overview", "topics"}:
        type_labels, type_values = _counter_query(
            filtered_rows.c.type,
            _("Unknown type"),
            limit=8,
        )
    oa_statuses, oa_values = ([], [])
    if active_section in {"all", "open_access", "topics"}:
        oa_statuses, oa_values = _counter_query(
            filtered_rows.c.oa_status,
            _("Unknown OA status"),
            limit=8,
        )
    oa_labels = [
        _("Unknown OA status")
        if status == _("Unknown OA status")
        else _openalex_oa_status_label(status)
        for status in oa_statuses
    ]
    field_labels, field_values = ([], [])
    domain_labels, domain_values = ([], [])
    source_labels, source_values = ([], [])
    language_codes, language_values = ([], [])
    if active_section in {"all", "topics"}:
        field_labels, field_values = _counter_query(
            filtered_rows.c.primary_topic_field,
            _("Unknown field"),
            limit=10,
        )
        domain_labels, domain_values = _counter_query(
            filtered_rows.c.primary_topic_domain,
            _("Unknown domain"),
            limit=10,
        )
        source_labels, source_values = _counter_query(
            filtered_rows.c.source_name,
            _("Unknown source"),
            limit=10,
        )
        language_codes, language_values = _counter_query(
            filtered_rows.c.language,
            _("Unknown language"),
            limit=10,
        )
    language_labels = [
        _("Unknown language")
        if code == _("Unknown language")
        else _openalex_language_label(code)
        for code in language_codes
    ]

    doc_type_trend_datasets = []
    oa_year_counts = {}
    if active_section in {"all", "overview"}:
        type_year_label_expr = func.coalesce(
            filtered_rows.c.type,
            _("Unknown type"),
        )
        type_year_rows = (
            db.session.query(
                filtered_rows.c.publication_year,
                type_year_label_expr,
                func.count(filtered_rows.c.row_id),
            )
            .select_from(filtered_rows)
            .filter(filtered_rows.c.publication_year.isnot(None))
            .group_by(filtered_rows.c.publication_year, type_year_label_expr)
            .all()
        )
        type_year_counts = {
            (str(year), label): count
            for year, label, count in type_year_rows
        }
        doc_type_trend_datasets = [
            {
                "label": label,
                "data": [
                    type_year_counts.get((year, label), 0)
                    for year in sorted_years
                ],
                "borderColor": _chart_color(index),
                "backgroundColor": _chart_color(index),
                "tension": 0.25,
                "fill": False,
            }
            for index, label in enumerate(type_labels[:6])
        ]
        oa_year_rows = (
            db.session.query(
                filtered_rows.c.publication_year,
                filtered_rows.c.is_oa,
                func.count(filtered_rows.c.row_id),
            )
            .select_from(filtered_rows)
            .filter(filtered_rows.c.publication_year.isnot(None))
            .group_by(filtered_rows.c.publication_year, filtered_rows.c.is_oa)
            .all()
        )
        oa_year_counts = {
            (str(year), bool(is_oa)): count
            for year, is_oa, count in oa_year_rows
        }

    priority_oa_status_expr = func.lower(filtered_rows.c.oa_status)
    priority_oa_rows = []
    if active_section in {"all", "open_access"}:
        priority_oa_rows = (
            db.session.query(
                priority_oa_status_expr,
                func.count(filtered_rows.c.row_id),
            )
            .select_from(filtered_rows)
            .filter(priority_oa_status_expr.in_(("diamond", "green")))
            .group_by(priority_oa_status_expr)
            .all()
        )
    priority_oa_counts = {status: count for status, count in priority_oa_rows}
    diamond_open_access_count = int(priority_oa_counts.get("diamond", 0) or 0)
    green_open_access_count = int(priority_oa_counts.get("green", 0) or 0)

    priority_oa_year_rows = []
    if active_section in {"all", "open_access"}:
        priority_oa_year_rows = (
            db.session.query(
                filtered_rows.c.publication_year,
                priority_oa_status_expr,
                func.count(filtered_rows.c.row_id),
            )
            .select_from(filtered_rows)
            .filter(filtered_rows.c.publication_year.isnot(None))
            .filter(priority_oa_status_expr.in_(("diamond", "green")))
            .group_by(filtered_rows.c.publication_year, priority_oa_status_expr)
            .all()
        )
    priority_oa_year_counts = {
        (str(year), status): count
        for year, status, count in priority_oa_year_rows
    }
    priority_open_access = (
        _priority_open_access_breakdown(doi_subquery)
        if active_section in {"all", "open_access"}
        else _empty_priority_open_access()
    )

    country_rows = []
    institution_rows = []
    chile_institution_rows = []
    author_rows = []
    if active_section in {"all", "collaboration"}:
        country_label_expr = func.coalesce(
            OpenAlexWorkInstitution.country_code,
            _("Unknown country"),
        )
        country_count_expr = func.count(
            func.distinct(OpenAlexWorkInstitution.doi_normalized)
        )
        country_rows = (
            db.session.query(
                country_label_expr,
                country_count_expr,
            )
            .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
            .group_by(country_label_expr)
            .order_by(country_count_expr.desc())
            .limit(12)
            .all()
        )
        institution_rows = (
            db.session.query(
                OpenAlexWorkInstitution.institution_name,
                OpenAlexWorkInstitution.ror_id,
                OpenAlexWorkInstitution.country_code,
                func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(OpenAlexWorkInstitution.author_count), 0).label("author_links"),
            )
            .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
            .group_by(
                OpenAlexWorkInstitution.institution_name,
                OpenAlexWorkInstitution.ror_id,
                OpenAlexWorkInstitution.country_code,
            )
            .order_by(func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).desc())
            .limit(12)
            .all()
        )
        chile_institution_rows = (
            db.session.query(
                OpenAlexWorkInstitution.institution_name,
                OpenAlexWorkInstitution.ror_id,
                func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).label("works_count"),
            )
            .join(doi_subquery, OpenAlexWorkInstitution.doi_normalized == doi_subquery.c.doi_normalized)
            .filter(OpenAlexWorkInstitution.country_code == "CL")
            .group_by(OpenAlexWorkInstitution.institution_name, OpenAlexWorkInstitution.ror_id)
            .order_by(func.count(func.distinct(OpenAlexWorkInstitution.doi_normalized)).desc())
            .limit(10)
            .all()
        )
        author_rows = (
            db.session.query(
                OpenAlexWorkAuthor.author_name,
                OpenAlexWorkAuthor.author_id,
                OpenAlexWorkAuthor.orcid,
                OpenAlexWorkAuthor.has_chile_affiliation,
                func.count(func.distinct(OpenAlexWorkAuthor.doi_normalized)).label("works_count"),
            )
            .join(doi_subquery, OpenAlexWorkAuthor.doi_normalized == doi_subquery.c.doi_normalized)
            .group_by(
                OpenAlexWorkAuthor.author_name,
                OpenAlexWorkAuthor.author_id,
                OpenAlexWorkAuthor.orcid,
                OpenAlexWorkAuthor.has_chile_affiliation,
            )
            .order_by(func.count(func.distinct(OpenAlexWorkAuthor.doi_normalized)).desc())
            .limit(12)
            .all()
        )

    def _affiliation_year_counts(fact_column, legacy_condition) -> dict:
        if active_section not in {"all", "overview"}:
            return {}
        if fact_enabled:
            rows = (
                db.session.query(
                    filtered_rows.c.publication_year,
                    func.count(filtered_rows.c.row_id),
                )
                .select_from(filtered_rows)
                .filter(
                    filtered_rows.c.publication_year.isnot(None),
                    fact_column.is_(True),
                )
                .group_by(filtered_rows.c.publication_year)
                .order_by(filtered_rows.c.publication_year.asc())
                .all()
            )
            return {str(year): count for year, count in rows}

        query = (
            db.session.query(
                OpenAlexWorkMetadata.publication_year,
                func.count(func.distinct(OpenAlexWorkMetadata.doi_normalized)),
            )
            .join(doi_subquery, OpenAlexWorkMetadata.doi_normalized == doi_subquery.c.doi_normalized)
        )
        query = query.join(
            OpenAlexWorkInstitution,
            OpenAlexWorkInstitution.doi_normalized
            == OpenAlexWorkMetadata.doi_normalized,
        ).filter(legacy_condition)
        rows = (
            query
            .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
            .group_by(OpenAlexWorkMetadata.publication_year)
            .order_by(OpenAlexWorkMetadata.publication_year.asc())
            .all()
        )
        return {str(year): count for year, count in rows}

    selected_inst_year_counts = _affiliation_year_counts(
        filtered_rows.c.has_selected_affiliation,
        OpenAlexWorkInstitution.ror_id == ror_id,
    )
    chile_year_counts = _affiliation_year_counts(
        filtered_rows.c.has_chile_affiliation,
        OpenAlexWorkInstitution.country_code == "CL",
    )

    top_cited_rows = []
    if active_section in {"all", "impact"}:
        top_cited_rows = (
            db.session.query(filtered_rows)
            .order_by(filtered_rows.c.cited_by_count.desc())
            .limit(10)
            .all()
        )
    top_cited = [
        {
            "title": row.title,
            "openalex_id": row.openalex_id,
            "publication_year": row.publication_year,
            "type": row.type,
            "source_name": row.source_name,
            "cited_by_count": row.cited_by_count,
            "fwci": row.fwci,
            "is_oa": row.is_oa,
            "oa_status": row.oa_status,
            "primary_topic_field": row.primary_topic_field,
            "primary_topic_domain": row.primary_topic_domain,
        }
        for row in top_cited_rows
    ]
    top_institutions = [
        {
            "institution_name": row.institution_name,
            "ror_id": row.ror_id,
            "country_code": row.country_code,
            "works_count": row.works_count,
            "author_links": row.author_links,
        }
        for row in institution_rows
    ]
    top_authors = [
        {
            "author_name": row.author_name,
            "author_id": row.author_id,
            "orcid": row.orcid,
            "has_chile_affiliation": row.has_chile_affiliation,
            "works_count": row.works_count,
        }
        for row in author_rows
    ]

    coverage_counts = {
        _("Matched"): summary["matched_dois"],
        _("Not found"): summary["not_found_dois"],
        _("Errors"): summary["error_dois"],
        _("Pending"): summary["pending_dois"],
    }
    coverage_counts = {label: count for label, count in coverage_counts.items() if count}

    affiliation_counts = {
        _("Selected institution"): selected_institution_count,
        _("Other Chile affiliation"): chile_without_selected_count,
        _("No Chile affiliation"): no_chile_count,
    }
    affiliation_counts = {label: count for label, count in affiliation_counts.items() if count}
    return {
        "summary": {
            "candidate_dois": summary["candidate_dois"],
            "enriched_dois": enriched_count,
            "total_enriched_dois": summary["matched_openalex_keys"],
            "processed_dois": summary["processed_dois"],
            "open_access_count": open_access_count,
            "open_access_percent": round((open_access_count / enriched_count * 100), 1) if enriched_count else 0,
            "diamond_open_access_count": diamond_open_access_count,
            "diamond_open_access_percent": round((diamond_open_access_count / enriched_count * 100), 1) if enriched_count else 0,
            "green_open_access_count": green_open_access_count,
            "green_open_access_percent": round((green_open_access_count / enriched_count * 100), 1) if enriched_count else 0,
            "priority_open_access_count": diamond_open_access_count + green_open_access_count,
            "priority_open_access_percent": round(((diamond_open_access_count + green_open_access_count) / enriched_count * 100), 1) if enriched_count else 0,
            "total_citations": total_citations,
            "average_citations": round((total_citations / enriched_count), 1) if enriched_count else 0,
            "average_fwci": round(float(average_fwci), 2) if average_fwci is not None else None,
            "selected_institution_count": selected_institution_count,
            "selected_institution_percent": round((selected_institution_count / enriched_count * 100), 1) if enriched_count else 0,
            "chile_affiliation_count": chile_affiliation_count,
            "chile_affiliation_percent": round((chile_affiliation_count / enriched_count * 100), 1) if enriched_count else 0,
            "international_collaboration_count": international_collaboration_count,
            "international_collaboration_percent": round((international_collaboration_count / chile_affiliation_count * 100), 1) if chile_affiliation_count else 0,
            "selected_international_count": selected_international_count,
            "selected_international_percent": round((selected_international_count / selected_institution_count * 100), 1) if selected_institution_count else 0,
            "unique_authors": unique_authors,
            "unique_institutions": unique_institutions,
        },
        "filters": {
            "year_from": year_from,
            "year_to": year_to,
            "type": selected_types,
            "oa_status": selected_oa_statuses,
            "language": selected_languages,
            "affiliation": selected_affiliations,
            "metrics": selected_metrics,
        },
        "metric_cards": [
            {"id": "filtered_articles", "label": _("Filtered OpenAlex Articles"), "value": enriched_count, "icon": "fas fa-check-circle", "color": "bg-success"},
            {"id": "selected_percent", "label": _("Associated with Selected Institution"), "value": f"{round((selected_institution_count / enriched_count * 100), 1) if enriched_count else 0}%", "icon": "fas fa-university", "color": "bg-primary"},
            {"id": "chile_percent", "label": _("Associated with Chile"), "value": f"{round((chile_affiliation_count / enriched_count * 100), 1) if enriched_count else 0}%", "icon": "fas fa-flag", "color": "bg-info"},
            {"id": "selected_international_percent", "label": _("Selected Institution + International"), "value": f"{round((selected_international_count / selected_institution_count * 100), 1) if selected_institution_count else 0}%", "icon": "fas fa-globe-americas", "color": "bg-secondary"},
            {"id": "open_access_percent", "label": _("Open Access"), "value": f"{round((open_access_count / enriched_count * 100), 1) if enriched_count else 0}%", "icon": "ai ai-open-access", "color": "bg-warning"},
            {"id": "total_citations", "label": _("Total Citations"), "value": total_citations, "icon": "fas fa-quote-right", "color": "bg-danger"},
            {"id": "selected_articles", "label": _("Selected Institution Articles"), "value": selected_institution_count, "icon": "fas fa-building", "color": "bg-primary"},
            {"id": "chile_articles", "label": _("Chile-Affiliated Articles"), "value": chile_affiliation_count, "icon": "fas fa-map-marker-alt", "color": "bg-info"},
            {"id": "international_articles", "label": _("International Collaboration"), "value": international_collaboration_count, "icon": "fas fa-globe", "color": "bg-dark"},
            {"id": "average_citations", "label": _("Avg. Citations"), "value": round((total_citations / enriched_count), 1) if enriched_count else 0, "icon": "fas fa-chart-line", "color": "bg-success"},
            {"id": "average_fwci", "label": _("Avg. FWCI"), "value": round(float(average_fwci), 2) if average_fwci is not None else "N/A", "icon": "fas fa-balance-scale", "color": "bg-secondary"},
            {"id": "openalex_institutions", "label": _("OpenAlex Institutions"), "value": unique_institutions, "icon": "fas fa-project-diagram", "color": "bg-indigo"},
        ],
        "filter_options": {
            "years": option_years,
            "types": option_types,
            "oa_statuses": [
                {"value": status, "label": _openalex_oa_status_label(status)}
                for status in option_oa_statuses
            ],
            "languages": [
                {"value": code, "label": _openalex_language_label(code)}
                for code in option_languages
            ],
            "affiliations": [
                {"value": "selected", "label": _("Selected institution")},
                {"value": "chile", "label": _("Any Chile affiliation")},
                {"value": "international", "label": _("International collaboration")},
                {"value": "not_selected", "label": _("Not associated with selected institution")},
            ],
        },
        "charts": {
            "years": sorted_years,
            "works_by_year": [row[1] for row in year_rows],
            "citations_by_year": [int(row[2] or 0) for row in year_rows],
            "selected_by_year": [selected_inst_year_counts.get(year, 0) for year in sorted_years],
            "chile_by_year": [chile_year_counts.get(year, 0) for year in sorted_years],
            "coverage_labels": list(coverage_counts.keys()),
            "coverage_values": list(coverage_counts.values()),
            "affiliation_labels": list(affiliation_counts.keys()),
            "affiliation_values": list(affiliation_counts.values()),
            "type_labels": type_labels,
            "type_values": type_values,
            "doc_type_trend_labels": sorted_years,
            "doc_type_trend_datasets": doc_type_trend_datasets,
            "open_access_year_values": [oa_year_counts.get((year, True), 0) for year in sorted_years],
            "closed_access_year_values": [oa_year_counts.get((year, False), 0) for year in sorted_years],
            "priority_oa_labels": [_("Diamond"), _("Green")],
            "priority_oa_values": [diamond_open_access_count, green_open_access_count],
            "priority_oa_colors": [
                _openalex_oa_status_color("diamond"),
                _openalex_oa_status_color("green"),
            ],
            "diamond_oa_year_values": [priority_oa_year_counts.get((year, "diamond"), 0) for year in sorted_years],
            "green_oa_year_values": [priority_oa_year_counts.get((year, "green"), 0) for year in sorted_years],
            "oa_labels": oa_labels,
            "oa_values": oa_values,
            "oa_colors": [
                _openalex_oa_status_color(
                    None
                    if status == _("Unknown OA status")
                    else status
                )
                for status in oa_statuses
            ],
            "field_labels": field_labels,
            "field_values": field_values,
            "domain_labels": domain_labels,
            "domain_values": domain_values,
            "source_labels": source_labels,
            "source_values": source_values,
            "language_labels": language_labels,
            "language_values": language_values,
            "country_labels": [row[0] for row in country_rows],
            "country_values": [row[1] for row in country_rows],
            "institution_labels": [
                f"{row.institution_name or _('Unknown institution')} ({row.ror_id or row.country_code or 'OA'})"
                for row in institution_rows
            ],
            "institution_values": [row.works_count for row in institution_rows],
            "chile_institution_labels": [
                f"{row.institution_name or _('Unknown institution')} ({row.ror_id or 'CL'})"
                for row in chile_institution_rows
            ],
            "chile_institution_values": [row.works_count for row in chile_institution_rows],
            "author_labels": [
                row.author_name or row.orcid or row.author_id or _("Unknown author")
                for row in author_rows
            ],
            "author_values": [row.works_count for row in author_rows],
        },
        "top_cited": top_cited,
        "top_institutions": top_institutions,
        "top_authors": top_authors,
        "priority_open_access": priority_open_access,
    }
