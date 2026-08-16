"""Work/funding cache management views and exports."""

import logging
from flask import request
from flask_babel import _
from sqlalchemy import and_, case, func, literal

from .. import db
from .works_openalex_data import (
    _chart_color,
    _empty_priority_open_access,
    _int_filter,
    _is_export_request,
    _list_filter,
    _openalex_oa_status_label,
    _priority_open_access_breakdown,
)
from .works_shared import (
    _association_summary,
    _chunks,
    _institution_lookup,
    _json_list,
    _openalex_cache_key_expr,
    _pagination_dict,
    _table_page_params,
    _table_sort_params,
)
logger = logging.getLogger(__name__)
from .works_state import _OPENALEX_PRIORITY_OA_COLORS
def _openalex_global_analytics(filters: dict | None = None) -> dict:
    """Build a staff-only cross-institution OpenAlex comparison."""
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
    active_tab = (filters.get("tab") or "overview").strip().lower()
    valid_tabs = {
        "overview",
        "open_access",
        "universities",
        "production",
        "institution_authors",
        "articles",
    }
    if active_tab not in valid_tabs:
        active_tab = "overview"

    university_sort, university_dir = _table_sort_params(
        "university",
        {"university", "orcid_articles", "openalex", "coverage", "own", "chile", "international", "open_access", "citations", "fwci"},
        "openalex",
    )
    author_sort, author_dir = _table_sort_params(
        "author",
        {"author", "works", "citations", "average_citations", "fwci", "chile"},
        "works",
    )
    institution_sort, institution_dir = _table_sort_params(
        "institution",
        {"institution", "country", "works", "author_links", "corresponding", "citations", "average_citations", "fwci"},
        "works",
    )
    institution_author_sort, institution_author_dir = _table_sort_params(
        "institution_author",
        {"university", "author", "works", "citations", "average_citations", "fwci", "latest_year", "chile"},
        "works",
    )
    author_page, author_per_page = _table_page_params("author", default_per_page=25)
    institution_page, institution_per_page = _table_page_params("institution", default_per_page=25)
    institution_author_page, institution_author_per_page = _table_page_params("institution_author", default_per_page=50, max_per_page=250)
    selected_institution_author_rors = _list_filter(filters.get("institution_author_ror"))

    fact_enabled = openalex_fact_available()
    if fact_enabled:
        local_pairs = (
            db.session.query(
                OpenAlexInstitutionWorkFact.ror_id.label("ror_id"),
                OpenAlexInstitutionWorkFact.openalex_cache_key.label("doi_normalized"),
                OpenAlexInstitutionWorkFact.has_selected_affiliation.label(
                    "has_selected_affiliation"
                ),
                OpenAlexInstitutionWorkFact.has_chile_affiliation.label(
                    "has_chile_affiliation"
                ),
                OpenAlexInstitutionWorkFact.has_non_chile_affiliation.label(
                    "has_non_chile_affiliation"
                ),
                OpenAlexInstitutionWorkFact.has_international_collaboration.label(
                    "has_international_collaboration"
                ),
            )
            .subquery()
        )
    else:
        cache_key = _openalex_cache_key_expr(WorkCache).label("doi_normalized")
        local_pairs = (
            db.session.query(
                WorkCache.ror_id.label("ror_id"),
                cache_key,
                literal(False).label("has_selected_affiliation"),
                literal(False).label("has_chile_affiliation"),
                literal(False).label("has_non_chile_affiliation"),
                literal(False).label("has_international_collaboration"),
            )
            .filter(
                WorkCache.ror_id.isnot(None),
                WorkCache.ror_id != "",
                WorkCache.type == "journal-article",
            )
            .distinct()
            .subquery()
        )

    option_years = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.publication_year)
            .join(local_pairs, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
            .distinct()
            .order_by(OpenAlexWorkMetadata.publication_year.desc())
            .all()
        )
    ]
    option_types = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.type)
            .join(local_pairs, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.type.isnot(None), OpenAlexWorkMetadata.type != "")
            .distinct()
            .order_by(OpenAlexWorkMetadata.type.asc())
            .all()
        )
    ]
    option_oa_statuses = [
        row[0]
        for row in (
            db.session.query(OpenAlexWorkMetadata.oa_status)
            .join(local_pairs, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .filter(OpenAlexWorkMetadata.oa_status.isnot(None), OpenAlexWorkMetadata.oa_status != "")
            .distinct()
            .order_by(OpenAlexWorkMetadata.oa_status.asc())
            .all()
        )
    ]

    filtered_pair_query = (
        db.session.query(
            local_pairs.c.ror_id,
            OpenAlexWorkMetadata.doi_normalized.label("doi_normalized"),
            local_pairs.c.has_selected_affiliation,
            local_pairs.c.has_chile_affiliation,
            local_pairs.c.has_non_chile_affiliation,
            local_pairs.c.has_international_collaboration,
        )
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
    )
    if year_from is not None:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.publication_year >= year_from)
    if year_to is not None:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.publication_year <= year_to)
    if selected_types:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.type.in_(selected_types))
    if selected_oa_statuses:
        filtered_pair_query = filtered_pair_query.filter(OpenAlexWorkMetadata.oa_status.in_(selected_oa_statuses))

    filtered_pairs = filtered_pair_query.distinct().subquery()
    global_filtered_dois = (
        db.session.query(filtered_pairs.c.doi_normalized.label("doi_normalized"))
        .select_from(filtered_pairs)
        .distinct()
        .subquery()
    )
    priority_open_access = (
        _priority_open_access_breakdown(global_filtered_dois)
        if active_tab == "open_access"
        else _empty_priority_open_access()
    )

    global_year_rows = (
        db.session.query(OpenAlexWorkMetadata.publication_year)
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .distinct()
        .order_by(OpenAlexWorkMetadata.publication_year.asc())
        .all()
    )
    global_years = [str(row[0]) for row in global_year_rows]

    global_type_label_expr = func.coalesce(OpenAlexWorkMetadata.type, _("Unknown type"))
    global_type_labels = [
        row[0]
        for row in (
            db.session.query(global_type_label_expr, func.count())
            .select_from(filtered_pairs)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
            .group_by(global_type_label_expr)
            .order_by(func.count().desc())
            .limit(6)
            .all()
        )
    ]
    global_type_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            global_type_label_expr,
            func.count(),
        )
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year, global_type_label_expr)
        .all()
    )
    global_type_year_counts = {
        (str(year), label): count
        for year, label, count in global_type_year_rows
    }
    global_doc_type_trend_datasets = [
        {
            "label": label,
            "data": [global_type_year_counts.get((year, label), 0) for year in global_years],
            "borderColor": _chart_color(index),
            "backgroundColor": _chart_color(index),
            "tension": 0.25,
            "fill": False,
        }
        for index, label in enumerate(global_type_labels)
    ]

    global_oa_year_rows = (
        db.session.query(
            OpenAlexWorkMetadata.publication_year,
            OpenAlexWorkMetadata.is_oa,
            func.count(),
        )
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .filter(OpenAlexWorkMetadata.publication_year.isnot(None))
        .group_by(OpenAlexWorkMetadata.publication_year, OpenAlexWorkMetadata.is_oa)
        .all()
    )
    global_oa_year_counts = {
        (str(year), bool(is_oa)): count
        for year, is_oa, count in global_oa_year_rows
    }

    institutions = _institution_lookup()
    priority_status_expr = func.lower(OpenAlexWorkMetadata.oa_status)
    priority_institution_metrics = []
    if active_tab == "open_access":
        priority_institution_metrics = (
            db.session.query(
                filtered_pairs.c.ror_id.label("ror_id"),
                func.count(func.distinct(case(
                    (
                        priority_status_expr == "diamond",
                        filtered_pairs.c.doi_normalized,
                    ),
                ))).label("diamond_articles"),
                func.count(func.distinct(case(
                    (
                        priority_status_expr == "green",
                        filtered_pairs.c.doi_normalized,
                    ),
                ))).label("green_articles"),
                func.coalesce(func.sum(case(
                    (
                        priority_status_expr == "diamond",
                        OpenAlexWorkMetadata.cited_by_count,
                    ),
                    else_=0,
                )), 0).label("diamond_citations"),
                func.coalesce(func.sum(case(
                    (
                        priority_status_expr == "green",
                        OpenAlexWorkMetadata.cited_by_count,
                    ),
                    else_=0,
                )), 0).label("green_citations"),
            )
            .select_from(filtered_pairs)
            .join(
                OpenAlexWorkMetadata,
                OpenAlexWorkMetadata.doi_normalized
                == filtered_pairs.c.doi_normalized,
            )
            .filter(priority_status_expr.in_(("diamond", "green")))
            .group_by(filtered_pairs.c.ror_id)
            .all()
        )
    priority_institution_rows = [
        {
            "ror_id": row.ror_id,
            "institution": institutions.get(row.ror_id) or row.ror_id,
            "diamond_articles": int(row.diamond_articles or 0),
            "green_articles": int(row.green_articles or 0),
            "articles": int(row.diamond_articles or 0) + int(row.green_articles or 0),
            "diamond_citations": int(row.diamond_citations or 0),
            "green_citations": int(row.green_citations or 0),
            "citations": int(row.diamond_citations or 0) + int(row.green_citations or 0),
        }
        for row in priority_institution_metrics
    ]
    all_priority_institution_rows = list(priority_institution_rows)
    priority_institution_sort, priority_institution_dir = _table_sort_params(
        "priority_institution",
        {
            "institution",
            "diamond_articles",
            "green_articles",
            "articles",
            "diamond_citations",
            "green_citations",
            "citations",
        },
        "articles",
    )
    priority_institution_search = (
        request.args.get("priority_institution_q") or ""
    ).strip()
    if priority_institution_search:
        search_value = priority_institution_search.casefold()
        priority_institution_rows = [
            row
            for row in priority_institution_rows
            if search_value in " ".join((
                row["institution"],
                row["ror_id"],
            )).casefold()
        ]

    priority_institution_rows.sort(
        key=lambda row: (
            row["institution"].casefold(),
            row["ror_id"],
        )
    )
    if priority_institution_sort == "institution":
        priority_institution_rows.sort(
            key=lambda row: (
                row["institution"].casefold(),
                row["ror_id"],
            ),
            reverse=priority_institution_dir == "desc",
        )
    else:
        priority_institution_rows.sort(
            key=lambda row: row[priority_institution_sort],
            reverse=priority_institution_dir == "desc",
        )
    priority_institution_page, priority_institution_per_page = (
        _table_page_params(
            "priority_institution",
            default_per_page=10,
            max_per_page=50,
        )
    )
    priority_institution_pagination = _pagination_dict(
        priority_institution_page,
        priority_institution_per_page,
        len(priority_institution_rows),
    )
    priority_institution_start = (
        priority_institution_pagination["page"] - 1
    ) * priority_institution_pagination["per_page"]
    paged_priority_institution_rows = priority_institution_rows[
        priority_institution_start:
        priority_institution_start + priority_institution_pagination["per_page"]
    ]

    def priority_institution_chart(metric_suffix: str) -> dict:
        total_key = "articles" if metric_suffix == "articles" else "citations"
        ranked_rows = sorted(
            all_priority_institution_rows,
            key=lambda row: (
                -row[total_key],
                row["institution"].lower(),
            ),
        )
        return {
            "labels": [row["institution"] for row in ranked_rows],
            "datasets": [
                {
                    "label": _openalex_oa_status_label(status),
                    "data": [row[f"{status}_{metric_suffix}"] for row in ranked_rows],
                    "backgroundColor": color,
                }
                for status, color in _OPENALEX_PRIORITY_OA_COLORS.items()
            ],
        }

    priority_open_access["institution_rows"] = paged_priority_institution_rows
    priority_open_access["tables"]["institutions"] = {
        "pagination": priority_institution_pagination,
        "per_page_options": [10, 25, 50],
        "search": priority_institution_search,
        "sort": priority_institution_sort,
        "dir": priority_institution_dir,
    }
    priority_open_access["charts"]["institutions_by_articles"] = (
        priority_institution_chart("articles")
    )
    priority_open_access["charts"]["institutions_by_citations"] = (
        priority_institution_chart("citations")
    )

    def _ordered(expression, direction: str):
        return expression.asc() if direction == "asc" else expression.desc()

    def _author_key_expr():
        return func.coalesce(
            OpenAlexWorkAuthor.author_id,
            OpenAlexWorkAuthor.orcid,
            OpenAlexWorkAuthor.author_name,
            literal("unknown"),
        )

    def _institution_key_expr():
        return func.coalesce(
            OpenAlexWorkInstitution.ror_id,
            OpenAlexWorkInstitution.institution_id,
            OpenAlexWorkInstitution.institution_name,
            literal("unknown"),
        )

    def _author_institution_associations(author_keys: list[str], ror_scope: list[str] | None = None) -> dict:
        if not author_keys:
            return {}

        author_key = _author_key_expr().label("author_key")
        query = (
            db.session.query(
                author_key,
                OpenAlexWorkAuthor.institution_names,
            )
            .select_from(OpenAlexWorkAuthor)
            .join(global_filtered_dois, OpenAlexWorkAuthor.doi_normalized == global_filtered_dois.c.doi_normalized)
        )
        if ror_scope:
            query = query.join(filtered_pairs, filtered_pairs.c.doi_normalized == OpenAlexWorkAuthor.doi_normalized)
            query = query.filter(filtered_pairs.c.ror_id.in_(ror_scope))

        counters: dict[str, dict[str, int]] = {key: {} for key in author_keys}
        for author_key_chunk in _chunks(author_keys, 5000):
            chunk_query = query.filter(author_key.in_(author_key_chunk))
            for row in chunk_query.all():
                counter = counters.setdefault(row.author_key, {})
                for name in _json_list(row.institution_names):
                    counter[name] = counter.get(name, 0) + 1

        return {key: _association_summary(counter) for key, counter in counters.items()}

    def _institution_author_associations(institution_keys: list[str]) -> dict:
        if not institution_keys:
            return {}

        institution_key = _institution_key_expr().label("institution_key")
        query = (
            db.session.query(
                institution_key,
                OpenAlexWorkInstitution.ror_id,
                OpenAlexWorkInstitution.institution_name,
                OpenAlexWorkAuthor.author_name,
                OpenAlexWorkAuthor.author_id,
                OpenAlexWorkAuthor.orcid,
                OpenAlexWorkAuthor.institution_rors,
                OpenAlexWorkAuthor.institution_names,
            )
            .select_from(OpenAlexWorkInstitution)
            .join(global_filtered_dois, OpenAlexWorkInstitution.doi_normalized == global_filtered_dois.c.doi_normalized)
            .join(OpenAlexWorkAuthor, OpenAlexWorkAuthor.doi_normalized == OpenAlexWorkInstitution.doi_normalized)
        )

        counters: dict[str, dict[str, int]] = {key: {} for key in institution_keys}
        for institution_key_chunk in _chunks(institution_keys, 5000):
            chunk_query = query.filter(institution_key.in_(institution_key_chunk))
            for row in chunk_query.all():
                author_rors = set(_json_list(row.institution_rors))
                author_institutions = set(_json_list(row.institution_names))
                if row.ror_id and row.ror_id not in author_rors:
                    continue
                if not row.ror_id and row.institution_name and row.institution_name not in author_institutions:
                    continue
                label = row.author_name or row.orcid or row.author_id or _("Unknown author")
                counter = counters.setdefault(row.institution_key, {})
                counter[label] = counter.get(label, 0) + 1

        return {key: _association_summary(counter) for key, counter in counters.items()}

    def _institution_author_row_associations(pairs: list[tuple[str, str]]) -> dict:
        if not pairs:
            return {}

        ror_scope = sorted({ror_id for ror_id, _ in pairs})
        author_keys = sorted({author_key for _, author_key in pairs})
        pair_set = set(pairs)
        author_key = _author_key_expr().label("author_key")
        query = (
            db.session.query(
                filtered_pairs.c.ror_id,
                author_key,
                OpenAlexWorkAuthor.institution_names,
            )
            .select_from(filtered_pairs)
            .join(OpenAlexWorkAuthor, OpenAlexWorkAuthor.doi_normalized == filtered_pairs.c.doi_normalized)
            .filter(filtered_pairs.c.ror_id.in_(ror_scope))
        )

        counters: dict[tuple[str, str], dict[str, int]] = {pair: {} for pair in pairs}
        for author_key_chunk in _chunks(author_keys, 5000):
            chunk_query = query.filter(author_key.in_(author_key_chunk))
            for row in chunk_query.all():
                pair = (row.ror_id, row.author_key)
                if pair not in pair_set:
                    continue
                counter = counters.setdefault(pair, {})
                for name in _json_list(row.institution_names):
                    counter[name] = counter.get(name, 0) + 1

        return {pair: _association_summary(counter) for pair, counter in counters.items()}

    top_authors = []
    top_institutions = []
    institution_author_rows = []
    author_pagination = _pagination_dict(author_page, author_per_page, 0)
    institution_pagination = _pagination_dict(institution_page, institution_per_page, 0)
    institution_author_pagination = _pagination_dict(institution_author_page, institution_author_per_page, 0)
    include_associations = not _is_export_request()

    if active_tab == "production":
        author_key = _author_key_expr().label("author_key")
        author_work_base = (
            db.session.query(
                author_key,
                OpenAlexWorkAuthor.author_name.label("author_name"),
                OpenAlexWorkAuthor.author_id.label("author_id"),
                OpenAlexWorkAuthor.orcid.label("orcid"),
                OpenAlexWorkAuthor.has_chile_affiliation.label("has_chile_affiliation"),
                OpenAlexWorkAuthor.doi_normalized.label("doi_normalized"),
                OpenAlexWorkMetadata.cited_by_count.label("cited_by_count"),
                OpenAlexWorkMetadata.fwci.label("fwci"),
            )
            .select_from(OpenAlexWorkAuthor)
            .join(global_filtered_dois, OpenAlexWorkAuthor.doi_normalized == global_filtered_dois.c.doi_normalized)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkAuthor.doi_normalized)
            .distinct()
            .subquery()
        )
        author_grouped = (
            db.session.query(
                author_work_base.c.author_key,
                func.max(author_work_base.c.author_name).label("author_name"),
                func.max(author_work_base.c.author_id).label("author_id"),
                func.max(author_work_base.c.orcid).label("orcid"),
                func.max(case((author_work_base.c.has_chile_affiliation.is_(True), 1), else_=0)).label("has_chile_affiliation"),
                func.count(func.distinct(author_work_base.c.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(author_work_base.c.cited_by_count), 0).label("total_citations"),
                func.avg(author_work_base.c.cited_by_count).label("average_citations"),
                func.avg(author_work_base.c.fwci).label("average_fwci"),
            )
            .select_from(author_work_base)
            .group_by(author_work_base.c.author_key)
            .subquery()
        )
        author_sort_columns = {
            "author": author_grouped.c.author_name,
            "works": author_grouped.c.works_count,
            "citations": author_grouped.c.total_citations,
            "average_citations": author_grouped.c.average_citations,
            "fwci": author_grouped.c.average_fwci,
            "chile": author_grouped.c.has_chile_affiliation,
        }
        author_total_rows = db.session.query(func.count()).select_from(author_grouped).scalar() or 0
        author_pagination = _pagination_dict(author_page, author_per_page, author_total_rows)
        author_rows = (
            db.session.query(author_grouped)
            .order_by(
                _ordered(author_sort_columns[author_sort], author_dir),
                author_grouped.c.works_count.desc(),
                author_grouped.c.author_name.asc(),
            )
            .offset((author_pagination["page"] - 1) * author_pagination["per_page"])
            .limit(author_pagination["per_page"])
            .all()
        )
        author_associations = _author_institution_associations([row.author_key for row in author_rows]) if include_associations else {}
        top_authors = [
            {
                "author_key": row.author_key,
                "author": row.author_name or row.orcid or row.author_id or _("Unknown author"),
                "author_id": row.author_id,
                "orcid": row.orcid,
                "has_chile_affiliation": bool(row.has_chile_affiliation),
                "works_count": int(row.works_count or 0),
                "total_citations": int(row.total_citations or 0),
                "average_citations": round(float(row.average_citations or 0), 1),
                "average_fwci": round(float(row.average_fwci), 2) if row.average_fwci is not None else None,
                "associated_institutions": author_associations.get(row.author_key, _association_summary({})),
            }
            for row in author_rows
        ]

        institution_key = _institution_key_expr().label("institution_key")
        institution_work_base = (
            db.session.query(
                institution_key,
                OpenAlexWorkInstitution.institution_name.label("institution_name"),
                OpenAlexWorkInstitution.institution_id.label("institution_id"),
                OpenAlexWorkInstitution.ror_id.label("ror_id"),
                OpenAlexWorkInstitution.country_code.label("country_code"),
                OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"),
                OpenAlexWorkInstitution.author_count.label("author_count"),
                OpenAlexWorkInstitution.has_corresponding_author.label("has_corresponding_author"),
                OpenAlexWorkMetadata.cited_by_count.label("cited_by_count"),
                OpenAlexWorkMetadata.fwci.label("fwci"),
            )
            .select_from(OpenAlexWorkInstitution)
            .join(global_filtered_dois, OpenAlexWorkInstitution.doi_normalized == global_filtered_dois.c.doi_normalized)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkInstitution.doi_normalized)
            .distinct()
            .subquery()
        )
        institution_grouped = (
            db.session.query(
                institution_work_base.c.institution_key,
                func.max(institution_work_base.c.institution_name).label("institution_name"),
                func.max(institution_work_base.c.institution_id).label("institution_id"),
                func.max(institution_work_base.c.ror_id).label("ror_id"),
                func.max(institution_work_base.c.country_code).label("country_code"),
                func.count(func.distinct(institution_work_base.c.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(institution_work_base.c.author_count), 0).label("author_links"),
                func.count(func.distinct(case(
                    (institution_work_base.c.has_corresponding_author.is_(True), institution_work_base.c.doi_normalized),
                ))).label("corresponding_works"),
                func.coalesce(func.sum(institution_work_base.c.cited_by_count), 0).label("total_citations"),
                func.avg(institution_work_base.c.cited_by_count).label("average_citations"),
                func.avg(institution_work_base.c.fwci).label("average_fwci"),
            )
            .select_from(institution_work_base)
            .group_by(institution_work_base.c.institution_key)
            .subquery()
        )
        institution_sort_columns = {
            "institution": institution_grouped.c.institution_name,
            "country": institution_grouped.c.country_code,
            "works": institution_grouped.c.works_count,
            "author_links": institution_grouped.c.author_links,
            "corresponding": institution_grouped.c.corresponding_works,
            "citations": institution_grouped.c.total_citations,
            "average_citations": institution_grouped.c.average_citations,
            "fwci": institution_grouped.c.average_fwci,
        }
        institution_total_rows = db.session.query(func.count()).select_from(institution_grouped).scalar() or 0
        institution_pagination = _pagination_dict(institution_page, institution_per_page, institution_total_rows)
        institution_rows = (
            db.session.query(institution_grouped)
            .order_by(
                _ordered(institution_sort_columns[institution_sort], institution_dir),
                institution_grouped.c.works_count.desc(),
                institution_grouped.c.institution_name.asc(),
            )
            .offset((institution_pagination["page"] - 1) * institution_pagination["per_page"])
            .limit(institution_pagination["per_page"])
            .all()
        )
        institution_associations = _institution_author_associations([row.institution_key for row in institution_rows]) if include_associations else {}
        top_institutions = [
            {
                "institution_key": row.institution_key,
                "institution": row.institution_name or row.ror_id or row.institution_id or _("Unknown institution"),
                "institution_id": row.institution_id,
                "ror_id": row.ror_id,
                "country_code": row.country_code or _("Unknown"),
                "works_count": int(row.works_count or 0),
                "author_links": int(row.author_links or 0),
                "corresponding_works": int(row.corresponding_works or 0),
                "total_citations": int(row.total_citations or 0),
                "average_citations": round(float(row.average_citations or 0), 1),
                "average_fwci": round(float(row.average_fwci), 2) if row.average_fwci is not None else None,
                "associated_authors": institution_associations.get(row.institution_key, _association_summary({})),
            }
            for row in institution_rows
        ]

    if active_tab == "institution_authors":
        author_key = _author_key_expr().label("author_key")
        institution_author_base_query = (
            db.session.query(
                filtered_pairs.c.ror_id.label("ror_id"),
                author_key,
                OpenAlexWorkAuthor.author_name.label("author_name"),
                OpenAlexWorkAuthor.author_id.label("author_id"),
                OpenAlexWorkAuthor.orcid.label("orcid"),
                OpenAlexWorkAuthor.has_chile_affiliation.label("has_chile_affiliation"),
                OpenAlexWorkAuthor.doi_normalized.label("doi_normalized"),
                OpenAlexWorkMetadata.cited_by_count.label("cited_by_count"),
                OpenAlexWorkMetadata.fwci.label("fwci"),
                OpenAlexWorkMetadata.publication_year.label("publication_year"),
            )
            .select_from(filtered_pairs)
            .join(OpenAlexWorkAuthor, OpenAlexWorkAuthor.doi_normalized == filtered_pairs.c.doi_normalized)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        )
        if selected_institution_author_rors:
            institution_author_base_query = institution_author_base_query.filter(filtered_pairs.c.ror_id.in_(selected_institution_author_rors))
        institution_author_base = institution_author_base_query.distinct().subquery()
        institution_author_grouped = (
            db.session.query(
                institution_author_base.c.ror_id,
                institution_author_base.c.author_key,
                func.max(institution_author_base.c.author_name).label("author_name"),
                func.max(institution_author_base.c.author_id).label("author_id"),
                func.max(institution_author_base.c.orcid).label("orcid"),
                func.max(case((institution_author_base.c.has_chile_affiliation.is_(True), 1), else_=0)).label("has_chile_affiliation"),
                func.count(func.distinct(institution_author_base.c.doi_normalized)).label("works_count"),
                func.coalesce(func.sum(institution_author_base.c.cited_by_count), 0).label("total_citations"),
                func.avg(institution_author_base.c.cited_by_count).label("average_citations"),
                func.avg(institution_author_base.c.fwci).label("average_fwci"),
                func.max(institution_author_base.c.publication_year).label("latest_year"),
            )
            .select_from(institution_author_base)
            .group_by(institution_author_base.c.ror_id, institution_author_base.c.author_key)
            .subquery()
        )
        institution_author_sort_columns = {
            "university": institution_author_grouped.c.ror_id,
            "author": institution_author_grouped.c.author_name,
            "works": institution_author_grouped.c.works_count,
            "citations": institution_author_grouped.c.total_citations,
            "average_citations": institution_author_grouped.c.average_citations,
            "fwci": institution_author_grouped.c.average_fwci,
            "latest_year": institution_author_grouped.c.latest_year,
            "chile": institution_author_grouped.c.has_chile_affiliation,
        }
        institution_author_total_rows = db.session.query(func.count()).select_from(institution_author_grouped).scalar() or 0
        institution_author_pagination = _pagination_dict(institution_author_page, institution_author_per_page, institution_author_total_rows)
        paged_institution_author_rows = (
            db.session.query(institution_author_grouped)
            .order_by(
                _ordered(institution_author_sort_columns[institution_author_sort], institution_author_dir),
                institution_author_grouped.c.works_count.desc(),
                institution_author_grouped.c.author_name.asc(),
            )
            .offset((institution_author_pagination["page"] - 1) * institution_author_pagination["per_page"])
            .limit(institution_author_pagination["per_page"])
            .all()
        )
        pair_associations = _institution_author_row_associations([
            (row.ror_id, row.author_key)
            for row in paged_institution_author_rows
        ]) if include_associations else {}
        institution_author_rows = [
            {
                "ror_id": row.ror_id,
                "university": institutions.get(row.ror_id) or row.ror_id,
                "author_key": row.author_key,
                "author": row.author_name or row.orcid or row.author_id or _("Unknown author"),
                "author_id": row.author_id,
                "orcid": row.orcid,
                "has_chile_affiliation": bool(row.has_chile_affiliation),
                "works_count": int(row.works_count or 0),
                "total_citations": int(row.total_citations or 0),
                "average_citations": round(float(row.average_citations or 0), 1),
                "average_fwci": round(float(row.average_fwci), 2) if row.average_fwci is not None else None,
                "latest_year": row.latest_year,
                "associated_institutions": pair_associations.get((row.ror_id, row.author_key), _association_summary({})),
            }
            for row in paged_institution_author_rows
        ]

    if fact_enabled:
        local_article_count_rows = (
            db.session.query(
                OpenAlexInstitutionWorkFact.ror_id,
                func.sum(OpenAlexInstitutionWorkFact.source_record_count),
            )
            .group_by(OpenAlexInstitutionWorkFact.ror_id)
            .all()
        )
    else:
        local_article_count_rows = (
            db.session.query(WorkCache.ror_id, func.count(WorkCache.id))
            .filter(
                WorkCache.ror_id.isnot(None),
                WorkCache.ror_id != "",
                WorkCache.type == "journal-article",
            )
            .group_by(WorkCache.ror_id)
            .all()
        )
    local_article_counts = {
        current_ror: int(count or 0)
        for current_ror, count in local_article_count_rows
    }
    local_key_counts = {
        ror_id: count
        for ror_id, count in (
            db.session.query(local_pairs.c.ror_id, func.count())
            .select_from(local_pairs)
            .group_by(local_pairs.c.ror_id)
            .all()
        )
    }
    matched_key_counts = {
        ror_id: count
        for ror_id, count in (
            db.session.query(
                local_pairs.c.ror_id,
                func.count(func.distinct(local_pairs.c.doi_normalized)),
            )
            .select_from(local_pairs)
            .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == local_pairs.c.doi_normalized)
            .group_by(local_pairs.c.ror_id)
            .all()
        )
    }

    if fact_enabled:
        selected_condition = filtered_pairs.c.has_selected_affiliation.is_(True)
        chile_condition = filtered_pairs.c.has_chile_affiliation.is_(True)
        international_condition = (
            filtered_pairs.c.has_international_collaboration.is_(True)
        )
        metric_query = (
            db.session.query(
                filtered_pairs.c.ror_id,
                func.count(func.distinct(filtered_pairs.c.doi_normalized)).label("enriched_count"),
                func.count(func.distinct(case(
                    (OpenAlexWorkMetadata.is_oa.is_(True), filtered_pairs.c.doi_normalized),
                ))).label("open_access_count"),
                func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0).label("total_citations"),
                func.avg(OpenAlexWorkMetadata.fwci).label("average_fwci"),
                func.count(func.distinct(case((
                    selected_condition,
                    filtered_pairs.c.doi_normalized,
                )))).label("selected_count"),
                func.count(func.distinct(case((
                    chile_condition,
                    filtered_pairs.c.doi_normalized,
                )))).label("chile_count"),
                func.count(func.distinct(case((
                    international_condition,
                    filtered_pairs.c.doi_normalized,
                )))).label("international_count"),
            )
            .select_from(filtered_pairs)
            .join(
                OpenAlexWorkMetadata,
                OpenAlexWorkMetadata.doi_normalized
                == filtered_pairs.c.doi_normalized,
            )
        )
    else:
        selected_pairs = (
            db.session.query(
                filtered_pairs.c.ror_id.label("ror_id"),
                filtered_pairs.c.doi_normalized.label("doi_normalized"),
            )
            .join(
                OpenAlexWorkInstitution,
                and_(
                    OpenAlexWorkInstitution.doi_normalized == filtered_pairs.c.doi_normalized,
                    OpenAlexWorkInstitution.ror_id == filtered_pairs.c.ror_id,
                ),
            )
            .distinct()
            .subquery()
        )
        chile_dois = (
            db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
            .filter(OpenAlexWorkInstitution.country_code == "CL")
            .distinct()
            .subquery()
        )
        non_chile_dois = (
            db.session.query(OpenAlexWorkInstitution.doi_normalized.label("doi_normalized"))
            .filter(OpenAlexWorkInstitution.country_code.isnot(None))
            .filter(OpenAlexWorkInstitution.country_code != "CL")
            .distinct()
            .subquery()
        )
        metric_query = (
        db.session.query(
            filtered_pairs.c.ror_id,
            func.count(func.distinct(filtered_pairs.c.doi_normalized)).label("enriched_count"),
            func.count(func.distinct(case(
                (OpenAlexWorkMetadata.is_oa.is_(True), filtered_pairs.c.doi_normalized),
            ))).label("open_access_count"),
            func.coalesce(func.sum(OpenAlexWorkMetadata.cited_by_count), 0).label("total_citations"),
            func.avg(OpenAlexWorkMetadata.fwci).label("average_fwci"),
            func.count(func.distinct(case(
                (selected_pairs.c.doi_normalized.isnot(None), filtered_pairs.c.doi_normalized),
            ))).label("selected_count"),
            func.count(func.distinct(case(
                (chile_dois.c.doi_normalized.isnot(None), filtered_pairs.c.doi_normalized),
            ))).label("chile_count"),
            func.count(func.distinct(case(
                (
                    and_(
                        chile_dois.c.doi_normalized.isnot(None),
                        non_chile_dois.c.doi_normalized.isnot(None),
                    ),
                    filtered_pairs.c.doi_normalized,
                ),
            ))).label("international_count"),
        )
        .select_from(filtered_pairs)
        .join(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == filtered_pairs.c.doi_normalized)
        .outerjoin(
            selected_pairs,
            and_(
                selected_pairs.c.ror_id == filtered_pairs.c.ror_id,
                selected_pairs.c.doi_normalized == filtered_pairs.c.doi_normalized,
            ),
        )
        .outerjoin(chile_dois, chile_dois.c.doi_normalized == filtered_pairs.c.doi_normalized)
        .outerjoin(non_chile_dois, non_chile_dois.c.doi_normalized == filtered_pairs.c.doi_normalized)
        )
    metric_rows = (
        metric_query
        .group_by(filtered_pairs.c.ror_id)
        .all()
    )

    metric_by_ror = {row.ror_id: row for row in metric_rows}
    all_ror_ids = sorted(set(local_article_counts) | set(local_key_counts) | set(metric_by_ror))
    rows = []
    for ror_id in all_ror_ids:
        metric = metric_by_ror.get(ror_id)
        enriched_count = int(getattr(metric, "enriched_count", 0) or 0)
        local_key_count = int(local_key_counts.get(ror_id, 0) or 0)
        matched_key_count = int(matched_key_counts.get(ror_id, 0) or 0)
        selected_count = int(getattr(metric, "selected_count", 0) or 0)
        chile_count = int(getattr(metric, "chile_count", 0) or 0)
        international_count = int(getattr(metric, "international_count", 0) or 0)
        open_access_count = int(getattr(metric, "open_access_count", 0) or 0)
        total_citations = int(getattr(metric, "total_citations", 0) or 0)
        average_fwci = getattr(metric, "average_fwci", None)
        rows.append({
            "ror_id": ror_id,
            "institution": institutions.get(ror_id) or ror_id,
            "article_works": int(local_article_counts.get(ror_id, 0) or 0),
            "openalex_candidates": local_key_count,
            "matched_keys_total": matched_key_count,
            "enriched_count": enriched_count,
            "coverage_percent": round((matched_key_count / local_key_count * 100), 1) if local_key_count else 0,
            "selected_count": selected_count,
            "selected_percent": round((selected_count / enriched_count * 100), 1) if enriched_count else 0,
            "chile_count": chile_count,
            "chile_percent": round((chile_count / enriched_count * 100), 1) if enriched_count else 0,
            "international_count": international_count,
            "international_percent": round((international_count / chile_count * 100), 1) if chile_count else 0,
            "open_access_count": open_access_count,
            "open_access_percent": round((open_access_count / enriched_count * 100), 1) if enriched_count else 0,
            "total_citations": total_citations,
            "average_citations": round((total_citations / enriched_count), 1) if enriched_count else 0,
            "average_fwci": round(float(average_fwci), 2) if average_fwci is not None else None,
        })

    chart_rows = sorted(rows, key=lambda row: (-row["enriched_count"], row["institution"]))[:12]
    university_sort_map = {
        "university": ("institution", "text"),
        "orcid_articles": ("article_works", "number"),
        "openalex": ("enriched_count", "number"),
        "coverage": ("coverage_percent", "number"),
        "own": ("selected_percent", "number"),
        "chile": ("chile_percent", "number"),
        "international": ("international_percent", "number"),
        "open_access": ("open_access_percent", "number"),
        "citations": ("total_citations", "number"),
        "fwci": ("average_fwci", "number"),
    }
    row_key, row_kind = university_sort_map[university_sort]
    if row_kind == "text":
        rows.sort(key=lambda row: (row.get(row_key) or "").lower(), reverse=university_dir == "desc")
    else:
        rows.sort(key=lambda row: row.get(row_key) if row.get(row_key) is not None else -1, reverse=university_dir == "desc")

    total_enriched = sum(row["enriched_count"] for row in rows)
    total_selected = sum(row["selected_count"] for row in rows)
    total_chile = sum(row["chile_count"] for row in rows)
    total_international = sum(row["international_count"] for row in rows)
    total_open_access = sum(row["open_access_count"] for row in rows)
    total_citations = sum(row["total_citations"] for row in rows)
    total_candidates = sum(row["openalex_candidates"] for row in rows)
    total_matched_keys = sum(row["matched_keys_total"] for row in rows)

    return {
        "active_tab": active_tab,
        "summary": {
            "institutions_count": len([row for row in rows if row["article_works"] or row["enriched_count"]]),
            "article_works": sum(row["article_works"] for row in rows),
            "openalex_candidates": total_candidates,
            "enriched_count": total_enriched,
            "matched_keys_total": total_matched_keys,
            "coverage_percent": round((total_matched_keys / total_candidates * 100), 1) if total_candidates else 0,
            "selected_count": total_selected,
            "selected_percent": round((total_selected / total_enriched * 100), 1) if total_enriched else 0,
            "chile_count": total_chile,
            "chile_percent": round((total_chile / total_enriched * 100), 1) if total_enriched else 0,
            "international_count": total_international,
            "international_percent": round((total_international / total_chile * 100), 1) if total_chile else 0,
            "open_access_count": total_open_access,
            "open_access_percent": round((total_open_access / total_enriched * 100), 1) if total_enriched else 0,
            "total_citations": total_citations,
            "average_citations": round((total_citations / total_enriched), 1) if total_enriched else 0,
        },
        "filters": {
            "year_from": year_from,
            "year_to": year_to,
            "type": selected_types,
            "oa_status": selected_oa_statuses,
            "tab": active_tab,
            "institution_author_ror": selected_institution_author_rors,
        },
        "filter_options": {
            "years": option_years,
            "types": option_types,
            "oa_statuses": [
                {"value": status, "label": _openalex_oa_status_label(status)}
                for status in option_oa_statuses
            ],
            "institutions": [
                {"ror_id": row["ror_id"], "name": row["institution"]}
                for row in sorted(rows, key=lambda row: row["institution"])
                if row["enriched_count"]
            ],
        },
        "charts": {
            "institution_labels": [row["institution"] for row in chart_rows],
            "enriched_values": [row["enriched_count"] for row in chart_rows],
            "coverage_values": [row["coverage_percent"] for row in chart_rows],
            "selected_values": [row["selected_percent"] for row in chart_rows],
            "chile_values": [row["chile_percent"] for row in chart_rows],
            "international_values": [row["international_percent"] for row in chart_rows],
            "citation_values": [row["total_citations"] for row in chart_rows],
            "trend_years": global_years,
            "doc_type_trend_datasets": global_doc_type_trend_datasets,
            "open_access_year_values": [global_oa_year_counts.get((year, True), 0) for year in global_years],
            "closed_access_year_values": [global_oa_year_counts.get((year, False), 0) for year in global_years],
            "oa_labels": [_("Open Access"), _("Closed")],
            "oa_values": [total_open_access, max(total_enriched - total_open_access, 0)],
        },
        "rows": rows,
        "top_authors": top_authors,
        "top_institutions": top_institutions,
        "institution_author_rows": institution_author_rows,
        "priority_open_access": priority_open_access,
        "tables": {
            "priority_sources": priority_open_access["tables"]["sources"],
            "priority_articles": priority_open_access["tables"]["articles"],
            "priority_institutions": priority_open_access["tables"]["institutions"],
            "universities": {
                "sort": university_sort,
                "dir": university_dir,
            },
            "authors": {
                "sort": author_sort,
                "dir": author_dir,
                "pagination": author_pagination,
                "per_page_options": [25, 50, 100],
            },
            "institutions": {
                "sort": institution_sort,
                "dir": institution_dir,
                "pagination": institution_pagination,
                "per_page_options": [25, 50, 100],
            },
            "institution_authors": {
                "sort": institution_author_sort,
                "dir": institution_author_dir,
                "pagination": institution_author_pagination,
                "per_page_options": [25, 50, 100, 250],
            },
        },
    }
