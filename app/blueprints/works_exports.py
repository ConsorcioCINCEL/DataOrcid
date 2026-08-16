"""Work/funding cache management views and exports."""

import logging

import pandas as pd
from flask import redirect, request, url_for
from flask_babel import _

from .. import db
from ..decorators import login_required, normalize_ror_id, staff_required
from ..utils.flashes import flash_err
from ..utils.session_helpers import get_active_ror_id

from .works_blueprint import bp_works
from .works_openalex_data import _send_openalex_export
from .works_shared import (
    _build_fundings_dataframe,
    _build_researchers_dataframe,
    _build_works_dataframe,
    _format_datetime,
    _institution_cache_summaries,
    _openalex_cache_key_expr,
    _openalex_export_load_options,
    _researcher_count,
    _send_dataframe_export,
)
logger = logging.getLogger(__name__)


def _background_export_requested() -> bool:
    return request.args.get("background") == "1"


def _queue_export(kind: str, label: str, *, ror_id: str | None = None):
    from ..services.export_jobs import queue_export_response

    return queue_export_response(
        kind,
        request.args.get("format") or "csv",
        {"ror_id": ror_id} if ror_id else {},
        label,
    )
@bp_works.route('/download/all-works/cache')
@login_required
def download_all_works_cache():
    """
    Exports the complete Works cache for the current institution.
    """
    from ..models import WorkCache
    ror_id = get_active_ror_id()

    query = WorkCache.query.filter_by(ror_id=ror_id)
    if db.session.query(query.exists()).scalar() is not True:
        flash_err(_('The publication cache is currently empty.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("institution_works", _("Scholarly works"), ror_id=ror_id)
    records = query.all()

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

    query = FundingCache.query.filter_by(ror_id=ror_id)
    if db.session.query(query.exists()).scalar() is not True:
        flash_err(_('The funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("institution_fundings", _("Funding and grants"), ror_id=ror_id)
    records = query.all()

    try:
        data_frame = _build_fundings_dataframe(records)
        file_base_name = f"orcid_fundings_cache_{ror_id}"
        return _send_dataframe_export(data_frame, file_base_name, 'Fundings')
    except Exception as exc:
        logger.exception("EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/researchers/cache')
@login_required
def download_researchers_cache():
    """Export all active researcher associations for the current institution."""
    ror_id = get_active_ror_id()
    if _researcher_count(ror_id) == 0:
        flash_err(_('No researcher association data available.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("institution_researchers", _("Associated researchers"), ror_id=ror_id)
    data_frame = _build_researchers_dataframe(ror_id)
    if data_frame.empty:
        flash_err(_('No researcher association data available.'))
        return redirect(url_for('works.cache_works_status'))

    try:
        return _send_dataframe_export(
            data_frame,
            f'orcid_researchers_{ror_id}',
            'Researchers',
        )
    except Exception as exc:
        logger.exception("RESEARCHERS EXPORT ERROR FOR ROR %s: %s", ror_id, exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/staff/institutions/cache-summary')
@staff_required
def download_institution_cache_summary():
    """Export the staff-facing cache summary for every known institution."""
    summaries = _institution_cache_summaries()
    if not summaries:
        flash_err(_('No institutional cache summary is available.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("institution_summary", _("Institution summary"))

    rows = [{
        "institution": item["name"],
        "ror_id": item["ror_id"],
        "researchers": item["researchers"],
        "works": item["works"],
        "fundings": item["fundings"],
        "openalex_matched": item["openalex_matched"],
        "openalex_candidates": item["openalex_candidates"],
        "openalex_coverage_percent": item["openalex_percent"],
        "last_update": _format_datetime(item["last_update"]),
        "cache_status": item["health"],
    } for item in summaries]
    return _send_dataframe_export(
        pd.DataFrame(rows),
        'institution_cache_summary',
        'Institution summary',
    )


@bp_works.route('/download/staff/institution/<ror_id>/<dataset_key>')
@staff_required
def download_staff_institution_cache(ror_id: str, dataset_key: str):
    """Export one institutional dataset without changing the active context."""
    from ..models import (
        FundingCache,
        OpenAlexWorkMetadata,
        OpenAlexWorkRawCache,
        WorkCache,
    )
    from ..services.institution_registry_service import get_institution_by_ror

    ror_id = normalize_ror_id(ror_id)
    if not ror_id or not get_institution_by_ror(ror_id):
        flash_err(_('Institution not found.'))
        return redirect(url_for('works.cache_works_status'))

    if dataset_key == 'works':
        query = WorkCache.query.filter_by(ror_id=ror_id)
        if db.session.query(query.exists()).scalar() is not True:
            flash_err(_('The publication cache is currently empty.'))
            return redirect(url_for('works.cache_works_status'))
        if _background_export_requested():
            return _queue_export("institution_works", _("Scholarly works"), ror_id=ror_id)
        records = query.all()
        return _send_dataframe_export(
            _build_works_dataframe(records),
            f'orcid_works_cache_{ror_id}',
            'Works',
        )

    if dataset_key == 'fundings':
        query = FundingCache.query.filter_by(ror_id=ror_id)
        if db.session.query(query.exists()).scalar() is not True:
            flash_err(_('The funding cache is empty.'))
            return redirect(url_for('works.cache_works_status'))
        if _background_export_requested():
            return _queue_export("institution_fundings", _("Funding and grants"), ror_id=ror_id)
        records = query.all()
        return _send_dataframe_export(
            _build_fundings_dataframe(records),
            f'orcid_fundings_cache_{ror_id}',
            'Fundings',
        )

    if dataset_key == 'researchers':
        if _researcher_count(ror_id) == 0:
            flash_err(_('No researcher association data available.'))
            return redirect(url_for('works.cache_works_status'))
        if _background_export_requested():
            return _queue_export("institution_researchers", _("Researchers"), ror_id=ror_id)
        data_frame = _build_researchers_dataframe(ror_id)
        if data_frame.empty:
            flash_err(_('No researcher association data available.'))
            return redirect(url_for('works.cache_works_status'))
        return _send_dataframe_export(
            data_frame,
            f'orcid_researchers_{ror_id}',
            'Researchers',
        )

    if dataset_key == 'openalex':
        cache_key = _openalex_cache_key_expr(WorkCache).label('openalex_cache_key')
        cache_keys = (
            db.session.query(cache_key)
            .filter(
                WorkCache.ror_id == ror_id,
                WorkCache.type == 'journal-article',
            )
            .distinct()
            .subquery()
        )
        records_query = (
            db.session.query(OpenAlexWorkRawCache, OpenAlexWorkMetadata)
            .join(
                cache_keys,
                OpenAlexWorkRawCache.doi_normalized == cache_keys.c.openalex_cache_key,
            )
            .outerjoin(
                OpenAlexWorkMetadata,
                OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkRawCache.doi_normalized,
            )
            .order_by(OpenAlexWorkRawCache.doi_normalized.asc())
        )
        if (request.args.get('format') or '').lower() == 'excel':
            records_query = records_query.options(
                *_openalex_export_load_options(
                    OpenAlexWorkRawCache,
                    OpenAlexWorkMetadata,
                )
            )
        if records_query.first() is None:
            flash_err(_('No OpenAlex cache data available.'))
            return redirect(url_for('works.cache_works_status'))
        if _background_export_requested():
            return _queue_export("raw_openalex", _("OpenAlex records"), ror_id=ror_id)
        return _send_openalex_export(
            records_query,
            f'openalex_articles_{ror_id}',
        )

    flash_err(_('Invalid dataset type.'))
    return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/researchers/cache')
@staff_required
def download_all_researchers_admin():
    """Export every institution-researcher pair for staff users."""
    if _researcher_count() == 0:
        flash_err(_('No researcher cache data available.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("staff_researchers", _("Researchers from all institutions"))
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
        logger.exception("STAFF RESEARCHERS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-works/cache')
@staff_required
def download_all_works_admin():
    """Export cached works for all institutions to staff users."""
    from ..models import WorkCache

    query = WorkCache.query.order_by(WorkCache.ror_id, WorkCache.orcid, WorkCache.id)
    if db.session.query(WorkCache.id).first() is None:
        flash_err(_('The publication cache is currently empty.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("staff_works", _("Scholarly works from all institutions"))
    records = query.all()

    try:
        data_frame = _build_works_dataframe(records)
        return _send_dataframe_export(
            data_frame,
            'orcid_works_all_institutions',
            'Works',
        )
    except Exception as exc:
        logger.exception("STAFF WORKS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/all-fundings/cache')
@staff_required
def download_all_fundings_admin():
    """Export cached fundings for all institutions to staff users."""
    from ..models import FundingCache

    query = FundingCache.query.order_by(FundingCache.ror_id, FundingCache.orcid, FundingCache.id)
    if db.session.query(FundingCache.id).first() is None:
        flash_err(_('The funding cache is empty.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("staff_fundings", _("Funding records from all institutions"))
    records = query.all()

    try:
        data_frame = _build_fundings_dataframe(records)
        return _send_dataframe_export(
            data_frame,
            'orcid_fundings_all_institutions',
            'Fundings',
        )
    except Exception as exc:
        logger.exception("STAFF FUNDINGS EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))


@bp_works.route('/download/admin/openalex/cache')
@staff_required
def download_openalex_admin():
    """Export cached OpenAlex work metadata for all institutions to staff users."""
    from ..models import OpenAlexWorkMetadata, OpenAlexWorkRawCache

    records_query = (
        db.session.query(OpenAlexWorkRawCache, OpenAlexWorkMetadata)
        .outerjoin(OpenAlexWorkMetadata, OpenAlexWorkMetadata.doi_normalized == OpenAlexWorkRawCache.doi_normalized)
        .order_by(OpenAlexWorkRawCache.doi_normalized.asc())
    )
    if (request.args.get('format') or '').lower() == 'excel':
        records_query = records_query.options(
            *_openalex_export_load_options(
                OpenAlexWorkRawCache,
                OpenAlexWorkMetadata,
            )
        )
    if not db.session.query(OpenAlexWorkRawCache.id).first():
        flash_err(_('The OpenAlex cache is empty.'))
        return redirect(url_for('works.cache_works_status'))
    if _background_export_requested():
        return _queue_export("raw_openalex", _("OpenAlex records from all institutions"))

    try:
        return _send_openalex_export(records_query)
    except Exception as exc:
        logger.exception("STAFF OPENALEX EXPORT ERROR: %s", exc)
        return redirect(url_for('works.cache_works_status'))
