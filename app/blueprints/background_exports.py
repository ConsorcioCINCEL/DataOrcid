"""Authenticated status and download routes for generated export files."""

from flask import Blueprint

from ..decorators import login_required
from ..services.export_jobs import (
    clear_current_user_exports,
    download_export_response,
    get_export_job_response,
    list_current_user_exports,
)


bp_background_exports = Blueprint(
    "background_exports",
    __name__,
    url_prefix="/exports/jobs",
)


@bp_background_exports.get("")
@login_required
def list_exports():
    return list_current_user_exports()


@bp_background_exports.post("/clear")
@login_required
def clear_exports():
    return clear_current_user_exports()


@bp_background_exports.get("/<job_id>")
@login_required
def export_status(job_id: str):
    return get_export_job_response(job_id)


@bp_background_exports.get("/<job_id>/download")
@login_required
def download_export(job_id: str):
    return download_export_response(job_id)
