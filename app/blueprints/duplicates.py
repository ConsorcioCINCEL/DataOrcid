"""Duplicate ORCID profile review and exports."""

from io import BytesIO

import pandas as pd
from flask import Blueprint, redirect, render_template, request, send_file, session, url_for
from flask_babel import _

from ..decorators import staff_required
from ..services.duplicate_profile_service import (
    build_duplicate_report,
    clear_duplicate_report_cache,
    flatten_duplicate_rows,
)
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

bp_duplicates = Blueprint("duplicates", __name__, url_prefix="/duplicates")


@bp_duplicates.route("/")
@staff_required
def index():
    """Render duplicate profile candidates for the current staff scope."""
    scope = _requested_scope()
    ror_ids = _resolve_ror_ids(scope)
    if ror_ids == []:
        flash_err(_("No active ROR context found. Please log in again or select an institution."))
        return redirect(url_for("main.index"))

    min_confidence = _safe_int(request.args.get("min_confidence"), default=0)
    report = build_duplicate_report(
        ror_ids=ror_ids,
        search=request.args.get("q"),
        min_confidence=min_confidence,
        force_refresh=request.args.get("refresh") == "1",
    )
    return render_template(
        "duplicates/index.html",
        report=report,
        scope=scope,
        query=request.args.get("q", ""),
        min_confidence=min_confidence,
        evidence_labels=_evidence_labels(),
        confidence_labels=_confidence_labels(),
    )


@bp_duplicates.route("/download")
@staff_required
def download():
    """Download duplicate profile candidates as CSV or Excel."""
    scope = _requested_scope()
    ror_ids = _resolve_ror_ids(scope)
    if ror_ids == []:
        flash_err(_("No active ROR context found. Please log in again or select an institution."))
        return redirect(url_for("duplicates.index"))

    report = build_duplicate_report(ror_ids=ror_ids)
    rows = _localized_export_rows(flatten_duplicate_rows(report["groups"]))
    fmt = request.args.get("format", "csv").lower()
    filename_scope = "all" if scope == "all" else "current"
    filename = f"duplicate-orcid-profiles-{filename_scope}"

    if fmt == "xlsx":
        return _excel_response(report, rows, f"{filename}.xlsx")
    return _csv_response(rows, f"{filename}.csv")


@bp_duplicates.route("/cache/clear", methods=["POST"])
@staff_required
def clear_cache():
    """Clear duplicate profile analysis cache for the selected staff scope."""
    scope = _requested_scope()
    ror_ids = _resolve_ror_ids(scope)
    if ror_ids == []:
        flash_err(_("No active ROR context found. Please log in again or select an institution."))
        return redirect(url_for("duplicates.index"))

    deleted_count = clear_duplicate_report_cache(ror_ids=ror_ids)
    flash_ok(_("Duplicate profile analysis cache cleared: %(count)s entries removed.", count=deleted_count))
    return redirect(url_for("duplicates.index", scope=scope, refresh="1"))


def _requested_scope() -> str:
    if session.get("is_admin"):
        return "current" if request.args.get("scope") == "current" else "all"
    return "current"


def _resolve_ror_ids(scope: str) -> list[str] | None:
    if scope == "all" and session.get("is_admin"):
        return None

    ror_id = get_active_ror_id()
    if not ror_id:
        return []
    return [ror_id]


def _safe_int(value: str | None, default: int = 0) -> int:
    try:
        return int(value or default)
    except ValueError:
        return default


def _csv_response(rows: list[dict], filename: str):
    output = BytesIO()
    dataframe = pd.DataFrame(rows, columns=_export_column_order())
    dataframe = dataframe.rename(columns=_export_column_labels())
    output.write(dataframe.to_csv(index=False).encode("utf-8-sig"))
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=filename, mimetype="text/csv")


def _localized_export_rows(rows: list[dict]) -> list[dict]:
    evidence_labels = _evidence_labels()
    confidence_labels = _confidence_labels()
    localized = []
    for row in rows:
        item = row.copy()
        evidence_keys = [key.strip() for key in item.get("evidence", "").split(",") if key.strip()]
        item["evidence"] = ", ".join(evidence_labels.get(key, key) for key in evidence_keys)
        item["confidence_level"] = confidence_labels.get(item.get("confidence_level"), item.get("confidence_level"))
        item["managed_by_am"] = _("Yes") if item.get("managed_by_am") else _("No")
        localized.append(item)
    return localized


def _excel_response(report: dict, rows: list[dict], filename: str):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        candidates = pd.DataFrame(rows, columns=_export_column_order())
        candidates.rename(columns=_export_column_labels()).to_excel(
            writer,
            sheet_name=_("Candidates"),
            index=False,
        )
        pd.DataFrame(report["institutions"]).rename(columns=_institution_column_labels()).to_excel(
            writer,
            sheet_name=_("Institutions"),
            index=False,
        )
        pd.DataFrame(report["profile_activity"], columns=_activity_column_order()).rename(
            columns=_activity_column_labels()
        ).to_excel(
            writer,
            sheet_name=_("ORCID Activity"),
            index=False,
        )
        pd.DataFrame(_methodology_rows()).to_excel(
            writer,
            sheet_name=_("Methodology"),
            index=False,
        )
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _export_column_order() -> list[str]:
    return [
        "group_id",
        "institution_name",
        "ror_id",
        "candidate_name",
        "normalized_name",
        "confidence",
        "confidence_level",
        "orcid",
        "orcid_url",
        "profile_display_name",
        "given_names",
        "family_name",
        "credit_name",
        "works_count",
        "fundings_count",
        "year_range",
        "managed_by_am",
        "evidence",
        "shared_dois",
    ]


def _export_column_labels() -> dict[str, str]:
    return {
        "group_id": _("Candidate Group"),
        "institution_name": _("Institution"),
        "ror_id": _("ROR ID"),
        "candidate_name": _("Candidate Name"),
        "normalized_name": _("Normalized Name"),
        "confidence": _("Confidence"),
        "confidence_level": _("Confidence Level"),
        "orcid": _("ORCID iD"),
        "orcid_url": _("ORCID URL"),
        "profile_display_name": _("Profile Name"),
        "given_names": _("Given Names"),
        "family_name": _("Family Names"),
        "credit_name": _("Credit Name"),
        "works_count": _("Works"),
        "fundings_count": _("Fundings"),
        "year_range": _("Year Range"),
        "managed_by_am": _("Managed by Affiliation Manager"),
        "evidence": _("Evidence"),
        "shared_dois": _("Shared DOIs"),
    }


def _institution_column_labels() -> dict[str, str]:
    return {
        "ror_id": _("ROR ID"),
        "institution_name": _("Institution"),
        "candidate_groups": _("Candidate Groups"),
        "duplicate_profiles": _("Duplicate Profiles"),
        "extra_profiles": _("Extra Profiles"),
        "highest_confidence": _("Highest Confidence"),
    }


def _activity_column_order() -> list[str]:
    return [
        "institution_name",
        "ror_id",
        "orcid",
        "display_name",
        "works_count",
        "fundings_count",
        "total_activity",
        "candidate_groups",
        "orcid_url",
    ]


def _activity_column_labels() -> dict[str, str]:
    return {
        "institution_name": _("Institution"),
        "ror_id": _("ROR ID"),
        "orcid": _("ORCID iD"),
        "display_name": _("Profile Name"),
        "works_count": _("Works"),
        "fundings_count": _("Fundings"),
        "total_activity": _("Total Activity"),
        "candidate_groups": _("Candidate Groups"),
        "orcid_url": _("ORCID URL"),
    }


def _methodology_rows() -> list[dict]:
    return [
        {
            _("Step"): _("Population"),
            _("Description"): _("The analysis uses ORCID iDs found in the local works and funding caches for the selected institution scope."),
        },
        {
            _("Step"): _("Candidate detection"),
            _("Description"): _("Profiles are grouped when their cached display names match after lowercasing, removing accents, punctuation, and extra spaces."),
        },
        {
            _("Step"): _("Confidence"),
            _("Description"): _("Scores increase when names have more tokens, each profile has cached activity, or profiles share DOI evidence."),
        },
        {
            _("Step"): _("Verification"),
            _("Description"): _("Each row keeps ORCID URLs, local activity counts, Affiliation Manager status, and shared DOI evidence for manual review."),
        },
    ]


def _evidence_labels() -> dict[str, str]:
    return {
        "exact_name_match": _("Exact normalized name match"),
        "shared_doi": _("Shared DOI evidence"),
        "managed_profile": _("Affiliation Manager evidence"),
    }


def _confidence_labels() -> dict[str, str]:
    return {
        "high": _("High"),
        "medium": _("Medium"),
        "low": _("Low"),
    }
