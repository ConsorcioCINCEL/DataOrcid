"""Duplicate ORCID profile review and exports."""

from email.utils import parseaddr
import math
import re
from io import BytesIO

import pandas as pd
from flask import Blueprint, redirect, render_template, request, send_file, session, url_for
from flask_babel import _

from .. import db
from ..decorators import institution_required, staff_required
from ..models import ResearcherCache
from ..services.institution_registry_service import get_institution_by_ror
from ..services.duplicate_profile_service import (
    DUPLICATE_REVIEW_STATUSES,
    build_duplicate_report,
    clear_duplicate_report_cache,
    filter_duplicate_report_by_status,
    flatten_duplicate_rows,
    save_duplicate_review,
)
from ..utils.flashes import flash_err, flash_ok
from ..utils.session_helpers import get_active_ror_id

bp_duplicates = Blueprint("duplicates", __name__, url_prefix="/duplicates")

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_ORCID_DUPLICATE_GUIDE_URL = (
    "https://support.orcid.org/hc/en-us/articles/"
    "360006896634-I-have-more-than-one-ORCID-iD"
)
_ORCID_SUPPORT_URL = "https://support.orcid.org/hc/en-us/requests/new"


@bp_duplicates.route("/")
@institution_required
def index():
    """Render duplicate profile candidates for the current user scope."""
    scope = _requested_scope()
    ror_ids = _resolve_ror_ids(scope)
    if ror_ids == []:
        flash_err(_("No active ROR context found. Please log in again or select an institution."))
        return redirect(url_for("main.index"))

    search_query = request.args.get("q", "")
    min_confidence = _safe_int(request.args.get("min_confidence"), default=0)
    case_status = request.args.get("case_status", "open")
    if case_status not in {"open", "all", *DUPLICATE_REVIEW_STATUSES}:
        case_status = "open"
    section = request.args.get("section", "candidates")
    if section not in {"candidates", "researchers", "institutions"}:
        section = "candidates"
    report = build_duplicate_report(
        ror_ids=ror_ids,
        search=search_query,
        min_confidence=min_confidence,
        force_refresh=request.args.get("refresh") == "1",
    )
    overview_summary = report.get("summary", {}).copy()
    high_confidence_count = sum(
        1 for group in report.get("groups", [])
        if group.get("confidence_level") == "high" or group.get("confidence", 0) >= 85
    )
    public_contacts = _load_public_email_contacts(report.get("groups", []))
    for group in report.get("groups", []):
        review = group.get("review") or {}
        preserve_notice = review.get("status") in {"notified", "resolved"}
        stored_notice = review.get("notice_message")
        if stored_notice and preserve_notice:
            group["notice_message"] = stored_notice
        else:
            group["notice_message"] = _researcher_notice(group)
        group["notice_subject"] = (
            review.get("notice_subject")
            if review.get("notice_subject") and preserve_notice
            else _researcher_notice_subject(group)
        )
        group["notice_recipient"] = (
            review.get("recipient_email") if preserve_notice else ""
        )
        group["contact_options"] = _contact_options_for_group(
            group,
            public_contacts,
        )
    report = filter_duplicate_report_by_status(report, case_status)
    requested_page = max(_safe_int(request.args.get("page"), default=1), 1)
    candidate_groups, candidate_pagination = _paginate_rows(
        report.get("groups", []), requested_page
    )
    researcher_rows, researcher_pagination = _paginate_rows(
        report.get("profile_activity", []), requested_page
    )
    return render_template(
        "duplicates/index.html",
        report=report,
        scope=scope,
        query=search_query,
        min_confidence=min_confidence,
        case_status=case_status,
        section=section,
        candidate_groups=candidate_groups,
        candidate_pagination=candidate_pagination,
        researcher_rows=researcher_rows,
        researcher_pagination=researcher_pagination,
        overview_summary=overview_summary,
        high_confidence_count=high_confidence_count,
        has_active_filters=bool(search_query.strip() or min_confidence or case_status != "open"),
        evidence_labels=_evidence_labels(),
        confidence_labels=_confidence_labels(),
        dismissal_reason_labels=_dismissal_reason_labels(),
        show_methodology=request.args.get("methodology") == "1",
        current_institution=_current_institution_context(scope),
    )


@bp_duplicates.route("/download")
@institution_required
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


@bp_duplicates.route("/review/<group_key>", methods=["POST"])
@institution_required
def review(group_key: str):
    """Persist a manual duplicate-candidate decision for the active scope."""
    scope = _requested_scope()
    ror_ids = _resolve_ror_ids(scope)
    if ror_ids == []:
        flash_err(_("No active ROR context found. Please log in again or select an institution."))
        return redirect(url_for("duplicates.index"))

    report = build_duplicate_report(ror_ids=ror_ids)
    group = next(
        (item for item in report.get("groups", []) if item.get("group_key") == group_key),
        None,
    )
    if not group:
        flash_err(_("Duplicate candidate group not found in the active scope."))
        return redirect(url_for("duplicates.index", scope=scope))

    submitted_recipient = request.form.get("recipient_email")
    recipient_email = (
        (submitted_recipient or "").strip()
        if submitted_recipient is not None
        else None
    )
    if recipient_email and not _is_valid_email(recipient_email):
        flash_err(_("Enter a valid recipient email address."))
        return redirect(url_for(
            "duplicates.index",
            scope=scope,
            section="candidates",
            q=request.form.get("q") or "",
            min_confidence=request.form.get("min_confidence") or 0,
            case_status=request.form.get("case_status") or "open",
            _anchor=f"candidate-{group_key}",
        ))

    public_contacts = _load_public_email_contacts([group])
    public_addresses = {
        contact["email"].casefold()
        for contact in _contact_options_for_group(group, public_contacts)
    }
    if recipient_email is None:
        recipient_source = None
    elif recipient_email and recipient_email.casefold() in public_addresses:
        recipient_source = "orcid_public"
    else:
        recipient_source = "manual" if recipient_email else ""

    try:
        save_duplicate_review(
            group,
            status=(request.form.get("status") or "pending").strip(),
            reviewer_user_id=session.get("user_id"),
            notes=request.form.get("notes"),
            dismissal_reason=request.form.get("dismissal_reason"),
            notice_message=request.form.get("notice_message"),
            notice_subject=request.form.get("notice_subject"),
            recipient_email=recipient_email,
            recipient_source=recipient_source,
        )
    except ValueError as exc:
        if "dismissal reason" in str(exc):
            flash_err(_("Select a reason before dismissing this candidate."))
        else:
            flash_err(_("Invalid duplicate case status."))
    else:
        status = (request.form.get("status") or "pending").strip()
        messages = {
            "pending": _("Case reopened."),
            "notified": _("Case marked as informed."),
            "dismissed": _("Candidate dismissed and kept in the review history."),
            "resolved": _("Case marked as resolved."),
        }
        flash_ok(messages.get(status, _("Duplicate case updated.")))
    return redirect(url_for(
        "duplicates.index",
        scope=scope,
        section="candidates",
        q=request.form.get("q") or "",
        min_confidence=request.form.get("min_confidence") or 0,
        case_status=request.form.get("case_status") or "open",
        _anchor=f"candidate-{group_key}",
    ))


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


def _current_institution_context(scope: str) -> dict | None:
    if scope == "all":
        return None

    ror_id = get_active_ror_id()
    if not ror_id:
        return None

    institution = get_institution_by_ror(ror_id)
    return {
        "ror_id": ror_id,
        "name": (institution or {}).get("name") or session.get("institution_name") or ror_id,
    }


def _safe_int(value: str | None, default: int = 0) -> int:
    try:
        return int(value or default)
    except ValueError:
        return default


def _paginate_rows(rows: list, page: int, per_page: int = 25) -> tuple[list, dict]:
    """Return a small server-rendered page for dense review sections."""
    total = len(rows)
    pages = max(math.ceil(total / per_page), 1)
    page = min(max(page, 1), pages)
    start = (page - 1) * per_page
    return rows[start:start + per_page], {
        "page": page,
        "pages": pages,
        "has_prev": page > 1,
        "has_next": page < pages,
        "prev_page": max(page - 1, 1),
        "next_page": min(page + 1, pages),
        "start": start + 1 if total else 0,
        "end": min(start + per_page, total),
        "total": total,
    }


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
    review_status_labels = _review_status_labels()
    dismissal_reason_labels = _dismissal_reason_labels()
    localized = []
    for row in rows:
        item = row.copy()
        evidence_keys = [key.strip() for key in item.get("evidence", "").split(",") if key.strip()]
        item["evidence"] = ", ".join(evidence_labels.get(key, key) for key in evidence_keys)
        item["confidence_level"] = confidence_labels.get(item.get("confidence_level"), item.get("confidence_level"))
        item["review_status"] = review_status_labels.get(item.get("review_status"), item.get("review_status"))
        item["dismissal_reason"] = dismissal_reason_labels.get(
            item.get("dismissal_reason"), item.get("dismissal_reason")
        )
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
        "review_status",
        "dismissal_reason",
        "review_notes",
        "reviewed_at",
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
        "review_status": _("Review Status"),
        "dismissal_reason": _("Dismissal Reason"),
        "review_notes": _("Review Notes"),
        "reviewed_at": _("Last Review"),
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
            _("Step"): _("Purpose"),
            _("Description"): _("The panel gathers algorithmic evidence so institutional staff can review possible duplicate ORCID records and prepare a neutral notice for the researcher."),
        },
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
            _("Step"): _("Institutional review"),
            _("Description"): _("Staff can dismiss a false positive or prepare and record a notice to the researcher. A dismissal remains in the history after the candidate analysis is refreshed."),
        },
        {
            _("Step"): _("Scope and limitation"),
            _("Description"): _("The system does not confirm identity, choose a primary ORCID iD, merge records, or send changes to ORCID. The researcher and ORCID are responsible for any record unification."),
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


def _review_status_labels() -> dict[str, str]:
    return {
        "pending": _("Pending review"),
        "notified": _("Researcher informed"),
        "dismissed": _("Dismissed"),
        "resolved": _("Resolved"),
    }


def _dismissal_reason_labels() -> dict[str, str]:
    return {
        "different_people": _("Different people with similar names"),
        "insufficient_evidence": _("Insufficient evidence"),
        "incorrect_shared_work": _("Shared work appears to be incorrectly attributed"),
        "ambiguous_affiliation": _("Affiliation evidence is ambiguous"),
        "other": _("Other reason"),
    }


def _is_valid_email(value: str | None) -> bool:
    """Return whether a single recipient address is safe to offer to a mail client."""
    address = (value or "").strip()
    return bool(
        address
        and len(address) <= 255
        and parseaddr(address)[1] == address
        and _EMAIL_PATTERN.fullmatch(address)
    )


def _load_public_email_contacts(groups: list[dict]) -> dict[str, dict]:
    """Load cached public ORCID contact details without duplicating them in reports."""
    orcids = sorted({
        profile.get("orcid")
        for group in groups
        for profile in group.get("profiles", [])
        if profile.get("orcid")
    })
    contacts = {}
    for start in range(0, len(orcids), 500):
        rows = (
            db.session.query(
                ResearcherCache.orcid,
                ResearcherCache.email,
                ResearcherCache.updated_at,
            )
            .filter(ResearcherCache.orcid.in_(orcids[start:start + 500]))
            .all()
        )
        for row in rows:
            email = (row.email or "").strip()
            if not _is_valid_email(email):
                continue
            contacts[row.orcid] = {
                "email": email,
                "updated_at": row.updated_at.isoformat() if row.updated_at else "",
            }
    return contacts


def _contact_options_for_group(
    group: dict,
    contacts_by_orcid: dict[str, dict],
) -> list[dict]:
    """Return unique public email choices with their associated ORCID records."""
    options = []
    seen_addresses = set()
    for profile in group.get("profiles", []):
        orcid = profile.get("orcid") or ""
        contact = contacts_by_orcid.get(orcid)
        if not contact:
            continue
        address_key = contact["email"].casefold()
        if address_key in seen_addresses:
            continue
        seen_addresses.add(address_key)
        options.append({
            "email": contact["email"],
            "orcid": orcid,
            "display_name": profile.get("display_name") or orcid,
            "updated_at": contact.get("updated_at") or "",
            "source": "orcid_public",
        })
    return options


def _researcher_notice_subject(_group: dict) -> str:
    return _("Possible duplicate ORCID records — review requested")


def _researcher_notice(group: dict) -> str:
    profile_lines = "\n".join(
        f"- {profile.get('display_name') or profile.get('orcid')}: {profile.get('orcid_url')}"
        for profile in group.get("profiles", [])
    )
    evidence_labels = _evidence_labels()
    evidence_lines = "\n".join(
        f"- {evidence_labels.get(key, key)}"
        for key in group.get("evidence_keys", [])
    )
    if group.get("shared_dois"):
        evidence_lines += "\n- " + _("Shared DOI(s): %(dois)s", dois=", ".join(group["shared_dois"]))

    return _(
        "Hello %(name)s,\n\n"
        "During an institutional metadata review at %(institution)s, we identified more than one ORCID iD that may refer to you:\n"
        "%(profiles)s\n\n"
        "Evidence available for review:\n%(evidence)s\n\n"
        "This is an algorithmic candidate, not a confirmation that the records belong to the same person. Please review both ORCID records.\n\n"
        "If, after reviewing them, you confirm that both records are yours, consult ORCID's official guide to remove the duplicate record you will not keep:\n%(guide_url)s\n\n"
        "Important: this action is irreversible, and the information and permissions in the duplicate record are not transferred to the primary record.\n\n"
        "If you need help or cannot access one of the records, contact ORCID Support:\n%(support_url)s\n\n"
        "If the records belong to different people, please let us know so we can dismiss the match.\n\n"
        "The institution cannot merge ORCID records or choose a primary ORCID iD on your behalf."
    ) % {
        "name": group.get("display_name") or _("researcher"),
        "institution": group.get("institution_name") or _("your institution"),
        "profiles": profile_lines,
        "evidence": evidence_lines,
        "guide_url": _ORCID_DUPLICATE_GUIDE_URL,
        "support_url": _ORCID_SUPPORT_URL,
    }
