"""Admin routes for users, institution context, and usage statistics."""

import logging
import secrets
import string
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, session, current_app, g, jsonify
)
from flask_babel import _
from sqlalchemy import and_, case, func, not_, or_

from .. import db
from ..models import SyncJob, SyncJobStep, TrackingLog, User, utc_now
from ..decorators import (
    login_required, admin_required,
    normalize_ror_id
)
from ..utils.flashes import flash_err, flash_ok, flash_info
from ..utils.emailer import send_email
from ..services.ror_service import fetch_grid_from_ror
from ..services.institution_registry_service import get_institution_by_ror, get_institution_options

bp_admin = Blueprint("admin", __name__, url_prefix="/admin")
logger = logging.getLogger(__name__)

_STATISTICS_PERIODS = {"24h", "7d", "30d", "all", "custom"}
_STATISTICS_KINDS = {"interactive", "background", "all"}
_STATISTICS_PER_PAGE = {25, 50, 100}
_USER_ROLES = {"all", "admin", "manager", "user"}
_USER_ACTIVITY_STATES = {"all", "active", "inactive", "never"}
_USER_SORT_OPTIONS = {"created", "name", "institution", "activity"}
_USER_PER_PAGE = {25, 50, 100}
_JOB_STATUSES = {"all", "active", "queued", "running", "success", "partial", "failed", "interrupted"}
_JOB_PER_PAGE = {10, 25, 50}
_JOB_SORT_OPTIONS = {"created", "started", "status", "type", "institution"}


def _background_request_condition():
    """Identify completed synchronization, build, and export requests."""
    return or_(
        TrackingLog.path.like("/openalex/sync%"),
        TrackingLog.path.like("/cache/%build%"),
        TrackingLog.path.like("/download/%"),
        TrackingLog.path.like("%/export/%"),
    )


def _statistics_period_bounds(
    period: str,
    date_from: str = "",
    date_to: str = "",
    now_utc: datetime | None = None,
) -> tuple[str, datetime | None, datetime | None, str, str]:
    """Return normalized UTC-naive bounds for the activity time filter."""
    period = period if period in _STATISTICS_PERIODS else "7d"
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    if period == "24h":
        return period, (now_utc - timedelta(hours=24)).replace(tzinfo=None), None, "", ""
    if period == "7d":
        return period, (now_utc - timedelta(days=7)).replace(tzinfo=None), None, "", ""
    if period == "30d":
        return period, (now_utc - timedelta(days=30)).replace(tzinfo=None), None, "", ""
    if period == "all":
        return period, None, None, "", ""

    try:
        start_date = datetime.strptime(date_from, "%Y-%m-%d").date()
        end_date = datetime.strptime(date_to, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return "7d", (now_utc - timedelta(days=7)).replace(tzinfo=None), None, "", ""

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    local_tz = ZoneInfo(current_app.config.get("BABEL_DEFAULT_TIMEZONE", "America/Santiago"))
    start_local = datetime(start_date.year, start_date.month, start_date.day, tzinfo=local_tz)
    end_local = datetime(end_date.year, end_date.month, end_date.day, tzinfo=local_tz) + timedelta(days=1)
    return (
        period,
        start_local.astimezone(timezone.utc).replace(tzinfo=None),
        end_local.astimezone(timezone.utc).replace(tzinfo=None),
        start_date.isoformat(),
        end_date.isoformat(),
    )


def _percentile(values: list[float], quantile: float) -> float:
    """Return a linearly interpolated percentile for non-PostgreSQL tests and deployments."""
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _latency_distribution(query) -> dict:
    """Return median, p95, and maximum duration for a filtered request query."""
    duration_query = query.with_entities(TrackingLog.duration_ms).filter(
        TrackingLog.duration_ms.isnot(None)
    )
    if db.engine.dialect.name == "postgresql":
        durations = duration_query.subquery()
        median, p95, maximum = (
            db.session.query(
                func.percentile_cont(0.5).within_group(durations.c.duration_ms),
                func.percentile_cont(0.95).within_group(durations.c.duration_ms),
                func.max(durations.c.duration_ms),
            )
            .select_from(durations)
            .one()
        )
        return {
            "median": float(median or 0),
            "p95": float(p95 or 0),
            "maximum": float(maximum or 0),
        }

    values = [float(row[0]) for row in duration_query.all() if row[0] is not None]
    return {
        "median": _percentile(values, 0.5),
        "p95": _percentile(values, 0.95),
        "maximum": max(values, default=0.0),
    }


def _request_summary(query) -> dict:
    """Build count, user, error, and latency metrics for a filtered request query."""
    summary_rows = query.with_entities(
        TrackingLog.username.label("username"),
        TrackingLog.status_code.label("status_code"),
    ).subquery()
    total, unique_users, errors = (
        db.session.query(
            func.count(),
            func.count(func.distinct(summary_rows.c.username)),
            func.coalesce(func.sum(case((summary_rows.c.status_code >= 400, 1), else_=0)), 0),
        )
        .select_from(summary_rows)
        .one()
    )
    total = int(total or 0)
    errors = int(errors or 0)
    return {
        "total_requests": total,
        "unique_users": int(unique_users or 0),
        "error_requests": errors,
        "error_rate": round((errors / total * 100), 1) if total else 0.0,
        **_latency_distribution(query),
    }


def _format_latency(value: float | int | None) -> str:
    """Format milliseconds with a readable, adaptive unit."""
    milliseconds = float(value or 0)
    if milliseconds < 1000:
        return f"{milliseconds:.0f} ms"
    if milliseconds < 60000:
        return f"{milliseconds / 1000:.1f} s"
    if milliseconds < 3600000:
        return f"{milliseconds / 60000:.1f} min"
    return f"{milliseconds / 3600000:.1f} h"


def _user_agent_summary(value: str | None) -> str:
    """Return a compact browser, platform, and device label."""
    agent = value or ""
    if "Werkzeug/" in agent:
        return "Werkzeug · API client"
    if "Edg/" in agent:
        browser = "Edge"
    elif "Chrome/" in agent:
        browser = "Chrome"
    elif "Firefox/" in agent:
        browser = "Firefox"
    elif "Safari/" in agent:
        browser = "Safari"
    elif agent.lower().startswith("curl/"):
        browser = "curl"
    else:
        browser = (agent.split("/", 1)[0] or _("Unknown client")).strip()

    if "Android" in agent:
        platform = "Android"
    elif any(token in agent for token in ("iPhone", "iPad", "iOS")):
        platform = "iOS"
    elif "Windows" in agent:
        platform = "Windows"
    elif "Mac OS" in agent or "Macintosh" in agent:
        platform = "macOS"
    elif "Linux" in agent:
        platform = "Linux"
    else:
        platform = "API" if browser in {"Werkzeug", "curl"} else _("Unknown platform")

    device = _("Mobile") if "Mobile" in agent else _("Desktop")
    if browser in {"Werkzeug", "curl"}:
        device = _("API client")
    return f"{browser} · {platform} · {device}"


def _format_job_duration(seconds: int | float | None) -> str:
    """Return a compact duration for background-job monitoring."""
    seconds = max(int(seconds or 0), 0)
    if seconds < 60:
        return _("%(count)s sec", count=seconds)
    if seconds < 3600:
        return _("%(count)s min", count=seconds // 60)
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    return _("%(hours)sh %(minutes)smin", hours=hours, minutes=minutes)


def _job_type_label(job_type: str | None) -> str:
    labels = {
        "full_institution_sync": _("Full institutional synchronization"),
        "full_system_sync": _("All-institution synchronization"),
        "openalex_institution_sync": _("Institutional OpenAlex synchronization"),
        "openalex_system_sync": _("System-wide OpenAlex synchronization"),
        "member_works_sync": _("Member API works synchronization"),
        "member_fundings_sync": _("Member API funding synchronization"),
        "association_backfill": _("Researcher relationship backfill"),
        "canonical_work_rebuild": _("Canonical work reconstruction"),
        "generic": _("Background operation"),
    }
    return labels.get(job_type or "", (job_type or _("Unknown job")).replace("_", " ").title())


def _job_step_label(
    step_name: str | None,
    institution_lookup: dict[str, str] | None = None,
) -> str:
    labels = {
        "researchers": _("Researchers"),
        "profiles": _("Profiles"),
        "works": _("Scholarly works"),
        "fundings": _("Funding and grants"),
        "canonical_works": _("Canonical works"),
        "openalex": _("OpenAlex enrichment"),
        "association_backfill": _("Researcher relationships"),
    }
    name = step_name or ""
    if name.startswith("institution:"):
        raw_ror = name.split(":", 1)[1]
        ror_id = normalize_ror_id(raw_ror) or raw_ror
        institution_name = (institution_lookup or {}).get(ror_id)
        if institution_name:
            return f"{institution_name} · {ror_id}"
        return _("Institution %(ror)s", ror=ror_id)
    return labels.get(name, name.replace("_", " ").title() or _("Unspecified step"))


def _job_progress_unit_label(unit: str | None) -> str:
    labels = {
        "candidates": _("candidates"),
        "institutions": _("institutions"),
        "records": _("records"),
        "items": _("items"),
    }
    return labels.get(unit or "", (unit or _("items")).replace("_", " "))


def _job_message_label(message: str | None) -> str:
    """Translate known internal messages while preserving useful unknown errors."""
    if not message:
        return ""
    labels = {
        "Background job started.": _("Background job started."),
        "Background job completed.": _("Background job completed."),
        "Background job completed with errors.": _("Background job completed with errors."),
        "Background job failed.": _("Background job failed."),
    }
    if message.startswith("OpenAlex:"):
        return _("OpenAlex candidate processing is in progress.")
    if message.startswith("Processed institution"):
        return _("Institution processing is in progress.")
    return labels.get(message, message)


def _require_admin_or_manager() -> bool:
    """
    Verifies if the current session belongs to an Admin or Manager.
    Used as an internal check for routes that don't use the @admin_required decorator.
    
    Returns:
        bool: True if authorized, False otherwise.
    """
    if not (session.get('is_admin') or session.get('is_manager')):
        flash_err(_('You need admin or manager role to access this section.'))
        return False
    return True


def generate_temp_password(length: int = 12) -> str:
    """
    Generates a secure random alphanumeric password using the secrets module.
    
    Args:
        length (int): The length of the password. Defaults to 12.
        
    Returns:
        str: A randomly generated password string.
    """
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def _users_return_url() -> str:
    """Return a safe user-list URL while preserving the current filters and page."""
    candidate = (request.form.get('return_to') or '').strip()
    users_path = url_for('admin.users_list')
    if candidate and candidate.split('?', 1)[0] == users_path:
        return candidate
    return users_path


@bp_admin.route('/users')
@login_required
def users_list():
    """
    Renders the user management dashboard.
    Supports search filtering by username, email, institution, or ROR ID.
    
    Returns:
        Template: 'admin/users.html' with the list of filtered users.
    """
    # Security Check
    if not _require_admin_or_manager():
        return redirect(url_for('main.index'))

    is_admin = bool(session.get('is_admin'))
    query_param = (request.args.get('q') or '').strip()
    selected_role = (request.args.get('role') or 'all').strip().lower()
    selected_role = selected_role if selected_role in _USER_ROLES else 'all'
    selected_activity = (request.args.get('activity') or 'all').strip().lower()
    selected_activity = selected_activity if selected_activity in _USER_ACTIVITY_STATES else 'all'
    selected_sort = (request.args.get('sort') or 'created').strip().lower()
    selected_sort = selected_sort if selected_sort in _USER_SORT_OPTIONS else 'created'
    selected_institution = normalize_ror_id(request.args.get('institution')) or ''
    page = max(request.args.get('page', 1, type=int), 1)
    requested_per_page = request.args.get('per_page', 25, type=int)
    per_page = requested_per_page if requested_per_page in _USER_PER_PAGE else 25

    scope_filters = []
    if not is_admin:
        scope_filters.append(User.ror_id == session.get('ror_id'))

    users_query = User.query
    if scope_filters:
        users_query = users_query.filter(*scope_filters)

    if query_param:
        search_filter = f"%{query_param}%"
        users_query = users_query.filter(
            or_(
                User.username.ilike(search_filter),
                User.email.ilike(search_filter),
                User.institution_name.ilike(search_filter),
                User.ror_id.ilike(search_filter)
            )
        )

    if selected_role == 'admin':
        users_query = users_query.filter(User.is_admin.is_(True))
    elif selected_role == 'manager':
        users_query = users_query.filter(User.is_manager.is_(True))
    elif selected_role == 'user':
        users_query = users_query.filter(
            User.is_admin.is_(False),
            User.is_manager.is_(False),
        )

    if selected_institution:
        users_query = users_query.filter(User.ror_id == selected_institution)

    active_cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    seen_usernames = (
        db.session.query(TrackingLog.username)
        .filter(TrackingLog.username.isnot(None), TrackingLog.username != '')
        .distinct()
    )
    active_usernames = (
        db.session.query(TrackingLog.username)
        .filter(
            TrackingLog.username.isnot(None),
            TrackingLog.username != '',
            TrackingLog.timestamp >= active_cutoff,
        )
        .distinct()
    )

    if selected_activity == 'active':
        users_query = users_query.filter(User.username.in_(active_usernames))
    elif selected_activity == 'inactive':
        users_query = users_query.filter(
            User.username.in_(seen_usernames),
            not_(User.username.in_(active_usernames)),
        )
    elif selected_activity == 'never':
        users_query = users_query.filter(not_(User.username.in_(seen_usernames)))

    if selected_sort == 'name':
        users_query = users_query.order_by(
            func.lower(User.last_name).asc(),
            func.lower(User.first_name).asc(),
            func.lower(User.username).asc(),
        )
    elif selected_sort == 'institution':
        users_query = users_query.order_by(
            func.lower(User.institution_name).asc(),
            func.lower(User.username).asc(),
        )
    elif selected_sort == 'activity':
        last_activity = (
            db.session.query(
                TrackingLog.username.label('activity_username'),
                func.max(TrackingLog.timestamp).label('last_seen'),
            )
            .filter(TrackingLog.username.isnot(None), TrackingLog.username != '')
            .group_by(TrackingLog.username)
            .subquery()
        )
        users_query = users_query.outerjoin(
            last_activity,
            last_activity.c.activity_username == User.username,
        ).order_by(
            case((last_activity.c.last_seen.is_(None), 1), else_=0),
            last_activity.c.last_seen.desc(),
            func.lower(User.username).asc(),
        )
    else:
        users_query = users_query.order_by(User.created_at.desc(), User.id.desc())

    pagination = users_query.paginate(page=page, per_page=per_page, error_out=False)
    users = pagination.items
    usernames = [user.username for user in users]
    activity_rows = []
    if usernames:
        activity_rows = (
            db.session.query(
                TrackingLog.username,
                func.max(TrackingLog.timestamp),
                func.count(TrackingLog.id),
            )
            .filter(TrackingLog.username.in_(usernames))
            .group_by(TrackingLog.username)
            .all()
        )
    activity_by_user = {
        username: {"last_seen": last_seen, "request_count": request_count}
        for username, last_seen, request_count in activity_rows
    }

    summary_row = (
        db.session.query(
            func.count(User.id),
            func.coalesce(func.sum(case((User.is_admin.is_(True), 1), else_=0)), 0),
            func.coalesce(func.sum(case((User.is_manager.is_(True), 1), else_=0)), 0),
            func.coalesce(
                func.sum(case((User.username.in_(active_usernames), 1), else_=0)),
                0,
            ),
        )
        .filter(*scope_filters)
        .one()
    )
    summary = {
        'total': int(summary_row[0] or 0),
        'admins': int(summary_row[1] or 0),
        'managers': int(summary_row[2] or 0),
        'active': int(summary_row[3] or 0),
    }

    institution_options = []
    if is_admin:
        institution_options = get_institution_options()
        # Reuse this request-scoped data in the global institution selector.
        g.institution_options = institution_options

    base_url_params = {
        'q': query_param,
        'role': selected_role,
        'activity': selected_activity,
        'institution': selected_institution,
        'sort': selected_sort,
        'per_page': per_page,
        'page': page,
    }

    def users_url(**overrides):
        params = {**base_url_params, **overrides}
        clean_params = {
            key: value
            for key, value in params.items()
            if value not in (None, '', 'all')
        }
        return url_for('admin.users_list', **clean_params)
    
    return render_template(
        'admin/users.html',
        users=users,
        pagination=pagination,
        q=query_param,
        selected_role=selected_role,
        selected_activity=selected_activity,
        selected_institution=selected_institution,
        selected_sort=selected_sort,
        per_page=per_page,
        per_page_options=sorted(_USER_PER_PAGE),
        summary=summary,
        activity_by_user=activity_by_user,
        institution_options=institution_options,
        users_url=users_url,
    )


@bp_admin.route('/users/new', methods=['POST'])
@login_required
@admin_required
def users_new():
    """
    Handles the creation of a new user account.
    
    Features:
    - Auto-normalization of ROR IDs.
    - Automatic fetching of GRID IDs if missing but ROR is provided.
    - Duplicate username detection.
    
    Returns:
        Redirect: Back to the users list with success/error flash message.
    """
    username = (request.form.get('username') or '').strip()
    if not username:
        flash_err(_('The "Username" field is required.'))
        return redirect(_users_return_url())

    # Extract and sanitize form data
    email = (request.form.get('email') or '').strip() or username
    first_name = (request.form.get('first_name') or '').strip()
    last_name = (request.form.get('last_name') or '').strip()
    position = (request.form.get('position') or '').strip()
    inst_name = (request.form.get('institution_name') or '').strip()
    
    # Identifiers
    ror_id = normalize_ror_id(request.form.get('ror_id'))
    grid_id = (request.form.get('grid_id') or '').strip()
    am_client_id = (request.form.get('am_client_id') or '').strip()
    
    # Roles and Preferences
    is_admin = bool(request.form.get('is_admin'))
    is_manager = bool(request.form.get('is_manager'))
    locale = request.form.get('locale') or 'en'

    # Automatic GRID lookup via ROR service (Data Healing)
    if ror_id and not grid_id:
        found_grid = fetch_grid_from_ror(ror_id)
        if found_grid:
            grid_id = found_grid
            flash_info(_("GRID ID '%(g)s' automatically found for ROR %(r)s.", g=grid_id, r=ror_id))

    # Duplicate Check
    if User.query.filter_by(username=username).first():
        flash_err(_('A user with username "%(u)s" already exists.', u=username))
        return redirect(_users_return_url())

    # Credential Generation
    temp_password = (request.form.get('password') or '').strip() or generate_temp_password()
    
    new_user = User(
        username=username,
        email=email,
        first_name=first_name,
        last_name=last_name,
        position=position,
        institution_name=inst_name,
        ror_id=ror_id,
        grid_id=grid_id,
        am_client_id=am_client_id if am_client_id else None,
        is_admin=is_admin,
        is_manager=is_manager,
        locale=locale
    )

    try:
        new_user.set_password(temp_password)
        db.session.add(new_user)
        db.session.commit()
        
        role_label = " (Admin)" if is_admin else (" (Manager)" if is_manager else "")
        flash_ok(_('User "%(u)s" created%(r)s. Password: %(p)s', 
                 u=username, r=role_label, p=temp_password))
    except Exception as exc:
        db.session.rollback()
        logger.exception("CRITICAL: Failed to create user: %s", exc)
        flash_err(_('Could not create user. Check logs.'))

    return redirect(_users_return_url())


@bp_admin.route('/users/<int:user_id>/reset-password', methods=['POST'])
@login_required
@admin_required
def users_reset_password(user_id: int):
    """
    Resets the password for a specific user to a randomly generated one.
    This action is logged via standard application logs.
    
    Args:
        user_id (int): The primary key of the user to reset.
    """
    user = db.get_or_404(User, user_id)
    new_pwd = generate_temp_password()

    try:
        user.set_password(new_pwd)
        db.session.commit()
        flash_ok(_('Password reset for %(u)s. New temporary: %(p)s', u=user.username, p=new_pwd))
    except Exception as exc:
        db.session.rollback()
        logger.exception("Error resetting password for %s: %s", user.username, exc)
        flash_err(_("Could not reset password."))

    return redirect(_users_return_url())


@bp_admin.route('/users/<int:user_id>/send-creds', methods=['POST'])
@login_required
@admin_required
def users_send_creds(user_id: int):
    """
    Resets user password and sends the new credentials via email.
    The email content is automatically translated based on the current locale.
    """
    user = db.get_or_404(User, user_id)
    recipient = (user.email or user.username)

    if not recipient or '@' not in recipient:
        flash_err(_("User does not have a valid email."))
        return redirect(_users_return_url())

    temp_pwd = generate_temp_password()
    user.set_password(temp_pwd)
    db.session.commit()

    base_url = current_app.config.get('APP_BASE_URL', '').rstrip('/')
    login_url = f"{base_url}{url_for('auth.login')}" if base_url else url_for('auth.login', _external=True)

    # Email Subject (Translated)
    subject = _("Access to Data ORCID-Chile (credentials)")

    # Email Body (Multi-language construction)
    # Using _() allows Babel to pick the translation from your .po files
    greeting = _("Hello")
    intro_text = _("Your access credentials for <strong>Data ORCID-Chile</strong> have been updated:")
    label_url = _("URL")
    label_user = _("Username")
    label_pass = _("Temporary Password")
    security_note = _("For security reasons, please change your password upon login.")

    email_html = f"""
    <p>{greeting} {user.first_name or user.username},</p>
    <p>{intro_text}</p>
    <ul>
      <li><b>{label_url}:</b> <a href="{login_url}">{login_url}</a></li>
      <li><b>{label_user}:</b> {user.username}</li>
      <li><b>{label_pass}:</b> {temp_pwd}</li>
    </ul>
    <p>{security_note}</p>
    """

    email_text = f"{greeting} {user.username}\n{label_url}: {login_url}\n{label_user}: {user.username}\n{label_pass}: {temp_pwd}"

    success, error = send_email(
        to_email=recipient,
        subject=subject,
        html=email_html,
        text=email_text,
    )

    if success:
        flash_ok(_("Credentials sent to %(r)s.", r=recipient))
    else:
        logger.error("Email Delivery Failed to %s: %s", recipient, error)
        flash_err(_("Could not send email."))

    return redirect(_users_return_url())


@bp_admin.route('/users/<int:user_id>/update', methods=['POST'])
@login_required
@admin_required
def users_update(user_id: int):
    """
    Updates an existing user's metadata and roles.
    Includes safeguards to prevent self-lockout (Admin removing their own admin role).
    
    Args:
        user_id (int): The ID of the user to update.
    """
    user = db.get_or_404(User, user_id)

    username = (request.form.get('username') or '').strip()
    if not username:
        flash_err(_('The "Username" field is required.'))
        return redirect(_users_return_url())

    duplicate = User.query.filter(
        func.lower(User.username) == username.lower(),
        User.id != user.id,
    ).first()
    if duplicate:
        flash_err(_('A user with username "%(u)s" already exists.', u=username))
        return redirect(_users_return_url())

    # Extract data from form
    user.username = username
    user.email = (request.form.get('email') or '').strip() or username
    user.first_name = (request.form.get('first_name') or '').strip()
    user.last_name = (request.form.get('last_name') or '').strip()
    user.position = (request.form.get('position') or '').strip()
    user.institution_name = (request.form.get('institution_name') or '').strip()
    user.ror_id = normalize_ror_id(request.form.get('ror_id'))
    user.grid_id = (request.form.get('grid_id') or '').strip()
    user.am_client_id = (request.form.get('am_client_id') or '').strip() or None
    
    want_admin = bool(request.form.get('is_admin'))
    want_manager = bool(request.form.get('is_manager'))
    locale = request.form.get('locale')

    # Security Safeguard: Admins cannot remove their own admin role
    if user.id == session.get('user_id') and not want_admin:
        flash_err(_('You cannot remove admin role from your own account.'))
        return redirect(_users_return_url())

    # GRID ID Healing (if missing but ROR is present)
    if user.ror_id and not user.grid_id:
        found_grid = fetch_grid_from_ror(user.ror_id)
        if found_grid:
            user.grid_id = found_grid
            flash_info(_("GRID ID '%(g)s' automatically found.", g=found_grid))

    try:
        user.is_admin = want_admin
        user.is_manager = want_manager
        
        # Update locale session if the user modifies their own profile
        if locale in ['es', 'en']:
            user.locale = locale
            if user.id == session.get('user_id'):
                session['locale'] = locale

        if user.id == session.get('user_id'):
            session['username'] = user.username
            session['first_name'] = user.first_name
            session['institution_name'] = user.institution_name
            session['ror_id'] = user.ror_id
            session['is_admin'] = bool(user.is_admin)
            session['is_manager'] = bool(user.is_manager)

        db.session.commit()
        flash_ok(_('User "%(u)s" updated successfully.', u=user.username))
    except Exception as exc:
        db.session.rollback()
        logger.exception("Error updating user %s: %s", user.username, exc)
        flash_err(_('Could not update user.'))

    return redirect(_users_return_url())


@bp_admin.route('/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def users_delete(user_id: int):
    """
    Permanently deletes a user from the database. 
    Hardcoded protection prevents deletion of the root 'admin' system account.
    """
    user = db.get_or_404(User, user_id)

    if user.id == session.get('user_id'):
        flash_err(_('You cannot delete your own account.'))
        return redirect(_users_return_url())
    
    # Root Protection
    if user.username == 'admin':
        flash_err(_('You cannot delete the main admin account.'))
        return redirect(_users_return_url())

    try:
        db.session.delete(user)
        db.session.commit()
        flash_ok(_('User deleted successfully.'))
    except Exception as exc:
        db.session.rollback()
        logger.exception("Error deleting user %s: %s", user.username, exc)
        flash_err(_("Could not delete user."))

    return redirect(_users_return_url())


@bp_admin.route('/set-ror/<ror_id>', methods=['POST'])
@login_required
def set_ror(ror_id: str):
    """
    Updates the active ROR context for the current session.
    
    This feature allows global administrators to 'impersonate' an institution
    context to view specific dashboards or data sets without logging out.
    """
    # Permission Check
    if not session.get('is_admin'):
        flash_err(_("Action not allowed"))
        return redirect(request.referrer or url_for('main.index'))

    ror_id = normalize_ror_id(ror_id)
    if not ror_id:
        flash_err(_("Invalid ROR ID."))
        return redirect(request.referrer or url_for('main.index'))

    institution = get_institution_by_ror(ror_id)
    if not institution:
        flash_err(_("Institution not found."))
        return redirect(request.referrer or url_for('main.index'))

    session['admin_selected_ror'] = ror_id
    
    display_name = institution.get("name") or ror_id
    
    flash_ok(_("Active institution changed to: %(r)s", r=display_name))
    return redirect(request.referrer or url_for('main.index'))


def _job_dashboard_context() -> dict:
    """Build the filtered, source-backed model for the admin job dashboard."""
    page = max(request.args.get("page", 1, type=int), 1)
    requested_per_page = request.args.get("per_page", 10, type=int)
    per_page = requested_per_page if requested_per_page in _JOB_PER_PAGE else 10
    search_query = request.args.get("q", "").strip()

    selected_status = request.args.get("status", "all").strip().lower()
    if selected_status not in _JOB_STATUSES:
        selected_status = "all"
    selected_type = request.args.get("type", "").strip()
    selected_institution_raw = request.args.get("institution", "").strip().lower()
    selected_institution = (
        "system"
        if selected_institution_raw == "system"
        else normalize_ror_id(selected_institution_raw)
    )
    selected_user = request.args.get("user", type=int)

    selected_period, period_start, period_end, date_from, date_to = _statistics_period_bounds(
        request.args.get("period", "7d").strip().lower(),
        request.args.get("date_from", "").strip(),
        request.args.get("date_to", "").strip(),
    )

    sort_key = request.args.get("sort", "created").strip().lower()
    sort_key = sort_key if sort_key in _JOB_SORT_OPTIONS else "created"
    sort_direction = request.args.get("dir", "desc").strip().lower()
    sort_direction = sort_direction if sort_direction in {"asc", "desc"} else "desc"

    def jobs_url(**updates):
        params = request.args.to_dict(flat=True)
        params.pop("fragment", None)
        params.update(updates)
        clean_params = {
            key: value
            for key, value in params.items()
            if value not in (None, "", "all")
        }
        return url_for("admin.jobs", **clean_params)

    dimension_query = SyncJob.query.outerjoin(User, User.id == SyncJob.requested_by_user_id)
    if selected_type:
        dimension_query = dimension_query.filter(SyncJob.job_type == selected_type)
    if selected_institution == "system":
        dimension_query = dimension_query.filter(SyncJob.ror_id.is_(None))
    elif selected_institution:
        dimension_query = dimension_query.filter(SyncJob.ror_id == selected_institution)
    if selected_user:
        dimension_query = dimension_query.filter(SyncJob.requested_by_user_id == selected_user)
    if search_query:
        pattern = f"%{search_query}%"
        dimension_query = dimension_query.filter(or_(
            SyncJob.id.ilike(pattern),
            SyncJob.name.ilike(pattern),
            SyncJob.message.ilike(pattern),
            SyncJob.error.ilike(pattern),
            SyncJob.ror_id.ilike(pattern),
            User.username.ilike(pattern),
        ))

    period_filters = []
    if period_start is not None:
        period_filters.append(SyncJob.created_at >= period_start)
    if period_end is not None:
        period_filters.append(SyncJob.created_at < period_end)
    period_query = dimension_query.filter(*period_filters)

    active_statuses = {"queued", "running"}
    table_query = dimension_query
    if period_filters:
        table_query = table_query.filter(or_(
            and_(*period_filters),
            SyncJob.status.in_(active_statuses),
        ))
    if selected_status == "active":
        table_query = table_query.filter(SyncJob.status.in_(active_statuses))
    elif selected_status != "all":
        table_query = table_query.filter(SyncJob.status == selected_status)

    stale_minutes = max(int(current_app.config.get("JOB_STALE_MINUTES", 30)), 1)
    stale_cutoff = utc_now() - timedelta(minutes=stale_minutes)
    summary = {
        "running": dimension_query.filter(SyncJob.status == "running").count(),
        "queued": dimension_query.filter(SyncJob.status == "queued").count(),
        "successful": period_query.filter(SyncJob.status == "success").count(),
        "attention": period_query.filter(
            SyncJob.status.in_({"partial", "failed", "interrupted"})
        ).count(),
        "stale": dimension_query.filter(
            SyncJob.status == "running",
            or_(SyncJob.heartbeat_at.is_(None), SyncJob.heartbeat_at < stale_cutoff),
        ).count(),
    }

    sort_columns = {
        "created": SyncJob.created_at,
        "started": SyncJob.started_at,
        "status": SyncJob.status,
        "type": SyncJob.job_type,
        "institution": SyncJob.ror_id,
    }
    sort_column = sort_columns[sort_key]
    primary_order = sort_column.asc() if sort_direction == "asc" else sort_column.desc()
    order_clauses = [primary_order]
    if sort_key != "created":
        order_clauses.append(SyncJob.created_at.desc())
    pagination = table_query.order_by(*order_clauses).paginate(
        page=page,
        per_page=per_page,
        error_out=False,
    )

    job_ids = [job.id for job in pagination.items]
    steps_by_job = {job_id: [] for job_id in job_ids}
    if job_ids:
        for step in (
            SyncJobStep.query
            .filter(SyncJobStep.sync_job_id.in_(job_ids))
            .order_by(SyncJobStep.sync_job_id.asc(), SyncJobStep.position.asc())
            .all()
        ):
            steps_by_job.setdefault(step.sync_job_id, []).append(step)

    requester_ids = {
        job.requested_by_user_id for job in pagination.items if job.requested_by_user_id
    }
    requester_lookup = {
        user.id: user.username
        for user in User.query.filter(User.id.in_(requester_ids)).all()
    } if requester_ids else {}
    institution_options = get_institution_options()
    institution_lookup = {
        option["ror_id"]: option.get("name") or option["ror_id"]
        for option in institution_options
        if option.get("ror_id")
    }

    now = utc_now()
    jobs = []
    terminal_step_statuses = {"success", "failed", "skipped", "interrupted"}
    for job in pagination.items:
        raw_steps = steps_by_job.get(job.id, [])
        completed_steps = sum(step.status in terminal_step_statuses for step in raw_steps)
        step_total = max(int(job.progress_total or 0), len(raw_steps))
        step_current = max(int(job.progress_current or 0), completed_steps)
        step_percent = round(step_current / step_total * 100, 1) if step_total else 0.0
        item_total = max(int(job.items_total or 0), 0)
        item_current = max(int(job.items_current or 0), 0)
        item_percent = round(min(item_current, item_total) / item_total * 100, 1) if item_total else 0.0

        current_step = next((step for step in raw_steps if step.status == "running"), None)
        if not current_step:
            current_step = next((step for step in raw_steps if step.status == "pending"), None)
        if not current_step and raw_steps:
            current_step = raw_steps[-1]

        started_at = job.started_at or job.created_at
        duration_end = job.finished_at or (now if job.status in active_statuses else started_at)
        duration_seconds = max(int((duration_end - started_at).total_seconds()), 0) if started_at else 0
        is_stale = bool(
            job.status == "running"
            and (not job.heartbeat_at or job.heartbeat_at < stale_cutoff)
        )

        step_rows = []
        for step in raw_steps:
            step_end = step.finished_at or (now if step.status == "running" else step.started_at)
            step_duration = (
                max(int((step_end - step.started_at).total_seconds()), 0)
                if step.started_at and step_end
                else None
            )
            step_rows.append({
                "name": step.name,
                "label": _job_step_label(step.name, institution_lookup),
                "status": step.status,
                "records_count": int(step.records_count or 0),
                "error": step.error,
                "started_at": step.started_at,
                "finished_at": step.finished_at,
                "duration": _format_job_duration(step_duration) if step_duration is not None else "",
            })

        jobs.append({
            "id": job.id,
            "short_id": job.id.split("-", 1)[0],
            "name": job.name,
            "job_type": job.job_type,
            "type_label": _job_type_label(job.job_type),
            "ror_id": job.ror_id,
            "institution": institution_lookup.get(job.ror_id, job.ror_id) if job.ror_id else _("System-wide"),
            "requester": requester_lookup.get(job.requested_by_user_id) or _("System process"),
            "status": job.status,
            "message": _job_message_label(job.message),
            "error": job.error,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "heartbeat_at": job.heartbeat_at,
            "duration": _format_job_duration(duration_seconds),
            "is_stale": is_stale,
            "step_current": step_current,
            "step_total": step_total,
            "step_percent": step_percent,
            "current_step": (
                _job_step_label(current_step.name, institution_lookup)
                if current_step
                else _("No steps recorded")
            ),
            "items_current": item_current,
            "items_total": item_total,
            "items_percent": item_percent,
            "progress_unit": _job_progress_unit_label(job.progress_unit),
            "steps": step_rows,
        })

    job_types = [
        value
        for (value,) in db.session.query(SyncJob.job_type)
        .filter(SyncJob.job_type.isnot(None), SyncJob.job_type != "")
        .distinct()
        .order_by(SyncJob.job_type.asc())
        .all()
        if value
    ]
    requester_options = (
        db.session.query(User.id, User.username)
        .join(SyncJob, SyncJob.requested_by_user_id == User.id)
        .distinct()
        .order_by(User.username.asc())
        .all()
    )
    period_labels = {
        "24h": _("Last 24 hours"),
        "7d": _("Last 7 days"),
        "30d": _("Last 30 days"),
        "all": _("All time"),
        "custom": _("Custom range"),
    }

    return {
        "jobs": jobs,
        "pagination": pagination,
        "summary": summary,
        "q": search_query,
        "selected_status": selected_status,
        "selected_type": selected_type,
        "selected_institution": selected_institution,
        "selected_user": selected_user,
        "selected_period": selected_period,
        "period_label": period_labels[selected_period],
        "date_from": date_from,
        "date_to": date_to,
        "sort_key": sort_key,
        "sort_direction": sort_direction,
        "per_page": per_page,
        "per_page_options": sorted(_JOB_PER_PAGE),
        "job_types": [{"value": value, "label": _job_type_label(value)} for value in job_types],
        "institution_options": institution_options,
        "requester_options": requester_options,
        "jobs_url": jobs_url,
        "has_active": bool(summary["running"] or summary["queued"]),
        "stale_minutes": stale_minutes,
        "refreshed_at": now,
    }


@bp_admin.route("/jobs")
@admin_required
def jobs():
    """Monitor durable background jobs, item progress, and execution steps."""
    context = _job_dashboard_context()
    if request.args.get("fragment") == "1":
        return jsonify({
            "html": render_template("admin/_jobs_table.html", **context),
            "summary": context["summary"],
            "has_active": context["has_active"],
            "refreshed_at": context["refreshed_at"].isoformat(),
        })
    return render_template("admin/jobs.html", **context)


@bp_admin.route("/statistics")
@login_required
def statistics():
    """Render filterable activity, performance trends, and request details."""
    if not _require_admin_or_manager():
        return redirect(url_for('main.index'))

    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 25, type=int)
    if per_page not in _STATISTICS_PER_PAGE:
        per_page = 25

    search_query = request.args.get("q", "").strip()
    selected_user = request.args.get("user", "").strip()
    show_anonymous = request.args.get("show_anonymous", "0") == "1"
    selected_method = request.args.get("method", "").strip().upper()
    selected_status = request.args.get("status", "").strip()
    selected_kind = request.args.get("kind", "interactive").strip().lower()
    if selected_kind not in _STATISTICS_KINDS:
        selected_kind = "interactive"

    selected_period, period_start, period_end, date_from, date_to = _statistics_period_bounds(
        request.args.get("period", "7d").strip().lower(),
        request.args.get("date_from", "").strip(),
        request.args.get("date_to", "").strip(),
    )

    sort_key = request.args.get("sort", "timestamp").strip().lower()
    sort_direction = request.args.get("dir", "desc").strip().lower()
    sort_columns = {
        "timestamp": TrackingLog.timestamp,
        "user": TrackingLog.username,
        "path": TrackingLog.path,
        "method": TrackingLog.method,
        "status": TrackingLog.status_code,
        "latency": TrackingLog.duration_ms,
    }
    if sort_key not in sort_columns:
        sort_key = "timestamp"
    if sort_direction not in {"asc", "desc"}:
        sort_direction = "desc"

    def statistics_url(**updates):
        params = request.args.to_dict(flat=False)
        for key, value in updates.items():
            if value is None or value == "" or value == []:
                params.pop(key, None)
            else:
                params[key] = value
        return url_for("admin.statistics", **params)

    log_query = TrackingLog.query

    if period_start is not None:
        log_query = log_query.filter(TrackingLog.timestamp >= period_start)
    if period_end is not None:
        log_query = log_query.filter(TrackingLog.timestamp < period_end)

    if not show_anonymous:
        log_query = log_query.filter(TrackingLog.username.isnot(None), TrackingLog.username != "")

    if selected_user:
        log_query = log_query.filter(TrackingLog.username == selected_user)

    if selected_method:
        log_query = log_query.filter(TrackingLog.method == selected_method)

    if selected_status == "success":
        log_query = log_query.filter(TrackingLog.status_code < 300)
    elif selected_status == "redirect":
        log_query = log_query.filter(TrackingLog.status_code >= 300, TrackingLog.status_code < 400)
    elif selected_status == "error":
        log_query = log_query.filter(TrackingLog.status_code >= 400)

    if search_query:
        search_filter = f"%{search_query}%"
        log_query = log_query.filter(
            (TrackingLog.username.ilike(search_filter)) |
            (TrackingLog.path.ilike(search_filter)) |
            (TrackingLog.ip.ilike(search_filter))
        )

    background_condition = _background_request_condition()
    background_query = log_query.filter(background_condition)
    interactive_query = log_query.filter(
        or_(TrackingLog.path.is_(None), not_(background_condition))
    )
    if selected_kind == "background":
        scoped_query = background_query
    elif selected_kind == "all":
        scoped_query = log_query
    else:
        scoped_query = interactive_query

    summary = _request_summary(scoped_query)
    background_summary = _request_summary(background_query)

    granularity = "hour" if selected_period == "24h" else "day"
    if selected_period == "custom" and period_start and period_end:
        if period_end - period_start <= timedelta(days=2):
            granularity = "hour"

    dialect = db.engine.dialect.name
    if dialect == "postgresql":
        bucket_expression = func.date_trunc(granularity, TrackingLog.timestamp)
        trend_latency_expression = func.percentile_cont(0.95).within_group(TrackingLog.duration_ms)
        trend_latency_label = _("P95 latency")
    elif dialect in {"mysql", "mariadb"}:
        date_format = "%Y-%m-%d %H:00" if granularity == "hour" else "%Y-%m-%d"
        bucket_expression = func.date_format(TrackingLog.timestamp, date_format)
        trend_latency_expression = func.avg(TrackingLog.duration_ms)
        trend_latency_label = _("Average latency")
    else:
        date_format = "%Y-%m-%d %H:00" if granularity == "hour" else "%Y-%m-%d"
        bucket_expression = func.strftime(date_format, TrackingLog.timestamp)
        trend_latency_expression = func.avg(TrackingLog.duration_ms)
        trend_latency_label = _("Average latency")

    trend_rows = (
        scoped_query.with_entities(
            bucket_expression.label("bucket"),
            func.count(TrackingLog.id).label("requests"),
            func.coalesce(
                func.sum(case((TrackingLog.status_code >= 400, 1), else_=0)),
                0,
            ).label("errors"),
            trend_latency_expression.label("latency"),
        )
        .group_by(bucket_expression)
        .order_by(bucket_expression.asc())
        .all()
    )
    trend = {
        "labels": [
            row.bucket.strftime("%Y-%m-%d %H:00" if granularity == "hour" else "%Y-%m-%d")
            if hasattr(row.bucket, "strftime") else str(row.bucket)
            for row in trend_rows
        ],
        "requests": [int(row.requests or 0) for row in trend_rows],
        "errors": [int(row.errors or 0) for row in trend_rows],
        "latency": [round(float(row.latency or 0), 1) for row in trend_rows],
        "latency_label": trend_latency_label,
    }

    order_column = sort_columns[sort_key]
    primary_order = order_column.asc() if sort_direction == "asc" else order_column.desc()
    order_clauses = [primary_order]
    if sort_key != "timestamp":
        order_clauses.append(TrackingLog.timestamp.desc())

    pagination = scoped_query.order_by(*order_clauses).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    distinct_users = db.session.query(TrackingLog.username).filter(
        TrackingLog.username.isnot(None),
        TrackingLog.username != "",
    ).distinct().order_by(TrackingLog.username.asc()).all()
    user_list = [u[0] for u in distinct_users if u[0]]

    period_labels = {
        "24h": _("Last 24 hours"),
        "7d": _("Last 7 days"),
        "30d": _("Last 30 days"),
        "all": _("All time"),
        "custom": _("Custom range"),
    }
    kind_labels = {
        "interactive": _("Interactive requests"),
        "background": _("Background operations"),
        "all": _("All requests"),
    }

    active_filters = []
    if search_query:
        active_filters.append({
            "label": _("Search"),
            "value": search_query,
            "url": statistics_url(q=None, page=1),
        })
    if selected_user:
        active_filters.append({
            "label": _("User"),
            "value": selected_user,
            "url": statistics_url(user=None, page=1),
        })
    if selected_method:
        active_filters.append({
            "label": _("Method"),
            "value": selected_method,
            "url": statistics_url(method=None, page=1),
        })
    if selected_status:
        status_labels = {
            "success": _("Success"),
            "redirect": _("Redirects"),
            "error": _("Errors"),
        }
        active_filters.append({
            "label": _("Status"),
            "value": status_labels.get(selected_status, selected_status),
            "url": statistics_url(status=None, page=1),
        })
    if show_anonymous:
        active_filters.append({
            "label": _("Access"),
            "value": _("Including anonymous"),
            "url": statistics_url(show_anonymous=None, page=1),
        })

    return render_template(
        "admin/statistics.html",
        logs=pagination.items,
        pagination=pagination,
        q=search_query,
        users=user_list,
        selected_user=selected_user,
        show_anonymous=show_anonymous,
        selected_method=selected_method,
        selected_status=selected_status,
        selected_kind=selected_kind,
        selected_period=selected_period,
        period_label=period_labels[selected_period],
        kind_label=kind_labels[selected_kind],
        date_from=date_from,
        date_to=date_to,
        per_page=per_page,
        per_page_options=sorted(_STATISTICS_PER_PAGE),
        sort_key=sort_key,
        sort_direction=sort_direction,
        summary=summary,
        background_summary=background_summary,
        trend=trend,
        active_filters=active_filters,
        statistics_url=statistics_url,
        format_latency=_format_latency,
        user_agent_summary=_user_agent_summary,
    )
