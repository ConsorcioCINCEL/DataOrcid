"""Admin routes for users, institution context, and usage statistics."""

import logging
import secrets
import string
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, session, current_app
)
from flask_babel import _
from sqlalchemy import case, func, or_

from .. import db
from ..models import User, TrackingLog
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

    # Search Logic
    query_param = (request.args.get('q') or '').strip()
    users_query = User.query
    if not session.get('is_admin'):
        users_query = users_query.filter(User.ror_id == session.get('ror_id'))

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

    users = users_query.order_by(User.created_at.desc()).all()
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
    institution_metadata = {
        item["ror_id"]: item
        for item in get_institution_options()
        if item.get("ror_id")
    }
    
    return render_template(
        'admin/users.html',
        users=users,
        q=query_param,
        activity_by_user=activity_by_user,
        institution_metadata=institution_metadata,
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
        return redirect(url_for('admin.users_list'))

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
    locale = request.form.get('locale') or 'es'

    # Automatic GRID lookup via ROR service (Data Healing)
    if ror_id and not grid_id:
        found_grid = fetch_grid_from_ror(ror_id)
        if found_grid:
            grid_id = found_grid
            flash_info(_("GRID ID '%(g)s' automatically found for ROR %(r)s.", g=grid_id, r=ror_id))

    # Duplicate Check
    if User.query.filter_by(username=username).first():
        flash_err(_('A user with username "%(u)s" already exists.', u=username))
        return redirect(url_for('admin.users_list'))

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

    return redirect(url_for('admin.users_list'))


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
    user = User.query.get_or_404(user_id)
    new_pwd = generate_temp_password()

    try:
        user.set_password(new_pwd)
        db.session.commit()
        flash_ok(_('Password reset for %(u)s. New temporary: %(p)s', u=user.username, p=new_pwd))
    except Exception as exc:
        db.session.rollback()
        logger.exception("Error resetting password for %s: %s", user.username, exc)
        flash_err(_("Could not reset password."))

    return redirect(url_for('admin.users_list'))


@bp_admin.route('/users/<int:user_id>/send-creds', methods=['POST'])
@login_required
@admin_required
def users_send_creds(user_id: int):
    """
    Resets user password and sends the new credentials via email.
    The email content is automatically translated based on the current locale.
    """
    user = User.query.get_or_404(user_id)
    recipient = (user.email or user.username)

    if not recipient or '@' not in recipient:
        flash_err(_("User does not have a valid email."))
        return redirect(url_for('admin.users_list'))

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

    return redirect(url_for('admin.users_list'))


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
    user = User.query.get_or_404(user_id)
    
    # Extract data from form
    user.email = (request.form.get('email') or '').strip() or user.username
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
        return redirect(url_for('admin.users_list'))

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

        db.session.commit()
        flash_ok(_('User "%(u)s" updated successfully.', u=user.username))
    except Exception as exc:
        db.session.rollback()
        logger.exception("Error updating user %s: %s", user.username, exc)
        flash_err(_('Could not update user.'))

    return redirect(url_for('admin.users_list'))


@bp_admin.route('/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def users_delete(user_id: int):
    """
    Permanently deletes a user from the database. 
    Hardcoded protection prevents deletion of the root 'admin' system account.
    """
    user = User.query.get_or_404(user_id)
    
    # Root Protection
    if user.username == 'admin':
        flash_err(_('You cannot delete the main admin account.'))
        return redirect(url_for('admin.users_list'))

    try:
        db.session.delete(user)
        db.session.commit()
        flash_ok(_('User deleted successfully.'))
    except Exception as exc:
        db.session.rollback()
        logger.exception("Error deleting user %s: %s", user.username, exc)
        flash_err(_("Could not delete user."))

    return redirect(url_for('admin.users_list'))


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


@bp_admin.route("/statistics")
@login_required
def statistics():
    """
    Renders system usage statistics and tracking logs.
    Includes pagination and filtering by User, IP, or Request Path.
    """
    if not _require_admin_or_manager():
        return redirect(url_for('main.index'))

    # Pagination and Filtering Params
    page = request.args.get("page", 1, type=int)
    search_query = request.args.get("q", "").strip()
    selected_user = request.args.get("user", "").strip()
    show_anonymous = request.args.get("show_anonymous", "0") == "1"
    selected_method = request.args.get("method", "").strip().upper()
    selected_status = request.args.get("status", "").strip()

    log_query = TrackingLog.query

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

    summary_subquery = log_query.with_entities(
        TrackingLog.username.label("username"),
        TrackingLog.status_code.label("status_code"),
        TrackingLog.method.label("method"),
        TrackingLog.duration_ms.label("duration_ms"),
    ).subquery()
    total_requests, unique_users, error_requests, post_requests, average_latency = (
        db.session.query(
            func.count(),
            func.count(func.distinct(summary_subquery.c.username)),
            func.coalesce(func.sum(case((summary_subquery.c.status_code >= 400, 1), else_=0)), 0),
            func.coalesce(func.sum(case((summary_subquery.c.method == "POST", 1), else_=0)), 0),
            func.coalesce(func.avg(summary_subquery.c.duration_ms), 0),
        )
        .select_from(summary_subquery)
        .one()
    )
    summary = {
        "total_requests": int(total_requests or 0),
        "unique_users": int(unique_users or 0),
        "error_requests": int(error_requests or 0),
        "post_requests": int(post_requests or 0),
        "average_latency": float(average_latency or 0),
    }

    pagination = log_query.order_by(TrackingLog.timestamp.desc()).paginate(
        page=page, per_page=25, error_out=False
    )
    
    distinct_users = db.session.query(TrackingLog.username).filter(
        TrackingLog.username.isnot(None),
        TrackingLog.username != "",
    ).distinct().order_by(TrackingLog.username.asc()).all()
    user_list = [u[0] for u in distinct_users if u[0]]

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
        summary=summary,
    )
