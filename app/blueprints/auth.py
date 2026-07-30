"""Authentication, password recovery, and account settings routes."""

import logging
import re
import threading
import time
from urllib.parse import urlsplit
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, session, current_app
)
from flask_babel import _
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from .. import db
from ..models import User
from ..utils.flashes import flash_err, flash_ok, flash_success
from ..decorators import login_required
from ..utils.emailer import send_email

bp_auth = Blueprint("auth", __name__, url_prefix="/auth")
logger = logging.getLogger(__name__)
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_BUCKETS: dict[str, list[float]] = {}
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+$")


def _safe_redirect_target(target: str | None) -> str | None:
    """Return a local redirect target or None when the target is unsafe."""
    if not target:
        return None
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None
    return target if target.startswith("/") else None


def _client_ip() -> str:
    """Return the best available client IP for local rate limiting."""
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    return (forwarded_for.split(",", 1)[0].strip() or request.remote_addr or "unknown")[:50]


def _is_rate_limited(action: str, limit: int, window_seconds: int) -> bool:
    """Apply a small process-local rate limit for sensitive auth actions."""
    now = time.time()
    cutoff = now - window_seconds
    key = f"{action}:{_client_ip()}"
    with _RATE_LIMIT_LOCK:
        attempts = [ts for ts in _RATE_LIMIT_BUCKETS.get(key, []) if ts >= cutoff]
        blocked = len(attempts) >= limit
        attempts.append(now)
        _RATE_LIMIT_BUCKETS[key] = attempts
        return blocked


def _clear_rate_limit(action: str) -> None:
    """Clear a client's failed-attempt history after successful authentication."""
    key = f"{action}:{_client_ip()}"
    with _RATE_LIMIT_LOCK:
        _RATE_LIMIT_BUCKETS.pop(key, None)


def make_password_reset_token(user: User) -> str:
    """Create a reset token bound to the user's current password hash."""
    serializer = _get_serializer()
    return serializer.dumps({'uid': user.id, 'pwd': (user.password_hash or '')[-12:]})


def make_password_reset_url(user: User) -> str:
    """Create an absolute password reset URL suitable for email delivery."""
    return _get_absolute_url('auth.reset_password', token=make_password_reset_token(user))


def _get_serializer() -> URLSafeTimedSerializer:
    """
    Creates a secure serializer for generating time-sensitive tokens.
    Used primarily for password reset links.
    
    Returns:
        URLSafeTimedSerializer: Configured with the application's SECRET_KEY and a specific salt.
    """
    secret = current_app.config.get('SECRET_KEY')
    # Salt adds an extra layer of security to the token signature
    salt = current_app.config.get('SECURITY_PASSWORD_SALT', 'CHANGE_ME_SALT')
    return URLSafeTimedSerializer(secret_key=secret, salt=salt)


def _get_absolute_url(endpoint: str, **values) -> str:
    """
    Constructs an absolute URL for external use (e.g., email links).
    Prioritizes the 'APP_BASE_URL' configuration to ensure correct links behind proxies.
    
    Args:
        endpoint (str): The target Flask endpoint (e.g., 'auth.reset_password').
        **values: Keyword arguments to be passed to url_for.
        
    Returns:
        str: The full absolute URL string.
    """
    base_url = (current_app.config.get('APP_BASE_URL') or '').rstrip('/')
    
    if base_url:
        path = url_for(endpoint, **values)
        # Ensure path starts with a slash for correct concatenation
        if not path.startswith('/'):
            path = '/' + path
        return f"{base_url}{path}"
    
    # Fallback: Let Flask construct the external URL based on the request context
    return url_for(endpoint, _external=True, **values)


@bp_auth.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """
    Initiates the password recovery process.
    If the user exists, a timed token is generated and sent via email.
    
    Security Note:
    - Returns a generic success message to prevent user enumeration.
    - Tokens expire after 24 hours.
    """
    if request.method == 'POST':
        if _is_rate_limited("forgot-password", limit=5, window_seconds=900):
            flash_err(_("Too many attempts. Please try again later."))
            return redirect(url_for('auth.login'))

        identifier = (request.form.get('email') or '').strip()
        user = None

        if identifier:
            # Allow recovery via Email or Username
            user = User.query.filter(
                (User.email == identifier) | (User.username == identifier)
            ).first()

        if user:
            try:
                reset_url = make_password_reset_url(user)

                email_html = f"""
                <p>Hello {user.first_name or user.username},</p>
                <p>A password reset was requested for your <strong>Data ORCID-Chile</strong> account.</p>
                <p>Click the link below to set a new password (valid for 24 hours):</p>
                <p><a href="{reset_url}">{reset_url}</a></p>
                <p>If you did not request this, please ignore this email.</p>
                """

                success, error = send_email(
                    to_email=user.email or user.username,
                    subject=_("Recover your password — Data ORCID-Chile"),
                    html=email_html,
                    text=f"Reset link: {reset_url}",
                )

                if not success:
                    logger.error("Email reset failed for user %s: %s", user.username, error)
                    flash_err(_("Could not send recovery email."))
            except Exception as exc:
                logger.exception("CRITICAL: Token generation error during password reset: %s", exc)

        # Always show success message to the user
        flash_ok(_("If the account exists, we will send instructions."))
        return redirect(url_for('auth.login'))

    return render_template('auth/forgot_password.html')


@bp_auth.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token: str):
    """
    Validates the recovery token and allows the user to set a new password.
    
    Args:
        token (str): The timed token received via email.
    """
    serializer = _get_serializer()
    user = None

    try:
        # Validate signature and check expiration (86400 seconds = 24 hours)
        data = serializer.loads(token, max_age=86400)
        user = db.session.get(User, data.get('uid'))
        
        if not user:
            flash_err(_("Invalid or non-existent user."))
            return redirect(url_for('auth.login'))
        if data.get('pwd') and data.get('pwd') != (user.password_hash or '')[-12:]:
            flash_err(_("Invalid token."))
            return redirect(url_for('auth.login'))
            
    except (SignatureExpired, BadSignature) as exc:
        msg = _("Token expired.") if isinstance(exc, SignatureExpired) else _("Invalid token.")
        flash_err(msg)
        return redirect(url_for('auth.login'))

    if request.method == 'POST':
        pwd = request.form.get('new_password')
        confirm = request.form.get('confirm_password')

        if not pwd or len(pwd) < 8:
            flash_err(_("Password must be at least 8 characters."))
        elif pwd != confirm:
            flash_err(_("Passwords do not match."))
        else:
            user.set_password(pwd)
            db.session.commit()
            flash_success(_("Password updated successfully. Please log in."))
            return redirect(url_for('auth.login'))
            
        return redirect(url_for('auth.reset_password', token=token))

    return render_template('auth/reset_password.html', token=token)


@bp_auth.route('/login', methods=['GET', 'POST'])
def login():
    """
    Handles user authentication and session initialization.
    Redirects authenticated users to the dashboard or their previous page.
    """
    next_url = _safe_redirect_target(request.args.get('next')) or url_for('main.index')
    if session.get('logged_in'):
        return redirect(next_url)

    if request.method == 'POST':
        if _is_rate_limited("login", limit=10, window_seconds=900):
            flash_err(_("Too many login attempts. Please try again later."))
            return render_template('auth/login.html'), 429

        username = (request.form.get('username') or "").strip()
        password = request.form.get('password') or ""
        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            # Populate Session Data
            _clear_rate_limit("login")
            session.permanent = request.form.get('remember') == 'on'
            session.update({
                'user_id': user.id,
                'username': user.username,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'position': user.position,
                'email': user.email,
                'display_name': user.full_name,
                'is_admin': bool(user.is_admin),
                'is_manager': bool(user.is_manager),
                'institution_name': user.institution_name,
                'ror_id': user.ror_id,
                'locale': user.locale or current_app.config.get('BABEL_DEFAULT_LOCALE', 'en'),
                'logged_in': True,
            })

            # Set initial context for admin multi-tenancy support.
            if user.is_admin and user.ror_id:
                session['admin_selected_ror'] = user.ror_id

            logger.info("User %s logged in successfully.", user.username)
            return redirect(next_url)

        flash_err(_("Invalid username or password."))

    return render_template('auth/login.html')


@bp_auth.route('/logout', methods=['POST'])
@login_required
def logout():
    """
    Terminates the user session and redirects to login.
    """
    session.clear()
    flash_success(_("Session closed successfully."))
    return redirect(url_for('auth.login'))


@bp_auth.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    """Allow authenticated users to update their own personal details."""
    user = db.session.get(User, session.get('user_id'))
    if not user:
        session.clear()
        return redirect(url_for('auth.login'))

    form_data = {
        'first_name': user.first_name or '',
        'last_name': user.last_name or '',
        'position': user.position or '',
        'email': user.email or '',
    }

    if request.method == 'POST':
        form_data = {
            field: (request.form.get(field) or '').strip()
            for field in ('first_name', 'last_name', 'position', 'email')
        }
        field_limits = {
            'first_name': (120, _("First name must be 120 characters or fewer.")),
            'last_name': (120, _("Last name must be 120 characters or fewer.")),
            'position': (180, _("Position must be 180 characters or fewer.")),
            'email': (255, _("Email must be 255 characters or fewer.")),
        }
        validation_error = next(
            (
                message
                for field, (limit, message) in field_limits.items()
                if len(form_data[field]) > limit
            ),
            None,
        )

        if validation_error:
            flash_err(validation_error)
        elif form_data['email'] and not _EMAIL_PATTERN.fullmatch(form_data['email']):
            flash_err(_("Enter a valid email address."))
        else:
            user.first_name = form_data['first_name'] or None
            user.last_name = form_data['last_name'] or None
            user.position = form_data['position'] or None
            user.email = form_data['email'] or None
            try:
                db.session.commit()
                session.update({
                    'first_name': user.first_name,
                    'last_name': user.last_name,
                    'position': user.position,
                    'email': user.email,
                    'display_name': user.full_name,
                })
                flash_success(_("Profile updated successfully."))
                return redirect(url_for('auth.profile'))
            except Exception as exc:
                db.session.rollback()
                logger.exception("Could not update profile for user %s: %s", user.username, exc)
                flash_err(_("Could not update your profile."))

    return render_template('auth/profile.html', user=user, form_data=form_data)


@bp_auth.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    """
    Allows an authenticated user to change their own password.
    Requires validation of the current password before applying changes.
    """
    user = db.session.get(User, session.get('user_id'))
    if not user:
        return redirect(url_for('auth.login'))

    if request.method == 'POST':
        current_pwd = request.form.get('current_password')
        new_pwd = request.form.get('new_password')
        confirm_pwd = request.form.get('confirm_password')

        if not user.check_password(current_pwd):
            flash_err(_("Current password is incorrect."))
        elif new_pwd != confirm_pwd:
            flash_err(_("New passwords do not match."))
        elif not new_pwd or len(new_pwd) < 8:
            flash_err(_("New password too short (min 8 chars)."))
        else:
            user.set_password(new_pwd)
            db.session.commit()
            flash_success(_("Password updated successfully."))
            return redirect(url_for('main.index'))

    return render_template('auth/change_password.html', user=user)


@bp_auth.route('/am-settings', methods=['GET', 'POST'])
@login_required
def am_settings():
    """
    Configuration page for the ORCID Affiliation Manager (AM).
    Allows managers to set their specific 'APP-ID' for institutional integrations.
    """
    user = db.session.get(User, session.get('user_id'))
    if not user:
        return redirect(url_for('auth.login'))

    if request.method == 'POST':
        am_id = (request.form.get('am_client_id') or '').strip()
        
        # Validation: AM Client IDs typically start with 'APP-'
        if am_id and not am_id.startswith('APP-'):
            flash_err(_("The Affiliation Manager ID must start with 'APP-'."))
        else:
            user.am_client_id = am_id if am_id else None
            try:
                db.session.commit()
                flash_success(_("Affiliation Manager identifier updated."))
            except Exception as exc:
                db.session.rollback()
                logger.exception("Error saving AM ID for user %s: %s", user.username, exc)
                flash_err(_("Internal database error while saving settings."))

    return render_template('auth/am_settings.html', user=user)
