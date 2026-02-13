"""
Module: auth.py
Description:
    Authentication Blueprint for DataOrcid-Chile.
    
    This module manages all aspects of user identity and access control:
    - Login/Logout session management.
    - Secure password recovery using timed cryptographic tokens.
    - User account settings, including password changes and Affiliation Manager configuration.
    
    Security Features:
    - Uses `itsdangerous` for tamper-proof token generation.
    - Enforces session clearing on logout.
    - Generic error messages to prevent username enumeration attacks.
"""

import logging
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

# --- Blueprint Configuration ---
bp_auth = Blueprint("auth", __name__, url_prefix="/auth")
logger = logging.getLogger(__name__)


# ============================================================
# INTERNAL HELPERS
# ============================================================

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


# ============================================================
# PASSWORD RECOVERY FLOW
# ============================================================

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
        identifier = (request.form.get('email') or '').strip()
        user = None

        if identifier:
            # Allow recovery via Email or Username
            user = User.query.filter(
                (User.email == identifier) | (User.username == identifier)
            ).first()

        if user:
            try:
                serializer = _get_serializer()
                # Embed user ID in the token payload
                token = serializer.dumps({'uid': user.id})
                reset_url = _get_absolute_url('auth.reset_password', token=token)

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
        user = User.query.get(data.get('uid'))
        
        if not user:
            flash_err(_("Invalid or non-existent user."))
            return redirect(url_for('auth.login'))
            
    except (SignatureExpired, BadSignature) as exc:
        msg = _("Token expired.") if isinstance(exc, SignatureExpired) else _("Invalid token.")
        flash_err(msg)
        return redirect(url_for('auth.login'))

    if request.method == 'POST':
        pwd = request.form.get('new_password')
        confirm = request.form.get('confirm_password')

        if len(pwd) < 6:
            flash_err(_("Password must be at least 6 characters."))
        elif pwd != confirm:
            flash_err(_("Passwords do not match."))
        else:
            user.set_password(pwd)
            db.session.commit()
            flash_success(_("Password updated successfully. Please log in."))
            return redirect(url_for('auth.login'))
            
        return redirect(url_for('auth.reset_password', token=token))

    return render_template('auth/reset_password.html', token=token)


# ============================================================
# SESSION MANAGEMENT
# ============================================================

@bp_auth.route('/login', methods=['GET', 'POST'])
def login():
    """
    Handles user authentication and session initialization.
    Redirects authenticated users to the dashboard or their previous page.
    """
    if session.get('logged_in'):
        return redirect(request.args.get('next') or url_for('main.index'))

    if request.method == 'POST':
        username = (request.form.get('username') or "").strip()
        password = request.form.get('password') or ""
        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            # Populate Session Data
            session.update({
                'user_id': user.id,
                'username': user.username,
                'first_name': user.first_name,
                'is_admin': bool(user.is_admin),
                'is_manager': bool(user.is_manager),
                'institution_name': user.institution_name,
                'ror_id': user.ror_id,
                'locale': user.locale or 'es', 
                'logged_in': True,
            })

            # Set initial context for admin multi-tenancy support
            if user.ror_id:
                session['admin_selected_ror'] = user.ror_id

            logger.info("User %s logged in successfully.", user.username)
            return redirect(request.args.get('next') or url_for('main.index'))

        flash_err(_("Invalid username or password."))

    return render_template('auth/login.html')


@bp_auth.route('/logout')
def logout():
    """
    Terminates the user session and redirects to login.
    """
    session.clear()
    flash_success(_("Session closed successfully."))
    return redirect(url_for('auth.login'))


# ============================================================
# ACCOUNT SETTINGS
# ============================================================

@bp_auth.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    """
    Allows an authenticated user to change their own password.
    Requires validation of the current password before applying changes.
    """
    user = User.query.get(session.get('user_id'))
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
        elif len(new_pwd) < 6:
            flash_err(_("New password too short (min 6 chars)."))
        else:
            user.set_password(new_pwd)
            db.session.commit()
            flash_success(_("Password updated successfully."))
            return redirect(url_for('main.index'))

    return render_template('auth/change_password.html')


@bp_auth.route('/am-settings', methods=['GET', 'POST'])
@login_required
def am_settings():
    """
    Configuration page for the ORCID Affiliation Manager (AM).
    Allows managers to set their specific 'APP-ID' for institutional integrations.
    """
    user = User.query.get(session.get('user_id'))
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
                flash_success(_("Affiliation Manager settings updated."))
            except Exception as exc:
                db.session.rollback()
                logger.exception("Error saving AM ID for user %s: %s", user.username, exc)
                flash_err(_("Internal database error while saving settings."))

    return render_template('auth/am_settings.html', user=user)