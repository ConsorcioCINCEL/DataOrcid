"""Route access-control decorators and small security helpers."""

import re
import secrets
import string
from functools import wraps
from flask import flash, g, redirect, session, url_for
from flask_babel import _


def _flash_err(msg: str) -> None:
    """Flash an error message without requiring eager utility imports."""
    try:
        from .utils.flashes import flash_err
        flash_err(msg)
    except ImportError:
        flash(msg, "danger")


def _authenticated_account():
    """Return the database account for the current authenticated session."""
    user_id = session.get("user_id")
    if not session.get("logged_in") or not user_id:
        return None

    from . import db
    from .models import User

    account = db.session.get(User, user_id)
    if account is None:
        session.clear()
        return None

    session["ror_id"] = account.ror_id
    g.current_account = account
    return account


def _effective_account_roles(account) -> tuple[bool, bool]:
    """
    Resolve privileges shared by the authenticated session and current account.

    Database revocations take effect immediately. Role grants require a new
    login so an existing session cannot gain privileges without reauthentication.
    """
    is_admin = bool(account.is_admin and session.get("is_admin"))
    is_manager = bool(account.is_manager and session.get("is_manager"))
    session["is_admin"] = is_admin
    session["is_manager"] = is_manager
    return is_admin, is_manager


def login_required(f):
    """Require an authenticated session."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            flash(_("You must log in to access this page."), "warning")
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Require an authenticated admin session."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            flash(_("You must log in to access this page."), "warning")
            return redirect(url_for("auth.login"))
        
        if not session.get("is_admin"):
            flash(_("Access restricted to administrators."), "danger")
            return redirect(url_for("main.index"))
            
        return f(*args, **kwargs)
    return wrapper


def staff_required(f):
    """Require a current database account with admin or manager privileges."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        account = _authenticated_account()
        if account is None:
            _flash_err(_("Your session has expired or you are not logged in."))
            return redirect(url_for("auth.login"))

        is_admin, is_manager = _effective_account_roles(account)
        if not (is_admin or is_manager):
            _flash_err(_("You do not have the required permissions to access this section."))
            return redirect(url_for("main.index"))

        return f(*args, **kwargs)
    return decorated_function


def institution_required(f):
    """
    Require an authenticated account and expose its authorized institution.

    Administrators may use their validated institution-switcher selection.
    Managers and standard users are always bound to the institution currently
    assigned to their database account, even when their session contains stale
    institutional context.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        account = _authenticated_account()
        if account is None:
            _flash_err(_("Your session has expired or you are not logged in."))
            return redirect(url_for("auth.login"))

        is_admin = _effective_account_roles(account)[0]
        if is_admin:
            from .utils.session_helpers import get_active_ror_id

            ror_id = normalize_ror_id(get_active_ror_id())
        else:
            session.pop("admin_selected_ror", None)
            ror_id = normalize_ror_id(account.ror_id)
            session["ror_id"] = ror_id or None

        if not ror_id:
            _flash_err(_("No active institution context found."))
            return redirect(url_for("main.index"))

        g.institution_ror_id = ror_id
        return f(*args, **kwargs)

    return decorated_function


def admin_or_manager_required(f):
    """Compatibility alias for routes that require admin or manager access."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not (session.get("is_admin") or session.get("is_manager")):
            flash(_("This action requires administrator or manager permissions."), "danger")
            return redirect(url_for("main.index"))
        return f(*args, **kwargs)
    return wrapped


ROR_ID_RE = re.compile(r"[a-z0-9]{4,11}", re.I)

def normalize_ror_id(raw: str) -> str:
    """
    Extract a normalized ROR suffix from a full URL or raw identifier.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    
    suffix = raw.split("/")[-1].strip().lower()
    match = ROR_ID_RE.search(suffix)
    
    return match.group(0).lower() if match else ""


def generate_password(length: int = 12) -> str:
    """Generate a cryptographically secure temporary alphanumeric password."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))
