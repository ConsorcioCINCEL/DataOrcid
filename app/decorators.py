"""Route access-control decorators and small security helpers."""

import re
import secrets
import string
from functools import wraps
from flask import session, redirect, url_for, flash
from flask_babel import _

def _flash_err(msg: str) -> None:
    """Flash an error message without requiring eager utility imports."""
    try:
        from .utils.flashes import flash_err
        flash_err(msg)
    except ImportError:
        flash(msg, "danger")


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
    """Require an authenticated admin or manager session."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("user_id"):
            _flash_err(_("Your session has expired or you are not logged in."))
            return redirect(url_for("auth.login"))
            
        # Role Check: Is Admin OR Is Manager?
        if not (session.get("is_admin") or session.get("is_manager")):
            _flash_err(_("You do not have the required permissions to access this section."))
            return redirect(url_for("main.index"))
            
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
