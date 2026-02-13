"""
Module: decorators.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Authentication & Authorization Decorators.
    
    This module centralizes the access control logic for the application.
    It provides decorators to protect routes based on user session state (RBAC),
    ensuring that only authenticated users with specific roles (Admin/Manager)
    can access sensitive endpoints.
    
    It also includes utility functions for data normalization (ROR IDs) and 
    secure password generation.
"""

import re
import secrets
import string
from functools import wraps
from flask import session, redirect, url_for, flash
from flask_babel import _

# ============================================================
# INTERNAL UTILITIES
# ============================================================

def _flash_err(msg: str) -> None:
    """
    Internal helper to dispatch error messages.
    
    It attempts to import the specialized `flash_err` utility from the 
    `utils` package. If that fails (e.g., due to circular imports), 
    it falls back to Flask's standard `flash` function.

    Args:
        msg (str): The error message content.
    """
    try:
        # Lazy import to avoid circular dependencies during module initialization
        from .utils.flashes import flash_err
        flash_err(msg)
    except ImportError:
        # Fallback for standalone testing or import failures
        flash(msg, "danger")


# ============================================================
# AUTHENTICATION & ACCESS CONTROL DECORATORS
# ============================================================

def login_required(f):
    """
    Route Decorator: Login Required.
    
    Restricts access to authenticated users only.
    Checks for the presence of the 'logged_in' flag in the user session.
    If missing, redirects the user to the login page with a warning.
    
    Usage:
        @bp.route('/dashboard')
        @login_required
        def dashboard(): ...
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            flash(_("You must log in to access this page."), "warning")
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """
    Route Decorator: Admin Privileges Required.
    
    Restricts access strictly to System Administrators.
    Verifies that the current user has the 'is_admin' session flag set to True.
    
    Usage:
        @bp.route('/users/delete/<id>')
        @admin_required
        def delete_user(id): ...
    """
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
    """
    Route Decorator: Staff Access (Admin OR Manager).
    
    Allows access to users who hold either an Administrator or a Manager role.
    This is commonly used for institutional management dashboards where 
    both roles share oversight capabilities.
    """
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
    """
    Route Decorator: Admin OR Manager Required (Alternative).
    
    Functionally identical to `staff_required`, kept for compatibility 
    with different blueprint naming conventions.
    """
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not (session.get("is_admin") or session.get("is_manager")):
            flash(_("This action requires administrator or manager permissions."), "danger")
            return redirect(url_for("main.index"))
        return f(*args, **kwargs)
    return wrapped


# ============================================================
# IDENTIFIER & SECURITY UTILITIES
# ============================================================

# Regex for validating ROR ID structure (alphanumeric, 4-11 chars, case-insensitive)
ROR_ID_RE = re.compile(r"[a-z0-9]{4,11}", re.I)

def normalize_ror_id(raw: str) -> str:
    """
    Normalizes a ROR identifier string to extract its unique leaf ID.

    This function handles various input formats such as:
    - Full URLs: 'https://ror.org/02ap3w078'
    - Short URLs: 'ror.org/02ap3w078'
    - Raw IDs: '02ap3w078'
    
    It strips whitespace and validates the suffix against the ROR regex pattern.

    Args:
        raw (str): The raw input string.

    Returns:
        str: The normalized, lowercase ROR ID (e.g., '02ap3w078'), or empty string if invalid.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    
    # Logic: Split by slash to handle URLs and take the last segment
    suffix = raw.split("/")[-1].strip().lower()
    
    # Validate against regex to ensure it looks like a valid ID
    match = ROR_ID_RE.search(suffix)
    
    return match.group(0).lower() if match else ""


def generate_password(length: int = 12) -> str:
    """
    Generates a cryptographically secure random password.
    
    Uses the `secrets` module (PEP 506) instead of `random` to ensure 
    unpredictability, making it suitable for temporary user credentials.

    Args:
        length (int): The length of the password. Defaults to 12.

    Returns:
        str: A random alphanumeric string.
    """
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))