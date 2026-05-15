"""Session helpers for institution-scoped views."""

from typing import Optional
from flask import session


def get_active_ror_id() -> Optional[str]:
    """
    Return the active ROR ID for the current session.

    Only admins can use the institution switcher override. Managers and standard
    users are always scoped to their own institution.
    """
    active_id = session.get("admin_selected_ror") if session.get("is_admin") else None

    if not active_id:
        active_id = session.get("ror_id")
        
    return active_id or None
