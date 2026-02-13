"""
Module: session_helpers.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Session Management Utilities.
    
    This module provides helper functions to abstract interaction with the Flask 
    session object. It centralizes the logic for retrieving the active institutional 
    context (ROR ID), ensuring consistency across all controllers.
    
    Key Features:
    - **Context Prioritization**: Handles administrative overrides ('impersonation') 
      seamlessly, allowing admins to manage different institutions without relogging.
"""

from typing import Optional
from flask import session


def get_active_ror_id() -> Optional[str]:
    """
    Retrieves the currently active Research Organization Registry (ROR) ID.

    This function determines the correct institutional context for the current request.
    It prioritizes administrative selection over the user's default assignment.

    Logic:
    1. **Check Administrative Override**: Look for `admin_selected_ror` in the session.
       This is set when a Global Admin or Manager explicitly selects an institution 
       from the dropdown menu to view its specific data.
    2. **Fallback to Default**: If no override exists, retrieve `ror_id` from the session.
       This represents the institution the logged-in user natively belongs to.

    Returns:
        Optional[str]: The active ROR ID string (e.g., '02ap3w078') if available, 
                       otherwise None.
    """
    # 1. Attempt to retrieve the administrative override (Context Switch)
    active_id = session.get("admin_selected_ror")
    
    # 2. Fallback to the user's default institutional assignment
    if not active_id:
        active_id = session.get("ror_id")
        
    return active_id or None