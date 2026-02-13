"""
Module: flashes.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    UI Utility for managing Flask flash messages.
    
    This module provides a standardized interface for categorizing user feedback.
    It wraps Flask's native `flash` function to ensure consistent usage of 
    Bootstrap alert classes (success, danger, info, warning) across the application.
"""

from flask import flash
from typing import Literal

# --- Type Definitions ---
# Restricts the allowed categories to match Bootstrap 4/5 alert classes
FlashCategory = Literal["success", "danger", "info", "warning"]


def flash_msg(message: str, category: FlashCategory = "info") -> None:
    """
    Dispatches a generic flash message with a specified category.
    
    This is the base function used by other helpers, but can also be called directly
    when a dynamic category is needed.

    Args:
        message (str): The text content of the message to display.
        category (FlashCategory): The visual category. Options:
            - 'success': Green (Positive action)
            - 'danger': Red (Error or Critical failure)
            - 'info': Blue (Neutral information)
            - 'warning': Yellow (Cautionary notice)
            Defaults to 'info'.
    """
    flash(message, category)


def flash_ok(message: str) -> None:
    """
    Shortcut for dispatching a SUCCESS message.
    
    Note:
    - Maintained for compatibility with legacy controller logic.
    - Maps to Bootstrap class 'alert-success'.

    Args:
        message (str): The success message content.
    """
    flash(message, "success")


def flash_success(message: str) -> None:
    """
    Shortcut for dispatching a SUCCESS message.
    
    - Standard naming convention for new implementations.
    - Maps to Bootstrap class 'alert-success'.

    Args:
        message (str): The success message content.
    """
    flash(message, "success")


def flash_err(message: str) -> None:
    """
    Shortcut for dispatching an ERROR message.
    
    - Maps to Bootstrap class 'alert-danger' for high visibility.
    - Used for exceptions, validation failures, and critical errors.

    Args:
        message (str): The error message content.
    """
    flash(message, "danger")


def flash_info(message: str) -> None:
    """
    Shortcut for dispatching an INFORMATIONAL message.
    
    - Maps to Bootstrap class 'alert-info'.
    - Used for status updates or neutral notifications.

    Args:
        message (str): The info message content.
    """
    flash(message, "info")


def flash_warn(message: str) -> None:
    """
    Shortcut for dispatching a WARNING message.
    
    - Maps to Bootstrap class 'alert-warning'.
    - Used for non-critical issues or cautionary advice.

    Args:
        message (str): The warning message content.
    """
    flash(message, "warning")