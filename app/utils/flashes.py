"""Small wrappers for Bootstrap-compatible Flask flash categories."""

from flask import flash
from typing import Literal

FlashCategory = Literal["success", "danger", "info", "warning"]


def flash_msg(message: str, category: FlashCategory = "info") -> None:
    """Flash a message with a Bootstrap alert category."""
    flash(message, category)


def flash_ok(message: str) -> None:
    """Flash a success message; kept for existing route code."""
    flash(message, "success")


def flash_success(message: str) -> None:
    """Flash a success message."""
    flash(message, "success")


def flash_err(message: str) -> None:
    """Flash an error message."""
    flash(message, "danger")


def flash_info(message: str) -> None:
    """Flash an informational message."""
    flash(message, "info")


def flash_warn(message: str) -> None:
    """Flash a warning message."""
    flash(message, "warning")
