"""Shared safeguards for values written to Excel workbooks."""

from __future__ import annotations

import re


EXCEL_CELL_TEXT_LIMIT = 32_767
_TRUNCATION_SUFFIX = "... [truncated for Excel]"
_INVALID_XML_CHARACTERS_RE = re.compile(
    r"[\x00-\x08\x0B\x0C\x0E-\x1F\uD800-\uDFFF\uFFFE\uFFFF]"
)


def excel_cell(value):
    """Return a value that OpenPyXL can safely serialize into one cell."""
    if not isinstance(value, str):
        return value
    safe_value = _INVALID_XML_CHARACTERS_RE.sub("", value)
    if len(safe_value) > EXCEL_CELL_TEXT_LIMIT:
        available = EXCEL_CELL_TEXT_LIMIT - len(_TRUNCATION_SUFFIX)
        safe_value = f"{safe_value[:available]}{_TRUNCATION_SUFFIX}"
    return safe_value


def excel_safe_dataframe(data_frame):
    """Copy a DataFrame and sanitize every text-bearing column for XLSX."""
    safe_frame = data_frame.copy()
    for column in safe_frame.select_dtypes(include=["object", "str"]).columns:
        safe_frame[column] = safe_frame[column].map(excel_cell)
    return safe_frame
