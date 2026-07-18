"""Shared DOI extraction and validation helpers."""

from __future__ import annotations

import re


DOI_PATTERN = re.compile(
    r"10\.\d{4,9}/[-._;()/:A-Z0-9]+",
    re.IGNORECASE,
)
MAX_DOI_LENGTH = 255


def normalize_doi(value: str | None) -> str | None:
    """Extract and normalize one valid DOI from an external identifier value.

    ORCID records occasionally label a URL or a complete citation as a DOI.
    Searching for the DOI pattern recovers an embedded DOI while rejecting
    unrelated values instead of persisting them as oversized identifiers.
    """
    text = str(value or "").strip()
    if not text:
        return None

    match = DOI_PATTERN.search(text)
    if not match:
        return None

    doi = match.group(0).lower().rstrip(".,;:")
    while doi.endswith(")") and doi.count(")") > doi.count("("):
        doi = doi[:-1]

    return doi if 0 < len(doi) <= MAX_DOI_LENGTH else None
