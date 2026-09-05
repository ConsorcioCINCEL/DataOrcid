"""Validate repository references for institution-owned private harvesting URLs."""

from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit, urlunsplit

from flask_babel import _


MAX_HARVESTERS = 20


def normalize_harvester_uri(value: str) -> str:
    """Return an HTTP(S) repository reference URI, never a source of trust."""
    value = (value or "").strip()
    error = _("Enter an HTTP or HTTPS repository URI without credentials, query parameters, or a fragment.")
    if not value or len(value) > 2048 or any(char.isspace() or ord(char) < 32 for char in value) or "\\" in value:
        raise ValueError(error)
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(error)
        if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            raise ValueError(error)
        hostname = parsed.hostname.rstrip(".").encode("idna").decode("ascii").lower()
        port = parsed.port
        try:
            address = ipaddress.ip_address(hostname)
            host = f"[{address}]" if address.version == 6 else str(address)
        except ValueError:
            if len(hostname) > 253 or any(
                not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                for label in hostname.split(".")
            ):
                raise ValueError(error)
            host = hostname
        if port is not None and port not in ({80} if parsed.scheme == "http" else {443}):
            if port == 0:
                raise ValueError(error)
            host = f"{host}:{port}"
        normalized = urlunsplit((parsed.scheme, host, parsed.path.rstrip("/"), "", ""))
        if len(normalized.encode("utf-8")) > 2048:
            raise ValueError(error)
        return normalized
    except (ValueError, UnicodeError):
        raise ValueError(error) from None



def redact_oai_urls(value: str) -> str:
    """Keep OAI URL credentials out of application activity and error logs."""
    return re.sub(r"(/oai/)[^\s/?#\"'<>]+(?:/[^\s/?#\"'<>]+)?", r"\1[REDACTED]", value)
