"""Persist sanitized application errors for administrator diagnostics."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from pathlib import Path
from threading import local
import traceback as traceback_module

from flask import current_app, g, has_app_context, has_request_context, request, session
from werkzeug.exceptions import HTTPException

from .. import db
from ..models import SystemError, utc_now
from .oai_access import redact_oai_urls


_STATE = local()
_MAX_MESSAGE_LENGTH = 8_000
_MAX_TRACEBACK_LENGTH = 32_000
_ALLOWED_CONTEXT_KEYS = {"job_name", "handler"}
_CREDENTIAL_URL_RE = re.compile(
    r"(?i)([a-z][a-z0-9+.-]*://[^:/\s]+:)[^@\s]+@"
)
_QUERY_STRING_RE = re.compile(r"((?:https?://|/)[^\s?#]+)\?[^\s#]+", re.IGNORECASE)
_AUTHORIZATION_RE = re.compile(
    r"(?i)(\bAuthorization\b\s*[=:]\s*)(?:(?:Bearer|Basic)\s+)?"
    r"[A-Za-z0-9._~+/=-]+"
)
_SENSITIVE_VALUE_RE = re.compile(
    r"(?i)([\"']?\b(?:password|passwd|secret|token|api[_-]?key|"
    r"client[_-]?secret)\b[\"']?)(\s*[=:]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,;&]+)"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")


def _redact(value: object, limit: int) -> str:
    """Remove common credential forms and cap stored diagnostic text."""
    text = redact_oai_urls(str(value or ""))
    text = _CREDENTIAL_URL_RE.sub(r"\1[REDACTED]@", text)
    text = _QUERY_STRING_RE.sub(r"\1?[REDACTED]", text)
    text = _AUTHORIZATION_RE.sub(r"\1[REDACTED]", text)
    text = _BEARER_RE.sub("Bearer [REDACTED]", text)
    text = _SENSITIVE_VALUE_RE.sub(r"\1\2[REDACTED]", text)
    return text[:limit]


def _request_context() -> dict:
    """Return a strict allowlist of request metadata; never include payloads."""
    if not has_request_context():
        return {}
    try:
        role = (
            "admin"
            if session.get("is_admin")
            else "manager"
            if session.get("is_manager")
            else "oai_user"
            if session.get("is_oai_user")
            else "user"
            if session.get("logged_in")
            else "anonymous"
        )
        return {
            "source": "request",
            "user_id": session.get("user_id"),
            "username": session.get("username"),
            "institution_ror": (
                session.get("admin_selected_ror")
                if session.get("is_admin")
                else session.get("ror_id")
            ),
            "role": role,
            "endpoint": request.endpoint,
            "method": request.method,
            # Deliberately omit query strings, form bodies, cookies and headers.
            "path": redact_oai_urls(request.path),
            "request_id": getattr(g, "request_id", None),
            "ip": request.remote_addr,
            "user_agent": request.user_agent.string or "",
        }
    except Exception:
        return {"source": "request"}


def _exception_details(record: logging.LogRecord) -> tuple[str | None, str | None, list]:
    exc_info = record.exc_info
    if not exc_info or not exc_info[0]:
        stack = _redact(record.stack_info, _MAX_TRACEBACK_LENGTH) if record.stack_info else None
        return None, stack, []

    exception_class, exception, traceback_value = exc_info
    exception_type = f"{exception_class.__module__}.{exception_class.__name__}"
    rendered = "".join(
        traceback_module.format_exception(exception_class, exception, traceback_value)
    )
    frames = [
        (Path(frame.filename).name, frame.name, frame.lineno)
        for frame in traceback_module.extract_tb(traceback_value)[-8:]
    ]
    return exception_type[:255], _redact(rendered, _MAX_TRACEBACK_LENGTH), frames


def _fingerprint(
    record: logging.LogRecord,
    exception_type: str | None,
    frames: list,
    message: str,
) -> str:
    signature = {
        "logger": record.name,
        "exception": exception_type,
        "frames": frames,
        "message": "" if frames else message[:500],
    }
    encoded = json.dumps(signature, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class SystemErrorLogHandler(logging.Handler):
    """Database-backed ERROR handler that operates only inside an app context."""

    _is_system_error_handler = True

    def __init__(self):
        super().__init__(level=logging.ERROR)

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(_STATE, "recording", False) or not has_app_context():
            return
        app = current_app._get_current_object()
        if not app.config.get("ERROR_MONITORING_ENABLED", True):
            return
        if app.config.get("TESTING") and not app.config.get(
            "ERROR_MONITORING_CAPTURE_IN_TESTS", False
        ):
            return

        _STATE.recording = True
        try:
            request_context = _request_context()
            supplied_context = getattr(record, "system_error_context", None)
            supplied_context = supplied_context if isinstance(supplied_context, dict) else {}
            context = {**request_context, **supplied_context}

            message = _redact(record.getMessage(), _MAX_MESSAGE_LENGTH)
            exception_type, rendered_traceback, frames = _exception_details(record)
            exception = record.exc_info[1] if record.exc_info else None
            status_code = context.get("status_code")
            if status_code is None and isinstance(exception, HTTPException):
                status_code = exception.code
            elif status_code is None and exception is not None and has_request_context():
                status_code = 500

            source = str(context.get("source") or (
                "request" if has_request_context() else "application"
            ))[:32]
            if source not in {"request", "background_job", "application"}:
                source = "application"
            public_context = {
                key: _redact(value, 500)
                for key, value in context.items()
                if key in _ALLOWED_CONTEXT_KEYS and value not in (None, "")
            }
            values = {
                "event_id": secrets.token_hex(16),
                "fingerprint": _fingerprint(
                    record, exception_type, frames, message
                ),
                "source": source,
                "severity": record.levelname[:16],
                "logger_name": (record.name or "")[:160] or None,
                "exception_type": exception_type,
                "message": message or "Application error",
                "traceback": rendered_traceback,
                "user_id": context.get("user_id"),
                "username": str(context.get("username") or "")[:80] or None,
                "institution_ror": str(context.get("institution_ror") or "")[:32] or None,
                "role": str(context.get("role") or "")[:24] or None,
                "endpoint": str(context.get("endpoint") or "")[:160] or None,
                "method": str(context.get("method") or "")[:10] or None,
                "path": str(context.get("path") or "")[:500] or None,
                "status_code": status_code,
                "request_id": str(context.get("request_id") or "")[:36] or None,
                "job_id": str(context.get("job_id") or "")[:36] or None,
                "ip": str(context.get("ip") or "")[:50] or None,
                "user_agent": str(context.get("user_agent") or "")[:255] or None,
                "process_id": record.process,
                "thread_name": (record.threadName or "")[:80] or None,
                "context_json": public_context or None,
                "occurred_at": utc_now(),
                "is_resolved": False,
            }
            with db.engine.begin() as connection:
                connection.execute(SystemError.__table__.insert().values(**values))
        except Exception:
            # Error monitoring must never hide or replace the original failure.
            self.handleError(record)
        finally:
            _STATE.recording = False


def install_error_monitoring(app) -> None:
    """Install one package-level handler shared safely by app factories."""
    package_logger = logging.getLogger("app")
    if not any(
        getattr(handler, "_is_system_error_handler", False)
        for handler in package_logger.handlers
    ):
        package_logger.addHandler(SystemErrorLogHandler())
