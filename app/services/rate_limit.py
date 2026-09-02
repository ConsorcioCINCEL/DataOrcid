"""Database-backed request throttling shared by every web worker."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import timedelta

from flask import request
from sqlalchemy import text

from .. import db
from ..models import AuthRateLimitEvent, utc_now

logger = logging.getLogger(__name__)
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_BUCKETS: dict[str, list[float]] = {}


def client_ip() -> str:
    """Return the client address after any explicitly configured ProxyFix."""
    return (request.remote_addr or "unknown")[:50]


def _rate_limit_key(action: str) -> tuple[str, str]:
    client_digest = hashlib.sha256(client_ip().encode("utf-8")).hexdigest()
    return action[:32], client_digest


def is_rate_limited(action: str, limit: int, window_seconds: int) -> bool:
    """Record an attempt and report whether its shared window is exhausted."""
    action_key, client_key = _rate_limit_key(action)
    cutoff_datetime = utc_now() - timedelta(seconds=window_seconds)
    try:
        if db.session.get_bind().dialect.name == "postgresql":
            db.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 2))"),
                {"key": f"{action_key}:{client_key}"},
            )
        AuthRateLimitEvent.query.filter_by(
            action=action_key,
            client_key=client_key,
        ).filter(AuthRateLimitEvent.occurred_at < cutoff_datetime).delete(
            synchronize_session=False
        )
        attempts = AuthRateLimitEvent.query.filter_by(
            action=action_key,
            client_key=client_key,
        ).filter(AuthRateLimitEvent.occurred_at >= cutoff_datetime).count()
        db.session.add(AuthRateLimitEvent(action=action_key, client_key=client_key))
        db.session.commit()
        return attempts >= limit
    except Exception as exc:
        db.session.rollback()
        logger.warning("Shared request rate limiter unavailable: %s", exc)

    now = time.time()
    cutoff = now - window_seconds
    key = f"{action_key}:{client_key}"
    with _RATE_LIMIT_LOCK:
        attempts = [ts for ts in _RATE_LIMIT_BUCKETS.get(key, []) if ts >= cutoff]
        blocked = len(attempts) >= limit
        attempts.append(now)
        _RATE_LIMIT_BUCKETS[key] = attempts
        return blocked


def clear_rate_limit(action: str) -> None:
    """Clear the current client's history after a successful protected action."""
    action_key, client_key = _rate_limit_key(action)
    try:
        AuthRateLimitEvent.query.filter_by(
            action=action_key,
            client_key=client_key,
        ).delete(synchronize_session=False)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        logger.warning("Could not clear shared request rate limit: %s", exc)

    key = f"{action_key}:{client_key}"
    with _RATE_LIMIT_LOCK:
        _RATE_LIMIT_BUCKETS.pop(key, None)
