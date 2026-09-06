"""Durable account-email delivery with retries and password-bound access links."""

from datetime import timedelta
import hashlib
import logging
import uuid

from flask import current_app, request, url_for
from sqlalchemy import and_, or_

from .. import db
from ..models import EmailOutbox, User, utc_now
from ..utils.emailer import send_email

logger = logging.getLogger(__name__)


def password_fingerprint(user):
    return hashlib.sha256(user.password_hash.encode()).hexdigest()


def queue_account_email(user, *, kind="access"):
    """Record an intent in the caller's transaction without changing credentials."""
    if kind not in {"welcome", "access", "password_reset"}:
        raise ValueError("Unsupported account email kind.")
    if "@" not in (user.email or user.username or ""):
        raise ValueError("The account has no valid email recipient.")
    db.session.flush()
    User.query.filter_by(id=user.id).with_for_update().first()
    fingerprint = password_fingerprint(user)
    existing = EmailOutbox.query.filter(
        EmailOutbox.user_id == user.id, EmailOutbox.kind == kind,
        EmailOutbox.password_fingerprint == fingerprint,
        EmailOutbox.status.in_(("pending", "sending")),
    ).first()
    if existing:
        return existing.id
    base = (current_app.config.get("APP_BASE_URL") or request.url_root).rstrip("/")
    message = EmailOutbox(
        id=str(uuid.uuid4()), user_id=user.id, kind=kind, base_url=base,
        password_fingerprint=fingerprint, status="pending",
    )
    db.session.add(message)
    return message.id


def deliver_next_email():
    """Claim one due intent; SMTP runs after the account transaction is committed.

    A stable Message-ID assists recipient deduplication if SMTP accepts a message
    but the worker stops before recording delivery. SMTP cannot promise exactly
    once delivery, so retries retain the same intent and password-bound link.
    """
    now = utc_now()
    query = EmailOutbox.query.filter(or_(
        and_(EmailOutbox.status == "pending", EmailOutbox.available_at <= now),
        and_(EmailOutbox.status == "sending", EmailOutbox.claimed_at < now - timedelta(minutes=5)),
    )).order_by(EmailOutbox.created_at).with_for_update(skip_locked=True)
    item = query.first()
    if not item:
        db.session.rollback()
        return None
    if item.attempts >= 5:
        item.status = "failed"
        item.error = "Email delivery was interrupted too many times. Queue a new access link."
        item.claim_token = None
        db.session.commit()
        return item.id
    claim = str(uuid.uuid4())
    item.status = "sending"
    item.claimed_at = now
    item.claim_token = claim
    item.attempts += 1
    item_id = item.id
    db.session.commit()
    try:
        from ..blueprints.auth import make_password_reset_token
        from .transactional_email import render_access_email, render_password_reset_email

        user = db.session.get(User, item.user_id)
        if not user or password_fingerprint(user) != item.password_fingerprint:
            item.status = "cancelled"
            item.claim_token = None
            db.session.commit()
            return item_id
        # Give URL builders a trusted request base inside the supervised worker.
        with current_app.test_request_context(base_url=item.base_url + "/"):
            token = make_password_reset_token(user)
            reset_url = item.base_url + url_for("auth.reset_password", token=token)
            if item.kind == "password_reset":
                rendered = render_password_reset_email(user, reset_url)
            else:
                rendered = render_access_email(user, reset_url, welcome=item.kind == "welcome")
        recipient = user.email or user.username
        db.session.rollback()
        success, error = send_email(to_email=recipient, message_id=f"<{item_id}@dataorcid.local>", **rendered)
    except Exception:
        db.session.rollback()
        logger.exception("Could not deliver account email %s.", item_id)
        success, error = False, "Could not prepare or deliver the account email."
    item = EmailOutbox.query.filter_by(id=item_id, claim_token=claim).first()
    if not item:
        return item_id
    item.status = "sent" if success else ("failed" if item.attempts >= 5 else "pending")
    item.sent_at = utc_now() if success else None
    item.error = None if success else error
    item.claim_token = None
    item.available_at = utc_now() + timedelta(seconds=min(3600, 60 * 2 ** item.attempts))
    db.session.commit()
    return item_id


def wake_email_delivery():
    """Best-effort local wake-up; the supervised worker also drains durable intents."""
    if current_app.config.get("TESTING") or current_app.config.get("JOB_EXECUTION_MODE") == "queue":
        return
    from .background_jobs import _EXECUTOR
    app = current_app._get_current_object()

    def deliver():
        with app.app_context():
            try:
                deliver_next_email()
            finally:
                db.session.remove()
    _EXECUTOR.submit(deliver)
