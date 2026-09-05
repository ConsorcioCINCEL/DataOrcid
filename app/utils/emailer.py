"""SMTP email helper for transactional messages."""

import smtplib
import logging
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid
from typing import Tuple, Optional
from flask import current_app

logger = logging.getLogger(__name__)


def send_email(
    to_email: str,
    subject: str,
    html: str,
    text: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Send an HTML email with a plain-text fallback for transactional messages.

    Returns `(success, error_message)` so routes can show user-friendly feedback.
    """
    try:
        # The application factory normalizes TOML and environment variables to
        # Flask's conventional MAIL_* keys. Keep the nested mapping only as a
        # compatibility fallback for small embedding applications.
        mail_conf = current_app.config.get("mail", {})
        enabled = current_app.config.get("MAIL_ENABLED")
        if enabled is None:
            enabled = mail_conf.get("enabled", False)
        if not enabled:
            msg = "Email service is disabled in configuration (MAIL_ENABLED=False)."
            logger.info(msg)
            return False, msg

        host = current_app.config.get("MAIL_SERVER") or mail_conf.get("smtp_host")
        port = current_app.config.get("MAIL_PORT") or mail_conf.get("smtp_port")
        user = current_app.config.get("MAIL_USERNAME") or mail_conf.get("smtp_user")
        pwd = current_app.config.get("MAIL_PASSWORD") or mail_conf.get("smtp_pass")

        use_tls = current_app.config.get("MAIL_USE_TLS")
        if use_tls is None:
            use_tls = mail_conf.get("use_tls", True)
        use_ssl = current_app.config.get("MAIL_USE_SSL")
        if use_ssl is None:
            use_ssl = mail_conf.get("use_ssl", False)
        force_ipv4 = current_app.config.get("MAIL_FORCE_IPV4")
        if force_ipv4 is None:
            force_ipv4 = mail_conf.get("force_ipv4", False)

        sender = current_app.config.get("MAIL_DEFAULT_SENDER")
        if isinstance(sender, (tuple, list)):
            from_name = (sender[0] if sender else None) or "Data ORCID-Chile"
            from_email = sender[1] if len(sender) > 1 else None
        else:
            from_name = mail_conf.get("from_name", "Data ORCID-Chile")
            from_email = sender or mail_conf.get("from_email", "no-reply@example.com")

        if not all([host, port, user, pwd]):
            msg = "Missing required SMTP parameters: host, port, username, or password."
            logger.error(msg)
            return False, msg

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = formataddr((from_name, from_email))
        msg["To"] = to_email
        msg["Date"] = formatdate(localtime=False)
        msg["Message-ID"] = make_msgid()
        reply_to = reply_to or current_app.config.get("MAIL_REPLY_TO")
        if reply_to:
            msg["Reply-To"] = reply_to

        plain_text = text or "Please use an HTML-compatible email client to view this message."
        msg.set_content(plain_text)
        msg.add_alternative(html, subtype="html")

        # SMTP_SSL is for implicit SSL, while SMTP + starttls is for explicit TLS.
        smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        
        logger.debug("Connecting to SMTP server at %s:%s (SSL=%s)", host, port, use_ssl)

        connection_options = {"timeout": 20}
        if force_ipv4:
            # Binding the IPv4 wildcard makes socket.create_connection skip
            # IPv6 results while preserving normal DNS resolution and TLS SNI.
            connection_options["source_address"] = ("0.0.0.0", 0)

        with smtp_class(host, port, **connection_options) as server:
            if use_tls and not use_ssl:
                server.starttls()

            server.login(user, pwd)
            refused = server.send_message(msg)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
            
            logger.info("Email successfully dispatched to %s. Subject: '%s'", to_email, subject)

        return True, None

    except smtplib.SMTPAuthenticationError:
        err = "SMTP Authentication failed: Invalid username or password."
        logger.error(err)
        return False, err

    except (smtplib.SMTPConnectError, ConnectionRefusedError):
        err = "Network Error: Could not establish a connection to the SMTP server."
        logger.error(err)
        return False, err

    except smtplib.SMTPException as exc:
        err = f"SMTP Protocol Error: {exc}"
        logger.error(err)
        return False, err

    except Exception as exc:
        err = f"Unexpected error dispatching email: {exc}"
        logger.exception(err)
        return False, err
