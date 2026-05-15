"""SMTP email helper for transactional messages."""

import smtplib
import logging
from email.message import EmailMessage
from typing import Tuple, Optional
from flask import current_app

logger = logging.getLogger(__name__)


def send_email(
    to_email: str,
    subject: str,
    html: str,
    text: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Send an HTML email with a plain-text fallback.

    Returns `(success, error_message)` so routes can show user-friendly feedback.
    """
    try:
        mail_conf = current_app.config.get("mail", {})
        if not mail_conf.get("enabled", False):
            msg = "Email service is disabled in configuration (MAIL_ENABLED=False)."
            logger.info(msg)
            return False, msg

        host = mail_conf.get("smtp_host")
        port = mail_conf.get("smtp_port")
        user = mail_conf.get("smtp_user")
        pwd = mail_conf.get("smtp_pass")
        
        use_tls = mail_conf.get("use_tls", True)
        use_ssl = mail_conf.get("use_ssl", False)
        
        from_name = mail_conf.get("from_name", "Data ORCID-Chile")
        from_email = mail_conf.get("from_email", "no-reply@example.com")

        if not all([host, port, user, pwd]):
            msg = "Missing required SMTP parameters: host, port, username, or password."
            logger.error(msg)
            return False, msg

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = to_email

        plain_text = text or "Please use an HTML-compatible email client to view this message."
        msg.set_content(plain_text)
        msg.add_alternative(html, subtype="html")

        # SMTP_SSL is for implicit SSL, while SMTP + starttls is for explicit TLS.
        smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        
        logger.debug("Connecting to SMTP server at %s:%s (SSL=%s)", host, port, use_ssl)

        with smtp_class(host, port, timeout=20) as server:
            if use_tls and not use_ssl:
                server.starttls()

            server.login(user, pwd)
            server.send_message(msg)
            
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
