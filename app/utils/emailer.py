"""
Module: emailer.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Email Dispatch Utility.
    
    This module provides a robust wrapper around Python's `smtplib` to send 
    transactional emails (e.g., password resets, notifications) from the application.
    
    Key Features:
    - Supports both SSL and STARTTLS security protocols.
    - Sends multi-part messages (HTML + Plain Text fallback) for better compatibility.
    - Centralized configuration via Flask app config variables (MAIL_*).
    - Detailed error logging for diagnosing SMTP connection issues.
"""

import smtplib
import logging
from email.message import EmailMessage
from typing import Tuple, Optional
from flask import current_app

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


def send_email(
    to_email: str,
    subject: str,
    html: str,
    text: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Sends a secure email using the configured SMTP server.

    This function handles the complexities of SMTP connections, including:
    1. Checking if mail is globally enabled via config.
    2. Validating required credentials (Host, Port, User, Password).
    3. Constructing a MIME multi-part message (HTML body with Text alternative).
    4. Managing the connection lifecycle (Open, TLS Handshake, Login, Send, Close).

    Args:
        to_email (str): Recipient email address.
        subject (str): Email subject line.
        html (str): The HTML content of the email body.
        text (Optional[str]): Plain text version for non-HTML clients. 
                              Defaults to a generic fallback message if None.

    Returns:
        Tuple[bool, Optional[str]]:
            - (True, None) if the email was sent successfully.
            - (False, error_message) if any error occurred.
    """
    try:
        # 1. Feature Flag Check
        if not current_app.config.get("MAIL_ENABLED", False):
            msg = "Email service is disabled in configuration (MAIL_ENABLED=False)."
            logger.info(msg)
            return False, msg

        # 2. Configuration Extraction
        conf = current_app.config
        host = conf.get("MAIL_HOST")
        port = conf.get("MAIL_PORT")
        user = conf.get("MAIL_USERNAME")
        pwd = conf.get("MAIL_PASSWORD")
        
        # Security defaults: TLS is preferred for modern SMTP (port 587)
        use_tls = conf.get("MAIL_USE_TLS", True)
        use_ssl = conf.get("MAIL_USE_SSL", False)
        
        from_name = conf.get("MAIL_FROM_NAME", "Data ORCID-Chile")
        from_email = conf.get("MAIL_FROM_EMAIL", "no-reply@example.com")

        # 3. Validation
        if not all([host, port, user, pwd]):
            msg = "Missing required SMTP parameters: host, port, username, or password."
            logger.error(msg)
            return False, msg

        # 4. Message Construction
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = to_email

        # Set content: Text is primary, HTML is added as an alternative view
        plain_text = text or "Please use an HTML-compatible email client to view this message."
        msg.set_content(plain_text)
        msg.add_alternative(html, subtype="html")

        # 5. Connection Strategy
        # Use SMTP_SSL for implicit SSL (usually port 465)
        # Use SMTP for explicit TLS (usually port 587)
        smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        
        logger.debug("Connecting to SMTP server at %s:%s (SSL=%s)", host, port, use_ssl)

        # 6. Transmission
        with smtp_class(host, port, timeout=20) as server:
            # Upgrade insecure connection to TLS if requested and not already SSL
            if use_tls and not use_ssl:
                server.starttls()
            
            # Authenticate
            server.login(user, pwd)
            
            # Dispatch
            server.send_message(msg)
            
            logger.info("Email successfully dispatched to %s. Subject: '%s'", to_email, subject)

        return True, None

    # 7. Error Handling
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