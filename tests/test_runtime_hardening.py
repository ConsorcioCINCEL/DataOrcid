"""Regression tests for application startup and shared authentication safety."""

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask
from sqlalchemy import inspect

from app import create_app, db
from app.blueprints.auth import _client_ip, _is_rate_limited
from app.models import AuthRateLimitEvent
from app.utils.emailer import send_email


class RuntimeHardeningTest(unittest.TestCase):
    def test_factory_does_not_create_schema_during_web_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "empty.sqlite"
            app = create_app({
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{database_path}",
            })
            with app.app_context():
                self.assertEqual([], inspect(db.engine).get_table_names())

    def test_insecure_production_configuration_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Unsafe runtime configuration"):
            create_app({
                "TESTING": False,
                "SQLALCHEMY_DATABASE_URI": "sqlite://",
                "APP_ENVIRONMENT": "production",
                "ALLOW_INSECURE_DEV_CONFIG": False,
                "SECRET_KEY": "s" * 40,
                "SECURITY_PASSWORD_SALT": "p" * 20,
                "SESSION_COOKIE_SECURE": False,
                "APP_BASE_URL": "http://example.test",
            })

    def test_development_bypass_cannot_disable_production_validation(self):
        with self.assertRaisesRegex(RuntimeError, "Unsafe runtime configuration"):
            create_app({
                "TESTING": False,
                "SQLALCHEMY_DATABASE_URI": "sqlite://",
                "APP_ENVIRONMENT": "production",
                "ALLOW_INSECURE_DEV_CONFIG": True,
                "SECRET_KEY": "CHANGEME_IN_RUNTIME",
                "SECURITY_PASSWORD_SALT": "CHANGE_ME_SALT",
                "SESSION_COOKIE_SECURE": False,
                "APP_BASE_URL": "http://example.test",
            })

    def test_emailer_uses_normalized_mail_configuration(self):
        app = Flask(__name__)
        app.config.update(
            MAIL_ENABLED=True,
            MAIL_SERVER="smtp.example.test",
            MAIL_PORT=2525,
            MAIL_USE_TLS=True,
            MAIL_USE_SSL=False,
            MAIL_FORCE_IPV4=True,
            MAIL_USERNAME="mailer@example.test",
            MAIL_PASSWORD="environment-secret",
            MAIL_DEFAULT_SENDER=("Data ORCID-Chile", "no-reply@example.test"),
            mail={"smtp_pass": "stale-toml-secret"},
        )
        smtp = MagicMock()
        smtp.__enter__.return_value = smtp
        smtp.send_message.return_value = {}
        with app.app_context(), patch(
            "app.utils.emailer.smtplib.SMTP", return_value=smtp
        ) as smtp_class:
            success, error = send_email(
                "user@example.test",
                "Subject",
                "<p>Body</p>",
                reply_to="visitor@example.test",
            )

        self.assertTrue(success)
        self.assertIsNone(error)
        smtp_class.assert_called_once_with(
            "smtp.example.test",
            2525,
            timeout=20,
            source_address=("0.0.0.0", 0),
        )
        smtp.login.assert_called_once_with("mailer@example.test", "environment-secret")
        message = smtp.send_message.call_args.args[0]
        self.assertEqual("visitor@example.test", message["Reply-To"])

    def test_rate_limit_is_shared_in_the_database_and_ignores_raw_forwarding(self):
        app = Flask(__name__)
        app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
        )
        db.init_app(app)
        with app.app_context():
            db.create_all()
            with app.test_request_context(
                "/auth/login",
                headers={"X-Forwarded-For": "203.0.113.50"},
                environ_base={"REMOTE_ADDR": "127.0.0.1"},
            ):
                self.assertEqual("127.0.0.1", _client_ip())
                self.assertFalse(_is_rate_limited("login", 1, 900))
                self.assertTrue(_is_rate_limited("login", 1, 900))

            rows = AuthRateLimitEvent.query.all()
            self.assertEqual(2, len(rows))
            self.assertEqual(
                hashlib.sha256(b"127.0.0.1").hexdigest(),
                rows[0].client_key,
            )
            db.session.remove()
            db.drop_all()
            db.engine.dispose()


if __name__ == "__main__":
    unittest.main()
