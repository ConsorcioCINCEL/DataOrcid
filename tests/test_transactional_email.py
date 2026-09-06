"""Verify localized messages, public PDF links, escaping, and SMTP failures."""

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from app import create_app, db
from app.models import SystemModule
from app.services.transactional_email import (
    render_contact_email, render_access_email, render_password_reset_email,
)
from app.utils.emailer import send_email


class TransactionalEmailTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app({
            "TESTING": True, "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "MAIL_ENABLED": True, "MAIL_SERVER": "smtp.example.test", "MAIL_PORT": 587,
            "MAIL_USERNAME": "test-sender", "MAIL_PASSWORD": "test-password",
            "MAIL_DEFAULT_SENDER": ("Data ORCID-Chile", "sender@example.test"),
            "MAIL_REPLY_TO": "support@example.test", "MAIL_USE_TLS": True, "MAIL_USE_SSL": False,
        })
        self.user = SimpleNamespace(
            username="researcher@example.test", first_name="Ana", locale="es",
            institution_name="Demonstration University",
        )
        with self.app.app_context():
            SystemModule.__table__.create(db.engine)

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            SystemModule.__table__.drop(engine)
            engine.dispose()

    def test_spanish_welcome_contains_public_pdf_link_and_both_mime_bodies(self):
        smtp = MagicMock()
        smtp.__enter__.return_value = smtp
        smtp.send_message.return_value = {}
        with self.app.test_request_context("/?lang=en"), patch("app.utils.emailer.smtplib.SMTP", return_value=smtp):
            rendered = render_access_email(self.user, "https://dataorcid.example/auth/reset-password/test-only", welcome=True)
            success, error = send_email(to_email="recipient@example.test", **rendered)
        self.assertTrue(success, error)
        self.assertEqual("Bienvenido a Data ORCID-Chile", rendered["subject"])
        message = smtp.send_message.call_args.args[0]
        self.assertEqual("multipart/alternative", message.get_content_type())
        self.assertIn("test-only", message.get_body(preferencelist=("plain",)).get_content())
        self.assertIn('lang="es"', message.get_body(preferencelist=("html",)).get_content())
        self.assertEqual([], list(message.iter_attachments()))
        self.assertIn("http://localhost/manuals/user-guide/es.pdf", rendered["text"])
        self.assertIn('href="http://localhost/manuals/user-guide/es.pdf"', rendered["html"])
        self.assertEqual("support@example.test", message["Reply-To"])
        self.assertIsNotNone(message["Date"])
        self.assertIsNotNone(message["Message-ID"])

    def test_manual_and_message_follow_account_language_including_regional_locales(self):
        with self.app.test_request_context("/?lang=es"):
            for language, expected in (("en", "en"), ("es", "es"), ("fr", "fr"),
                                       ("pt", "pt"), ("de", "de"), ("pt_BR", "pt"),
                                       ("fr-CA", "fr"), ("it", "en")):
                with self.subTest(language=language):
                    self.user.locale = language
                    rendered = render_access_email(self.user, "https://example.test/reset-password/test-only")
                    self.assertIn(f'lang="{expected}"', rendered["html"])
                    self.assertIn(f"/manuals/user-guide/{expected}.pdf", rendered["text"])
                    self.assertIn(f'/manuals/user-guide/{expected}.pdf"', rendered["html"])
                    manual_labels = {"en": "User manual (English)", "es": "Manual de usuario (español)",
                                     "fr": "Manuel d’utilisation (français)", "pt": "Manual do usuário (português)",
                                     "de": "Benutzerhandbuch (Deutsch)"}
                    self.assertIn(manual_labels[expected], rendered["text"])
                    self.assertNotIn("attachments", rendered)

    def test_missing_manual_does_not_send_a_broken_download_link(self):
        with tempfile.TemporaryDirectory() as folder:
            self.app.config["USER_MANUAL_DIRECTORY"] = folder
            with self.app.test_request_context(), self.assertRaises(FileNotFoundError):
                render_access_email(self.user, "https://example.test/reset-password/test-only", welcome=True)

    def test_account_and_contact_content_is_escaped_in_html(self):
        self.user.first_name = '<img src=x onerror="alert(1)">'
        with self.app.test_request_context():
            rendered = render_access_email(self.user, "https://example.test/reset-password/?value=<unsafe&value>")
            self.assertNotIn("<img src=x", rendered["html"])
            self.assertIn("&lt;img", rendered["html"])
            self.assertIn("&lt;unsafe&amp;value&gt;", rendered["html"])
            inquiry = SimpleNamespace(name="<script>bad</script>", email="visitor@example.test",
                                      institution="A & B", message="First line\n<b>Second line</b>")
            rendered = render_contact_email(inquiry, "<a>Topic</a>")
            self.assertNotIn("<script>bad", rendered["html"])
            self.assertIn("First line<br>&lt;b&gt;Second line&lt;/b&gt;", rendered["html"])
            self.assertIn("First line\n<b>Second line</b>", rendered["text"])

    def test_password_reset_uses_account_locale_and_preserves_reset_link(self):
        with self.app.test_request_context("/?lang=en"):
            rendered = render_password_reset_email(self.user, "https://example.test/reset/test-token?a=1&b=2")
            self.assertIn('lang="es"', rendered["html"])
            self.assertIn("24 horas", rendered["text"])
            self.assertIn("test-token?a=1&amp;b=2", rendered["html"])
            self.assertIn("test-token?a=1&b=2", rendered["text"])
            self.assertNotIn("attachments", rendered)

    def test_public_pdf_download_and_conditional_requests(self):
        client = self.app.test_client()
        for language in ("en", "es", "fr", "pt", "de"):
            with self.subTest(language=language):
                response = client.get(f"/manuals/user-guide/{language}.pdf")
                self.assertEqual(200, response.status_code)
                self.assertEqual("application/pdf", response.mimetype)
                self.assertTrue(response.data.startswith(b"%PDF-"))
                self.assertIn(f"v2.1-{language}.pdf", response.headers["Content-Disposition"])
                self.assertEqual(304, client.get(f"/manuals/user-guide/{language}.pdf", headers={"If-None-Match":response.headers["ETag"]}).status_code)
                partial = client.get(f"/manuals/user-guide/{language}.pdf", headers={"Range":"bytes=0-4"})
                self.assertEqual(206, partial.status_code)
                self.assertEqual(b"%PDF-", partial.data)
        for path in ("/manuals/user-guide/it.pdf", "/manuals/user-guide/source.pdf", "/manuals/user-guide/../source/content.py", "/manuals/source/content.py"):
            self.assertEqual(404, client.get(path).status_code)

    def test_manual_link_uses_configured_public_base_url(self):
        self.app.config["APP_BASE_URL"] = "https://dataorcid.example/subpath"
        with self.app.test_request_context(base_url="http://untrusted.example"):
            rendered = render_access_email(self.user, "https://dataorcid.example/auth/reset-password/test-only")
            self.assertIn("https://dataorcid.example/subpath/manuals/user-guide/es.pdf", rendered["text"])
            self.assertNotIn("untrusted.example", rendered["html"])

    def test_smtp_recipient_refusal_is_reported_as_failure(self):
        smtp = MagicMock()
        smtp.__enter__.return_value = smtp
        smtp.send_message.return_value = {"recipient@example.test": (550, b"rejected")}
        with self.app.app_context(), patch("app.utils.emailer.smtplib.SMTP", return_value=smtp):
            success, error = send_email("recipient@example.test", "Test", "<p>Test</p>")
        self.assertFalse(success)
        self.assertIn("SMTP", error)


if __name__ == "__main__":
    unittest.main()
