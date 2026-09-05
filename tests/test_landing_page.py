"""Regression tests for the optional public landing page and contact flow."""

import unittest
from unittest.mock import patch

from app import create_app, db
from app.models import ContactInquiry, SystemModule, User
from app.services.module_access import MODULE_KEYS


class LandingPageTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app({
            "TESTING": True,
            "SECRET_KEY": "test-secret",
            "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "WTF_CSRF_ENABLED": False,
            "MAIL_ENABLED": True,
            "MAIL_REPLY_TO": "contact@example.test",
            "MAIL_DEFAULT_SENDER": ("Data ORCID-Chile", "no-reply@example.test"),
        })
        with self.app.app_context():
            db.create_all()
            user = User(username="landing-user@example.test", locale="es")
            user.set_password("test-password")
            admin = User(username="landing-admin@example.test", is_admin=True, locale="es")
            admin.set_password("test-password")
            db.session.add_all([user, admin])
            db.session.commit()
            self.user_id = user.id
            self.admin_id = admin.id

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            engine.dispose()

    @staticmethod
    def _login(client, user_id, username, *, is_admin=False):
        with client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=user_id,
                username=username,
                is_admin=is_admin,
                is_manager=False,
                is_oai_user=False,
                locale="es",
            )

    def _valid_contact_data(self, **overrides):
        data = {
            "name": "María Pérez",
            "email": "maria@example.test",
            "institution": "Universidad de Prueba",
            "topic": "demo",
            "message": "Queremos conocer la plataforma para nuestra institución.",
            "privacy": "accepted",
        }
        data.update(overrides)
        return data

    def test_anonymous_root_explains_service_and_offers_contact_and_login(self):
        response = self.app.test_client().get("/")
        html = response.get_data(as_text=True)

        self.assertEqual(200, response.status_code)
        self.assertIn("Build an auditable institutional evidence base from", html)
        self.assertIn("Coverage-aware analytics", html)
        self.assertIn("OAI-PMH publishing", html)
        self.assertIn('action="/contact"', html)
        self.assertIn('href="/auth/login"', html)
        self.assertIn('lang="en"', html)
        self.assertIn('lang="es"', html)
        self.assertIn('lang="fr"', html)
        self.assertIn('lang="pt"', html)
        self.assertIn('lang="de"', html)
        self.assertIn("landing.css?v=2.1", html)
        self.assertNotIn("app-sidebar", html)

    def test_public_page_is_translated_in_every_supported_language(self):
        expected_headings = {
            "en": "Build an auditable institutional evidence base from",
            "es": "Construye una base de evidencia institucional auditable a partir de",
            "fr": "Constituez une base institutionnelle de données probantes auditables à partir de",
            "pt": "Construa uma base institucional de evidências auditáveis a partir de",
            "de": "Erstellen Sie eine überprüfbare institutionelle Evidenzbasis aus",
        }

        for locale, heading in expected_headings.items():
            with self.subTest(locale=locale):
                response = self.app.test_client().get(f"/?lang={locale}")
                html = response.get_data(as_text=True)
                self.assertEqual(200, response.status_code)
                self.assertIn(f'<html lang="{locale}">', html)
                self.assertIn(heading, html)
                self.assertIn('action="/contact"', html)

    def test_authenticated_root_remains_the_private_dashboard(self):
        client = self.app.test_client()
        self._login(client, self.user_id, "landing-user@example.test")

        response = client.get("/")
        html = response.get_data(as_text=True)

        self.assertEqual(200, response.status_code)
        self.assertIn("app-sidebar", html)
        self.assertNotIn("Construye una base de evidencia institucional", html)

    def _set_landing_locale(self, language):
        with self.app.app_context():
            row = db.session.get(SystemModule, "landing_page")
            if row is None:
                row = SystemModule(key="landing_page", is_enabled=True)
                db.session.add(row)
            row.default_locale = language
            db.session.commit()

    def test_admin_default_is_persisted_and_applies_to_visitors_without_a_choice(self):
        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        visitor = self.app.test_client()

        for language in ("fr", "pt", "de", "es", "en"):
            with self.subTest(language=language):
                response = admin.post("/admin/modules", data={
                    "enabled": sorted(MODULE_KEYS), "landing_default_locale": language,
                })
                self.assertEqual(302, response.status_code)
                with self.app.app_context():
                    setting = db.session.get(SystemModule, "landing_page")
                    self.assertEqual(language, setting.default_locale)
                    self.assertTrue(setting.is_enabled)
                    self.assertEqual(self.admin_id, setting.updated_by_user_id)
                    self.assertEqual("landing-admin@example.test", setting.updated_by_username)
                    self.assertEqual("es", db.session.get(User, self.admin_id).locale)

                html = visitor.get("/").get_data(as_text=True)
                self.assertIn(f'<html lang="{language}">', html)
                with visitor.session_transaction() as saved_session:
                    self.assertNotIn("locale", saved_session)
                settings = admin.get("/admin/modules").get_data(as_text=True)
                self.assertIn(f'<option value="{language}" selected>', settings)
        self.assertEqual("en", self.app.config["BABEL_DEFAULT_LOCALE"])

    def test_visitor_selection_and_account_locale_take_priority_over_landing_default(self):
        self._set_landing_locale("fr")
        visitor = self.app.test_client()
        self.assertIn('<html lang="pt">', visitor.get("/?lang=pt").get_data(as_text=True))
        self._set_landing_locale("de")
        self.assertIn('<html lang="pt">', visitor.get("/").get_data(as_text=True))
        self.assertIn('<html lang="pt">', visitor.get("/?lang=invalid").get_data(as_text=True))
        self.assertIn('<html lang="de">', self.app.test_client().get("/").get_data(as_text=True))

        account = self.app.test_client()
        self._login(account, self.user_id, "landing-user@example.test")
        with account.session_transaction() as saved_session:
            saved_session.pop("locale", None)
        html = account.get("/").get_data(as_text=True)
        self.assertIn('<html lang="es">', html)
        self.assertIn("app-sidebar", html)

    def test_contact_validation_uses_the_landing_default_language(self):
        self._set_landing_locale("fr")
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email"
        ) as sender:
            response = self.app.test_client().post("/contact", data=self._valid_contact_data(email="invalid"))
        self.assertEqual(422, response.status_code)
        self.assertIn('<html lang="fr">', response.get_data(as_text=True))
        sender.assert_not_called()

    def test_invalid_default_does_not_change_language_or_module_availability(self):
        self._set_landing_locale("fr")
        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        for language in ("it", "", "<script>", "FR", "fr-CA"):
            with self.subTest(language=language):
                response = admin.post("/admin/modules", data={
                    "mode": "disable_all", "landing_default_locale": language,
                })
                self.assertEqual(302, response.status_code)
                with self.app.app_context():
                    setting = db.session.get(SystemModule, "landing_page")
                    self.assertEqual("fr", setting.default_locale)
                    self.assertTrue(setting.is_enabled)
                    self.assertEqual(1, SystemModule.query.count())

    def test_removed_language_falls_back_and_cannot_be_selected_by_admin(self):
        self._set_landing_locale("de")
        self.app.config.update(LANGUAGES=["en", "es"], BABEL_DEFAULT_LOCALE="es")
        html = self.app.test_client().get("/?lang=unknown").get_data(as_text=True)
        self.assertIn('<html lang="es">', html)
        self.assertNotIn('lang="de"', html)
        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        response = admin.post("/admin/modules", data={
            "mode": "disable_all", "landing_default_locale": "de",
        })
        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertTrue(db.session.get(SystemModule, "landing_page").is_enabled)

    def test_disabling_and_reenabling_the_landing_preserves_its_language(self):
        self._set_landing_locale("pt")
        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        self.assertEqual(302, admin.post("/admin/modules", data={"mode": "disable_all"}).status_code)
        visitor = self.app.test_client()
        self.assertTrue(visitor.get("/").headers["Location"].endswith("/auth/login"))
        settings = admin.get("/admin/modules").get_data(as_text=True)
        self.assertIn('<option value="pt" selected>', settings)
        self.assertEqual(302, admin.post("/admin/modules", data={"mode": "enable_all"}).status_code)
        self.assertIn('<html lang="pt">', visitor.get("/").get_data(as_text=True))

    def test_only_database_admins_can_change_the_landing_default(self):
        self._set_landing_locale("fr")
        anonymous = self.app.test_client().post("/admin/modules", data={"landing_default_locale": "es"})
        self.assertEqual(302, anonymous.status_code)
        self.assertTrue(anonymous.headers["Location"].endswith("/auth/login"))
        for role in ("user", "is_manager", "is_oai_user"):
            with self.subTest(role=role):
                with self.app.app_context():
                    account = db.session.get(User, self.user_id)
                    account.is_manager = role == "is_manager"
                    account.is_oai_user = role == "is_oai_user"
                    db.session.commit()
                client = self.app.test_client()
                self._login(client, self.user_id, "landing-user@example.test", is_admin=True)
                with client.session_transaction() as saved_session:
                    saved_session[role] = True
                response = client.post("/admin/modules", data={
                    "enabled": sorted(MODULE_KEYS), "landing_default_locale": "es",
                })
                self.assertEqual(302, response.status_code)
                with self.app.app_context():
                    self.assertEqual("fr", db.session.get(SystemModule, "landing_page").default_locale)

    def test_admin_switch_can_disable_public_page_and_contact_endpoint(self):
        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        modules = admin.get("/admin/modules").get_data(as_text=True)
        self.assertIn("Página pública de presentación", modules)
        self.assertIn('value="landing_page"', modules)

        with self.app.app_context():
            db.session.add(SystemModule(key="landing_page", is_enabled=False))
            db.session.commit()

        visitor = self.app.test_client()
        root = visitor.get("/")
        contact = visitor.post("/contact", data=self._valid_contact_data())

        self.assertEqual(302, root.status_code)
        self.assertTrue(root.headers["Location"].endswith("/auth/login"))
        self.assertEqual(403, contact.status_code)
        self.assertIn("currently unavailable", contact.get_data(as_text=True))

    def test_contact_validation_preserves_and_escapes_safe_fields(self):
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email"
        ) as sender:
            response = client.post(
                "/contact",
                data=self._valid_contact_data(
                    name="<script>alert(1)</script>",
                    email="invalid-address",
                    privacy="",
                ),
            )

        html = response.get_data(as_text=True)
        self.assertEqual(422, response.status_code)
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertIn("Enter a valid email address", html)
        self.assertIn("You must consent to the use of these data", html)
        sender.assert_not_called()

    def test_contact_rejects_multiple_reply_to_addresses(self):
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email"
        ) as sender:
            response = client.post(
                "/contact",
                data=self._valid_contact_data(email="maria@example.test,other"),
            )

        self.assertEqual(422, response.status_code)
        self.assertIn("Enter a valid email address", response.get_data(as_text=True))
        sender.assert_not_called()

    def test_contact_delivery_uses_fixed_recipient_and_visitor_reply_to(self):
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email", return_value=(True, None)
        ) as sender:
            response = client.post("/contact", data=self._valid_contact_data())

        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/#contact"))
        sender.assert_called_once()
        values = sender.call_args.kwargs
        self.assertEqual("contact@example.test", values["to_email"])
        self.assertEqual("maria@example.test", values["reply_to"])
        self.assertEqual("New inquiry from Data ORCID-Chile", values["subject"])
        self.assertIn("Universidad de Prueba", values["text"])
        with self.app.app_context():
            inquiry = ContactInquiry.query.one()
            self.assertEqual("delivered", inquiry.notification_status)
            self.assertFalse(inquiry.is_resolved)
            self.assertEqual("maria@example.test", inquiry.email)

    def test_honeypot_returns_generic_success_without_sending_email(self):
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email"
        ) as sender:
            response = client.post(
                "/contact",
                data=self._valid_contact_data(website="https://spam.example"),
            )

        self.assertEqual(302, response.status_code)
        sender.assert_not_called()

    def test_contact_rate_limit_returns_429_without_attempting_delivery(self):
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=True), patch(
            "app.blueprints.main.send_email"
        ) as sender:
            response = client.post("/contact", data=self._valid_contact_data())

        self.assertEqual(429, response.status_code)
        self.assertIn("Please try again later", response.get_data(as_text=True))
        sender.assert_not_called()

    def test_mail_failure_preserves_message_in_private_admin_inbox(self):
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email", return_value=(False, "SMTP offline")
        ):
            response = client.post("/contact", data=self._valid_contact_data())

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            inquiry = ContactInquiry.query.one()
            self.assertEqual("delivery_failed", inquiry.notification_status)
            self.assertEqual("Universidad de Prueba", inquiry.institution)

        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        inbox = admin.get("/admin/contact-inquiries")
        html = inbox.get_data(as_text=True)
        self.assertEqual(200, inbox.status_code)
        self.assertIn("María Pérez", html)
        self.assertIn("Aviso no enviado", html)
        self.assertIn("Queremos conocer la plataforma", html)

    def test_disabled_email_still_preserves_the_message(self):
        self.app.config["MAIL_ENABLED"] = False
        client = self.app.test_client()
        with patch("app.blueprints.main.is_rate_limited", return_value=False), patch(
            "app.blueprints.main.send_email"
        ) as sender:
            response = client.post("/contact", data=self._valid_contact_data())

        self.assertEqual(302, response.status_code)
        sender.assert_not_called()
        with self.app.app_context():
            inquiry = ContactInquiry.query.one()
            self.assertEqual("not_configured", inquiry.notification_status)

    def test_admin_can_resolve_reopen_and_delete_a_contact_inquiry(self):
        with self.app.app_context():
            inquiry = ContactInquiry(
                name="Ana Soto",
                email="ana@example.test",
                institution="Institución",
                topic="support",
                message="Necesitamos orientación sobre el servicio.",
                notification_status="not_configured",
            )
            db.session.add(inquiry)
            db.session.commit()
            inquiry_id = inquiry.id

        admin = self.app.test_client()
        self._login(admin, self.admin_id, "landing-admin@example.test", is_admin=True)
        resolved = admin.post(
            f"/admin/contact-inquiries/{inquiry_id}/status",
            data={"action": "resolve"},
        )
        self.assertEqual(302, resolved.status_code)
        with self.app.app_context():
            inquiry = db.session.get(ContactInquiry, inquiry_id)
            self.assertTrue(inquiry.is_resolved)
            self.assertEqual("landing-admin@example.test", inquiry.resolved_by_username)

        reopened = admin.post(
            f"/admin/contact-inquiries/{inquiry_id}/status",
            data={"action": "reopen"},
        )
        self.assertEqual(302, reopened.status_code)
        with self.app.app_context():
            self.assertFalse(db.session.get(ContactInquiry, inquiry_id).is_resolved)

        deleted = admin.post(
            f"/admin/contact-inquiries/{inquiry_id}/status",
            data={"action": "delete"},
        )
        self.assertEqual(302, deleted.status_code)
        with self.app.app_context():
            self.assertIsNone(db.session.get(ContactInquiry, inquiry_id))


if __name__ == "__main__":
    unittest.main()
