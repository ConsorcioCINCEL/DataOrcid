"""Regression tests for shared interface and authentication helpers."""

import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from babel.messages.pofile import read_po
from flask import render_template_string, session

from app import DEFAULT_LANGUAGES, create_app, db, locale_url, plain_text
from app.models import User
from app.services import orcid_service


class InterfaceHelperTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app({
            "TESTING": True,
            "WTF_CSRF_ENABLED": False,
            "SQLALCHEMY_DATABASE_URI": "sqlite://",
        })
        with self.app.app_context():
            db.create_all()
            account = User(username="interface@example.org", ror_id="01test123")
            account.set_password("test-password")
            db.session.add(account)
            db.session.commit()
            self.user_id = account.id

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            engine.dispose()

    def test_plain_text_removes_external_markup_and_decodes_entities(self):
        value = "A <i>useful</i> result with CO<sub>2</sub> &amp; context"
        self.assertEqual(plain_text(value), "A useful result with CO2 & context")

    def test_locale_url_preserves_repeated_filters(self):
        with self.app.test_request_context(
            "/openalex/analytics?year_from=2020&type=article&type=book"
        ):
            query = locale_url("es")
        self.assertIn("year_from=2020", query)
        self.assertIn("type=article", query)
        self.assertIn("type=book", query)
        self.assertIn("lang=es", query)

    def test_public_language_selector_offers_every_supported_locale(self):
        response = self.app.test_client().get("/auth/login")
        html = response.get_data(as_text=True)

        self.assertEqual(200, response.status_code)
        for code, label in (
            ("en", "English"),
            ("es", "Español"),
            ("fr", "Français"),
            ("pt", "Português"),
            ("de", "Deutsch"),
        ):
            self.assertIn(f'lang="{code}"', html)
            self.assertIn(label, html)

    def test_locale_query_accepts_supported_languages_and_ignores_unknown_ones(self):
        client = self.app.test_client()

        response = client.get("/auth/login?lang=fr")

        self.assertEqual(200, response.status_code)
        with client.session_transaction() as client_session:
            self.assertEqual("fr", client_session["locale"])

        unknown_client = self.app.test_client()
        response = unknown_client.get("/auth/login?lang=it")

        self.assertEqual(200, response.status_code)
        with unknown_client.session_transaction() as client_session:
            self.assertNotIn("locale", client_session)

    def test_new_language_catalogs_cover_every_source_message(self):
        translations = Path(__file__).resolve().parents[1] / "app" / "translations"
        with (translations / "fr" / "LC_MESSAGES" / "messages.po").open(
            encoding="utf-8"
        ) as handle:
            source_catalog = read_po(handle, locale="fr")
        source_ids = {message.id for message in source_catalog if message.id}
        expected_sign_in = {
            "fr": "Connexion",
            "pt": "Entrar",
            "de": "Anmelden",
        }

        self.assertEqual(("en", "es", "fr", "pt", "de"), DEFAULT_LANGUAGES)
        for locale, sign_in in expected_sign_in.items():
            with (translations / locale / "LC_MESSAGES" / "messages.po").open(
                encoding="utf-8"
            ) as handle:
                catalog = read_po(handle, locale=locale)
            messages = {message.id: message for message in catalog if message.id}
            self.assertEqual(source_ids, set(messages))
            self.assertTrue(all(message.string and not message.fuzzy for message in messages.values()))
            self.assertEqual(sign_in, messages["Sign in"].string)

    def test_remember_me_marks_the_session_permanent(self):
        user = Mock(
            id=42,
            username="qa@example.org",
            first_name="QA",
            last_name="Tester",
            full_name="QA Tester",
            position="Analyst",
            email="qa@example.org",
            is_admin=False,
            is_manager=False,
            institution_name="QA Institution",
            ror_id="012345678",
            locale="en",
        )
        user.check_password.return_value = True

        user_model = Mock()
        user_model.query.filter_by.return_value.first.return_value = user
        with patch("app.blueprints.auth.User", user_model), patch(
            "app.blueprints.auth._is_rate_limited", return_value=False
        ):
            client = self.app.test_client()
            response = client.post(
                "/auth/login",
                data={"username": user.username, "password": "valid", "remember": "on"},
            )

            self.assertEqual(response.status_code, 302)
            with client.session_transaction() as session:
                self.assertTrue(session.permanent)
                self.assertTrue(session["logged_in"])

    def test_security_headers_are_added_by_flask(self):
        response = self.app.test_client().get("/auth/login")

        self.assertEqual("nosniff", response.headers["X-Content-Type-Options"])
        self.assertEqual("SAMEORIGIN", response.headers["X-Frame-Options"])
        self.assertEqual(
            "strict-origin-when-cross-origin",
            response.headers["Referrer-Policy"],
        )
        self.assertIn("camera=()", response.headers["Permissions-Policy"])

    def test_chart_exports_offer_compact_mobile_download_menu(self):
        with self.app.test_request_context():
            html = render_template_string(
                "{% from 'components/ui.html' import chart_exports %}"
                "{{ chart_exports('testChart') }}"
            )

        self.assertEqual(2, html.count("app-chart-export-desktop"))
        self.assertIn("app-chart-export-mobile", html)
        self.assertIn('id="chartExportMenutestChart"', html)
        self.assertEqual(4, html.count('data-chart-id="testChart"'))

    def test_priority_open_access_badges_share_the_academic_icon_and_status_colors(self):
        with self.app.test_request_context():
            html = render_template_string(
                "{% from 'components/ui.html' import priority_oa_badge %}"
                "{{ priority_oa_badge('diamond', 'Diamond Open Access') }}"
                "{{ priority_oa_badge('green', 'Green Open Access') }}"
            )

        self.assertEqual(2, html.count("ai-open-access"))
        self.assertIn("openalex-oa-badge-diamond", html)
        self.assertIn("openalex-oa-badge-green", html)

    def test_base_template_loads_the_pinned_academicons_release(self):
        with self.app.test_request_context("/"):
            html = render_template_string(
                "{% extends 'base.html' %}{% block content %}{% endblock %}"
            )

        self.assertIn(
            "jpswalsh/academicons@1.9.4/css/academicons.min.css",
            html,
        )

    def test_oai_workspace_navigation_explains_and_marks_each_section(self):
        with self.app.test_request_context("/oai-pmh/metadata/"):
            html = render_template_string(
                "{% include 'oai_pmh/_tabs.html' %}",
                active_tab="metadata",
            )

        self.assertIn("oai-workspace-navigation", html)
        self.assertEqual(4, html.count('class="oai-workspace-tab '))
        self.assertEqual(1, html.count('aria-current="page">'))
        self.assertIn("Repository workspace", html)
        self.assertIn("Current section", html)
        self.assertIn("Test formats and customize", html)
        self.assertIn("Review which institutional works", html)

    def test_sidebar_children_follow_task_oriented_visual_groups(self):
        with self.app.test_request_context("/"):
            session.update(
                logged_in=True,
                user_id=self.user_id,
                username="interface@example.org",
                is_admin=False,
                is_manager=False,
            )
            html = render_template_string(
                "{% include 'layout/sidebar.html' %}"
            )

        expected_order = [
            "People",
            "Researchers",
            "Indicators",
            "ORCID analytics",
            "OpenAlex analytics",
            "Update",
            "Synchronization and downloads",
            "OpenAlex enrichment",
            "Review",
            "Data quality",
            "Duplicate profiles",
            "Read from the API",
            "Affiliation Manager",
            "Repositories",
            "OAI-PMH publishing",
            "Support",
            "Help center",
            "ORCID resources",
        ]
        positions = [html.index(label) for label in expected_order]
        self.assertEqual(sorted(positions), positions)
        self.assertEqual(6, html.count("app-sidebar-subheader"))

    def test_oai_child_routes_keep_integrate_group_and_child_active(self):
        with self.app.test_request_context("/oai-pmh/articles/"):
            session.update(
                logged_in=True,
                user_id=self.user_id,
                username="interface@example.org",
                is_admin=False,
                is_manager=False,
            )
            html = render_template_string(
                "{% include 'layout/sidebar.html' %}"
            )

        self.assertIn("app-sidebar-group menu-open", html)
        self.assertIn(
            'href="/oai-pmh/" class="nav-link active"',
            html,
        )

    def test_administration_children_are_grouped_by_responsibility(self):
        with self.app.test_request_context("/admin/modules"):
            session.update(
                logged_in=True,
                user_id=self.user_id,
                username="interface@example.org",
                is_admin=True,
                is_manager=False,
            )
            html = render_template_string(
                "{% include 'layout/sidebar.html' %}"
            )

        expected_order = [
            "Access and configuration",
            "Users",
            "Module availability",
            "Interoperability",
            "OAI-PMH repositories",
            "Monitoring",
            "Activity",
            "Background jobs",
            "System errors",
        ]
        positions = [html.index(label) for label in expected_order]
        self.assertEqual(sorted(positions), positions)

    def test_legacy_cache_dashboard_redirects_to_the_canonical_view(self):
        client = self.app.test_client()
        with client.session_transaction() as session:
            session.update(logged_in=True, user_id=self.user_id, ror_id="01test123")

        response = client.get("/cache/dashboard")

        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/cache/works/status"))

    def test_funding_cache_routes_have_one_registered_handler(self):
        routes = [
            rule
            for rule in self.app.url_map.iter_rules()
            if rule.rule in {"/cache/fundings/build", "/download/all-fundings/cache"}
        ]

        self.assertEqual(2, len(routes))
        self.assertEqual(
            {"works.cache_fundings_build", "works.download_all_fundings_cache"},
            {rule.endpoint for rule in routes},
        )

    def test_orcid_profile_is_reused_until_explicit_refresh(self):
        self.app.config.update(
            ORCID_MEMBER_URL="https://example.test",
            ORCID_PROFILE_CACHE_TTL=900,
        )
        orcid_service._PROFILE_CACHE.clear()
        profile = {"orcid-identifier": {"path": "0000-0001"}}

        with self.app.app_context(), patch(
            "app.services.orcid_service.get_client_credentials_token",
            return_value="token",
        ), patch(
            "app.services.orcid_service.fetch_single_profile",
            return_value=profile,
        ) as fetch:
            first = orcid_service.get_full_orcid_profile("0000-0001")
            second = orcid_service.get_full_orcid_profile("0000-0001")
            refreshed = orcid_service.get_full_orcid_profile(
                "0000-0001", force_refresh=True
            )

        self.assertIs(profile, first)
        self.assertIs(profile, second)
        self.assertIs(profile, refreshed)
        self.assertEqual(2, fetch.call_count)


if __name__ == "__main__":
    unittest.main()
