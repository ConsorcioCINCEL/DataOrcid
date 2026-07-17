"""Regression tests for shared interface and authentication helpers."""

import unittest
from unittest.mock import Mock, patch

from app import create_app, locale_url, plain_text
from app.services import orcid_service


class InterfaceHelperTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)

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

    def test_legacy_cache_dashboard_redirects_to_the_canonical_view(self):
        client = self.app.test_client()
        with client.session_transaction() as session:
            session.update(logged_in=True, user_id=1, ror_id="01test123")

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
