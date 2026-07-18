"""Regression tests for the system activity dashboard."""

import unittest
from datetime import datetime, timezone

from flask import session

from app import create_app
from app.blueprints.admin import (
    _format_latency,
    _statistics_period_bounds,
    _user_agent_summary,
)


class AdminStatisticsTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=1,
                username="qa-admin@example.org",
                is_admin=True,
                is_manager=False,
                locale="en",
                ror_id="02ap3w078",
                admin_selected_ror="02ap3w078",
            )

    def test_custom_period_normalizes_reversed_dates(self):
        with self.app.app_context():
            period, start, end, date_from, date_to = _statistics_period_bounds(
                "custom",
                "2026-07-16",
                "2026-07-14",
                now_utc=datetime(2026, 7, 16, 12, tzinfo=timezone.utc),
            )

        self.assertEqual("custom", period)
        self.assertEqual("2026-07-14", date_from)
        self.assertEqual("2026-07-16", date_to)
        self.assertLess(start, end)

    def test_latency_and_client_labels_are_readable(self):
        self.assertEqual("38 ms", _format_latency(38))
        self.assertEqual("2.5 s", _format_latency(2500))
        with self.app.test_request_context("/admin/statistics"):
            session["locale"] = "en"
            label = _user_agent_summary(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/150.0 Safari/537.36"
            )
        self.assertEqual("Chrome · Linux · Desktop", label)

    def test_statistics_route_renders_the_new_dashboard_states(self):
        response = self.client.get(
            "/admin/statistics?period=24h&kind=background&per_page=50&sort=latency&dir=desc"
        )
        html = response.get_data(as_text=True)

        self.assertEqual(200, response.status_code)
        self.assertIn('main id="main-content"', html)
        self.assertIn('id="activityTrendChart"', html)
        self.assertIn("Background operations", html)
        self.assertIn("Median latency", html)
        self.assertIn("activity-mobile-list", html)
        self.assertIn('value="50"', html)


if __name__ == "__main__":
    unittest.main()
