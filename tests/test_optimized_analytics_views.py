"""Regression tests for the compact researcher and analytics view filters."""

import unittest

from flask import Flask

from app import db
from app.blueprints.main import (
    _filter_researcher_directory,
    _filtered_funding_query,
    _filtered_work_query,
    _metrics_filter_values,
    _pagination_page_numbers,
)
from app.models import FundingCache, WorkCache


class OptimizedAnalyticsViewTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(self.app)
        with self.app.app_context():
            db.create_all()

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def test_researcher_directory_combines_filters_and_sorts_activity(self):
        rows = [
            {
                "display-name": "Ana Example",
                "orcid-id": "0000-0001-0000-0001",
                "email": "ana@example.org",
                "institution-name": "Example University",
                "is_managed": True,
                "profile_complete": True,
                "association_verified": True,
                "matched_identifiers": {"ror": True, "grid": False, "ringgold": False},
                "works_count": 1,
                "fundings_count": 2,
                "total_activity": 3,
            },
            {
                "display-name": "Berta Example",
                "orcid-id": "0000-0002-0000-0002",
                "email": "berta@example.org",
                "institution-name": "Example University",
                "is_managed": True,
                "profile_complete": True,
                "association_verified": True,
                "matched_identifiers": {"ror": True, "grid": False, "ringgold": False},
                "works_count": 8,
                "fundings_count": 1,
                "total_activity": 9,
            },
            {
                "display-name": "Carlos Other",
                "orcid-id": "0000-0003-0000-0003",
                "email": "carlos@example.org",
                "institution-name": "Other University",
                "is_managed": False,
                "profile_complete": False,
                "association_verified": False,
                "matched_identifiers": {"ror": False, "grid": True, "ringgold": False},
                "works_count": 20,
                "fundings_count": 0,
                "total_activity": 20,
            },
        ]

        with self.app.test_request_context(
            "/researcher-list?q=example&am=managed&profile=incomplete&match=ror&sort=activity&dir=desc"
        ):
            filtered, values = _filter_researcher_directory(rows)

        self.assertEqual(
            ["0000-0002-0000-0002", "0000-0001-0000-0001"],
            [row["orcid-id"] for row in filtered],
        )
        self.assertEqual("activity", values["sort"])
        self.assertEqual("desc", values["dir"])
        self.assertNotIn("profile", values)

        with self.app.test_request_context(
            "/researcher-list?sort=works&dir=desc"
        ):
            sorted_rows, values = _filter_researcher_directory(rows)

        self.assertEqual(
            ["0000-0003-0000-0003", "0000-0002-0000-0002", "0000-0001-0000-0001"],
            [row["orcid-id"] for row in sorted_rows],
        )
        self.assertEqual("works", values["sort"])

    def test_researcher_pagination_shows_all_normal_page_numbers(self):
        self.assertEqual([1], _pagination_page_numbers(1, 1))
        self.assertEqual(list(range(1, 11)), _pagination_page_numbers(1, 10))
        self.assertEqual(
            list(range(1, 11)),
            _pagination_page_numbers(5, 10),
        )
        self.assertEqual(list(range(1, 11)), _pagination_page_numbers(10, 10))
        self.assertEqual(
            [1, None, 28, 29, 30, 31, 32, None, 60],
            _pagination_page_numbers(30, 60),
        )

    def test_metrics_filters_normalize_year_order_and_multiple_values(self):
        with self.app.test_request_context(
            "/metrics-panel?year_from=2025&year_to=2020&work_type=journal-article"
            "&work_type=conference-paper&funding_type=grant&researcher=0000-0001"
        ):
            values = _metrics_filter_values()

        self.assertEqual(2020, values["year_from"])
        self.assertEqual(2025, values["year_to"])
        self.assertEqual(["journal-article", "conference-paper"], values["work_type"])
        self.assertEqual(["grant"], values["funding_type"])
        self.assertEqual("0000-0001", values["researcher"])

    def test_metrics_queries_apply_shared_scope_and_filters(self):
        filters = {
            "year_from": 2020,
            "year_to": 2024,
            "work_type": ["journal-article"],
            "funding_type": ["grant"],
            "researcher": "0000-0001",
        }
        with self.app.app_context():
            db.session.add_all(
                [
                    WorkCache(ror_id="01example", orcid="0000-0001", pub_year="2023", type="journal-article"),
                    WorkCache(ror_id="01example", orcid="0000-0001", pub_year="2019", type="journal-article"),
                    WorkCache(ror_id="01example", orcid="0000-0002", pub_year="2023", type="journal-article"),
                    WorkCache(ror_id="02other", orcid="0000-0001", pub_year="2023", type="journal-article"),
                    FundingCache(ror_id="01example", orcid="0000-0001", start_y="2022", type="grant"),
                    FundingCache(ror_id="01example", orcid="0000-0001", start_y="2022", type="award"),
                ]
            )
            db.session.commit()

            works = _filtered_work_query(WorkCache.query, "01example", filters).all()
            fundings = _filtered_funding_query(FundingCache.query, "01example", filters).all()

        self.assertEqual(1, len(works))
        self.assertEqual("2023", works[0].pub_year)
        self.assertEqual(1, len(fundings))
        self.assertEqual("grant", fundings[0].type)


if __name__ == "__main__":
    unittest.main()
