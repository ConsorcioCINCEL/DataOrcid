"""Regression tests for equivalent-period dashboard comparisons."""

import datetime as dt
import unittest

from flask import Flask

from app import db
from app.blueprints.main import _dashboard_openalex_summary, _year_to_date_comparison
from app.models import OpenAlexWorkMetadata, WorkCache


class DashboardComparisonTest(unittest.TestCase):
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

    def _add_work(self, year, month=None, day=None):
        db.session.add(
            WorkCache(
                ror_id="01test123",
                orcid="0000-0001-0000-0001",
                title=f"Work {year}-{month}-{day}",
                pub_year=str(year),
                pub_month=str(month) if month is not None else None,
                pub_day=str(day) if day is not None else None,
            )
        )

    def test_comparison_uses_the_same_completed_months_in_both_years(self):
        with self.app.app_context():
            self._add_work(2026, 1)
            self._add_work(2026, 6)
            self._add_work(2026, 7)
            self._add_work(2026)

            self._add_work(2025, 1)
            self._add_work(2025, 2)
            self._add_work(2025, 5)
            self._add_work(2025, 6)
            self._add_work(2025, 7)
            self._add_work(2025)
            db.session.commit()

            comparison = _year_to_date_comparison(
                WorkCache,
                WorkCache.pub_year,
                WorkCache.pub_month,
                WorkCache.pub_day,
                "01test123",
                today=dt.date(2026, 7, 16),
            )

            self.assertEqual(2, comparison["current_count"])
            self.assertEqual(4, comparison["previous_count"])
            self.assertEqual(-50, comparison["change"])
            self.assertEqual(dt.date(2026, 6, 30), comparison["cutoff_date"])
            self.assertEqual(2025, comparison["previous_year"])

    def test_openalex_summary_uses_only_enriched_articles_in_active_scope(self):
        with self.app.app_context():
            matched_doi = WorkCache(
                ror_id="01test123",
                orcid="0000-0001-0000-0001",
                type="journal-article",
                doi="https://doi.org/10.1234/Matched.",
            )
            matched_title = WorkCache(
                ror_id="01test123",
                orcid="0000-0001-0000-0002",
                type="journal-article",
                title="Title-only match",
            )
            unmatched = WorkCache(
                ror_id="01test123",
                orcid="0000-0001-0000-0003",
                type="journal-article",
                doi="10.1234/unmatched",
            )
            other_institution = WorkCache(
                ror_id="02other456",
                orcid="0000-0001-0000-0004",
                type="journal-article",
                doi="10.1234/other",
            )
            db.session.add_all([matched_doi, matched_title, unmatched, other_institution])
            db.session.flush()
            db.session.add_all(
                [
                    OpenAlexWorkMetadata(
                        doi_normalized="10.1234/matched",
                        openalex_id="W1",
                        publication_year=2024,
                        cited_by_count=7,
                        is_oa=True,
                    ),
                    OpenAlexWorkMetadata(
                        doi_normalized=f"work:{matched_title.id}",
                        openalex_id="W2",
                        publication_year=2025,
                        cited_by_count=2,
                        is_oa=False,
                    ),
                    OpenAlexWorkMetadata(
                        doi_normalized="10.1234/other",
                        openalex_id="W3",
                        publication_year=2025,
                        cited_by_count=100,
                        is_oa=True,
                    ),
                ]
            )
            db.session.commit()

            summary = _dashboard_openalex_summary("01test123")

            self.assertEqual(3, summary["eligible"])
            self.assertEqual(2, summary["matched"])
            self.assertEqual(67, summary["coverage"])
            self.assertEqual(9, summary["citations"])
            self.assertEqual(1, summary["open_access"])
            self.assertEqual(["2024", "2025"], summary["trend_years"])
            self.assertEqual([1, 1], summary["trend_counts"])


if __name__ == "__main__":
    unittest.main()
