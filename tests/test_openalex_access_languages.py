"""Regression tests for the ANID open-access and language analytics."""

import json
import unittest
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.works import (
    _openalex_analytics,
    _openalex_global_analytics,
    _openalex_language_label,
    _openalex_oa_status_color,
)
from app.models import OpenAlexSyncRun, OpenAlexWorkMetadata, WorkCache


class OpenAlexAccessLanguageAnalyticsTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SECRET_KEY="test",
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            BABEL_DEFAULT_LOCALE="es",
        )
        db.init_app(self.app)
        babel.init_app(self.app, locale_selector=lambda: "es")
        with self.app.app_context():
            db.create_all()

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def _seed_work(
        self,
        suffix: str,
        year: int,
        language: str,
        oa_status: str,
        *,
        citations: int = 0,
        ror_id: str = "01test123",
        source_name: str | None = None,
        source_issn_l: str | None = None,
    ):
        doi = f"10.1234/{suffix}"
        db.session.add(
            WorkCache(
                ror_id=ror_id,
                orcid=f"0000-0001-0000-{suffix.zfill(4)}",
                type="journal-article",
                title=f"Work {suffix}",
                doi=doi,
            )
        )
        db.session.add(
            OpenAlexWorkMetadata(
                doi_normalized=doi,
                openalex_id=f"W{suffix}",
                title=f"Work {suffix}",
                publication_year=year,
                language=language,
                cited_by_count=citations,
                is_oa=oa_status != "closed",
                oa_status=oa_status,
                source_name=source_name,
                source_issn_l=source_issn_l,
            )
        )
        return doi

    def test_analytics_highlights_native_diamond_and_green_statuses(self):
        with self.app.app_context(), self.app.test_request_context("/openalex/analytics?lang=es"):
            self._seed_work(
                "1",
                2023,
                "es",
                "diamond",
                citations=12,
                source_name="Journal A",
                source_issn_l="1111-1111",
            )
            self._seed_work(
                "2",
                2024,
                "en",
                "green",
                citations=8,
                source_name="Journal A",
                source_issn_l="1111-1111",
            )
            self._seed_work("3", 2024, "pt", "gold")
            self._seed_work(
                "4",
                2025,
                "es",
                "green",
                citations=30,
                source_name="Journal B",
                source_issn_l="2222-2222",
            )
            db.session.add(OpenAlexSyncRun(ror_id="01test123", status="success"))
            db.session.commit()

            analytics = _openalex_analytics("01test123")

            self.assertEqual(1, analytics["summary"]["diamond_open_access_count"])
            self.assertEqual(2, analytics["summary"]["green_open_access_count"])
            self.assertEqual(3, analytics["summary"]["priority_open_access_count"])
            self.assertEqual(75.0, analytics["summary"]["priority_open_access_percent"])
            self.assertEqual(["2023", "2024", "2025"], analytics["charts"]["years"])
            self.assertEqual([1, 0, 0], analytics["charts"]["diamond_oa_year_values"])
            self.assertEqual([0, 1, 1], analytics["charts"]["green_oa_year_values"])
            self.assertIn("Español (es)", analytics["charts"]["language_labels"])
            self.assertIn("Inglés (en)", analytics["charts"]["language_labels"])
            priority = analytics["priority_open_access"]
            self.assertEqual(3, priority["summary"]["articles"])
            self.assertEqual(50, priority["summary"]["citations"])
            self.assertEqual("Journal A", priority["charts"]["sources_by_articles"]["labels"][0])
            self.assertEqual(
                [1, 1],
                [
                    dataset["data"][0]
                    for dataset in priority["charts"]["sources_by_articles"]["datasets"]
                ],
            )
            self.assertEqual(
                ["#595959", "#00b050"],
                [
                    dataset["backgroundColor"]
                    for dataset in priority["charts"]["sources_by_articles"]["datasets"]
                ],
            )
            self.assertEqual(
                ["#595959", "#00b050"],
                analytics["charts"]["priority_oa_colors"],
            )
            self.assertEqual(
                {"#595959", "#00b050", "#bf8f00"},
                set(analytics["charts"]["oa_colors"]),
            )
            self.assertEqual("Work 4", priority["top_cited_articles"][0]["title"])
            self.assertEqual(
                {"diamond", "green"},
                {row["oa_status"] for row in priority["source_rows"]},
            )
            json.dumps(analytics)

    def test_language_filter_updates_denominators_and_labels(self):
        with self.app.app_context(), self.app.test_request_context("/openalex/analytics?lang=es"):
            self._seed_work("1", 2023, "es", "diamond")
            self._seed_work("2", 2024, "en", "green")
            self._seed_work("3", 2025, "es", "green")
            db.session.commit()

            analytics = _openalex_analytics("01test123", {"language": ["es"]})

            self.assertEqual(2, analytics["summary"]["enriched_dois"])
            self.assertEqual(1, analytics["summary"]["diamond_open_access_count"])
            self.assertEqual(1, analytics["summary"]["green_open_access_count"])
            self.assertEqual(["es"], analytics["filters"]["language"])
            self.assertEqual(["Español (es)"], analytics["charts"]["language_labels"])
            self.assertEqual([2], analytics["charts"]["language_values"])
            self.assertEqual("Portugués (pt)", _openalex_language_label("pt"))

    def test_global_priority_metrics_deduplicate_articles_and_compare_institutions(self):
        with self.app.app_context(), self.app.test_request_context(
            "/openalex/global?tab=open_access&lang=en"
        ):
            shared_doi = self._seed_work(
                "1",
                2024,
                "en",
                "diamond",
                citations=10,
                ror_id="01alpha123",
                source_name="Journal A",
            )
            db.session.add(
                WorkCache(
                    ror_id="01beta456",
                    orcid="0000-0001-0000-9001",
                    type="journal-article",
                    title="Shared Work",
                    doi=shared_doi,
                )
            )
            self._seed_work(
                "2",
                2025,
                "es",
                "green",
                citations=5,
                ror_id="01beta456",
                source_name="Journal B",
            )
            db.session.commit()

            with patch(
                "app.blueprints.works._institution_lookup",
                return_value={
                    "01alpha123": "Alpha University",
                    "01beta456": "Beta University",
                },
            ):
                analytics = _openalex_global_analytics({"tab": "open_access"})

            priority = analytics["priority_open_access"]
            self.assertEqual(2, priority["summary"]["articles"])
            self.assertEqual(15, priority["summary"]["citations"])
            institution_rows = {
                row["ror_id"]: row
                for row in priority["institution_rows"]
            }
            self.assertEqual(1, institution_rows["01alpha123"]["articles"])
            self.assertEqual(2, institution_rows["01beta456"]["articles"])
            self.assertEqual(10, institution_rows["01alpha123"]["citations"])
            self.assertEqual(15, institution_rows["01beta456"]["citations"])
            self.assertEqual(
                ["2024", "2025"],
                priority["charts"]["trend_years"],
            )
            self.assertEqual(
                [1, 0],
                priority["charts"]["article_trend_datasets"][0]["data"],
            )
            self.assertEqual(
                [0, 1],
                priority["charts"]["article_trend_datasets"][1]["data"],
            )
            self.assertEqual(
                [10, 0],
                priority["charts"]["citation_trend_datasets"][0]["data"],
            )
            self.assertEqual(
                [0, 5],
                priority["charts"]["citation_trend_datasets"][1]["data"],
            )
            self.assertEqual(
                ["#595959", "#00b050"],
                [
                    dataset["borderColor"]
                    for dataset in priority["charts"]["article_trend_datasets"]
                ],
            )
            self.assertEqual("open_access", analytics["active_tab"])
            json.dumps(analytics)

    def test_igi_open_access_palette_matches_the_reference_guide(self):
        self.assertEqual("#595959", _openalex_oa_status_color("diamond"))
        self.assertEqual("#00b050", _openalex_oa_status_color("green"))
        self.assertEqual("#2f5496", _openalex_oa_status_color("blue"))
        self.assertEqual("#eab200", _openalex_oa_status_color("yellow"))
        self.assertEqual("#ed7d31", _openalex_oa_status_color("hybrid"))
        self.assertEqual("#bf8f00", _openalex_oa_status_color("gold"))
        self.assertEqual("#806000", _openalex_oa_status_color("bronze"))
        self.assertEqual("#a0a0a0", _openalex_oa_status_color("white"))
        self.assertEqual("#000000", _openalex_oa_status_color("black"))
        self.assertEqual("#a0a0a0", _openalex_oa_status_color("closed"))

    def test_priority_source_and_article_tables_are_paginated_independently(self):
        with self.app.app_context(), self.app.test_request_context(
            "/openalex/analytics?priority_source_page=2"
            "&priority_source_sort=citations&priority_source_dir=asc"
            "&priority_article_page=2"
            "&priority_article_sort=citations&priority_article_dir=asc"
        ):
            for index in range(1, 13):
                self._seed_work(
                    str(index),
                    2024,
                    "es",
                    "diamond",
                    citations=index,
                    source_name=f"Journal {index:02d}",
                )
            db.session.commit()

            analytics = _openalex_analytics("01test123")
            priority = analytics["priority_open_access"]

            source_pagination = priority["tables"]["sources"]["pagination"]
            article_pagination = priority["tables"]["articles"]["pagination"]
            self.assertEqual(12, source_pagination["total_rows"])
            self.assertEqual(2, source_pagination["page"])
            self.assertEqual(2, len(priority["source_rows"]))
            self.assertEqual(
                [11, 12],
                [row["citations"] for row in priority["source_rows"]],
            )
            self.assertEqual(12, article_pagination["total_rows"])
            self.assertEqual(2, article_pagination["page"])
            self.assertEqual(2, len(priority["top_cited_articles"]))
            self.assertEqual(
                [11, 12],
                [
                    row["citations"]
                    for row in priority["top_cited_articles"]
                ],
            )
            self.assertEqual("citations", priority["tables"]["sources"]["sort"])
            self.assertEqual("asc", priority["tables"]["sources"]["dir"])
            self.assertEqual("citations", priority["tables"]["articles"]["sort"])
            self.assertEqual("asc", priority["tables"]["articles"]["dir"])

    def test_priority_table_search_filters_the_complete_result_set(self):
        with self.app.app_context(), self.app.test_request_context(
            "/openalex/analytics?priority_source_page=2"
            "&priority_source_q=Journal+12"
            "&priority_article_page=2"
            "&priority_article_q=Work+11"
        ):
            for index in range(1, 13):
                self._seed_work(
                    str(index),
                    2024,
                    "es",
                    "diamond",
                    citations=index,
                    source_name=f"Journal {index:02d}",
                )
            db.session.commit()

            priority = _openalex_analytics("01test123")[
                "priority_open_access"
            ]

            source_table = priority["tables"]["sources"]
            article_table = priority["tables"]["articles"]
            self.assertEqual(1, source_table["pagination"]["total_rows"])
            self.assertEqual(1, source_table["pagination"]["page"])
            self.assertEqual("Journal 12", source_table["search"])
            self.assertEqual(
                ["Journal 12"],
                [row["source_name"] for row in priority["source_rows"]],
            )
            self.assertEqual(1, article_table["pagination"]["total_rows"])
            self.assertEqual(1, article_table["pagination"]["page"])
            self.assertEqual("Work 11", article_table["search"])
            self.assertEqual(
                ["Work 11"],
                [row["title"] for row in priority["top_cited_articles"]],
            )

    def test_global_priority_institution_table_is_paginated(self):
        with self.app.app_context(), self.app.test_request_context(
            "/openalex/global?tab=open_access"
            "&priority_institution_page=2"
            "&priority_institution_sort=citations"
            "&priority_institution_dir=asc"
        ):
            institution_names = {}
            for index in range(1, 16):
                ror_id = f"01inst{index:03d}"
                institution_names[ror_id] = f"University {index:02d}"
                self._seed_work(
                    str(index),
                    2024,
                    "es",
                    "diamond",
                    citations=index,
                    ror_id=ror_id,
                    source_name="Journal A",
                )
            db.session.commit()

            with patch(
                "app.blueprints.works._institution_lookup",
                return_value=institution_names,
            ):
                analytics = _openalex_global_analytics({"tab": "open_access"})

            priority = analytics["priority_open_access"]
            pagination = priority["tables"]["institutions"]["pagination"]
            self.assertEqual(15, pagination["total_rows"])
            self.assertEqual(2, pagination["page"])
            self.assertEqual(5, len(priority["institution_rows"]))
            self.assertEqual(
                [11, 12, 13, 14, 15],
                [row["citations"] for row in priority["institution_rows"]],
            )
            self.assertEqual(
                15,
                len(priority["charts"]["institutions_by_articles"]["labels"]),
            )
            self.assertEqual(
                15,
                len(priority["charts"]["institutions_by_citations"]["labels"]),
            )
            self.assertEqual(
                "citations",
                priority["tables"]["institutions"]["sort"],
            )
            self.assertEqual(
                "asc",
                priority["tables"]["institutions"]["dir"],
            )

    def test_global_priority_institution_search_uses_all_rows(self):
        with self.app.app_context(), self.app.test_request_context(
            "/openalex/global?tab=open_access"
            "&priority_institution_page=2"
            "&priority_institution_q=University+12"
        ):
            institution_names = {}
            for index in range(1, 13):
                ror_id = f"01inst{index:03d}"
                institution_names[ror_id] = f"University {index:02d}"
                self._seed_work(
                    str(index),
                    2024,
                    "es",
                    "diamond",
                    citations=index,
                    ror_id=ror_id,
                    source_name="Journal A",
                )
            db.session.commit()

            with patch(
                "app.blueprints.works._institution_lookup",
                return_value=institution_names,
            ):
                priority = _openalex_global_analytics({
                    "tab": "open_access",
                })["priority_open_access"]

            table = priority["tables"]["institutions"]
            self.assertEqual(1, table["pagination"]["total_rows"])
            self.assertEqual(1, table["pagination"]["page"])
            self.assertEqual("University 12", table["search"])
            self.assertEqual(
                ["University 12"],
                [
                    row["institution"]
                    for row in priority["institution_rows"]
                ],
            )


if __name__ == "__main__":
    unittest.main()
