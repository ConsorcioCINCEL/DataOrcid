"""Regression tests for cache-status summaries and synchronization history."""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.works import (
    _institution_cache_summaries,
    _openalex_work_rows,
    _page_params,
    _recent_sync_runs,
    _researcher_count,
    _run_full_sync_for_ror,
    bp_works,
)
from app.models import (
    FundingCache,
    FundingCacheRun,
    InstitutionRegistry,
    InstitutionResearcher,
    OpenAlexSyncRun,
    OpenAlexWorkMetadata,
    OpenAlexWorkRawCache,
    ResearcherStatus,
    SyncJob,
    User,
    WorkCache,
    WorkCacheRun,
)
from app.services.institution_registry_service import get_institution_options


class CacheStatusSummaryTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SECRET_KEY="test-key",
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
        )
        db.init_app(self.app)
        babel.init_app(self.app)
        self.app.register_blueprint(bp_works)
        self.app.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")
        self.app.add_url_rule("/", endpoint="main.index", view_func=lambda: "home")

        with self.app.app_context():
            db.create_all()
            institution = InstitutionRegistry(ror_id="01test123", name="Test University")
            other_institution = InstitutionRegistry(ror_id="02test456", name="Other University")
            db.session.add_all([institution, other_institution])
            db.session.flush()

            db.session.add_all([
                InstitutionResearcher(institution_id=institution.id, orcid="0000-0001"),
                WorkCache(
                    ror_id=institution.ror_id,
                    orcid="0000-0001",
                    type="journal-article",
                    doi="https://doi.org/10.1234/Test",
                ),
                FundingCache(ror_id=institution.ror_id, orcid="0000-0002"),
                ResearcherStatus(ror_id=institution.ror_id, orcid="0000-0002"),
                WorkCache(ror_id=other_institution.ror_id, orcid="0000-0003"),
                OpenAlexWorkMetadata(
                    doi_normalized="10.1234/test",
                    openalex_id="W123",
                    title="A matched work",
                ),
                OpenAlexWorkRawCache(
                    doi_normalized="10.1234/test",
                    source_doi="https://doi.org/10.1234/Test",
                    openalex_id="W123",
                    status="found",
                    raw_json={"id": "https://openalex.org/W123"},
                ),
            ])

            account = User(
                username="manager@example.org",
                institution_name="Account-specific institution label",
                ror_id=institution.ror_id,
                is_manager=True,
            )
            account.set_password("test-password")
            db.session.add(account)

            now = datetime.now(timezone.utc).replace(tzinfo=None)
            db.session.add_all([
                WorkCacheRun(
                    ror_id=institution.ror_id,
                    status="success",
                    rows_count=12,
                    started_at=now - timedelta(minutes=5),
                    finished_at=now - timedelta(minutes=4),
                ),
                FundingCacheRun(
                    ror_id=institution.ror_id,
                    status="failed",
                    error="upstream failure",
                    started_at=now - timedelta(minutes=3),
                    finished_at=now - timedelta(minutes=2),
                ),
                OpenAlexSyncRun(
                    ror_id=institution.ror_id,
                    status="running",
                    matched_count=8,
                    started_at=now - timedelta(minutes=1),
                ),
            ])
            db.session.commit()
            self.manager_id = account.id

        self.client = self.app.test_client()

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def test_researcher_count_deduplicates_pairs_across_cache_sources(self):
        with self.app.app_context():
            self.assertEqual(2, _researcher_count("01test123"))
            self.assertEqual(3, _researcher_count())

    def test_recent_runs_share_a_sorted_presentation_shape(self):
        with self.app.app_context():
            runs = _recent_sync_runs("01test123")

        self.assertEqual(["openalex", "fundings", "works"], [run["kind"] for run in runs])
        self.assertEqual("running", runs[0]["status"])
        self.assertEqual(8, runs[0]["records"])
        self.assertEqual(1, runs[1]["errors"])
        self.assertEqual(60, runs[2]["duration_seconds"])

    def test_system_history_includes_unscoped_openalex_jobs(self):
        with self.app.app_context():
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            db.session.add_all([
                OpenAlexSyncRun(
                    ror_id=None,
                    status="running",
                    started_at=now,
                ),
                SyncJob(
                    id="system-openalex-job",
                    name="openalex-system-missing",
                    job_type="openalex_system_sync",
                    ror_id=None,
                    status="queued",
                    result_json={
                        "openalex": {"matched_count": 12},
                        "errors": [],
                    },
                ),
            ])
            db.session.commit()
            runs = _recent_sync_runs("01test123", include_system=True, limit=10)

        system_job = next(run for run in runs if run["id"] == "system-openalex-job")
        self.assertEqual("openalex", system_job["kind"])
        self.assertEqual(12, system_job["records"])

    def test_institution_summaries_group_counts_coverage_and_freshness(self):
        with self.app.app_context():
            summaries = _institution_cache_summaries([
                {"ror_id": "01test123", "name": "Test University"},
                {"ror_id": "02test456", "name": "Other University"},
            ])

        by_ror = {item["ror_id"]: item for item in summaries}
        self.assertEqual(2, by_ror["01test123"]["researchers"])
        self.assertEqual(1, by_ror["01test123"]["works"])
        self.assertEqual(1, by_ror["01test123"]["fundings"])
        self.assertEqual(1, by_ror["01test123"]["openalex_matched"])
        self.assertEqual(100.0, by_ror["01test123"]["openalex_percent"])
        self.assertEqual("attention", by_ror["01test123"]["health"])
        self.assertEqual("attention", by_ror["02test456"]["health"])

    def test_openalex_work_review_filters_searches_and_sorts_on_the_server(self):
        with self.app.app_context():
            matched_work = WorkCache.query.filter_by(ror_id="01test123").first()
            matched_work.title = "Highly cited matched article"
            matched_metadata = OpenAlexWorkMetadata.query.filter_by(openalex_id="W123").first()
            matched_metadata.cited_by_count = 42
            matched_metadata.source_name = "Journal of Testing"

            db.session.add_all([
                WorkCache(
                    ror_id="01test123",
                    orcid="0000-0002",
                    type="journal-article",
                    title="Pending needle article",
                    doi="10.1234/pending",
                ),
                WorkCache(
                    ror_id="01test123",
                    orcid="0000-0003",
                    type="journal-article",
                    title="Article without identifier",
                    doi=None,
                ),
                WorkCache(
                    ror_id="01test123",
                    orcid="0000-0004",
                    type="journal-article",
                    title="Article not found upstream",
                    doi="10.1234/not-found",
                ),
                OpenAlexWorkRawCache(
                    doi_normalized="10.1234/not-found",
                    source_doi="10.1234/not-found",
                    status="not_found",
                ),
            ])
            db.session.commit()

            all_rows, summary, pagination = _openalex_work_rows(
                "01test123", sort="citations", direction="desc"
            )
            pending_rows, _, _ = _openalex_work_rows("01test123", coverage="missing")
            no_doi_rows, _, _ = _openalex_work_rows("01test123", coverage="no_doi")
            not_found_rows, _, _ = _openalex_work_rows("01test123", coverage="not_found")
            search_rows, _, _ = _openalex_work_rows("01test123", search="pending needle")

        self.assertEqual(4, pagination["total_rows"])
        self.assertEqual("Highly cited matched article", all_rows[0]["title"])
        self.assertEqual(1, len(pending_rows))
        self.assertEqual("Pending needle article", pending_rows[0]["title"])
        self.assertEqual(1, len(no_doi_rows))
        self.assertEqual(1, len(not_found_rows))
        self.assertEqual(1, len(search_rows))
        self.assertEqual(
            {"all": 4, "enriched": 1, "missing": 1, "not_found": 1, "no_doi": 1},
            summary["coverage_counts"],
        )

    def test_registry_name_remains_canonical_for_the_top_selector(self):
        with self.app.app_context():
            options = get_institution_options()

        selected = next(item for item in options if item["ror_id"] == "01test123")
        self.assertEqual("Test University", selected["name"])

    def test_active_institution_view_defers_the_system_summary(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="01test123",
            )

        with patch(
            "app.blueprints.works._institution_cache_summaries"
        ) as summaries, patch(
            "app.blueprints.works.render_template", return_value="ok"
        ) as render:
            response = self.client.get("/cache/works/status")

        self.assertEqual(200, response.status_code)
        summaries.assert_not_called()
        self.assertEqual("institution", render.call_args.kwargs["cache_scope"])
        self.assertEqual([], render.call_args.kwargs["institution_summaries"])

    def test_system_summary_is_paginated_on_the_server(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="01test123",
            )

        rows = [
            {
                "ror_id": f"{index:09d}",
                "name": f"Institution {index:02d}",
                "researchers": index,
                "works": index,
                "fundings": index,
                "openalex_percent": float(index),
                "last_update": None,
            }
            for index in range(23)
        ]
        with patch(
            "app.blueprints.works._institution_cache_summaries", return_value=rows
        ) as summaries, patch(
            "app.blueprints.works.render_template", return_value="ok"
        ) as render:
            response = self.client.get(
                "/cache/works/status?scope=system&institution_page=2"
            )

        self.assertEqual(200, response.status_code)
        summaries.assert_called_once()
        context = render.call_args.kwargs
        self.assertEqual("system", context["cache_scope"])
        self.assertEqual(10, len(context["institution_summaries"]))
        self.assertEqual(23, context["institution_pagination"]["total_rows"])
        self.assertEqual(2, context["institution_pagination"]["page"])

    def test_openalex_review_uses_ten_rows_by_default(self):
        with self.app.test_request_context("/openalex/works"):
            self.assertEqual((1, 10), _page_params(default_per_page=10))

    def test_staff_can_download_all_institution_summary(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="01test123",
            )

        response = self.client.get("/download/staff/institutions/cache-summary")

        self.assertEqual(200, response.status_code)
        self.assertIn("institution_cache_summary.csv", response.headers["Content-Disposition"])
        self.assertIn(b"Test University", response.data)
        self.assertIn(b"Other University", response.data)

    def test_staff_can_download_one_institution_without_switching_context(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="02test456",
            )

        response = self.client.get(
            "/download/staff/institution/01test123/works"
        )

        self.assertEqual(200, response.status_code)
        self.assertIn("orcid_works_cache_01test123.csv", response.headers["Content-Disposition"])
        self.assertIn(b"10.1234/Test", response.data)

    def test_manager_can_download_global_works_export(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="01test123",
            )

        response = self.client.get("/download/admin/all-works/cache")

        self.assertEqual(200, response.status_code)
        self.assertIn("orcid_works_all_institutions.csv", response.headers["Content-Disposition"])
        self.assertIn(b"01test123", response.data)
        self.assertIn(b"02test456", response.data)

    def test_manager_can_download_global_openalex_export(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="01test123",
            )

        response = self.client.get("/download/admin/openalex/cache")

        self.assertEqual(200, response.status_code)
        self.assertIn("openalex_articles_all_institutions.csv", response.headers["Content-Disposition"])
        self.assertIn(b"W123", response.data)

    def test_staff_can_queue_refresh_for_any_listed_institution(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="02test456",
            )

        with patch(
            "app.services.background_jobs.submit_background_job",
            return_value="job-123",
        ) as submit_job:
            response = self.client.post(
                "/cache/staff/institution/01test123/build"
            )

        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/cache/works/status?scope=system"))
        self.assertEqual("full-cache-01test123", submit_job.call_args.args[1])
        self.assertEqual("01test123", submit_job.call_args.args[3])

    def test_institution_openalex_sync_is_queued_and_deduplicated(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=True,
                ror_id="01test123",
            )

        with patch(
            "app.services.background_jobs.submit_background_job",
            return_value="openalex-job",
        ) as submit_job:
            response = self.client.post("/openalex/sync", data={"mode": "missing"})

        self.assertEqual(302, response.status_code)
        self.assertEqual("openalex-01test123-missing", submit_job.call_args.args[1])
        self.assertEqual("01test123", submit_job.call_args.args[3])
        self.assertEqual("missing", submit_job.call_args.args[4])
        self.assertTrue(submit_job.call_args.kwargs["deduplicate"])

    def test_system_openalex_sync_is_queued_without_ror_scope(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=True,
                is_manager=True,
                ror_id="01test123",
            )

        with patch(
            "app.services.background_jobs.submit_background_job",
            return_value="system-openalex-job",
        ) as submit_job:
            response = self.client.post(
                "/openalex/sync-system",
                data={"mode": "title"},
            )

        self.assertEqual(302, response.status_code)
        self.assertEqual("openalex-system-title", submit_job.call_args.args[1])
        self.assertIsNone(submit_job.call_args.args[3])
        self.assertEqual("title", submit_job.call_args.args[4])
        self.assertTrue(submit_job.call_args.kwargs["deduplicate"])

    def test_full_institution_refresh_includes_orcid_and_openalex(self):
        call_order = []

        def build_orcid(*args, **kwargs):
            call_order.append("orcid")
            return {"researchers": 4, "profiles": 4, "works": 12, "fundings": 3}

        def build_openalex(**kwargs):
            call_order.append("openalex")
            return {
                "status": "success",
                "works_seen": 8,
                "fetched_count": 8,
                "matched_count": 7,
                "not_found_count": 1,
                "error_count": 0,
                "skipped_count": 0,
                "error": None,
            }

        with self.app.app_context(), patch(
            "app.services.cache_service.build_full_cache_for_ror",
            side_effect=build_orcid,
        ), patch(
            "app.services.openalex_service.sync_openalex_works",
            side_effect=build_openalex,
        ) as openalex_sync:
            result = _run_full_sync_for_ror(
                "01test123",
                "https://pub.orcid.org/v3.0/",
                {"Accept": "application/json"},
            )

        self.assertEqual(["orcid", "openalex"], call_order)
        self.assertEqual(12, result["works"])
        self.assertEqual(3, result["fundings"])
        self.assertEqual(7, result["openalex"]["matched_count"])
        self.assertEqual([], result["errors"])
        openalex_sync.assert_called_once_with(
            ror_id="01test123",
            force_refresh=True,
            stale_days=0,
            articles_only=True,
        )

    def test_standard_user_cannot_download_all_institution_summary(self):
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.manager_id,
                is_admin=False,
                is_manager=False,
                ror_id="01test123",
            )

        response = self.client.get("/download/staff/institutions/cache-summary")

        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/"))


if __name__ == "__main__":
    unittest.main()
