"""Regression tests for bounded and durable OpenAlex synchronization."""

import unittest
from datetime import timedelta
from unittest.mock import patch

from flask import Flask

from app import db
from app.commands import register_commands
from app.models import OpenAlexWorkRawCache, SyncJob, WorkCache, utc_now
from app.services.background_jobs import submit_background_job
from app.services.openalex_service import (
    _final_sync_status,
    collect_title_match_candidates,
    collect_work_dois,
    should_refresh_raw,
    sync_work_by_doi,
)


class _FailingOpenAlexClient:
    def __init__(self):
        self.calls = 0

    def fetch_work_by_doi(self, doi):
        self.calls += 1
        return {
            "status": "error",
            "http_status": 503,
            "payload": None,
            "error": "Service unavailable",
        }


class OpenAlexSyncResilienceTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            OPENALEX_STALE_DAYS=30,
            OPENALEX_ERROR_RETRY_MINUTES=15,
            OPENALEX_ERROR_RETRY_MAX_HOURS=24,
            TESTING=True,
        )
        db.init_app(self.app)
        register_commands(self.app)
        with self.app.app_context():
            db.create_all()

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def test_transient_errors_use_persistent_backoff(self):
        client = _FailingOpenAlexClient()
        with self.app.app_context():
            first = sync_work_by_doi("10.1234/retry", client=client, stale_days=0)
            raw_row = OpenAlexWorkRawCache.query.filter_by(
                doi_normalized="10.1234/retry"
            ).one()
            next_retry_at = raw_row.next_retry_at
            second = sync_work_by_doi("10.1234/retry", client=client, stale_days=0)

        self.assertEqual("error", first["status"])
        self.assertEqual("skipped", second["status"])
        self.assertEqual(1, client.calls)
        self.assertGreater(next_retry_at, utc_now())

    def test_expired_backoff_and_force_refresh_are_retryable(self):
        with self.app.app_context():
            row = OpenAlexWorkRawCache(
                doi_normalized="10.1234/old-error",
                status="error",
                attempt_count=2,
                next_retry_at=utc_now() - timedelta(minutes=1),
            )
            db.session.add(row)
            db.session.commit()

            self.assertTrue(should_refresh_raw(row, stale_days=0))
            row.next_retry_at = utc_now() + timedelta(hours=1)
            self.assertFalse(should_refresh_raw(row, stale_days=0))
            self.assertTrue(should_refresh_raw(row, stale_days=0, force_refresh=True))

    def test_candidate_limits_are_applied_during_collection(self):
        with self.app.app_context():
            for index in range(8):
                db.session.add(
                    WorkCache(
                        ror_id="01test123",
                        orcid=f"0000-{index:04d}",
                        title=f"Work {index}",
                        type="journal-article",
                        doi=f"10.1234/{index}" if index < 4 else None,
                    )
                )
            db.session.commit()

            works_seen, doi_candidates = collect_work_dois(limit=2)
            title_works_seen, title_candidates = collect_title_match_candidates(limit=3)

        self.assertEqual(8, works_seen)
        self.assertEqual(8, title_works_seen)
        self.assertEqual(2, len(doi_candidates))
        self.assertEqual(3, len(title_candidates))

    def test_system_cli_uses_one_unscoped_run(self):
        summary = {
            "ror_id": None,
            "works_seen": 10,
            "dois_found": 1,
            "workers": 1,
            "fetched_count": 0,
            "matched_count": 0,
            "not_found_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "status": "dry_run",
            "error": None,
        }
        with patch(
            "app.services.openalex_service.sync_openalex_works",
            return_value=summary,
        ) as sync:
            result = self.app.test_cli_runner().invoke(
                args=["sync-openalex-works", "--system", "--dry-run", "--limit", "1"]
            )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(None, sync.call_args.kwargs["ror_id"])
        self.assertEqual(1, sync.call_args.kwargs["limit"])

    def test_mixed_results_are_reported_as_partial(self):
        summary = {
            "matched_count": 2,
            "not_found_count": 1,
            "skipped_count": 0,
            "error_count": 1,
        }
        self.assertEqual("partial", _final_sync_status(summary))
        summary.update(matched_count=0, not_found_count=0)
        self.assertEqual("failed", _final_sync_status(summary))

    def test_duplicate_active_background_job_is_reused(self):
        with self.app.app_context():
            db.session.add(
                SyncJob(
                    id="active-job",
                    name="openalex-system-missing",
                    job_type="openalex_system_sync",
                    status="running",
                )
            )
            db.session.commit()
            with patch("app.services.background_jobs._EXECUTOR.submit") as submit:
                job_id = submit_background_job(
                    self.app,
                    "openalex-system-missing",
                    lambda: None,
                    deduplicate=True,
                )

        self.assertEqual("active-job", job_id)
        submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
