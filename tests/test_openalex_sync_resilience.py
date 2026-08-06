"""Regression tests for bounded and durable OpenAlex synchronization."""

import unittest
from datetime import timedelta
from unittest.mock import patch

from flask import Flask

from app import db
from app.commands import register_commands
from app.models import (
    OpenAlexWorkMetadata,
    OpenAlexWorkRawCache,
    SyncJob,
    WorkCache,
    utc_now,
)
from app.services.background_jobs import submit_background_job
from app.services.openalex_service import (
    _final_sync_status,
    collect_title_match_candidates,
    collect_work_dois,
    extract_work_metadata,
    rebuild_openalex_metadata,
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

    def test_extended_work_metadata_is_flattened_for_fast_exports(self):
        payload = {
            "id": "https://openalex.org/W123",
            "title": "Exportable work",
            "ids": {
                "pmid": "https://pubmed.ncbi.nlm.nih.gov/123",
                "pmcid": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC123",
            },
            "biblio": {
                "volume": "12",
                "issue": "3",
                "first_page": "10",
                "last_page": "20",
            },
            "primary_location": {
                "landing_page_url": "https://example.org/work",
                "pdf_url": "https://example.org/work.pdf",
                "license": "cc-by",
                "version": "publishedVersion",
                "source": {
                    "id": "https://openalex.org/S123",
                    "display_name": "Journal",
                    "issn": ["1234-5678", "8765-4321"],
                    "host_organization_name": "Publisher",
                },
            },
            "authorships": [{
                "author": {
                    "id": "https://openalex.org/A123",
                    "display_name": "Ada Researcher",
                    "orcid": "https://orcid.org/0000-0001-0000-0001",
                },
                "is_corresponding": True,
                "countries": ["CL"],
                "raw_affiliation_strings": ["Test University, Chile"],
                "institutions": [{
                    "display_name": "Test University",
                    "ror": "https://ror.org/01test123",
                    "country_code": "CL",
                }],
            }],
            "topics": [{
                "id": "https://openalex.org/T123",
                "display_name": "Research topic",
                "score": 0.9,
            }],
            "keywords": [{
                "id": "https://openalex.org/keywords/research",
                "display_name": "Research",
                "score": 0.8,
            }],
            "sustainable_development_goals": [{
                "id": "https://metadata.un.org/sdg/4",
                "display_name": "Quality education",
                "score": 0.7,
            }],
            "funders": [{
                "id": "https://openalex.org/F123",
                "display_name": "Research Agency",
                "ror": "https://ror.org/02funder1",
            }],
            "awards": [{
                "id": "https://openalex.org/G123",
                "funder_award_id": "GRANT-123",
                "display_name": "Research grant",
                "funder_display_name": "Research Agency",
            }],
            "citation_normalized_percentile": {
                "value": 0.95,
                "is_in_top_1_percent": False,
                "is_in_top_10_percent": True,
            },
            "cited_by_percentile_year": {"min": 95, "max": 99},
            "apc_list": {"value": 1200, "currency": "USD", "value_usd": 1200},
            "apc_paid": {"value": 1000, "currency": "USD", "value_usd": 1000},
            "indexed_in": ["crossref", "doaj"],
            "abstract_inverted_index": {"Test": [0]},
            "has_fulltext": True,
            "referenced_works_count": 25,
            "institutions_distinct_count": 1,
            "countries_distinct_count": 1,
            "locations_count": 2,
            "created_date": "2026-01-01T00:00:00",
        }

        values = extract_work_metadata(payload, "10.1234/export")

        self.assertEqual("A123", values["author_ids"])
        self.assertEqual("Ada Researcher", values["corresponding_author_names"])
        self.assertEqual("Test University", values["institution_names"])
        self.assertEqual("1234-5678; 8765-4321", values["source_issns"])
        self.assertEqual("crossref; doaj", values["indexed_in"])
        self.assertIn("T123 | Research topic | 0.9000", values["topics"])
        self.assertIn("GRANT-123", values["awards"])
        self.assertTrue(values["is_in_top_10_percent"])
        self.assertEqual(1200, values["apc_list_value_usd"])
        self.assertEqual("2026-01-01T00:00:00", values["raw_created_date"])
        self.assertEqual(
            "work:123",
            extract_work_metadata(payload, "work:123")["doi_normalized"],
        )

    def test_metadata_backfill_uses_raw_cache_without_api_calls(self):
        with self.app.app_context():
            db.session.add(OpenAlexWorkRawCache(
                doi_normalized="10.1234/backfill",
                status="found",
                raw_json={
                    "id": "https://openalex.org/WBACKFILL",
                    "title": "Backfilled work",
                    "ids": {"pmid": "pmid:123"},
                    "keywords": [{
                        "id": "https://openalex.org/keywords/test",
                        "display_name": "Test",
                        "score": 0.75,
                    }],
                },
            ))
            db.session.commit()

            summary = rebuild_openalex_metadata(batch_size=1)
            metadata = OpenAlexWorkMetadata.query.filter_by(
                doi_normalized="10.1234/backfill"
            ).one()

        self.assertEqual(1, summary["processed"])
        self.assertEqual("WBACKFILL", metadata.openalex_id)
        self.assertEqual("pmid:123", metadata.pmid)
        self.assertIn("Test", metadata.keywords)

    def test_metadata_backfill_preserves_title_match_cache_keys(self):
        with self.app.app_context():
            db.session.add_all([
                OpenAlexWorkRawCache(
                    doi_normalized="work:42",
                    status="found",
                    raw_json={
                        "id": "https://openalex.org/WTITLE",
                        "title": "Title matched work",
                    },
                ),
                OpenAlexWorkMetadata(
                    doi_normalized="work:42",
                    openalex_id="WTITLE",
                ),
            ])
            db.session.commit()

            rebuild_openalex_metadata(batch_size=1)
            metadata = OpenAlexWorkMetadata.query.filter_by(
                openalex_id="WTITLE"
            ).one()

            self.assertEqual("work:42", metadata.doi_normalized)

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

    def test_oversized_source_doi_is_preserved_without_becoming_a_candidate(self):
        oversized_doi = f"10.1234/{'x' * 1000}"
        with self.app.app_context():
            work = WorkCache(
                ror_id="01test123",
                orcid="0000-0001",
                title="Fallback title",
                type="journal-article",
                doi=oversized_doi,
            )
            db.session.add(work)
            db.session.commit()

            stored = db.session.get(WorkCache, work.id)
            works_seen, doi_candidates = collect_work_dois()
            _title_works_seen, title_candidates = collect_title_match_candidates()

        self.assertEqual(oversized_doi, stored.doi)
        self.assertIsNone(stored.doi_normalized)
        self.assertEqual(1, works_seen)
        self.assertEqual([], doi_candidates)
        self.assertEqual(f"work:{stored.id}", title_candidates[0]["cache_key"])

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
