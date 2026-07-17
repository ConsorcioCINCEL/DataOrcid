"""Regression tests for version 2.0 data trust features."""

import unittest

from flask import Flask

from app import db
from app.models import (
    DuplicateProfileReview,
    FundingCache,
    InstitutionRegistry,
    InstitutionResearcher,
    WorkCache,
)
from app.services.canonical_work_service import rebuild_canonical_works
from app.services.data_trust_service import backfill_inferred_associations
from app.services.duplicate_profile_service import _attach_reviews, save_duplicate_review


class DataTrustFeatureTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
        )
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.session.remove()
        db.engine.dispose()
        self.context.pop()

    def test_canonical_outputs_collapse_doi_records_without_merging_untitled_rows(self):
        db.session.add_all([
            WorkCache(ror_id="01test", orcid="0000-0001", put_code=1, title="One", doi="https://doi.org/10.1/ABC", pub_year="2024"),
            WorkCache(ror_id="01test", orcid="0000-0002", put_code=2, title="One copy", doi="doi:10.1/abc", pub_year="2024"),
            WorkCache(ror_id="01test", orcid="0000-0001", put_code=3, title="Fallback title", pub_year="2023"),
            WorkCache(ror_id="01test", orcid="0000-0001", put_code=4, title=None, pub_year=None),
            WorkCache(ror_id="01test", orcid="0000-0001", put_code=5, title=None, pub_year=None),
        ])
        db.session.commit()

        summary = rebuild_canonical_works("01test")

        self.assertEqual(5, summary["source_records"])
        self.assertEqual(4, summary["unique_outputs"])
        self.assertEqual(1, summary["doi_outputs"])

    def test_cache_associations_are_explicitly_inferred_and_preserve_verified_links(self):
        institution = InstitutionRegistry(ror_id="01test", name="Test University")
        db.session.add(institution)
        db.session.flush()
        verified = InstitutionResearcher(
            institution_id=institution.id,
            orcid="0000-0002",
            is_verified=True,
            evidence_type="verified_search",
        )
        db.session.add_all([
            verified,
            WorkCache(ror_id="01test", orcid="0000-0001", put_code=1),
            FundingCache(ror_id="01test", orcid="0000-0001"),
            WorkCache(ror_id="01test", orcid="0000-0002", put_code=2),
        ])
        db.session.commit()

        summary = backfill_inferred_associations("01test")
        inferred = InstitutionResearcher.query.filter_by(orcid="0000-0001").one()

        self.assertEqual(2, summary["associations"])
        self.assertEqual(1, summary["created"])
        self.assertFalse(inferred.is_verified)
        self.assertEqual("cache_inference", inferred.evidence_type)
        self.assertEqual(["funding_cache", "works_cache"], inferred.evidence_sources)
        self.assertTrue(verified.is_verified)
        self.assertEqual("verified_search", verified.evidence_type)

    def test_duplicate_review_remains_separate_from_analysis_cache(self):
        group = {
            "group_key": "stable-key",
            "ror_id": "01test",
            "normalized_name": "ada lovelace",
            "display_name": "Ada Lovelace",
            "confidence": 95,
            "profiles": [{"orcid": "0000-0001"}, {"orcid": "0000-0002"}],
        }
        save_duplicate_review(
            group,
            status="notified",
            reviewer_user_id=7,
            notes="Notice sent by registry staff.",
            notice_message="Please review both ORCID records.",
        )
        report = _attach_reviews({"groups": [dict(group)]})

        self.assertEqual("notified", report["groups"][0]["review"]["status"])
        self.assertEqual(
            "Please review both ORCID records.",
            report["groups"][0]["review"]["notice_message"],
        )
        self.assertEqual(1, report["review_summary"]["notified"])
        self.assertIsNone(DuplicateProfileReview.query.one().selected_orcid)

    def test_dismissed_duplicate_requires_and_preserves_a_structured_reason(self):
        group = {
            "group_key": "dismissed-key",
            "ror_id": "01test",
            "normalized_name": "grace hopper",
            "display_name": "Grace Hopper",
            "confidence": 85,
            "profiles": [{"orcid": "0000-0003"}, {"orcid": "0000-0004"}],
        }

        with self.assertRaises(ValueError):
            save_duplicate_review(
                group,
                status="dismissed",
                reviewer_user_id=7,
            )

        save_duplicate_review(
            group,
            status="dismissed",
            reviewer_user_id=7,
            dismissal_reason="different_people",
            notes="The affiliations and publication histories differ.",
        )
        report = _attach_reviews({"groups": [dict(group)]})

        review = report["groups"][0]["review"]
        self.assertEqual("dismissed", review["status"])
        self.assertEqual("different_people", review["dismissal_reason"])
        self.assertEqual(1, report["review_summary"]["dismissed"])

    def test_legacy_confirmed_status_returns_to_pending_review(self):
        db.session.add(DuplicateProfileReview(
            group_key="legacy-key",
            ror_id="01test",
            normalized_name="legacy researcher",
            status="confirmed",
            selected_orcid="0000-0005",
        ))
        db.session.commit()
        report = _attach_reviews({"groups": [{
            "group_key": "legacy-key",
            "ror_id": "01test",
            "normalized_name": "legacy researcher",
        }]})

        self.assertEqual("pending", report["groups"][0]["review"]["status"])


if __name__ == "__main__":
    unittest.main()
