"""Regression tests for version 2.0 data trust features."""

import unittest
from unittest.mock import patch

from flask import Flask
from flask_babel import Babel

from app import db
from app.blueprints.duplicates import (
    _contact_options_for_group,
    _is_valid_email,
    _researcher_notice,
    _researcher_notice_subject,
    bp_duplicates,
)
from app.models import (
    DuplicateProfileReview,
    FundingCache,
    InstitutionRegistry,
    InstitutionResearcher,
    ResearcherCache,
    User,
    WorkCache,
)
from app.services.cache_service import (
    _update_researcher_from_expanded,
    _update_researcher_from_profile,
)
from app.services.canonical_work_service import rebuild_canonical_works
from app.services.data_trust_service import backfill_inferred_associations
from app.services.duplicate_profile_service import _attach_reviews, save_duplicate_review


class DataTrustFeatureTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SECRET_KEY="test-key",
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
        )
        db.init_app(self.app)
        Babel(self.app)
        self.app.register_blueprint(bp_duplicates)
        self.app.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")
        self.app.add_url_rule("/", endpoint="main.index", view_func=lambda: "home")
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.session.remove()
        db.engine.dispose()
        self.context.pop()

    def test_canonical_outputs_collapse_doi_records_without_merging_untitled_rows(self):
        db.session.add_all([
            WorkCache(ror_id="01test", orcid="0000-0001", put_code=1, title="One", doi="https://doi.org/10.1000/ABC", pub_year="2024"),
            WorkCache(ror_id="01test", orcid="0000-0002", put_code=2, title="One copy", doi="doi:10.1000/abc", pub_year="2024"),
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
            notice_subject="Please review your ORCID records",
            recipient_email="ada@example.org",
            recipient_source="orcid_public",
        )
        report = _attach_reviews({"groups": [dict(group)]})

        self.assertEqual("notified", report["groups"][0]["review"]["status"])
        self.assertEqual(
            "Please review both ORCID records.",
            report["groups"][0]["review"]["notice_message"],
        )
        self.assertEqual(
            "Please review your ORCID records",
            report["groups"][0]["review"]["notice_subject"],
        )
        self.assertEqual(
            "ada@example.org",
            report["groups"][0]["review"]["recipient_email"],
        )
        self.assertEqual(
            "orcid_public",
            report["groups"][0]["review"]["recipient_source"],
        )
        self.assertEqual(1, report["review_summary"]["notified"])
        self.assertIsNone(DuplicateProfileReview.query.one().selected_orcid)

        save_duplicate_review(
            group,
            status="resolved",
            reviewer_user_id=7,
        )
        resolved = _attach_reviews({"groups": [dict(group)]})["groups"][0]["review"]
        self.assertEqual("ada@example.org", resolved["recipient_email"])
        self.assertEqual(
            "Please review your ORCID records",
            resolved["notice_subject"],
        )

    def test_public_email_cache_is_cleared_when_orcid_stops_returning_it(self):
        researcher = ResearcherCache(
            orcid="0000-0001",
            email="previously-public@example.org",
        )
        db.session.add(researcher)
        db.session.flush()

        _update_researcher_from_profile(
            researcher.orcid,
            {"person": {"emails": {"email": []}}},
            {researcher.orcid: researcher},
        )
        self.assertIsNone(researcher.email)

        _update_researcher_from_expanded(
            {"orcid-id": researcher.orcid, "email": ["public@example.org"]},
            {researcher.orcid: researcher},
        )
        self.assertEqual("public@example.org", researcher.email)

        _update_researcher_from_expanded(
            {"orcid-id": researcher.orcid, "email": []},
            {researcher.orcid: researcher},
        )
        self.assertIsNone(researcher.email)

    def test_notice_email_helpers_keep_the_manager_in_control(self):
        group = {
            "display_name": "Ada Lovelace",
            "institution_name": "Test University",
            "profiles": [
                {
                    "orcid": "0000-0001",
                    "display_name": "Ada Lovelace",
                    "orcid_url": "https://orcid.org/0000-0001",
                },
                {
                    "orcid": "0000-0002",
                    "display_name": "Ada Lovelace",
                    "orcid_url": "https://orcid.org/0000-0002",
                },
            ],
            "evidence_keys": ["exact_name_match"],
            "shared_dois": [],
        }
        contacts = {
            "0000-0001": {"email": "ada@example.org", "updated_at": "2026-08-10"},
            "0000-0002": {"email": "ADA@example.org", "updated_at": "2026-08-10"},
        }

        options = _contact_options_for_group(group, contacts)
        self.assertEqual(1, len(options))
        self.assertEqual("ada@example.org", options[0]["email"])
        self.assertTrue(_is_valid_email(options[0]["email"]))
        self.assertFalse(_is_valid_email("Ada <ada@example.org>"))

        with self.app.test_request_context("/"):
            subject = _researcher_notice_subject(group)
            notice = _researcher_notice(group)
        self.assertIn("duplicate ORCID", subject)
        self.assertIn("360006896634-I-have-more-than-one-ORCID-iD", notice)
        self.assertIn("https://support.orcid.org/hc/en-us/requests/new", notice)
        self.assertIn("irreversible", notice)

    def test_standard_institution_user_can_prepare_and_record_a_notice(self):
        account = User(
            username="authority@example.org",
            ror_id="01test",
            is_admin=False,
            is_manager=False,
        )
        account.set_password("test-password")
        researcher = ResearcherCache(
            orcid="0000-0001",
            given_names="Ada",
            family_name="Lovelace",
            email="ada@example.org",
        )
        db.session.add_all([account, researcher])
        db.session.commit()

        group = {
            "group_key": "authority-key",
            "ror_id": "01test",
            "institution_name": "Test University",
            "normalized_name": "ada lovelace",
            "display_name": "Ada Lovelace",
            "confidence": 90,
            "confidence_level": "high",
            "profiles": [
                {
                    "orcid": "0000-0001",
                    "display_name": "Ada Lovelace",
                    "orcid_url": "https://orcid.org/0000-0001",
                    "works_count": 1,
                    "fundings_count": 0,
                },
                {
                    "orcid": "0000-0002",
                    "display_name": "Ada Lovelace",
                    "orcid_url": "https://orcid.org/0000-0002",
                    "works_count": 1,
                    "fundings_count": 0,
                },
            ],
            "evidence_keys": ["exact_name_match"],
            "shared_dois": [],
            "works_count": 2,
            "fundings_count": 0,
            "review": {
                "status": "pending",
                "notes": "",
                "notice_message": "",
                "notice_subject": "",
                "recipient_email": "",
            },
        }
        report = {
            "groups": [group],
            "institutions": [],
            "profile_activity": [],
            "summary": {"candidate_groups": 1, "duplicate_profiles": 2},
            "review_summary": {
                "pending": 1,
                "notified": 0,
                "dismissed": 0,
                "resolved": 0,
            },
            "cache": {},
        }
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=account.id,
                is_admin=False,
                is_manager=False,
                ror_id="stale-ror",
            )

        with patch(
            "app.blueprints.duplicates.build_duplicate_report",
            return_value=report,
        ) as report_builder, patch(
            "app.blueprints.duplicates.render_template",
            return_value="duplicates",
        ) as renderer:
            response = self.client.get("/duplicates/?case_status=all")

        self.assertEqual(200, response.status_code)
        self.assertEqual(["01test"], report_builder.call_args.kwargs["ror_ids"])
        rendered_group = renderer.call_args.kwargs["candidate_groups"][0]
        self.assertEqual("ada@example.org", rendered_group["contact_options"][0]["email"])

        with patch(
            "app.blueprints.duplicates.build_duplicate_report",
            return_value=report,
        ), patch(
            "app.blueprints.duplicates.save_duplicate_review",
        ) as review_saver:
            response = self.client.post(
                "/duplicates/review/authority-key?scope=current",
                data={
                    "status": "notified",
                    "notice_subject": "Please review your ORCID records",
                    "notice_message": "Please review both records.",
                    "recipient_email": "ada@example.org",
                },
            )

        self.assertEqual(302, response.status_code)
        self.assertEqual("orcid_public", review_saver.call_args.kwargs["recipient_source"])
        self.assertEqual("ada@example.org", review_saver.call_args.kwargs["recipient_email"])

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
