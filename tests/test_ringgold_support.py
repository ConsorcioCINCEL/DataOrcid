"""Regression tests for institutional identifiers and researcher discovery."""

from pathlib import Path
from datetime import datetime
import json
import unittest
from unittest.mock import patch

from flask import Flask
from sqlalchemy import Text

from app import db, datetimeformat
from app.models import (
    FundingCache,
    InstitutionIdentifier,
    InstitutionRegistry,
    InstitutionResearcher,
    ResearcherCache,
    WorkCache,
)
from app.services.cache_service import (
    build_full_cache_for_ror,
    _persist_discovered_researchers,
    _update_researcher_from_profile,
)
from app.services.institution_registry_service import seed_chilean_universities
from app.services.orcid_service import OrcidSearchError, _expanded_search, list_orcids_for_institution


DATASET_PATH = Path(__file__).resolve().parents[1] / "app" / "datasets" / "chilean_universities_ror.json"


class _JsonResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class RinggoldDatasetTest(unittest.TestCase):
    def test_all_bundled_universities_have_unique_validated_ringgold_ids(self):
        payload = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
        institutions = payload["institutions"]
        ringgold_ids = [item["ringgold_id"] for item in institutions]

        self.assertEqual(56, len(institutions))
        self.assertEqual(56, len(set(ringgold_ids)))
        self.assertEqual("2026-07-14", payload["ringgold_validation"]["validated_at"])

        by_ror = {item["ror_id"]: item["ringgold_id"] for item in institutions}
        self.assertEqual("28069", by_ror["00txsqk22"])
        self.assertEqual("28072", by_ror["0184kye93"])
        self.assertEqual("117438", by_ror["027y3mp05"])
        self.assertEqual("153570", by_ror["02e698f45"])
        self.assertEqual("28087", by_ror["01qq57711"])
        self.assertEqual("28049", by_ror["02akpm128"])

    def test_funding_grant_number_accepts_long_external_identifiers(self):
        self.assertIsInstance(FundingCache.__table__.c.grant_number.type, Text)

    def test_work_doi_accepts_long_external_identifiers(self):
        self.assertIsInstance(WorkCache.__table__.c.doi.type, Text)

    def test_utc_timestamps_are_displayed_in_chile_time(self):
        self.assertEqual("2026-07-14 12:34", datetimeformat(datetime(2026, 7, 14, 16, 34)))
        self.assertEqual("2026-01-14 12:34", datetimeformat(datetime(2026, 1, 14, 15, 34)))


class OrcidInstitutionSearchTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            ORCID_SEARCH_URL="https://api.orcid.org/v3.0/",
            ORCID_CLIENT_ID=None,
            ORCID_CLIENT_SECRET=None,
        )

    def test_search_deduplicates_results_and_preserves_match_provenance(self):
        records_by_query = {
            'ror-org-id:"https://ror.org/047gc3g35"': [
                {"orcid-id": "0000-0001-0000-0001", "given-names": "Ada"},
            ],
            'grid-org-id:"grid.443909.3"': [
                {"orcid-id": "0000-0001-0000-0001", "family-names": "Lovelace"},
                {"orcid-id": "0000-0001-0000-0002"},
            ],
            'ringgold-org-id:"14655"': [
                {"orcid-id": "0000-0001-0000-0002"},
                {"orcid-id": "0000-0001-0000-0003"},
            ],
        }

        with self.app.app_context(), patch(
            "app.services.orcid_service.get_client_credentials_token",
            return_value=None,
        ), patch(
            "app.services.orcid_service._expanded_search",
            side_effect=lambda _, query, __, **___: records_by_query[query],
        ):
            results = list_orcids_for_institution(
                "047gc3g35",
                "grid.443909.3",
                ringgold_ids=["14655"],
                grid_ids=["grid.443909.3"],
            )

        by_orcid = {item["orcid-id"]: item for item in results}
        self.assertEqual(3, len(by_orcid))
        self.assertEqual(
            {"ror": ["047gc3g35"], "grid": ["grid.443909.3"]},
            by_orcid["0000-0001-0000-0001"]["matched_identifiers"],
        )
        self.assertEqual(
            {"grid": ["grid.443909.3"], "ringgold": ["14655"]},
            by_orcid["0000-0001-0000-0002"]["matched_identifiers"],
        )

    def test_expanded_search_rejects_an_incomplete_result_set(self):
        responses = [
            _JsonResponse({"num-found": 2, "expanded-result": [{"orcid-id": "one"}]}),
            _JsonResponse({"num-found": 2, "expanded-result": []}),
        ]
        with patch("app.services.orcid_service.safe_get", side_effect=responses):
            with self.assertRaises(OrcidSearchError):
                _expanded_search(
                    "https://pub.orcid.org/v3.0/",
                    'ringgold-org-id:"14655"',
                    {},
                    rows=1,
                    delay=0,
                )

    def test_expanded_search_continues_after_a_short_nonfinal_page(self):
        responses = [
            _JsonResponse({"num-found": 3, "expanded-result": [{"orcid-id": "one"}]}),
            _JsonResponse({
                "num-found": 3,
                "expanded-result": [
                    {"orcid-id": "two"},
                    {"orcid-id": "three"},
                ],
            }),
        ]
        with patch("app.services.orcid_service.safe_get", side_effect=responses):
            results = _expanded_search(
                "https://api.orcid.org/v3.0/",
                'ringgold-org-id:"14655"',
                {},
                rows=1000,
                delay=0,
            )

        self.assertEqual(["one", "two", "three"], [item["orcid-id"] for item in results])


class InstitutionPersistenceTest(unittest.TestCase):
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

    def test_dataset_seed_persists_ringgold_as_verified_identifier(self):
        with self.app.app_context():
            self.assertEqual(56, seed_chilean_universities())
            university = InstitutionRegistry.query.filter_by(ror_id="047gc3g35").one()
            ringgold = InstitutionIdentifier.query.filter_by(
                institution_id=university.id,
                scheme="ringgold",
            ).one()

            self.assertEqual("14655", ringgold.value)
            self.assertTrue(ringgold.is_verified)
            self.assertEqual("orcid-public-api", ringgold.source)

    def test_additional_ringgold_seed_only_updates_existing_registry_rows(self):
        with self.app.app_context():
            makerere = InstitutionRegistry(
                ror_id="03dmz0111",
                name="Makerere University",
                country_code="UG",
                institution_type="university",
                source="users",
                is_active=True,
            )
            db.session.add(makerere)
            db.session.commit()

            seed_chilean_universities()

            identifier = InstitutionIdentifier.query.filter_by(
                institution_id=makerere.id,
                scheme="ringgold",
            ).one()
            self.assertEqual("58588", identifier.value)
            self.assertTrue(identifier.is_verified)

    def test_discovery_keeps_records_without_public_names_or_activities(self):
        with self.app.app_context():
            records = [
                {
                    "orcid-id": "0000-0001-0000-0001",
                    "given-names": None,
                    "family-names": None,
                    "matched_identifiers": {"ringgold": ["14655"]},
                },
                {
                    "orcid-id": "0000-0001-0000-0002",
                    "given-names": "Grace",
                    "matched_identifiers": {"ror": ["047gc3g35"]},
                },
            ]
            institution_id = _persist_discovered_researchers("047gc3g35", records)

            associations = InstitutionResearcher.query.filter_by(
                institution_id=institution_id,
                is_active=True,
            ).all()
            self.assertEqual(2, len(associations))
            self.assertEqual(2, ResearcherCache.query.count())

            unnamed = db.session.get(ResearcherCache, "0000-0001-0000-0001")
            _update_researcher_from_profile(
                unnamed.orcid,
                {"person": {"name": None}},
                {unnamed.orcid: unnamed},
            )
            db.session.commit()
            self.assertIsNone(unnamed.given_names)

    def test_profile_failure_preserves_previous_activity_metadata(self):
        with self.app.app_context():
            institution = InstitutionRegistry(
                ror_id="047gc3g35",
                name="University of Chile",
                country_code="CL",
                institution_type="university",
                source="ror",
                is_active=True,
            )
            db.session.add(institution)
            db.session.flush()
            successful_orcid = "0000-0001-0000-0001"
            failed_orcid = "0000-0001-0000-0002"
            db.session.add_all([
                InstitutionResearcher(
                    institution_id=institution.id,
                    orcid=successful_orcid,
                ),
                InstitutionResearcher(
                    institution_id=institution.id,
                    orcid=failed_orcid,
                ),
                WorkCache(
                    ror_id=institution.ror_id,
                    orcid=successful_orcid,
                    title="Stale successful profile work",
                ),
                WorkCache(
                    ror_id=institution.ror_id,
                    orcid=failed_orcid,
                    title="Preserved failed profile work",
                ),
            ])
            db.session.commit()

            researchers = [
                {"orcid-id": successful_orcid},
                {"orcid-id": failed_orcid},
            ]
            profiles = {
                successful_orcid: {
                    "person": {},
                    "activities-summary": {},
                },
            }
            with patch(
                "app.services.cache_service.discover_researchers_for_ror",
                return_value=(researchers, institution.id),
            ), patch(
                "app.services.cache_service.get_all_profiles_concurrently",
                return_value=profiles,
            ):
                result = build_full_cache_for_ror(
                    institution.ror_id,
                    "https://api.orcid.org/v3.0/",
                    {},
                )

            remaining = WorkCache.query.filter_by(ror_id=institution.ror_id).all()
            self.assertEqual(1, result["works"])
            self.assertEqual([failed_orcid], [row.orcid for row in remaining])


if __name__ == "__main__":
    unittest.main()
