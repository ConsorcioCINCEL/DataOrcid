"""Regression tests for derived OpenAlex analytics facts."""

import unittest

from flask import Flask

from app import db
from app.models import (
    AnalyticsDataVersion,
    OpenAlexInstitutionWorkFact,
    OpenAlexWorkAuthor,
    OpenAlexWorkInstitution,
    OpenAlexWorkMetadata,
    OpenAlexWorkRawCache,
    WorkCache,
)
from app.services.analytics_service import refresh_openalex_facts


class OpenAlexAnalyticsFactTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
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

    def test_refresh_deduplicates_works_and_materializes_affiliation_flags(self):
        with self.app.app_context():
            db.session.add_all([
                WorkCache(
                    ror_id="01test123",
                    orcid="0000-0001",
                    title="First copy",
                    type="journal-article",
                    doi="https://doi.org/10.1234/Example",
                ),
                WorkCache(
                    ror_id="01test123",
                    orcid="0000-0002",
                    title="Second copy",
                    type="journal-article",
                    doi="doi:10.1234/example",
                ),
            ])
            db.session.add(
                OpenAlexWorkRawCache(
                    doi_normalized="10.1234/example",
                    status="found",
                )
            )
            db.session.add(
                OpenAlexWorkMetadata(
                    doi_normalized="10.1234/example",
                    openalex_id="W1",
                    title="Example",
                    publication_year=2024,
                    type="article",
                    language="es",
                    cited_by_count=12,
                    fwci=1.5,
                    is_oa=True,
                    oa_status="diamond",
                )
            )
            db.session.add_all([
                OpenAlexWorkInstitution(
                    doi_normalized="10.1234/example",
                    institution_id="I1",
                    ror_id="01test123",
                    institution_name="Selected",
                    country_code="CL",
                ),
                OpenAlexWorkInstitution(
                    doi_normalized="10.1234/example",
                    institution_id="I2",
                    ror_id="02other",
                    institution_name="International",
                    country_code="US",
                ),
                OpenAlexWorkAuthor(
                    doi_normalized="10.1234/example",
                    author_id="A1",
                    author_name="Researcher",
                ),
            ])
            db.session.commit()

            summary = refresh_openalex_facts("01test123")
            fact = OpenAlexInstitutionWorkFact.query.one()
            scope_version = db.session.get(
                AnalyticsDataVersion,
                "openalex:ror:01test123",
            )
            global_version = db.session.get(
                AnalyticsDataVersion,
                "openalex:global",
            )

        self.assertEqual(2, summary["source_records"])
        self.assertEqual(1, summary["rows"])
        self.assertEqual(2, fact.source_record_count)
        self.assertTrue(fact.has_valid_doi)
        self.assertTrue(fact.has_selected_affiliation)
        self.assertTrue(fact.has_chile_affiliation)
        self.assertTrue(fact.has_non_chile_affiliation)
        self.assertTrue(fact.has_international_collaboration)
        self.assertEqual(1, fact.author_count)
        self.assertEqual(2, fact.institution_count)
        self.assertEqual(1, scope_version.version)
        self.assertEqual(1, global_version.version)


if __name__ == "__main__":
    unittest.main()
