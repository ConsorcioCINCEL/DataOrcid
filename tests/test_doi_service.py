"""Regression tests for malformed DOI values received from ORCID."""

import unittest

from app.services.doi_service import normalize_doi


class DoiServiceTest(unittest.TestCase):
    def test_normalizes_plain_and_url_dois(self):
        self.assertEqual("10.1234/example", normalize_doi("10.1234/Example"))
        self.assertEqual(
            "10.1234/example",
            normalize_doi("https://doi.org/10.1234/Example."),
        )

    def test_extracts_a_doi_embedded_in_citation_text(self):
        value = (
            "Article title and author names "
            "http://doi.org/10.46652/pacha.v6i19.469"
        )
        self.assertEqual("10.46652/pacha.v6i19.469", normalize_doi(value))

    def test_rejects_a_non_doi_url_mislabeled_as_a_doi(self):
        value = (
            "http://www.academia.edu/31064273/"
            "a_very_long_publication_page_that_is_not_a_doi"
        )
        self.assertIsNone(normalize_doi(value))

    def test_removes_unmatched_closing_parenthesis_from_prose(self):
        self.assertEqual("10.1000/example", normalize_doi("(doi:10.1000/example)"))

    def test_preserves_balanced_parentheses_in_a_doi(self):
        self.assertEqual("10.1000/example(1)", normalize_doi("10.1000/example(1)"))


if __name__ == "__main__":
    unittest.main()
