"""Conservative PDF selection for OAI-ORE, without network requests."""

import unittest

from app.services.oai_ore_service import select_ore_files


class OaiOreFileSelectionTest(unittest.TestCase):
    @staticmethod
    def _location(url="https://repository.example.org/article.pdf", **values):
        return {
            "pdf_url": url, "is_oa": True, "license": "cc-by",
            "version": "publishedVersion", **values,
        }

    def test_selects_one_version_and_does_not_repeat_mirrored_pdfs(self):
        accepted = self._location("https://repository.example.org/accepted.pdf", version="acceptedVersion")
        published = self._location("https://publisher.example.org/article.pdf")
        files = select_ore_files([accepted, published, dict(published)])
        self.assertEqual([{
            "url": published["pdf_url"], "license": "cc-by", "version": "publishedVersion",
        }], files)

    def test_uses_the_license_of_each_location_not_the_work_or_publisher(self):
        locations = [
            self._location(license=None),
            self._location("https://repository.example.org/accepted.pdf", version="acceptedVersion"),
        ]
        files = select_ore_files(locations)
        self.assertEqual("https://repository.example.org/accepted.pdf", files[0]["url"])
        self.assertEqual("cc-by", files[0]["license"])

    def test_closed_unknown_and_restricted_licenses_do_not_advertise_files(self):
        for license_condition in (None, "", "other-oa", "publisher-specific-oa", "cc-by-nc", "cc-by-nd", "cc-by-nc-nd"):
            with self.subTest(license=license_condition):
                self.assertEqual([], select_ore_files([self._location(license=license_condition)]))
        self.assertEqual([], select_ore_files([self._location(is_oa=False)]))
        for license_condition in ("cc-by", "cc-by-sa", "cc0", "public-domain"):
            with self.subTest(license=license_condition):
                self.assertEqual(license_condition, select_ore_files([self._location(license=license_condition)])[0]["license"])

    def test_rejects_credentials_private_addresses_and_non_http_urls(self):
        for url in (
            None, "", "javascript:alert(1)", "file:///etc/passwd", "ftp://example.org/article.pdf",
            "https://user:password@example.org/article.pdf", "https://example.org:0/file.pdf",
            "http://127.0.0.1/file.pdf", "http://127.1/file.pdf", "http://169.254.169.254/file.pdf",
            "http://10.0.0.1/file.pdf", "http://[::1]/file.pdf", "http://localhost/file.pdf",
            "https://repository.local/file.pdf", "https://repository.internal/file.pdf",
            "https://example.org/file.pdf#fragment", "https://example.org/unsafe\n.pdf",
            "https://example.org/invalid\ud800.pdf", "https://example.org/invalid\uffff.pdf",
            "https://example.org\\@127.0.0.1/file.pdf", "https://example.org/file.pdf?api_key=private",
            "https://example.org/file.pdf?API%5FKEY=private", "https://example.org/file.pdf?access_token=private",
            "https://example.org/file.pdf?X-Amz-Signature=private",
            "https://content.openalex.org/works/W123.pdf",
            "https://content.openalex.org/works/W123.pdf?api_key=private",
        ):
            with self.subTest(url=url):
                self.assertEqual([], select_ore_files([self._location(url)]))

    def test_allows_public_download_parameters_without_inferring_pdf_availability(self):
        url = "https://repository.example.org/download?id=123&format=pdf"
        self.assertEqual(url, select_ore_files([self._location(url)])[0]["url"])
        self.assertEqual([], select_ore_files([self._location(None, landing_page_url=url)]))

    def test_malformed_optional_metadata_does_not_break_metadata_harvesting(self):
        for locations in (None, {}, "invalid", [None, False, "invalid"], [{"license": []}], [self._location(license=[])]):
            with self.subTest(locations=locations):
                self.assertEqual([], select_ore_files(locations))
        files = select_ore_files([self._location(version={})])
        self.assertIsNone(files[0]["version"])


if __name__ == "__main__":
    unittest.main()
