"""Regression tests for the module-aware User and OAI User help center."""

import unittest

from app import create_app, db
from app.models import SystemModule, User


class HelpCenterTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app({
            "TESTING": True,
            "APP_VERSION": "2.2",
            "SECRET_KEY": "help-test-secret",
            "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "WTF_CSRF_ENABLED": False,
        })
        with self.app.app_context():
            db.create_all()
            accounts = [
                User(username="user@example.test", ror_id="01help123", locale="en"),
                User(
                    username="oai@example.test",
                    ror_id="01help123",
                    locale="en",
                    is_oai_user=True,
                ),
                User(
                    username="manager@example.test",
                    ror_id="01help123",
                    locale="en",
                    is_manager=True,
                ),
                User(
                    username="admin@example.test",
                    ror_id="01help123",
                    locale="en",
                    is_admin=True,
                ),
            ]
            for account in accounts:
                account.set_password("test-password")
            db.session.add_all(accounts)
            db.session.commit()
            self.account_ids = {
                account.username: account.id
                for account in accounts
            }

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            engine.dispose()

    def _client_for(self, username, *, is_admin=False, is_manager=False, is_oai_user=False):
        client = self.app.test_client()
        with client.session_transaction() as client_session:
            client_session.update(
                logged_in=True,
                user_id=self.account_ids[username],
                username=username,
                locale="en",
                is_admin=is_admin,
                is_manager=is_manager,
                is_oai_user=is_oai_user,
            )
        return client

    def _set_modules(self, values):
        with self.app.app_context():
            for key, enabled in values.items():
                row = db.session.get(SystemModule, key)
                if row is None:
                    row = SystemModule(key=key)
                    db.session.add(row)
                row.is_enabled = enabled
            db.session.commit()

    @staticmethod
    def _help_content(response):
        html = response.get_data(as_text=True)
        return html.split('<main class="help-content">', 1)[1].split("</main>", 1)[0]

    def test_help_is_visible_to_every_role_with_the_same_user_facing_content(self):
        clients = [
            self._client_for("user@example.test"),
            self._client_for("oai@example.test", is_oai_user=True),
            self._client_for("manager@example.test", is_manager=True),
            self._client_for("admin@example.test", is_admin=True),
        ]

        responses = [client.get("/help/") for client in clients]
        self.assertTrue(all(response.status_code == 200 for response in responses))
        contents = [self._help_content(response) for response in responses]
        self.assertTrue(all(content == contents[0] for content in contents[1:]))
        self.assertIn("User and OAI User access", contents[0])
        self.assertIn("Version 2.2", contents[0])
        self.assertNotIn('href="/admin/', contents[0])
        self.assertNotIn("Manage accounts and system activity", contents[0])
        self.assertNotIn("Monitor all background jobs", contents[0])
        self.assertNotIn("Review system error diagnostics", contents[0])
        self.assertNotIn("Control global module availability", contents[0])

    def test_disabled_modules_remove_help_blocks_links_and_oai_role_content(self):
        self._set_modules({
            "openalex_analytics": False,
            "openalex_enrichment": False,
            "oai_pmh": False,
            "resources": False,
        })
        client = self._client_for("user@example.test")

        response = client.get("/help/")
        content = self._help_content(response)

        self.assertEqual(200, response.status_code)
        self.assertNotIn("OpenAlex", content)
        self.assertNotIn("OAI-PMH", content)
        self.assertNotIn("OAI User", content)
        self.assertNotIn("OAI-ORE", content)
        self.assertNotIn("DSpace imports metadata but no PDF", content)
        self.assertNotIn('href="/resources"', content)
        self.assertIn("User access and privacy", content)

    def test_topic_route_is_unavailable_when_all_of_its_modules_are_disabled(self):
        self._set_modules({
            "synchronization": False,
            "openalex_enrichment": False,
            "researchers": False,
            "orcid_analytics": False,
            "openalex_analytics": False,
            "duplicates": False,
            "api_read": False,
            "orcid_write": False,
            "affiliation_manager": False,
            "oai_pmh": False,
        })
        client = self._client_for("user@example.test")

        index = client.get("/help/")
        content = self._help_content(index)

        self.assertEqual(404, client.get("/help/topic/synchronization/").status_code)
        self.assertEqual(404, client.get("/help/topic/integrations/").status_code)
        self.assertNotIn('/help/topic/synchronization/', content)
        self.assertNotIn('/help/topic/integrations/', content)

    def test_help_module_itself_still_uses_global_access_enforcement(self):
        self._set_modules({"help": False})
        client = self._client_for("user@example.test")

        self.assertEqual(403, client.get("/help/").status_code)
        self.assertEqual(403, client.get("/help/topic/getting-started/").status_code)

    def test_release_notes_and_integration_help_explain_ore_and_its_limits(self):
        client = self._client_for("user@example.test")
        release = client.get("/help/topic/release-notes/")
        self.assertEqual(200, release.status_code)
        content = self._help_content(release)
        self.assertIn("Version 2.2", content)
        self.assertIn("OAI-ORE adds resource maps", content)
        self.assertNotIn("Version 2.1", content)
        integration = client.get("/help/topic/integrations/")
        self.assertEqual(200, integration.status_code)
        content = self._help_content(integration)
        for term in ("PDF harvesting with OAI-ORE", "Simple Dublin Core", "metadata and bitstream", "cc-by-sa", "metadata-only"):
            self.assertIn(term, content)

    def test_help_release_heading_follows_the_application_version(self):
        self.app.config["APP_VERSION"] = "2.3-test"
        client = self._client_for("user@example.test")
        response = client.get("/help/topic/release-notes/")
        self.assertIn("Version 2.3-test", self._help_content(response))

    def test_ore_help_is_translated_in_all_five_languages(self):
        from pathlib import Path
        import tempfile
        from babel.messages.mofile import write_mo
        from babel.messages.pofile import read_po
        from app import babel, get_locale

        headings = {
            "en": ("Version 2.2", "PDF harvesting with OAI-ORE"),
            "es": ("Versión 2.2", "Recolección de PDF con OAI-ORE"),
            "fr": ("Version 2.2", "Moissonnage de PDF avec OAI-ORE"),
            "pt": ("Versão 2.2", "Coleta de PDFs com OAI-ORE"),
            "de": ("Version 2.2", "PDF-Harvesting mit OAI-ORE"),
        }
        with tempfile.TemporaryDirectory() as directory:
            for language in headings:
                source = Path(__file__).resolve().parents[1] / f"app/translations/{language}/LC_MESSAGES/messages.po"
                target = Path(directory) / language / "LC_MESSAGES/messages.mo"
                target.parent.mkdir(parents=True)
                with source.open("rb") as handle, target.open("wb") as output:
                    write_mo(output, read_po(handle, locale=language))
            self.app.config["BABEL_TRANSLATION_DIRECTORIES"] = directory
            babel.init_app(self.app, locale_selector=get_locale)
            client = self._client_for("user@example.test")
            for language, expected in headings.items():
                with self.subTest(language=language):
                    response = client.get(f"/help/topic/release-notes/?lang={language}")
                    self.assertEqual(200, response.status_code)
                    content = self._help_content(response)
                    for heading in expected:
                        self.assertIn(heading, content)


if __name__ == "__main__":
    unittest.main()
