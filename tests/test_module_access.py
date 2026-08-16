"""Regression tests for global module visibility and route enforcement."""

import tempfile
import unittest

from app import create_app, db
from app.models import SyncJob, SystemModule, User
from app.services.module_access import MODULE_KEYS, module_for_endpoint


class ModuleAccessTest(unittest.TestCase):
    def setUp(self):
        self.export_root = tempfile.TemporaryDirectory()
        self.app = create_app({
            "TESTING": True,
            "SECRET_KEY": "test-secret",
            "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "JOB_EXECUTION_MODE": "queue",
            "EXPORT_DIRECTORY": self.export_root.name,
            "WTF_CSRF_ENABLED": False,
        })
        with self.app.app_context():
            db.create_all()
            admin = User(
                username="admin@example.test",
                is_admin=True,
                locale="en",
            )
            admin.set_password("test-password")
            user = User(username="user@example.test", locale="en")
            user.set_password("test-password")
            db.session.add_all([admin, user])
            db.session.commit()
            self.admin_id = admin.id
            self.user_id = user.id

        self.admin = self.app.test_client()
        self.user = self.app.test_client()
        self._login(self.admin, self.admin_id, "admin@example.test", is_admin=True)
        self._login(self.user, self.user_id, "user@example.test")

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            engine.dispose()
        self.export_root.cleanup()

    @staticmethod
    def _login(client, user_id, username, *, is_admin=False):
        with client.session_transaction() as client_session:
            client_session.update(
                logged_in=True,
                user_id=user_id,
                username=username,
                is_admin=is_admin,
                is_manager=False,
                is_oai_user=False,
            )

    def _set_module(self, module_key, enabled):
        with self.app.app_context():
            row = db.session.get(SystemModule, module_key)
            if row is None:
                row = SystemModule(key=module_key)
                db.session.add(row)
            row.is_enabled = enabled
            db.session.commit()

    def test_modules_are_enabled_by_default_and_admin_control_is_recoverable(self):
        response = self.user.get("/")

        self.assertEqual(200, response.status_code)
        self.assertIn('href="/researcher-list"', response.get_data(as_text=True))
        self.assertEqual(200, self.admin.get("/admin/modules").status_code)

    def test_admin_can_disable_module_menu_and_every_direct_browser_access(self):
        enabled = sorted(MODULE_KEYS - {"researchers"})
        response = self.admin.post(
            "/admin/modules",
            data={"enabled": enabled},
        )

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertFalse(db.session.get(SystemModule, "researchers").is_enabled)

        user_page = self.user.get("/researcher-list")
        admin_page = self.admin.get("/researcher-list")
        menu = self.user.get("/").get_data(as_text=True)

        self.assertEqual(403, user_page.status_code)
        self.assertEqual(403, admin_page.status_code)
        self.assertIn("This module is currently unavailable", user_page.get_data(as_text=True))
        self.assertNotIn('href="/researcher-list"', menu)
        self.assertEqual(200, self.admin.get("/admin/modules").status_code)

        restored = self.admin.post(
            "/admin/modules",
            data={"mode": "enable_all"},
        )
        self.assertEqual(302, restored.status_code)
        with self.app.app_context():
            self.assertTrue(db.session.get(SystemModule, "researchers").is_enabled)
        self.assertNotEqual(403, self.user.get("/researcher-list").status_code)
        self.assertIn('href="/researcher-list"', self.user.get("/").get_data(as_text=True))

    def test_disable_all_keeps_core_access_and_module_recovery_available(self):
        response = self.admin.post(
            "/admin/modules",
            data={"mode": "disable_all"},
        )

        self.assertEqual(302, response.status_code)
        self.assertEqual(200, self.user.get("/").status_code)
        self.assertEqual(200, self.admin.get("/admin/modules").status_code)
        self.assertEqual(200, self.admin.get("/auth/profile").status_code)
        menu = self.admin.get("/").get_data(as_text=True)
        self.assertIn('href="/admin/modules"', menu)
        self.assertNotIn('href="/admin/jobs"', menu)
        self.assertNotIn('href="/help/"', menu)
        self.assertNotIn(">People<", menu)
        self.assertNotIn(">Indicators<", menu)
        self.assertNotIn(">Repositories<", menu)
        self.assertNotIn(">Monitoring<", menu)
        self.assertIn("Access and configuration", menu)

    def test_post_json_and_public_provider_access_are_denied(self):
        self._set_module("data_quality", False)
        self._set_module("api_read", False)
        self._set_module("oai_pmh", False)

        form_response = self.admin.post("/data-quality/backfill-associations")
        json_response = self.user.get(
            "/download_orcid?year=2026",
            headers={"Accept": "application/json"},
        )
        provider_response = self.app.test_client().get(
            "/oai/public-key?verb=Identify"
        )

        self.assertEqual(403, form_response.status_code)
        self.assertEqual(403, json_response.status_code)
        self.assertEqual("api_read", json_response.get_json()["module"])
        self.assertEqual(403, provider_response.status_code)
        self.assertEqual("text/plain", provider_response.mimetype)

    def test_disabled_module_hides_and_blocks_existing_export_jobs(self):
        job_id = "00000000-0000-0000-0000-000000000081"
        with self.app.app_context():
            db.session.add(SyncJob(
                id=job_id,
                name="disabled-module-export",
                job_type="export",
                requested_by_user_id=self.user_id,
                status="queued",
                payload_json={
                    "args": [
                        "institution_works",
                        "csv",
                        {"ror_id": None},
                        "Works",
                    ]
                },
            ))
            db.session.add(SystemModule(key="synchronization", is_enabled=False))
            db.session.commit()

        listing = self.user.get("/exports/jobs").get_json()["jobs"]
        status = self.user.get(
            f"/exports/jobs/{job_id}",
            headers={"Accept": "application/json"},
        )
        download = self.user.get(f"/exports/jobs/{job_id}/download")
        new_export = self.user.get(
            "/download/all-works/cache?background=1",
            headers={"Accept": "application/json"},
        )

        self.assertEqual([], listing)
        self.assertEqual(403, status.status_code)
        self.assertEqual("synchronization", status.get_json()["module"])
        self.assertEqual(403, download.status_code)
        self.assertEqual(403, new_export.status_code)

    def test_every_non_core_route_is_assigned_to_a_module(self):
        core_endpoints = {
            "static",
            "main.index",
            "admin.modules",
            "admin.set_ror",
            "auth.login",
            "auth.logout",
            "auth.profile",
            "auth.change_password",
            "auth.forgot_password",
            "auth.reset_password",
            "background_exports.list_exports",
            "background_exports.clear_exports",
            "background_exports.export_status",
            "background_exports.download_export",
        }
        uncovered = {
            rule.endpoint
            for rule in self.app.url_map.iter_rules()
            if rule.endpoint not in core_endpoints
            and module_for_endpoint(rule.endpoint) is None
        }

        self.assertEqual(set(), uncovered)
        self.assertEqual(
            "openalex_enrichment",
            module_for_endpoint(
                "works.download_staff_institution_cache",
                {"dataset_key": "openalex"},
            ),
        )


if __name__ == "__main__":
    unittest.main()
