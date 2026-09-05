"""Regression tests for administrator runtime-error diagnostics."""

from datetime import timedelta
import unittest

from app import create_app, csrf, db
from app.models import SyncJob, SystemError, User, utc_now
from app.services.background_jobs import _run_job
from app.services.error_monitoring import _redact


def _raise_background_failure():
    raise ValueError("background failed api_key=private-value")


class AdminSystemErrorsTest(unittest.TestCase):
    def test_sensitive_diagnostic_text_is_redacted(self):
        source = (
            "postgresql://user:db-secret@localhost/database "
            "https://service.test/path?email=user@example.test&token=query-secret "
            "https://dataorcid.test/oai/institution-secret/harvester-secret?verb=Identify "
            "Authorization: Bearer header-secret "
            "{'password': 'form secret', 'client_secret': \"client-secret\"}"
        )

        redacted = _redact(source, 8_000)

        for secret in (
            "db-secret",
            "user@example.test",
            "query-secret",
            "header-secret",
            "institution-secret",
            "harvester-secret",
            "form secret",
            "client-secret",
        ):
            self.assertNotIn(secret, redacted)
        self.assertGreaterEqual(redacted.count("[REDACTED]"), 5)

    def setUp(self):
        self.app = create_app({
            "TESTING": True,
            "PROPAGATE_EXCEPTIONS": False,
            "SECRET_KEY": "test-secret",
            "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "SQLALCHEMY_TRACK_MODIFICATIONS": False,
            "ERROR_MONITORING_ENABLED": True,
            "ERROR_MONITORING_CAPTURE_IN_TESTS": True,
            "WTF_CSRF_ENABLED": False,
            "JOB_EXECUTION_MODE": "queue",
        })

        @self.app.post("/_test/runtime-error")
        @csrf.exempt
        def runtime_error():
            raise RuntimeError("diagnostic failure token=exception-secret")

        with self.app.app_context():
            db.create_all()
            admin = User(username="admin@example.test", is_admin=True, ror_id="01admin123")
            admin.set_password("test-password")
            standard = User(username="user@example.test", ror_id="01user456")
            standard.set_password("test-password")
            db.session.add_all([admin, standard])
            db.session.commit()
            self.admin_id = admin.id
            self.standard_id = standard.id

        self.client = self.app.test_client()
        self._login(self.client, self.admin_id, "admin@example.test", is_admin=True)

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            engine.dispose()

    @staticmethod
    def _login(client, user_id, username, *, is_admin=False):
        with client.session_transaction() as client_session:
            client_session.clear()
            client_session.update(
                logged_in=True,
                user_id=user_id,
                username=username,
                ror_id="01admin123" if is_admin else "01user456",
                admin_selected_ror="01admin123" if is_admin else None,
                is_admin=is_admin,
                is_manager=False,
                is_oai_user=False,
                locale="en",
            )

    def _add_error(self, event_id="event-one", **values):
        defaults = {
            "fingerprint": "f" * 64,
            "source": "request",
            "severity": "ERROR",
            "logger_name": "app.test",
            "exception_type": "builtins.RuntimeError",
            "message": "Visible diagnostic message",
            "username": "admin@example.test",
            "endpoint": "main.index",
            "method": "GET",
            "path": "/",
            "status_code": 500,
            "request_id": "request-one",
            "occurred_at": utc_now(),
            "is_resolved": False,
        }
        defaults.update(values)
        error = SystemError(event_id=event_id, **defaults)
        db.session.add(error)
        db.session.commit()
        return error

    def test_unhandled_request_error_records_user_view_time_and_sanitized_trace(self):
        response = self.client.post(
            "/_test/runtime-error?token=query-secret",
            data={"password": "form-secret"},
            headers={"X-Request-ID": "caller-controlled"},
        )

        self.assertEqual(500, response.status_code)
        with self.app.app_context():
            error = SystemError.query.one()
            combined_detail = f"{error.message}\n{error.traceback or ''}"
            self.assertEqual("admin@example.test", error.username)
            self.assertEqual(self.admin_id, error.user_id)
            self.assertEqual("01admin123", error.institution_ror)
            self.assertEqual("runtime_error", error.endpoint)
            self.assertEqual("/_test/runtime-error", error.path)
            self.assertEqual("POST", error.method)
            self.assertEqual(500, error.status_code)
            self.assertEqual(response.headers["X-Request-ID"], error.request_id)
            self.assertNotEqual("caller-controlled", error.request_id)
            self.assertEqual("builtins.RuntimeError", error.exception_type)
            self.assertIn("token=[REDACTED]", combined_detail)
            self.assertNotIn("query-secret", combined_detail)
            self.assertNotIn("form-secret", combined_detail)
            self.assertNotIn("exception-secret", combined_detail)

    def test_background_error_keeps_job_and_requester_context(self):
        with self.app.app_context():
            db.session.add(SyncJob(
                id="job-123",
                name="institution export",
                job_type="export",
                requested_by_user_id=self.admin_id,
                ror_id="01admin123",
                status="queued",
                handler="app.services.exports:run",
                payload_json={"args": [], "kwargs": {}},
            ))
            db.session.commit()

        _run_job(self.app, "job-123", _raise_background_failure, (), {})

        with self.app.app_context():
            error = SystemError.query.one()
            job = db.session.get(SyncJob, "job-123")
            self.assertEqual("failed", job.status)
            self.assertEqual("background_job", error.source)
            self.assertEqual("job-123", error.job_id)
            self.assertEqual(self.admin_id, error.user_id)
            self.assertEqual("institution export", error.context_json["job_name"])
            self.assertNotIn("private-value", error.traceback)

    def test_error_dashboard_is_admin_only_filterable_and_escapes_details(self):
        with self.app.app_context():
            self._add_error(
                message="Unsafe <script>alert(1)</script>",
                traceback="Trace <strong>not markup</strong>",
            )

        response = self.client.get("/admin/errors?period=all&status=all&source=request")
        self.assertEqual(200, response.status_code)
        self.assertIn(b"Unsafe", response.data)
        self.assertNotIn(b"<script>alert(1)</script>", response.data)
        self.assertIn(b"&lt;script&gt;alert(1)&lt;/script&gt;", response.data)
        self.assertIn(b"Trace &lt;strong&gt;not markup&lt;/strong&gt;", response.data)

        with self.client.session_transaction() as client_session:
            client_session["locale"] = "es"
        spanish = self.client.get("/admin/errors?period=all&status=all")
        self.assertIn("Errores del sistema".encode(), spanish.data)

        outsider = self.app.test_client()
        self._login(outsider, self.standard_id, "user@example.test")
        denied = outsider.get("/admin/errors")
        self.assertEqual(302, denied.status_code)

    def test_admin_can_resolve_and_reopen_an_error(self):
        with self.app.app_context():
            self._add_error()

        resolved = self.client.post(
            "/admin/errors/event-one/status",
            data={"action": "resolve", "resolution_note": "Fixed in deployment 42."},
        )
        self.assertEqual(302, resolved.status_code)
        with self.app.app_context():
            error = SystemError.query.filter_by(event_id="event-one").one()
            self.assertTrue(error.is_resolved)
            self.assertEqual("admin@example.test", error.resolved_by_username)
            self.assertEqual("Fixed in deployment 42.", error.resolution_note)
            self.assertIsNotNone(error.resolved_at)

        reopened = self.client.post(
            "/admin/errors/event-one/status",
            data={"action": "reopen"},
        )
        self.assertEqual(302, reopened.status_code)
        with self.app.app_context():
            error = SystemError.query.filter_by(event_id="event-one").one()
            self.assertFalse(error.is_resolved)
            self.assertIsNone(error.resolution_note)

    def test_cleanup_command_applies_error_retention(self):
        with self.app.app_context():
            self._add_error(event_id="old-error", occurred_at=utc_now() - timedelta(days=100))
            self._add_error(event_id="recent-error", fingerprint="a" * 64)

        result = self.app.test_cli_runner().invoke(
            args=["cleanup-system-errors", "--days", "90"]
        )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("Deleted 1 system error row", result.output)
        with self.app.app_context():
            self.assertEqual(["recent-error"], [row.event_id for row in SystemError.query.all()])


if __name__ == "__main__":
    unittest.main()
