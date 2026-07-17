"""Regression tests for the administrator background-job dashboard."""

import unittest
from datetime import timedelta
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.admin import bp_admin
from app.models import SyncJob, SyncJobStep, User, utc_now
from app.services.background_jobs import recover_interrupted_jobs, update_job_progress


class AdminJobsDashboardTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SECRET_KEY="test-key",
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            JOB_STALE_MINUTES=30,
            TESTING=True,
        )
        db.init_app(self.app)
        babel.init_app(self.app)
        self.app.register_blueprint(bp_admin)
        self.app.add_url_rule("/", endpoint="main.index", view_func=lambda: "home")
        self.app.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")

        now = utc_now()
        with self.app.app_context():
            db.create_all()
            admin = User(username="admin@example.org", is_admin=True)
            admin.set_password("test-password")
            manager = User(username="manager@example.org", is_manager=True)
            manager.set_password("test-password")
            db.session.add_all([admin, manager])
            db.session.flush()

            running = SyncJob(
                id="running-job",
                name="openalex-01test123-missing",
                job_type="openalex_institution_sync",
                ror_id="01test123",
                requested_by_user_id=admin.id,
                status="running",
                progress_current=1,
                progress_total=2,
                items_current=50,
                items_total=100,
                progress_unit="candidates",
                created_at=now - timedelta(hours=2),
                started_at=now - timedelta(hours=2),
                heartbeat_at=now - timedelta(hours=1),
            )
            success = SyncJob(
                id="success-job",
                name="full-cache-01test123",
                job_type="full_institution_sync",
                ror_id="01test123",
                requested_by_user_id=admin.id,
                status="success",
                progress_current=2,
                progress_total=2,
                created_at=now - timedelta(hours=3),
                started_at=now - timedelta(hours=3),
                finished_at=now - timedelta(hours=2),
                heartbeat_at=now - timedelta(hours=2),
            )
            failed = SyncJob(
                id="failed-job",
                name="openalex-system-title",
                job_type="openalex_system_sync",
                status="failed",
                error="Upstream failure",
                created_at=now - timedelta(hours=4),
                started_at=now - timedelta(hours=4),
                finished_at=now - timedelta(hours=3),
                heartbeat_at=now - timedelta(hours=3),
            )
            old_success = SyncJob(
                id="old-job",
                name="old-job",
                job_type="generic",
                status="success",
                created_at=now - timedelta(days=10),
            )
            db.session.add_all([running, success, failed, old_success])
            db.session.add_all([
                SyncJobStep(
                    sync_job_id=running.id,
                    name="works",
                    position=1,
                    status="success",
                    records_count=20,
                    started_at=now - timedelta(hours=2),
                    finished_at=now - timedelta(hours=1, minutes=30),
                ),
                SyncJobStep(
                    sync_job_id=running.id,
                    name="openalex",
                    position=2,
                    status="running",
                    started_at=now - timedelta(hours=1, minutes=30),
                ),
            ])
            db.session.commit()
            self.admin_id = admin.id
            self.manager_id = manager.id

        self.client = self.app.test_client()
        self._login(self.admin_id, is_admin=True)

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def _login(self, user_id, *, is_admin=False, is_manager=False):
        with self.client.session_transaction() as session:
            session.clear()
            session.update(
                logged_in=True,
                user_id=user_id,
                is_admin=is_admin,
                is_manager=is_manager,
                locale="en",
            )

    def test_dashboard_reconciles_live_and_period_metrics(self):
        captured = {}

        def capture_template(template_name, **context):
            captured.update(context)
            return template_name

        with patch(
            "app.blueprints.admin.render_template",
            side_effect=capture_template,
        ), patch(
            "app.blueprints.admin.get_institution_options",
            return_value=[{"ror_id": "01test123", "name": "Test University"}],
        ):
            response = self.client.get("/admin/jobs?period=24h")

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {"running": 1, "queued": 0, "successful": 1, "attention": 1, "stale": 1},
            captured["summary"],
        )
        self.assertEqual(3, captured["pagination"].total)
        running = next(job for job in captured["jobs"] if job["id"] == "running-job")
        self.assertEqual(50.0, running["step_percent"])
        self.assertEqual(50.0, running["items_percent"])
        self.assertEqual("OpenAlex enrichment", running["current_step"])
        self.assertTrue(running["is_stale"])

    def test_fragment_returns_refreshable_html_and_summary(self):
        with patch(
            "app.blueprints.admin.render_template",
            return_value="<section>jobs</section>",
        ), patch("app.blueprints.admin.get_institution_options", return_value=[]):
            response = self.client.get("/admin/jobs?fragment=1&period=24h")

        payload = response.get_json()
        self.assertEqual(200, response.status_code)
        self.assertEqual("<section>jobs</section>", payload["html"])
        self.assertEqual(1, payload["summary"]["running"])
        self.assertTrue(payload["has_active"])

    def test_status_filter_limits_the_history_table(self):
        captured = {}
        with patch(
            "app.blueprints.admin.render_template",
            side_effect=lambda template_name, **context: captured.update(context) or template_name,
        ), patch("app.blueprints.admin.get_institution_options", return_value=[]):
            response = self.client.get("/admin/jobs?status=failed&period=24h")

        self.assertEqual(200, response.status_code)
        self.assertEqual(["failed-job"], [job["id"] for job in captured["jobs"]])

    def test_manager_cannot_access_the_job_dashboard(self):
        self._login(self.manager_id, is_manager=True)
        response = self.client.get("/admin/jobs")
        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/"))

    def test_item_progress_is_persisted_separately_from_steps(self):
        with self.app.app_context():
            update_job_progress("running-job", 75, 100, "candidates")
            job = db.session.get(SyncJob, "running-job")
            self.assertEqual(75, job.items_current)
            self.assertEqual(100, job.items_total)
            self.assertEqual("candidates", job.progress_unit)
            self.assertEqual(1, job.progress_current)

    def test_stale_recovery_closes_running_steps(self):
        with self.app.app_context():
            recovered = recover_interrupted_jobs(stale_minutes=30)
            job = db.session.get(SyncJob, "running-job")
            steps = {
                step.name: step
                for step in SyncJobStep.query.filter_by(sync_job_id=job.id).all()
            }

            self.assertEqual(1, recovered)
            self.assertEqual("interrupted", job.status)
            self.assertEqual("interrupted", steps["openalex"].status)
            self.assertIsNotNone(steps["openalex"].finished_at)


if __name__ == "__main__":
    unittest.main()
