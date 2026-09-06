"""Regression tests for updating distinct users from the admin table."""

import unittest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.admin import bp_admin
from app.models import EmailOutbox, TrackingLog, User
from app.services.email_outbox import deliver_next_email


class AdminUserUpdateTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "app/templates"))
        self.app.config.update(
            SECRET_KEY="test-key",
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
            MAIL_ENABLED=False,
            BABEL_TRANSLATION_DIRECTORIES=str(Path(__file__).resolve().parents[1] / "app/translations"),
        )
        db.init_app(self.app)
        babel.init_app(self.app)
        self.app.register_blueprint(bp_admin)
        self.app.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")
        self.app.add_url_rule("/reset-password/<token>", endpoint="auth.reset_password", view_func=lambda token: "reset")
        self.app.add_url_rule("/manuals/user-guide/<language>.pdf", endpoint="main.user_manual", view_func=lambda language: "PDF")

        with self.app.app_context():
            db.create_all()
            admin = User(username="admin@example.org", is_admin=True)
            admin.set_password("test-password")
            second = User(username="second@example.org", email="second@example.org")
            second.set_password("test-password")
            db.session.add_all([admin, second])
            db.session.commit()
            self.admin_id = admin.id
            self.second_id = second.id

        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session.update(
                logged_in=True,
                user_id=self.admin_id,
                username="admin@example.org",
                is_admin=True,
                is_manager=False,
            )

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def test_update_route_edits_the_selected_user_and_username(self):
        response = self.client.post(
            f"/admin/users/{self.second_id}/update",
            data={
                "username": "renamed@example.org",
                "email": "renamed@example.org",
                "first_name": "Renamed",
                "locale": "en",
            },
        )

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            selected = db.session.get(User, self.second_id)
            admin = db.session.get(User, self.admin_id)
            self.assertEqual("renamed@example.org", selected.username)
            self.assertEqual("Renamed", selected.first_name)
            self.assertEqual("admin@example.org", admin.username)

    def test_update_route_rejects_another_users_username(self):
        response = self.client.post(
            f"/admin/users/{self.second_id}/update",
            data={"username": "admin@example.org", "locale": "en"},
        )

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            selected = db.session.get(User, self.second_id)
            self.assertEqual("second@example.org", selected.username)

    def test_update_route_accepts_new_locales_and_ignores_unknown_ones(self):
        response = self.client.post(
            f"/admin/users/{self.second_id}/update",
            data={"username": "second@example.org", "locale": "de"},
        )

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertEqual("de", db.session.get(User, self.second_id).locale)

        response = self.client.post(
            f"/admin/users/{self.second_id}/update",
            data={"username": "second@example.org", "locale": "it"},
        )

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertEqual("de", db.session.get(User, self.second_id).locale)

    def test_admin_can_assign_institution_scoped_oai_user_role(self):
        response = self.client.post(
            f"/admin/users/{self.second_id}/update",
            data={
                "username": "second@example.org",
                "email": "second@example.org",
                "institution_name": "Institution A",
                "ror_id": "01aaa1111",
                "is_oai_user": "on",
                "locale": "en",
            },
        )

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            selected = db.session.get(User, self.second_id)
            self.assertTrue(selected.is_oai_user)
            self.assertFalse(selected.is_manager)
            self.assertFalse(selected.is_admin)
            self.assertEqual("01aaa1111", selected.ror_id)

    def test_users_list_paginates_filters_and_summarizes_accounts(self):
        with self.app.app_context():
            extra_users = []
            for index in range(30):
                user = User(
                    username=f"user-{index:02d}@example.org",
                    email=f"user-{index:02d}@example.org",
                    first_name=f"User {index:02d}",
                    institution_name="Institution A" if index < 20 else "Institution B",
                    ror_id="01aaa1111" if index < 20 else "02bbb2222",
                    is_manager=index == 0,
                )
                user.set_password("test-password")
                extra_users.append(user)
            db.session.add_all(extra_users)
            db.session.flush()
            db.session.add(
                TrackingLog(
                    user_id=extra_users[1].id,
                    username=extra_users[1].username,
                    method="GET",
                    path="/",
                    status_code=200,
                    timestamp=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=2),
                )
            )
            db.session.commit()

        captured = {}

        def capture_template(template_name, **context):
            captured.update(context)
            return template_name

        with (
            patch("app.blueprints.admin.render_template", side_effect=capture_template),
            patch(
                "app.blueprints.admin.get_institution_options",
                return_value=[
                    {"ror_id": "01aaa1111", "name": "Institution A", "grid_id": ""},
                    {"ror_id": "02bbb2222", "name": "Institution B", "grid_id": ""},
                ],
            ),
        ):
            response = self.client.get(
                "/admin/users?role=user&institution=01aaa1111&per_page=25&page=1"
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(19, captured["pagination"].total)
        self.assertEqual(19, len(captured["users"]))
        self.assertEqual(32, captured["summary"]["total"])
        self.assertEqual(1, captured["summary"]["admins"])
        self.assertEqual(1, captured["summary"]["managers"])
        self.assertEqual(1, captured["summary"]["active"])

    def test_users_list_can_filter_recent_activity(self):
        with self.app.app_context():
            active = db.session.get(User, self.second_id)
            db.session.add(
                TrackingLog(
                    user_id=active.id,
                    username=active.username,
                    method="GET",
                    path="/admin/users",
                    status_code=200,
                    timestamp=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1),
                )
            )
            db.session.commit()

        captured = {}

        def capture_template(template_name, **context):
            captured.update(context)
            return template_name

        with (
            patch("app.blueprints.admin.render_template", side_effect=capture_template),
            patch("app.blueprints.admin.get_institution_options", return_value=[]),
        ):
            response = self.client.get("/admin/users?activity=active")

        self.assertEqual(200, response.status_code)
        self.assertEqual(["second@example.org"], [user.username for user in captured["users"]])

    def test_update_returns_to_the_filtered_user_page(self):
        response = self.client.post(
            f"/admin/users/{self.second_id}/update",
            data={
                "username": "second@example.org",
                "locale": "en",
                "return_to": "/admin/users?role=user&page=2",
            },
        )

        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/admin/users?role=user&page=2"))

    def test_admin_cannot_delete_their_own_account(self):
        response = self.client.post(f"/admin/users/{self.admin_id}/delete")

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertIsNotNone(db.session.get(User, self.admin_id))

    def test_manager_list_is_scoped_to_their_institution(self):
        with self.app.app_context():
            manager = db.session.get(User, self.admin_id)
            manager.is_admin = False
            manager.is_manager = True
            manager.ror_id = "01aaa1111"
            managed = db.session.get(User, self.second_id)
            managed.ror_id = "01aaa1111"
            outside = User(username="outside@example.org", ror_id="02bbb2222")
            outside.set_password("test-password")
            db.session.add(outside)
            db.session.commit()

        with self.client.session_transaction() as session:
            session.update(
                is_admin=False,
                is_manager=True,
                ror_id="01aaa1111",
            )

        captured = {}

        def capture_template(template_name, **context):
            captured.update(context)
            return template_name

        with patch("app.blueprints.admin.render_template", side_effect=capture_template):
            response = self.client.get("/admin/users")

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {"admin@example.org", "second@example.org"},
            {user.username for user in captured["users"]},
        )
        self.assertEqual(2, captured["summary"]["total"])

    def test_failed_credential_email_keeps_the_existing_password(self):
        with self.app.app_context():
            before = db.session.get(User, self.second_id).password_hash

        with patch("app.services.email_outbox.send_email", return_value=(False, "SMTP offline")):
            response = self.client.post(f"/admin/users/{self.second_id}/send-creds")

        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            after = db.session.get(User, self.second_id).password_hash
        self.assertEqual(before, after)

    def test_new_account_sends_welcome_and_manual_in_recipient_language(self):
        with patch("app.services.email_outbox.send_email", return_value=(True, None)) as sender:
            response = self.client.post("/admin/users/new", data={
                "username": "new@example.org", "email": "new@example.org", "locale": "es",
                "first_name": "Ana", "password": "temporary-test-password",
            })
        self.assertEqual(302, response.status_code)
        with self.app.app_context(), patch("app.services.email_outbox.send_email", return_value=(True, None)) as sender:
            deliver_next_email()
        sender.assert_called_once()
        message = sender.call_args.kwargs
        self.assertEqual("new@example.org", message["to_email"])
        self.assertEqual("Bienvenido a Data ORCID-Chile", message["subject"])
        self.assertIn("/manuals/user-guide/es.pdf", message["text"])
        self.assertNotIn("attachments", message)
        with self.app.app_context():
            self.assertTrue(User.query.filter_by(username="new@example.org").one().check_password("temporary-test-password"))

    def test_failed_welcome_preserves_created_account_for_retry(self):
        with patch("app.services.email_outbox.send_email", return_value=(False, "SMTP offline")):
            response = self.client.post("/admin/users/new", data={"username": "retry@example.org"})
        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertIsNotNone(User.query.filter_by(username="retry@example.org").first())
        with self.app.app_context(), patch("app.services.email_outbox.send_email", return_value=(False, "SMTP offline")):
            deliver_next_email()
            item = EmailOutbox.query.one()
            self.assertEqual("pending", item.status)
            self.assertEqual(1, item.attempts)

    def test_missing_manual_keeps_password_and_does_not_send_credentials(self):
        with self.app.app_context():
            before = db.session.get(User, self.second_id).password_hash
        with tempfile.TemporaryDirectory() as folder:
            self.app.config["USER_MANUAL_DIRECTORY"] = folder
            with patch("app.services.email_outbox.send_email") as sender:
                response = self.client.post(f"/admin/users/{self.second_id}/send-creds")
            sender.assert_not_called()
        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertEqual(before, db.session.get(User, self.second_id).password_hash)

    def test_successful_credential_resend_also_includes_manual(self):
        with patch("app.services.email_outbox.send_email", return_value=(True, None)) as sender:
            response = self.client.post(f"/admin/users/{self.second_id}/send-creds")
        self.assertEqual(302, response.status_code)
        with self.app.app_context(), patch("app.services.email_outbox.send_email", return_value=(True, None)) as sender:
            deliver_next_email()
        self.assertIn("/manuals/user-guide/en.pdf", sender.call_args.kwargs["text"])
        self.assertIn("/reset-password/", sender.call_args.kwargs["text"])
        self.assertNotIn("Temporary Password", sender.call_args.kwargs["text"])


if __name__ == "__main__":
    unittest.main()
