"""Regression tests for authenticated self-service profile editing."""

import unittest
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.auth import bp_auth
from app.models import User


class AuthProfileTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            SECRET_KEY="test-key",
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
        )
        db.init_app(self.app)
        babel.init_app(self.app)
        self.app.register_blueprint(bp_auth)

        with self.app.app_context():
            db.create_all()
            user = User(
                username="profile@example.org",
                email="old@example.org",
                first_name="Old",
                last_name="Name",
                position="Analyst",
                institution_name="Example University",
            )
            user.set_password("test-password")
            db.session.add(user)
            db.session.commit()
            self.user_id = user.id

        self.client = self.app.test_client()
        with self.client.session_transaction() as client_session:
            client_session.update(
                logged_in=True,
                user_id=self.user_id,
                username="profile@example.org",
                first_name="Old",
                display_name="Old Name",
            )

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    def test_profile_update_changes_personal_details_and_session_name(self):
        response = self.client.post(
            "/auth/profile",
            data={
                "username": "cannot-change@example.org",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "position": "Research Director",
                "email": "ada@example.org",
            },
        )

        self.assertEqual(302, response.status_code)
        self.assertTrue(response.headers["Location"].endswith("/auth/profile"))
        with self.app.app_context():
            user = db.session.get(User, self.user_id)
            self.assertEqual("profile@example.org", user.username)
            self.assertEqual("Ada", user.first_name)
            self.assertEqual("Lovelace", user.last_name)
            self.assertEqual("Research Director", user.position)
            self.assertEqual("ada@example.org", user.email)
        with self.client.session_transaction() as client_session:
            self.assertEqual("Ada Lovelace", client_session["display_name"])
            self.assertEqual("ada@example.org", client_session["email"])

    def test_profile_update_rejects_invalid_email_and_keeps_form_values(self):
        captured = {}

        def capture_template(template_name, **context):
            captured.update(context)
            return template_name

        with patch("app.blueprints.auth.render_template", side_effect=capture_template):
            response = self.client.post(
                "/auth/profile",
                data={
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "position": "Research Director",
                    "email": "not-an-email",
                },
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual("not-an-email", captured["form_data"]["email"])
        with self.app.app_context():
            user = db.session.get(User, self.user_id)
            self.assertEqual("Old", user.first_name)
            self.assertEqual("old@example.org", user.email)

    def test_profile_update_enforces_database_field_lengths(self):
        with patch("app.blueprints.auth.render_template", return_value="profile"):
            response = self.client.post(
                "/auth/profile",
                data={"first_name": "A" * 121, "email": "valid@example.org"},
            )

        self.assertEqual(200, response.status_code)
        with self.app.app_context():
            user = db.session.get(User, self.user_id)
            self.assertEqual("Old", user.first_name)

    def test_change_password_view_uses_the_shared_account_summary(self):
        captured = {}

        def capture_template(template_name, **context):
            captured.update(context)
            return template_name

        with patch("app.blueprints.auth.render_template", side_effect=capture_template):
            response = self.client.get("/auth/change-password")

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.user_id, captured["user"].id)


if __name__ == "__main__":
    unittest.main()
