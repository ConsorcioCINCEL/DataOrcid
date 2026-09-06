"""Regression tests for authenticated self-service profile editing."""

import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.auth import bp_auth
from app.models import User


class AuthProfileTest(unittest.TestCase):
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

    def test_bcrypt_length_limit_is_handled_without_server_error(self):
        too_long = "á" * 37  # 74 UTF-8 bytes despite being only 37 characters.
        with patch("app.blueprints.auth.render_template", return_value="change-password"):
            response = self.client.post(
                "/auth/change-password",
                data={
                    "current_password": "test-password",
                    "new_password": too_long,
                    "confirm_password": too_long,
                },
            )

        self.assertEqual(200, response.status_code)
        with self.app.app_context():
            user = db.session.get(User, self.user_id)
            self.assertTrue(user.check_password("test-password"))
            self.assertFalse(user.check_password("x" * 73))
            with self.assertRaisesRegex(ValueError, "72 UTF-8 bytes"):
                user.set_password("x" * 73)

    def test_password_setting_link_stops_working_after_use(self):
        from app.blueprints.auth import make_password_reset_token
        with self.app.app_context():
            token = make_password_reset_token(db.session.get(User, self.user_id))
        first = self.client.post('/auth/reset-password/' + token, data={
            'new_password': 'new-password-one', 'confirm_password': 'new-password-one',
        })
        second = self.client.post('/auth/reset-password/' + token, data={
            'new_password': 'new-password-two', 'confirm_password': 'new-password-two',
        })
        self.assertEqual(302, first.status_code)
        self.assertEqual(302, second.status_code)
        with self.app.app_context():
            user = db.session.get(User, self.user_id)
            self.assertTrue(user.check_password('new-password-one'))
            self.assertFalse(user.check_password('new-password-two'))

    def test_password_setting_link_expires_after_twenty_four_hours(self):
        import time
        from app.blueprints.auth import make_password_reset_token
        old_time = time.time() - 86500
        with self.app.app_context(), patch('itsdangerous.timed.time.time', return_value=old_time):
            token = make_password_reset_token(db.session.get(User, self.user_id))
        response = self.client.post('/auth/reset-password/' + token, data={
            'new_password': 'expired-link-password', 'confirm_password': 'expired-link-password',
        })
        self.assertEqual(302, response.status_code)
        with self.app.app_context():
            self.assertTrue(db.session.get(User, self.user_id).check_password('test-password'))

    def test_password_recovery_does_not_reveal_email_delivery_failure(self):
        with patch("app.services.email_outbox.send_email", return_value=(False, "SMTP offline")):
            response = self.client.post(
                "/auth/forgot-password",
                data={"email": "old@example.org"},
            )

        self.assertEqual(302, response.status_code)
        from app.models import EmailOutbox
        from app.services.email_outbox import deliver_next_email
        with self.app.app_context(), patch("app.services.email_outbox.send_email", return_value=(False, "SMTP offline")):
            deliver_next_email()
            self.assertEqual("pending", EmailOutbox.query.one().status)
        with self.client.session_transaction() as client_session:
            flashes = client_session.get("_flashes", [])
        self.assertEqual(1, len(flashes))
        self.assertNotIn("could not", flashes[0][1].lower())


if __name__ == "__main__":
    unittest.main()
