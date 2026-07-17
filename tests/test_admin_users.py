"""Regression tests for updating distinct users from the admin table."""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from flask import Flask

from app import babel, db
from app.blueprints.admin import bp_admin
from app.models import TrackingLog, User


class AdminUserUpdateTest(unittest.TestCase):
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
        self.app.register_blueprint(bp_admin)

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
        self.assertEqual(["second@example.org"], [user.username for user in captured["users"]])
        self.assertEqual(1, captured["summary"]["total"])


if __name__ == "__main__":
    unittest.main()
