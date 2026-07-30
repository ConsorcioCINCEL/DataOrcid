"""Role and institution isolation tests for OpenAlex analytics routes."""

import unittest
from unittest.mock import patch

from flask import Flask, Response

from app import babel, db
from app.blueprints.works import bp_works
from app.models import User


class OpenAlexAccessControlTest(unittest.TestCase):
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
        self.app.register_blueprint(bp_works)
        self.app.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")
        self.app.add_url_rule("/", endpoint="main.index", view_func=lambda: "home")

        with self.app.app_context():
            db.create_all()
            standard_user = self._create_user(
                "user@example.org",
                ror_id="01user123",
            )
            manager = self._create_user(
                "manager@example.org",
                ror_id="01manager1",
                is_manager=True,
            )
            admin = self._create_user(
                "admin@example.org",
                ror_id="01admin123",
                is_admin=True,
            )
            db.session.commit()
            self.standard_user_id = standard_user.id
            self.manager_id = manager.id
            self.admin_id = admin.id

        self.client = self.app.test_client()

    def tearDown(self):
        with self.app.app_context():
            engine = db.engine
            db.session.remove()
            db.drop_all()
            db.session.remove()
            engine.dispose()

    @staticmethod
    def _create_user(username: str, **attributes) -> User:
        user = User(username=username, **attributes)
        user.set_password("test-password")
        db.session.add(user)
        return user

    def _login(
        self,
        user_id: int,
        *,
        ror_id: str,
        is_admin: bool = False,
        is_manager: bool = False,
        admin_selected_ror: str | None = None,
    ):
        with self.client.session_transaction() as client_session:
            client_session.clear()
            client_session.update(
                logged_in=True,
                user_id=user_id,
                is_admin=is_admin,
                is_manager=is_manager,
                ror_id=ror_id,
                locale="en",
            )
            if admin_selected_ror:
                client_session["admin_selected_ror"] = admin_selected_ror

    def test_anonymous_user_is_redirected_to_login(self):
        institutional_response = self.client.get("/openalex/analytics")
        comparison_response = self.client.get("/openalex/global")

        self.assertEqual(302, institutional_response.status_code)
        self.assertEqual(302, comparison_response.status_code)
        self.assertTrue(institutional_response.headers["Location"].endswith("/login"))
        self.assertTrue(comparison_response.headers["Location"].endswith("/login"))

    def test_standard_user_analytics_uses_database_institution_assignment(self):
        self._login(
            self.standard_user_id,
            ror_id="02stale456",
            admin_selected_ror="03override7",
        )

        with patch(
            "app.blueprints.works._openalex_institution_analytics_with_cache",
            return_value={"summary": {}},
        ) as analytics_builder, patch(
            "app.blueprints.works.render_template",
            return_value="analytics",
        ):
            response = self.client.get("/openalex/analytics")

        self.assertEqual(200, response.status_code)
        self.assertEqual("01user123", analytics_builder.call_args.args[0])
        with self.client.session_transaction() as client_session:
            self.assertEqual("01user123", client_session["ror_id"])
            self.assertNotIn("admin_selected_ror", client_session)

    def test_standard_user_export_uses_database_institution_assignment(self):
        self._login(self.standard_user_id, ror_id="02stale456")

        with patch(
            "app.blueprints.works._openalex_analytics",
            return_value={"top_cited": []},
        ) as analytics_builder, patch(
            "app.blueprints.works._send_dataframe_export",
            return_value=Response("export", status=200),
        ):
            response = self.client.get("/openalex/analytics/export/top_cited")

        self.assertEqual(200, response.status_code)
        self.assertEqual("01user123", analytics_builder.call_args.args[0])

    def test_standard_user_can_export_priority_sources_for_assigned_institution(self):
        self._login(self.standard_user_id, ror_id="02stale456")
        priority_open_access = {
            "source_rows": [{
                "source_name": "Journal A",
                "source_issn_l": "1111-1111",
                "oa_status_label": "Diamond",
                "articles": 2,
                "citations": 10,
                "average_citations": 5.0,
            }],
        }

        with patch(
            "app.blueprints.works._openalex_analytics",
            return_value={"priority_open_access": priority_open_access},
        ) as analytics_builder, patch(
            "app.blueprints.works._send_dataframe_export",
            return_value=Response("export", status=200),
        ) as export_sender:
            response = self.client.get(
                "/openalex/analytics/export/priority_sources"
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual("01user123", analytics_builder.call_args.args[0])
        export_sender.assert_called_once()

    def test_manager_institutional_analytics_ignores_admin_override(self):
        self._login(
            self.manager_id,
            ror_id="02stale456",
            is_manager=True,
            admin_selected_ror="03override7",
        )

        with patch(
            "app.blueprints.works._openalex_institution_analytics_with_cache",
            return_value={"summary": {}},
        ) as analytics_builder, patch(
            "app.blueprints.works.render_template",
            return_value="analytics",
        ):
            response = self.client.get("/openalex/analytics")

        self.assertEqual(200, response.status_code)
        self.assertEqual("01manager1", analytics_builder.call_args.args[0])

    def test_administrator_can_use_selected_institution_context(self):
        self._login(
            self.admin_id,
            ror_id="01admin123",
            is_admin=True,
            admin_selected_ror="02target45",
        )

        with patch(
            "app.blueprints.works._openalex_institution_analytics_with_cache",
            return_value={"summary": {}},
        ) as analytics_builder, patch(
            "app.blueprints.works.render_template",
            return_value="analytics",
        ):
            response = self.client.get("/openalex/analytics")

        self.assertEqual(200, response.status_code)
        self.assertEqual("02target45", analytics_builder.call_args.args[0])

    def test_standard_user_cannot_open_or_export_global_comparison(self):
        self._login(
            self.standard_user_id,
            ror_id="01user123",
            is_manager=True,
        )

        with patch(
            "app.blueprints.works._openalex_global_analytics_with_cache",
        ) as analytics_builder:
            page_response = self.client.get("/openalex/global")
            export_response = self.client.get(
                "/openalex/global/export/universities"
            )

        self.assertEqual(302, page_response.status_code)
        self.assertEqual(302, export_response.status_code)
        self.assertTrue(page_response.headers["Location"].endswith("/"))
        analytics_builder.assert_not_called()

    def test_role_grant_requires_a_new_authenticated_session(self):
        self._login(
            self.manager_id,
            ror_id="01manager1",
            is_manager=False,
        )

        with patch(
            "app.blueprints.works._openalex_global_analytics_with_cache",
        ) as analytics_builder:
            response = self.client.get("/openalex/global")

        self.assertEqual(302, response.status_code)
        analytics_builder.assert_not_called()

    def test_manager_and_administrator_can_open_global_comparison(self):
        for user_id, ror_id, role_values in (
            (self.manager_id, "01manager1", {"is_manager": True}),
            (self.admin_id, "01admin123", {"is_admin": True}),
        ):
            with self.subTest(user_id=user_id):
                self._login(user_id, ror_id=ror_id, **role_values)
                with patch(
                    "app.blueprints.works._openalex_global_analytics_with_cache",
                    return_value={},
                ) as analytics_builder, patch(
                    "app.blueprints.works.render_template",
                    return_value="comparison",
                ):
                    response = self.client.get("/openalex/global")

                self.assertEqual(200, response.status_code)
                analytics_builder.assert_called_once()

    def test_manager_can_export_global_priority_institution_statistics(self):
        self._login(
            self.manager_id,
            ror_id="01manager1",
            is_manager=True,
        )
        priority_open_access = {
            "institution_rows": [{
                "institution": "Manager University",
                "ror_id": "01manager1",
                "diamond_articles": 2,
                "green_articles": 3,
                "articles": 5,
                "diamond_citations": 10,
                "green_citations": 15,
                "citations": 25,
            }],
        }

        with patch(
            "app.blueprints.works._openalex_global_analytics",
            return_value={"priority_open_access": priority_open_access},
        ) as analytics_builder, patch(
            "app.blueprints.works._send_dataframe_export",
            return_value=Response("export", status=200),
        ) as export_sender:
            response = self.client.get(
                "/openalex/global/export/priority_universities"
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            "open_access",
            analytics_builder.call_args.args[0]["tab"],
        )
        export_sender.assert_called_once()


if __name__ == "__main__":
    unittest.main()
