"""Regression tests for the persistent OpenAlex analytics cache."""

import tempfile
import unittest
from collections import OrderedDict
from threading import RLock
from unittest.mock import Mock, patch

from flask import session

from app import create_app
from app.blueprints import works


class OpenAlexAnalyticsCacheTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config.update(TESTING=True, OPENALEX_ANALYTICS_CACHE_TTL=900)
        self.cache_root = tempfile.TemporaryDirectory()
        self.app.instance_path = self.cache_root.name
        self.memory_cache = OrderedDict()
        self.cache_lock = RLock()

    def tearDown(self):
        self.cache_root.cleanup()

    def _request_context(self, query=""):
        return self.app.test_request_context(f"/openalex/analytics{query}")

    def test_cached_analytics_survive_an_empty_memory_cache(self):
        builder = Mock(return_value={"summary": {"works": 12}})
        signature = {"works": {"count": 12, "latest": "2026-07-16"}}

        with patch.object(works, "_openalex_data_signature", return_value=signature):
            with self._request_context():
                session["locale"] = "en"
                first = works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )
                second = works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )

            self.memory_cache.clear()
            with self._request_context():
                session["locale"] = "en"
                persistent = works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )

        self.assertEqual(1, builder.call_count)
        self.assertEqual("database", first["cache"]["layer"])
        self.assertEqual("memory", second["cache"]["layer"])
        self.assertEqual("persistent", persistent["cache"]["layer"])
        self.assertEqual(12, persistent["summary"]["works"])

    def test_refresh_rebuilds_and_replaces_the_cached_value(self):
        builder = Mock(side_effect=[{"value": 1}, {"value": 2}])
        signature = {"works": {"count": 12, "latest": "2026-07-16"}}

        with patch.object(works, "_openalex_data_signature", return_value=signature):
            with self._request_context():
                session["locale"] = "en"
                works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )

            with self._request_context("?refresh_cache=1"):
                session["locale"] = "en"
                refreshed = works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )

        self.assertEqual(2, builder.call_count)
        self.assertEqual(2, refreshed["value"])
        self.assertEqual("database", refreshed["cache"]["layer"])

    def test_presentation_section_does_not_duplicate_the_analytics_cache(self):
        builder = Mock(return_value={"summary": {"works": 12}})
        signature = {"works": {"count": 12, "latest": "2026-07-16"}}

        with patch.object(works, "_openalex_data_signature", return_value=signature):
            with self._request_context("?section=overview"):
                session["locale"] = "en"
                works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )
            with self._request_context("?section=topics"):
                session["locale"] = "en"
                result = works._openalex_analytics_with_cache(
                    "test",
                    {},
                    builder,
                    self.memory_cache,
                    self.cache_lock,
                    ror_id="01test123",
                )

        self.assertEqual(1, builder.call_count)
        self.assertEqual("memory", result["cache"]["layer"])

    def test_global_presentation_tabs_share_the_common_aggregate(self):
        builder = Mock(
            side_effect=lambda filters: {
                "active_tab": filters["tab"],
                "filters": {"tab": filters["tab"]},
            }
        )
        signature = {"works": {"count": 12, "latest": "2026-07-16"}}

        with patch.object(works, "_openalex_data_signature", return_value=signature), patch.object(
            works, "_openalex_global_analytics", builder
        ), patch.object(
            works, "_OPENALEX_GLOBAL_ANALYTICS_CACHE", self.memory_cache
        ), patch.object(
            works, "_OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK", self.cache_lock
        ):
            with self.app.test_request_context("/openalex/global?tab=overview"):
                session["locale"] = "en"
                works._openalex_global_analytics_with_cache({"tab": "overview"})
            with self.app.test_request_context("/openalex/global?tab=universities"):
                session["locale"] = "en"
                result = works._openalex_global_analytics_with_cache({"tab": "universities"})
            with self.app.test_request_context("/openalex/global?tab=open_access"):
                session["locale"] = "en"
                open_access_result = works._openalex_global_analytics_with_cache(
                    {"tab": "open_access"}
                )

        self.assertEqual(1, builder.call_count)
        self.assertEqual("universities", result["active_tab"])
        self.assertEqual("universities", result["filters"]["tab"])
        self.assertEqual("memory", result["cache"]["layer"])
        self.assertEqual("open_access", open_access_result["active_tab"])
        self.assertEqual("open_access", open_access_result["filters"]["tab"])
        self.assertEqual("memory", open_access_result["cache"]["layer"])


if __name__ == "__main__":
    unittest.main()
