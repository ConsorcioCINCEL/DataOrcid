"""Protect the complete production cron pipeline and its execution lock."""

import fcntl
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import update_caches as refresh


class ScheduledRefreshTest(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.lock_path = Path(self.directory.name) / "refresh.lock"
        lock_patch = patch.object(refresh, "LOCK_PATH", self.lock_path)
        lock_patch.start()
        self.addCleanup(lock_patch.stop)

    def test_scheduled_run_includes_orcid_openalex_and_integrity(self):
        with patch.object(refresh.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run:
            self.assertEqual(0, refresh.main([]))
        self.assertEqual(
            [
                ["rebuild-caches", "--target", "both"],
                ["sync-openalex-works", "--system", "--include-all-types"],
                ["repair-openalex-integrity"],
            ],
            [call.args[0][5:] for call in run.call_args_list],
        )

    def test_failed_stage_stops_later_stages_and_returns_failure(self):
        with patch.object(refresh.subprocess, "run", side_effect=[
            SimpleNamespace(returncode=0), SimpleNamespace(returncode=17),
        ]) as run:
            self.assertEqual(17, refresh.main([]))
        self.assertEqual(2, run.call_count)

    def test_another_refresh_holding_the_lock_prevents_execution(self):
        with self.lock_path.open("a+") as owner:
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(refresh.subprocess, "run") as run:
                self.assertEqual(75, refresh.main([]))
                run.assert_not_called()

    def test_check_mode_only_invokes_command_help(self):
        with patch.object(refresh.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run:
            self.assertEqual(0, refresh.main(["--check"]))
        self.assertEqual(3, run.call_count)
        self.assertTrue(all(call.args[0][-1] == "--help" for call in run.call_args_list))
