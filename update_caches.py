"""Scheduled end-to-end refresh for ORCID and OpenAlex production data."""

from __future__ import annotations

import fcntl
import logging
import os
from pathlib import Path
import subprocess
import sys


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

LOCK_PATH = Path(
    os.environ.get(
        "ORCID_REFRESH_LOCK",
        "/run/lock/orcid-cincel-cache-refresh.lock",
    )
)

TASKS = (
    (
        "ORCID institutional caches",
        ("rebuild-caches", "--target", "both"),
    ),
    (
        "OpenAlex works",
        (
            "sync-openalex-works",
            "--system",
            "--include-all-types",
        ),
    ),
    (
        "OpenAlex dimensions and analytics",
        (
            "rebuild-openalex-dimensions",
            "--missing-only",
            "--batch-size",
            "100",
        ),
    ),
)


def _python_executable(project_root: Path) -> Path:
    candidates = (
        project_root / "venv" / "bin" / "python",
        project_root / "venv" / "Scripts" / "python.exe",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return Path(sys.executable)


def _acquire_lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = LOCK_PATH.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.close()
        return None
    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(f"{os.getpid()}\n")
    lock_handle.flush()
    return lock_handle


def main() -> int:
    """Run the complete production refresh once, without overlapping jobs."""
    lock_handle = _acquire_lock()
    if lock_handle is None:
        logger.warning(
            "Another ORCID/OpenAlex refresh owns %s; this run will exit.",
            LOCK_PATH,
        )
        return 75

    project_root = Path(__file__).resolve().parent
    python = _python_executable(project_root)
    if python == Path(sys.executable):
        logger.warning(
            "Virtual environment not found at the expected path; using %s.",
            python,
        )

    os.chdir(project_root)
    environment = os.environ.copy()
    environment["FLASK_APP"] = "run.py"
    environment["PYTHONUNBUFFERED"] = "1"

    logger.info("Starting automated ORCID → OpenAlex synchronization.")
    try:
        for task_name, task_arguments in TASKS:
            command = [
                str(python),
                "-m",
                "flask",
                "--app",
                "run.py",
                *task_arguments,
            ]
            logger.info("Starting task: %s", task_name)
            result = subprocess.run(
                command,
                check=False,
                cwd=project_root,
                env=environment,
            )
            if result.returncode != 0:
                logger.error(
                    "Task %s failed with exit code %d; later tasks were skipped.",
                    task_name,
                    result.returncode,
                )
                return result.returncode
            logger.info("Completed task: %s", task_name)
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()

    logger.info("Automated ORCID → OpenAlex synchronization finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
