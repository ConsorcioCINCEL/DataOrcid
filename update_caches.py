"""Scheduled maintenance wrapper for ORCID cache refresh commands."""

import os
import sys
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """Run cache rebuild and profile-name sync through the Flask CLI."""
    logger.info("Starting automated ORCID synchronization sequence...")

    project_root = os.path.dirname(os.path.abspath(__file__))
    venv_python = os.path.join(project_root, "venv", "bin", "python")
    if not os.path.exists(venv_python):
        venv_python = os.path.join(project_root, "venv", "Scripts", "python.exe")
    
    if not os.path.exists(venv_python):
        venv_python = sys.executable
        logger.warning("Virtual Environment not found at expected path. Using: %s", venv_python)

    os.chdir(project_root)

    env_context = os.environ.copy()
    env_context["FLASK_APP"] = "run.py"

    tasks = [
        ["rebuild-caches", "--target", "both"],
        ["sync-researcher-names"]
    ]

    for task_args in tasks:
        cmd = [venv_python, "-m", "flask"] + task_args
        
        if task_args[0] == "rebuild-caches" and len(sys.argv) > 1:
            cmd.extend(sys.argv[1:])
            
        logger.info("Running command: %s", " ".join(cmd))

        try:
            result = subprocess.run(
                cmd, 
                check=False,
                capture_output=True,
                text=True,
                env=env_context
            )

            if result.stdout:
                print(f"\n--- Output of {task_args[0]} ---\n{result.stdout}")
            
            if result.stderr:
                logger.warning("%s generated error output:\n%s", task_args[0], result.stderr)

            if result.returncode == 0:
                logger.info("Successfully completed task: %s", task_args[0])
            else:
                logger.error("Task %s failed with exit code %d.", task_args[0], result.returncode)

        except Exception as exc:
            logger.exception("A critical exception occurred during %s: %s", task_args[0], exc)

    logger.info("Automated synchronization sequence finished.")


if __name__ == "__main__":
    main()
