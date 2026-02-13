"""
Module: update_caches.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Automated Maintenance Bridge.
    
    This script is a standalone utility designed to be executed by system schedulers 
    (like Linux Cron or Windows Task Scheduler). It automates the synchronization 
    of ORCID data by safely invoking the Flask CLI commands within the correct 
    Virtual Environment (VENV).

    Key Features:
    - **VENV Auto-Detection**: Dynamically locates the Python interpreter in 'venv/'.
    - **Context Management**: Sets FLASK_APP and working directory automatically.
    - **Sequential Tasking**: Runs both cache rebuilding and profile name syncing.
    - **Audit Logging**: Captures stdout/stderr for external monitoring.
"""

import os
import sys
import subprocess
import datetime as dt
import logging

# --- Logging Setup for Automated Tasks ---
# Standard output logging is ideal for capturing logs via redirection (e.g., >> sync.log)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    """
    Orchestrates the automated update sequence.
    
    1. Identifies the project structure and the appropriate Python executable.
    2. Sets up the environment variables required by Flask.
    3. Runs the 'rebuild-caches' and 'sync-researcher-names' commands sequentially.
    """
    logger.info("Starting automated ORCID synchronization sequence...")

    # ---------------------------------------------------------
    # 1. Path & Interpreter Detection
    # ---------------------------------------------------------
    # Determine the project root based on this script's location
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Locate the Virtual Environment interpreter to ensure all dependencies are met
    # Compatible with Unix (bin) and Windows (Scripts)
    venv_python = os.path.join(project_root, "venv", "bin", "python")
    if not os.path.exists(venv_python):
        venv_python = os.path.join(project_root, "venv", "Scripts", "python.exe")
    
    if not os.path.exists(venv_python):
        # Fallback to the system's python if VENV is not found
        venv_python = sys.executable
        logger.warning("Virtual Environment not found at expected path. Using: %s", venv_python)

    # ---------------------------------------------------------
    # 2. Environment Preparation
    # ---------------------------------------------------------
    # Move the current process to the project root so Flask can find 'app' and 'config.toml'
    os.chdir(project_root)
    
    # Prepare environment variables for the subprocess
    env_context = os.environ.copy()
    env_context["FLASK_APP"] = "run.py"

    # Define the sequence of Flask CLI commands to execute
    tasks = [
        ["rebuild-caches", "--target", "both"],
        ["sync-researcher-names"]
    ]

    # [Image of a sequence diagram for automated task execution]

    # ---------------------------------------------------------
    # 3. Execution Loop
    # ---------------------------------------------------------
    for task_args in tasks:
        # Construct the full command: venv/bin/python -m flask [task]
        cmd = [venv_python, "-m", "flask"] + task_args
        
        # If user passed arguments to this script, append them to the first task
        if task_args[0] == "rebuild-caches" and len(sys.argv) > 1:
            cmd.extend(sys.argv[1:])
            
        logger.info("Running command: %s", " ".join(cmd))

        try:
            # Execute the task and capture output
            result = subprocess.run(
                cmd, 
                check=False,
                capture_output=True,
                text=True,
                env=env_context
            )

            # Output results for log capturing
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