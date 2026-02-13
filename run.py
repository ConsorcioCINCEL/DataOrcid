"""
Module: run.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Main Entry Point of the Application.
    
    This script is responsible for bootstrapping the Flask application instance.
    It manages the transition between the application factory and the 
    underlying WSGI server, handling environment-based configuration.

    Flow:
    1. Import the application factory (create_app).
    2. Instantiate the Flask app.
    3. Configure logging and debug parameters.
    4. Start the server (Dev server or preparation for WSGI).
"""

import os
import logging
from app import create_app

# --- Application Initialization ---
# The create_app() function follows the Application Factory pattern, 
# ensuring that the app is configured according to the TOML file.
app = create_app()

# --- Execution Configuration ---
# Safety first: Debug mode is explicitly disabled unless FLASK_DEBUG is '1'
debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"

# Basic logging configuration for the runner
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)
logger = logging.getLogger(__name__)

# [Image of Flask application factory pattern diagram]

if __name__ == "__main__":
    """
    Local Development Server Execution.
    
    This block only executes when running the script directly (e.g., `python run.py`).
    It is not used when the app is invoked by a production WSGI server.
    
    Environment Variables:
        - HOST: Bind address (Default: 0.0.0.0 for container compatibility).
        - PORT: Listen port (Default: 5000).
        - FLASK_DEBUG: Set to '1' for hot-reloading and detailed error pages.
        
    Deployment Note: 
    For production, use a production-grade WSGI server like Gunicorn or uWSGI:
        $ gunicorn --workers 4 --bind 0.0.0.0:5000 "run:app"
    """
    # Configuration via environment or defaults
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 5000))

    # Log operational status
    logger.info("Starting DataOrcid-Chile Gateway...")
    logger.info("Local URL: http://%s:%s", host, port)
    
    if debug_mode:
        logger.warning("Caution: DEBUG MODE is ACTIVE. Enhanced logging enabled.")

    # Start the Flask development server
    app.run(host=host, port=port, debug=debug_mode)