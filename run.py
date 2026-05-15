"""WSGI entrypoint and local development runner."""

import os
import logging
from app import create_app

app = create_app()

debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 5000))

    logger.info("Starting DataOrcid-Chile Gateway...")
    logger.info("Local URL: http://%s:%s", host, port)
    
    if debug_mode:
        logger.warning("Caution: DEBUG MODE is ACTIVE. Enhanced logging enabled.")

    app.run(host=host, port=port, debug=debug_mode)
