"""
Module: routes.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Routing and Template Context Configuration.
    
    This module acts as the central registry for the application's modular components. 
    It is responsible for:
    1. Registering all functional Blueprints to the Flask application instance.
    2. Injecting global variables into the Jinja2 template engine, ensuring that
       data such as the institution list and cache status are available on every page.
"""

import logging
from flask import session
from .blueprints.auth import bp_auth
from .blueprints.main import bp_main
from .blueprints.export import bp_export
from .blueprints.works import bp_works
from .blueprints.fundings import bp_fund
from .blueprints.admin import bp_admin
from .blueprints.dashboard import bp_dash
from .blueprints.api_misc import bp_api
from . import db

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


def init_routes(app):
    """
    Registers all application blueprints and configures global template context.

    This function is typically called during the Application Factory initialization
    to link all modular routes to the main app context.

    Args:
        app (Flask): The active Flask application instance.
    """
    
    # ---------------------------------------------------------
    # 1. Blueprint Registration
    # ---------------------------------------------------------
    # Each blueprint encapsulates a specific domain of the application.
    try:
        app.register_blueprint(bp_auth)      # Authentication and Session logic
        app.register_blueprint(bp_main)      # Primary navigation and landing pages
        app.register_blueprint(bp_export)    # Data export services (Excel/CSV)
        app.register_blueprint(bp_works)     # Works synchronization management
        app.register_blueprint(bp_fund)      # Funding synchronization management
        app.register_blueprint(bp_admin)     # User and System administration
        app.register_blueprint(bp_dash)      # Analytics and cache dashboards
        app.register_blueprint(bp_api)       # Miscellaneous internal API endpoints
        
        logger.info("Application blueprints registered successfully.")
    except Exception as exc:
        logger.exception("CRITICAL: Failed to register application blueprints: %s", exc)

    # ---------------------------------------------------------
    # 2. Global Template Context Processor
    # ---------------------------------------------------------
    @app.context_processor
    def inject_global_data():
        """
        Injects dynamic variables into all Jinja2 templates automatically.
        
        This avoids having to pass the same data (like the institution list 
        for the sidebar dropdown) in every single route handler.
        
        Returns:
            dict: A dictionary of variables that will be merged into the template context.
        """
        from .models import WorkCacheRun, User

        institutions = []
        last_works_update = None

        # A. Multi-Institutional List (Restricted Access)
        # Only Admins or Managers can switch institutional context.
        if session.get("is_admin") or session.get("is_manager"):
            try:
                # Fetch a distinct list of organizations that have valid ROR IDs
                rows = (
                    db.session.query(User.institution_name, User.ror_id)
                    .filter(User.ror_id.isnot(None), User.institution_name.isnot(None))
                    .distinct()
                    .order_by(User.institution_name.asc())
                    .all()
                )
                institutions = [{"name": inst[0], "ror_id": inst[1]} for inst in rows]
            except Exception as exc:
                logger.error("Context Processor Error: Failed to load institution list: %s", exc)

        # B. Cache Freshness Metadata
        # Identifies the last time data was successfully fetched from ORCID.
        active_ror = session.get("admin_selected_ror") or session.get("ror_id")
        if active_ror:
            try:
                # Query the 'work_cache_run' audit log for the most recent success
                last_run = (
                    WorkCacheRun.query.filter_by(ror_id=active_ror, status="success")
                    .order_by(WorkCacheRun.finished_at.desc())
                    .first()
                )
                if last_run:
                    # Provide either the finish time or start time as a fallback
                    last_works_update = last_run.finished_at or last_run.started_at
            except Exception as exc:
                # Log as debug to avoid noise if the table is empty
                logger.debug("Could not retrieve cache update date for ROR %s: %s", active_ror, exc)

        return dict(
            institutions=institutions, 
            last_works_update=last_works_update
        )