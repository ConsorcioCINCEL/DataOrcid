"""Legacy blueprint registration helper."""

import logging
from flask import session
from .blueprints.auth import bp_auth
from .blueprints.main import bp_main
from .blueprints.export import bp_export
from .blueprints.works import bp_works
from .blueprints.admin import bp_admin
from .blueprints.dashboard import bp_dash
from .blueprints.api_misc import bp_api
from .utils.session_helpers import get_active_ror_id
from . import db

logger = logging.getLogger(__name__)


def init_routes(app):
    """Register blueprints and global template context on an app instance."""
    try:
        app.register_blueprint(bp_auth)
        app.register_blueprint(bp_main)
        app.register_blueprint(bp_export)
        app.register_blueprint(bp_works)
        app.register_blueprint(bp_admin)
        app.register_blueprint(bp_dash)
        app.register_blueprint(bp_api)
        
        logger.info("Application blueprints registered successfully.")
    except Exception as exc:
        logger.exception("CRITICAL: Failed to register application blueprints: %s", exc)

    @app.context_processor
    def inject_global_data():
        """Expose institution switcher and cache freshness data to templates."""
        from .models import WorkCacheRun, User

        institutions = []
        last_works_update = None

        if session.get("is_admin"):
            try:
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

        active_ror = get_active_ror_id()
        if active_ror:
            try:
                last_run = (
                    WorkCacheRun.query.filter_by(ror_id=active_ror, status="success")
                    .order_by(WorkCacheRun.finished_at.desc())
                    .first()
                )
                if last_run:
                    last_works_update = last_run.finished_at or last_run.started_at
            except Exception as exc:
                logger.debug("Could not retrieve cache update date for ROR %s: %s", active_ror, exc)

        return dict(
            institutions=institutions, 
            last_works_update=last_works_update
        )
