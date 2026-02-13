"""
Module: __init__.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Application Factory and Entry Point.
    
    This module initializes the Flask application instance. It is responsible for:
    1. Loading configuration from TOML files (supporting environment overrides).
    2. Initializing Flask extensions (SQLAlchemy, Migrate, Babel).
    3. Registering Blueprints (modular application components).
    4. Setting up global middleware (Request Tracking) and template filters.
"""

import os
import toml
import datetime as dt
from pathlib import Path
from flask import Flask, session, request, g
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_babel import Babel

# --- Global Extensions ---
# These are initialized here but attached to the app inside the factory function.
db = SQLAlchemy()
migrate = Migrate()
babel = Babel()


# ============================================================
# I18N AND LOCALIZATION
# ============================================================

def get_locale() -> str:
    """
    Determines the best language (locale) for the current request.
    
    Selection Priority:
    1. URL Query Parameter: '?lang=en' (Overrides everything and saves to session/DB).
    2. Session: Use the language previously selected by the user.
    3. User Profile: Use the language stored in the user's database settings.
    4. Default: Fallback to the system default (usually 'es').
    
    Returns:
        str: The selected language code (e.g., 'es', 'en').
    """
    # Import current_app inside function to avoid circular import at module level
    from flask import current_app
    
    supported = current_app.config.get('LANGUAGES', ['es', 'en'])
    
    # 1. Check for URL override (e.g., switching language via flag icon)
    lang_arg = request.args.get('lang')
    if lang_arg in supported:
        session['locale'] = lang_arg
        # If user is logged in, persist this preference to the database
        if session.get('user_id'):
            try:
                from .models import User
                user = User.query.get(session['user_id'])
                if user:
                    user.locale = lang_arg
                    db.session.commit()
            except Exception:
                db.session.rollback()
        return lang_arg

    # 2. Check Session (Transient preference)
    if session.get('locale'):
        return session['locale']
    
    # 3. Check User Database Profile (Persistent preference)
    if session.get('user_id'):
        try:
             from .models import User
             user = User.query.get(session['user_id'])
             if user and user.locale in supported:
                 session['locale'] = user.locale
                 return user.locale
        except Exception:
            pass

    # 4. Fallback to System Default
    return current_app.config.get('BABEL_DEFAULT_LOCALE', 'es')


# ============================================================
# JINJA CUSTOM FILTERS
# ============================================================

def datetimeformat(value, format: str = "%Y-%m-%d %H:%M") -> str:
    """
    Template filter to format datetime objects or ISO strings into readable text.
    Usage in templates: {{ my_date | datetimeformat }}
    """
    if not value:
        return ""
    if isinstance(value, (dt.datetime, dt.date)):
        return value.strftime(format)
    
    # Attempt to parse varied string formats (ISO 8601, SQL standard, etc.)
    s = str(value)
    fmts = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in fmts:
        try:
            return dt.datetime.strptime(s, fmt).strftime(format)
        except ValueError:
            continue
    return s


def timestamp_to_date(value) -> str:
    """
    Template filter to convert Unix millisecond timestamps to readable dates.
    Useful for JavaScript-generated timestamps.
    """
    try:
        if not value:
            return "N/A"
        ms = int(value)
        return dt.datetime.fromtimestamp(ms / 1000.0).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, Exception):
        return "N/A"


# ============================================================
# APPLICATION FACTORY
# ============================================================

def create_app() -> Flask:
    """
    Initializes and configures the Flask application.
    
    This function sets up the entire application context, including:
    - Configuration loading (config.toml).
    - Database connection.
    - Security headers.
    - API clients (ORCID, ROR).
    - Blueprint registration.
    
    Returns:
        Flask: The fully configured application instance.
    """
    app = Flask(__name__)

    # ---------------------------------------------------------
    # 1. Configuration Loading (TOML)
    # ---------------------------------------------------------
    # Logic to locate 'config.toml' in the project root or a 'config' subdirectory
    project_root = Path(__file__).resolve().parent.parent
    cfg_env = os.environ.get("ORCID_APP_CONFIG")
    cfg_path = Path(cfg_env) if cfg_env else project_root / "config.toml"

    # Fallback path if root config is missing
    if not cfg_path.is_file():
         cfg_path = project_root / "config" / "config.toml"

    if not cfg_path.is_file():
        raise FileNotFoundError(f"CRITICAL: Missing configuration file at: {cfg_path}")

    config_data = toml.load(str(cfg_path))
    # Preserve the full nested structure in app.config
    app.config.update(config_data)

    # ---------------------------------------------------------
    # 2. Database & Security Setup
    # ---------------------------------------------------------
    app.config["SQLALCHEMY_DATABASE_URI"] = config_data["database"]["uri"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    
    flask_sec = config_data.get("flask", {})
    app.config["SECRET_KEY"] = flask_sec.get("secret_key", "dev_key_only")
    app.config["SECURITY_PASSWORD_SALT"] = flask_sec.get("password_salt", "CHANGE_ME_SALT")
    
    # Cookie Security Settings (Essential for production)
    app.config["SESSION_COOKIE_SECURE"] = flask_sec.get("session_cookie_secure", False)
    app.config["SESSION_COOKIE_HTTPONLY"] = flask_sec.get("session_cookie_httponly", True)
    app.config["SESSION_COOKIE_SAMESITE"] = flask_sec.get("session_cookie_samesite", "Lax")
    
    app.config["APP_BASE_URL"] = config_data.get("app", {}).get("base_url", "").rstrip("/")

    # ---------------------------------------------------------
    # 3. Extension Configuration
    # ---------------------------------------------------------
    # Localization
    app.config["LANGUAGES"] = config_data.get("languages", {}).get("supported", ["es", "en"])
    app.config["BABEL_DEFAULT_LOCALE"] = config_data.get("languages", {}).get("default", "es")

    # ORCID API Credentials
    orcid_cfg = config_data.get("orcid", {})
    api_cfg = config_data.get("api", {})

    app.config.update(
        ORCID_CLIENT_ID=orcid_cfg.get("client_id"),
        ORCID_CLIENT_SECRET=orcid_cfg.get("client_secret"),
        ORCID_TOKEN_URL=orcid_cfg.get("token_url", "https://orcid.org/oauth/token"),
        # Map generic API keys to specific internal config names
        ORCID_MEMBER_URL=api_cfg.get("members_url") or orcid_cfg.get("base_url_member") or "https://api.orcid.org/v3.0/",
        ORCID_SEARCH_URL=api_cfg.get("search_url") or orcid_cfg.get("base_url_public") or "https://pub.orcid.org/v3.0/"
    )

    # File System Paths
    datasets_cfg = config_data.get("paths", {}).get("datasets_dir", "app/datasets")
    datasets_dir = Path(datasets_cfg)
    if not datasets_dir.is_absolute(): 
        datasets_dir = project_root / datasets_dir
    app.config["DATASETS_DIR"] = str(datasets_dir)

    # SMTP Mail Settings
    mail_cfg = config_data.get("mail", {})
    app.config.update(
        MAIL_ENABLED=bool(mail_cfg.get("enabled", False)),
        MAIL_SERVER=mail_cfg.get("smtp_host"),
        MAIL_PORT=int(mail_cfg.get("smtp_port", 587)),
        MAIL_USE_TLS=bool(mail_cfg.get("use_tls", True)),
        MAIL_USE_SSL=bool(mail_cfg.get("use_ssl", False)),
        MAIL_USERNAME=mail_cfg.get("smtp_user"),
        MAIL_PASSWORD=mail_cfg.get("smtp_pass"),
        MAIL_DEFAULT_SENDER=(mail_cfg.get("from_name", "DataOrcid"), mail_cfg.get("from_email")),
    )

    # ---------------------------------------------------------
    # 4. Template & Context Processors
    # ---------------------------------------------------------
    app.jinja_env.filters["datetimeformat"] = datetimeformat
    app.jinja_env.filters["timestamp_to_date"] = timestamp_to_date

    @app.context_processor
    def inject_global_template_vars():
        """
        Injects variables available globally in all Jinja templates.
        Specifically handles the 'Institution Switcher' logic for Admins.
        """
        institutions = []
        try:
            # Only fetch institution list for Admins/Managers to populate the dropdown
            if session.get("logged_in") and (session.get("is_admin") or session.get("is_manager")):
                from .models import User
                rows = (
                    db.session.query(User.ror_id, User.institution_name)
                    .filter(User.ror_id.isnot(None), User.ror_id != "")
                    .distinct()
                    .all()
                )
                
                seen_rors = set()
                for ror, name in rows:
                    if ror not in seen_rors:
                        institutions.append({"ror_id": ror, "name": (name or ror)})
                        seen_rors.add(ror)

                # Ensure current user's institution is included
                my_ror = session.get("ror_id")
                if my_ror and my_ror not in seen_rors:
                    institutions.append({"ror_id": my_ror, "name": session.get("institution_name") or my_ror})

                # Default selection if none exists
                if institutions and not session.get("admin_selected_ror") and my_ror:
                    session["admin_selected_ror"] = my_ror
        except Exception:
            pass 

        return {
            "current_year": dt.datetime.now().year,
            "institutions": institutions
        }

    # ---------------------------------------------------------
    # 5. Middleware (Request Tracking)
    # ---------------------------------------------------------
    EXCLUDED_LOG_PATHS = ("/static/", "/favicon.ico", "/robots.txt", "/health")

    @app.before_request
    def track_request_start():
        """Marks the start time of a request for duration calculation."""
        g._start_time = dt.datetime.utcnow()

    @app.after_request
    def track_request_end(response):
        """
        Logs user activity and request performance to the 'TrackingLog' table.
        Skips static assets to avoid database bloat.
        """
        try:
            if request.path.startswith(EXCLUDED_LOG_PATHS):
                return response
            
            duration = None
            if hasattr(g, "_start_time"):
                # Calculate execution time in milliseconds
                duration = (dt.datetime.utcnow() - g._start_time).total_seconds() * 1000

            from .models import TrackingLog
            log_entry = TrackingLog(
                user_id=session.get("user_id"),
                username=session.get("username"),
                method=request.method,
                path=request.path,
                status_code=response.status_code,
                ip=request.headers.get("X-Forwarded-For", request.remote_addr),
                user_agent=(request.user_agent.string or "")[:250],
                duration_ms=duration,
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as exc:
            app.logger.error("Request tracking failed: %s", exc)
        return response

    # ---------------------------------------------------------
    # 6. Initialization & Registration
    # ---------------------------------------------------------
    db.init_app(app)
    migrate.init_app(app, db)
    babel.init_app(app, locale_selector=get_locale)

    with app.app_context():
        # Ensure database tables exist
        db.create_all()

        # Register Blueprints (Modular Routes)
        from .blueprints.main import bp_main
        from .blueprints.cache_control import bp_cache
        from .blueprints.admin import bp_admin
        from .blueprints.auth import bp_auth
        from .blueprints.works import bp_works
        from .blueprints.fundings import bp_fund
        from .blueprints.export import bp_export
        from .blueprints.dashboard import bp_dash
        from .blueprints.api_misc import bp_api

        app.register_blueprint(bp_main)
        app.register_blueprint(bp_cache)
        app.register_blueprint(bp_admin)
        app.register_blueprint(bp_auth)
        app.register_blueprint(bp_works)
        app.register_blueprint(bp_fund)
        app.register_blueprint(bp_export)
        app.register_blueprint(bp_dash)
        app.register_blueprint(bp_api)

        # Register CLI Commands
        try:
            from .commands import register_commands
            register_commands(app)
        except ImportError:
            app.logger.warning("CLI commands module not found.")

    return app