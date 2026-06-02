"""Application factory and shared Flask extension instances."""

import os
import toml
import datetime as dt
from pathlib import Path
from flask import Flask, session, request, g
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_babel import Babel
from flask_wtf.csrf import CSRFProtect

db = SQLAlchemy()
migrate = Migrate()
babel = Babel()
csrf = CSRFProtect()


def get_locale() -> str:
    """
    Resolve the request locale from URL, session, user preference, then default.

    URL changes are persisted so the language selector updates the user's stored
    preference when they are logged in.
    """
    from flask import current_app
    
    supported = current_app.config.get('LANGUAGES', ['es', 'en'])
    
    lang_arg = request.args.get('lang')
    if lang_arg in supported:
        session['locale'] = lang_arg
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

    if session.get('locale'):
        return session['locale']

    if session.get('user_id'):
        try:
             from .models import User
             user = User.query.get(session['user_id'])
             if user and user.locale in supported:
                 session['locale'] = user.locale
                 return user.locale
        except Exception:
            pass

    return current_app.config.get('BABEL_DEFAULT_LOCALE', 'es')


def datetimeformat(value, format: str = "%Y-%m-%d %H:%M") -> str:
    """Format datetime objects and common API date strings for templates."""
    if not value:
        return ""
    if isinstance(value, (dt.datetime, dt.date)):
        return value.strftime(format)
    
    s = str(value)
    fmts = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in fmts:
        try:
            return dt.datetime.strptime(s, fmt).strftime(format)
        except ValueError:
            continue
    return s


def timestamp_to_date(value) -> str:
    """Convert Unix millisecond timestamps to display dates."""
    try:
        if not value:
            return "N/A"
        ms = int(value)
        return dt.datetime.fromtimestamp(ms / 1000.0).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, Exception):
        return "N/A"


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    project_root = Path(__file__).resolve().parent.parent
    cfg_env = os.environ.get("ORCID_APP_CONFIG")
    cfg_path = Path(cfg_env) if cfg_env else project_root / "config.toml"

    if not cfg_path.is_file():
         cfg_path = project_root / "config" / "config.toml"

    if not cfg_path.is_file():
        raise FileNotFoundError(f"CRITICAL: Missing configuration file at: {cfg_path}")

    config_data = toml.load(str(cfg_path))
    app.config.update(config_data)

    app.config["SQLALCHEMY_DATABASE_URI"] = config_data["database"]["uri"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    
    flask_sec = config_data.get("flask", {})
    app.config["SECRET_KEY"] = flask_sec.get("secret_key", "dev_key_only")
    app.config["SECURITY_PASSWORD_SALT"] = flask_sec.get("password_salt", "CHANGE_ME_SALT")
    
    app.config["SESSION_COOKIE_SECURE"] = flask_sec.get("session_cookie_secure", False)
    app.config["SESSION_COOKIE_HTTPONLY"] = flask_sec.get("session_cookie_httponly", True)
    app.config["SESSION_COOKIE_SAMESITE"] = flask_sec.get("session_cookie_samesite", "Lax")
    
    app.config["APP_BASE_URL"] = config_data.get("app", {}).get("base_url", "").rstrip("/")

    app.config["LANGUAGES"] = config_data.get("languages", {}).get("supported", ["es", "en"])
    app.config["BABEL_DEFAULT_LOCALE"] = config_data.get("languages", {}).get("default", "es")

    orcid_cfg = config_data.get("orcid", {})
    api_cfg = config_data.get("api", {})

    member_url = api_cfg.get("members_url") or orcid_cfg.get("base_url_member") or "https://api.orcid.org/v3.0/"
    search_url = api_cfg.get("search_url") or orcid_cfg.get("base_url_public") or "https://pub.orcid.org/v3.0/"

    app.config.update(
        ORCID_CLIENT_ID=orcid_cfg.get("client_id"),
        ORCID_CLIENT_SECRET=orcid_cfg.get("client_secret"),
        ORCID_TOKEN_URL=orcid_cfg.get("token_url", "https://orcid.org/oauth/token"),
        # Canonical ORCID endpoints used by the service layer.
        ORCID_MEMBER_URL=member_url,
        ORCID_SEARCH_URL=search_url,
        # Backward-compatible aliases for older modules and deployments.
        ORCID_BASE_URL_MEMBER=member_url,
        ORCID_BASE_URL_PUBLIC=search_url,
        ORCID_BASE_URL_PUB=search_url,
    )

    openalex_cfg = config_data.get("openalex", {})
    app.config.update(
        OPENALEX_BASE_URL=(openalex_cfg.get("base_url") or "https://api.openalex.org").rstrip("/"),
        OPENALEX_API_KEY=os.environ.get("OPENALEX_API_KEY") or openalex_cfg.get("api_key"),
        OPENALEX_MAILTO=os.environ.get("OPENALEX_MAILTO") or openalex_cfg.get("mailto"),
        OPENALEX_TIMEOUT=int(openalex_cfg.get("timeout", 20)),
        OPENALEX_STALE_DAYS=int(openalex_cfg.get("stale_days", 30)),
    )

    datasets_cfg = config_data.get("paths", {}).get("datasets_dir", "app/datasets")
    datasets_dir = Path(datasets_cfg)
    if not datasets_dir.is_absolute(): 
        datasets_dir = project_root / datasets_dir
    app.config["DATASETS_DIR"] = str(datasets_dir)

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

    @app.context_processor
    def inject_global_template_vars():
        """Expose global template data used by the layout."""
        institutions = []
        try:
            # Only global admins can switch institutional context.
            if session.get("logged_in") and session.get("is_admin"):
                from .services.institution_registry_service import get_institution_options
                institutions = get_institution_options()
                seen_rors = {item["ror_id"] for item in institutions}

                my_ror = session.get("ror_id")
                if my_ror and my_ror not in seen_rors:
                    institutions.append({"ror_id": my_ror, "name": session.get("institution_name") or my_ror})

                if institutions and not session.get("admin_selected_ror") and my_ror:
                    session["admin_selected_ror"] = my_ror
        except Exception:
            pass 

        return {
            "current_year": dt.datetime.now().year,
            "institutions": institutions
        }

    EXCLUDED_LOG_PATHS = ("/static/", "/favicon.ico", "/robots.txt", "/health")

    @app.before_request
    def track_request_start():
        g._start_time = dt.datetime.utcnow()

    @app.after_request
    def track_request_end(response):
        """
        Log request metadata for the usage dashboard.

        Static and health-check routes are excluded to avoid database noise.
        """
        try:
            if request.path.startswith(EXCLUDED_LOG_PATHS):
                return response
            
            duration = None
            if hasattr(g, "_start_time"):
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

    db.init_app(app)
    migrate.init_app(app, db)
    babel.init_app(app, locale_selector=get_locale)
    csrf.init_app(app)
    app.jinja_env.filters["datetimeformat"] = datetimeformat
    app.jinja_env.filters["timestamp_to_date"] = timestamp_to_date

    with app.app_context():
        db.create_all()
        try:
            from .services.institution_registry_service import seed_chilean_universities
            seed_chilean_universities()
        except Exception as exc:
            db.session.rollback()
            app.logger.warning("Institution registry seed failed: %s", exc)

        from .blueprints.main import bp_main
        from .blueprints.cache_control import bp_cache
        from .blueprints.admin import bp_admin
        from .blueprints.auth import bp_auth
        from .blueprints.works import bp_works
        from .blueprints.fundings import bp_fund
        from .blueprints.export import bp_export
        from .blueprints.dashboard import bp_dash
        from .blueprints.api_misc import bp_api
        from .blueprints.duplicates import bp_duplicates

        app.register_blueprint(bp_main)
        app.register_blueprint(bp_cache)
        app.register_blueprint(bp_admin)
        app.register_blueprint(bp_auth)
        app.register_blueprint(bp_works)
        app.register_blueprint(bp_fund)
        app.register_blueprint(bp_export)
        app.register_blueprint(bp_dash)
        app.register_blueprint(bp_api)
        app.register_blueprint(bp_duplicates)

        try:
            from .commands import register_commands
            register_commands(app)
        except ImportError:
            app.logger.warning("CLI commands module not found.")

    return app
