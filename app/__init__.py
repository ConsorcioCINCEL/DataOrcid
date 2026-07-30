"""Application factory and shared Flask extension instances."""

import os
import toml
import datetime as dt
import html
import re
from pathlib import Path
from zoneinfo import ZoneInfo
from flask import Flask, session, request, g, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_babel import Babel
from flask_wtf.csrf import CSRFProtect

db = SQLAlchemy()
migrate = Migrate()
babel = Babel()
csrf = CSRFProtect()
CHILE_TIMEZONE = ZoneInfo("America/Santiago")
APP_VERSION = "2.0"


def get_locale() -> str:
    """
    Resolve the request locale from URL, session, user preference, then default.

    URL changes are persisted so the language selector updates the user's stored
    preference when they are logged in.
    """
    from flask import current_app
    
    supported = current_app.config.get('LANGUAGES', ['en', 'es'])
    
    lang_arg = request.args.get('lang')
    if lang_arg in supported:
        session['locale'] = lang_arg
        if session.get('user_id'):
            try:
                from .models import User
                user = db.session.get(User, session['user_id'])
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
             user = db.session.get(User, session['user_id'])
             if user and user.locale in supported:
                 session['locale'] = user.locale
                 return user.locale
        except Exception:
            pass

    return current_app.config.get('BABEL_DEFAULT_LOCALE', 'en')


def datetimeformat(value, format: str = "%Y-%m-%d %H:%M") -> str:
    """Format UTC timestamps in Chile's current civil time."""
    if not value:
        return ""
    if isinstance(value, dt.datetime):
        return _to_chile_time(value).strftime(format)
    if isinstance(value, dt.date):
        return value.strftime(format)
    
    s = str(value)
    fmts = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in fmts:
        try:
            parsed = dt.datetime.strptime(s, fmt)
            if fmt == "%Y-%m-%d":
                return parsed.strftime(format)
            return _to_chile_time(parsed).strftime(format)
        except ValueError:
            continue
    return s


def _to_chile_time(value: dt.datetime) -> dt.datetime:
    """Convert an aware or naive UTC datetime to America/Santiago."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(CHILE_TIMEZONE)


def timestamp_to_date(value) -> str:
    """Convert Unix millisecond timestamps to display dates."""
    try:
        if not value:
            return "N/A"
        ms = int(value)
        value = dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)
        return value.astimezone(CHILE_TIMEZONE).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, Exception):
        return "N/A"


def plain_text(value) -> str:
    """Return external metadata as readable text without embedded markup."""
    if value is None:
        return ""
    decoded = html.unescape(str(value))
    without_tags = re.sub(r"<[^>]*>", "", decoded)
    return re.sub(r"\s+", " ", without_tags).strip()


def locale_url(language: str) -> str:
    """Build a language-switch URL while preserving route and query context."""
    if not request.endpoint:
        return request.path

    values = dict(request.view_args or {})
    for key, items in request.args.lists():
        if key == "lang":
            continue
        values[key] = items if len(items) > 1 else items[0]
    values["lang"] = language
    return url_for(request.endpoint, **values)


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
    
    app_cfg = config_data.get("app", {})
    app.config["APP_BASE_URL"] = app_cfg.get("base_url", "").rstrip("/")
    app.config["APP_VERSION"] = str(app_cfg.get("version") or APP_VERSION)
    cache_cfg = config_data.get("cache", {})
    tracking_cfg = config_data.get("tracking", {})
    jobs_cfg = config_data.get("jobs", {})
    app.config["CACHE_STALE_DAYS"] = int(cache_cfg.get("stale_days", 30))
    app.config["TRACKING_RETENTION_DAYS"] = int(tracking_cfg.get("retention_days", 90))
    app.config["JOB_STALE_MINUTES"] = int(jobs_cfg.get("stale_minutes", 30))

    app.config["LANGUAGES"] = config_data.get("languages", {}).get("supported", ["en", "es"])
    app.config["BABEL_DEFAULT_LOCALE"] = config_data.get("languages", {}).get("default", "en")
    app.config["BABEL_DEFAULT_TIMEZONE"] = "America/Santiago"

    orcid_cfg = config_data.get("orcid", {})
    api_cfg = config_data.get("api", {})

    member_url = api_cfg.get("members_url") or orcid_cfg.get("base_url_member") or "https://api.orcid.org/v3.0/"
    public_url = (
        api_cfg.get("public_url")
        or orcid_cfg.get("base_url_public")
        or orcid_cfg.get("base_url_pub")
        or orcid_cfg.get("base_url")
        or "https://pub.orcid.org/v3.0/"
    )
    search_url = api_cfg.get("search_url") or orcid_cfg.get("search_url") or member_url

    app.config.update(
        ORCID_CLIENT_ID=orcid_cfg.get("client_id"),
        ORCID_CLIENT_SECRET=orcid_cfg.get("client_secret"),
        ORCID_TOKEN_URL=orcid_cfg.get("token_url", "https://orcid.org/oauth/token"),
        # Canonical ORCID endpoints used by the service layer.
        ORCID_MEMBER_URL=member_url,
        ORCID_SEARCH_URL=search_url,
        ORCID_PUBLIC_URL=public_url,
        ORCID_PROFILE_CACHE_TTL=int(orcid_cfg.get("profile_cache_ttl", 900)),
        # Backward-compatible aliases for older modules and deployments.
        ORCID_BASE_URL_MEMBER=member_url,
        ORCID_BASE_URL_PUBLIC=public_url,
        ORCID_BASE_URL_PUB=public_url,
    )

    openalex_cfg = config_data.get("openalex", {})
    app.config.update(
        OPENALEX_BASE_URL=(openalex_cfg.get("base_url") or "https://api.openalex.org").rstrip("/"),
        OPENALEX_API_KEY=os.environ.get("OPENALEX_API_KEY") or openalex_cfg.get("api_key"),
        OPENALEX_MAILTO=os.environ.get("OPENALEX_MAILTO") or openalex_cfg.get("mailto"),
        OPENALEX_TIMEOUT=int(openalex_cfg.get("timeout", 20)),
        OPENALEX_STALE_DAYS=int(openalex_cfg.get("stale_days", 30)),
        OPENALEX_ERROR_RETRY_MINUTES=int(openalex_cfg.get("error_retry_minutes", 15)),
        OPENALEX_ERROR_RETRY_MAX_HOURS=int(openalex_cfg.get("error_retry_max_hours", 24)),
        OPENALEX_WORKERS=int(os.environ.get("OPENALEX_WORKERS") or openalex_cfg.get("workers", 4)),
        OPENALEX_TITLE_WORKERS=int(os.environ.get("OPENALEX_TITLE_WORKERS") or openalex_cfg.get("title_workers", 2)),
        OPENALEX_ANALYTICS_CACHE_TTL=int(openalex_cfg.get("analytics_cache_ttl", 86400)),
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
                institutions = getattr(g, "institution_options", None)
                if institutions is None:
                    from .services.institution_registry_service import get_institution_options
                    institutions = get_institution_options()
                    g.institution_options = institutions
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
            "institutions": institutions,
            "locale_url": locale_url,
        }

    EXCLUDED_LOG_PATHS = ("/static/", "/favicon.ico", "/robots.txt", "/health")

    @app.before_request
    def track_request_start():
        g._start_time = dt.datetime.now(dt.timezone.utc)

    @app.after_request
    def track_request_end(response):
        """
        Log request metadata for the usage dashboard.

        Static and health-check routes are excluded to avoid database noise.
        """
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")

        try:
            if app.config.get("TESTING") or request.path.startswith(EXCLUDED_LOG_PATHS):
                return response
            
            duration = None
            if hasattr(g, "_start_time"):
                duration = (dt.datetime.now(dt.timezone.utc) - g._start_time).total_seconds() * 1000

            from .models import TrackingLog
            role = (
                "admin"
                if session.get("is_admin")
                else "manager"
                if session.get("is_manager")
                else "user"
                if session.get("logged_in")
                else "anonymous"
            )
            log_entry = TrackingLog(
                user_id=session.get("user_id"),
                username=session.get("username"),
                institution_ror=(
                    session.get("admin_selected_ror")
                    if session.get("is_admin")
                    else session.get("ror_id")
                ),
                role=role,
                action=request.endpoint,
                job_id=request.values.get("job_id"),
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
    app.jinja_env.filters["plain_text"] = plain_text

    with app.app_context():
        # Register every model before create_all() checks the database schema.
        from . import models as _models  # noqa: F401

        db.create_all()
        try:
            from .services.institution_registry_service import seed_chilean_universities
            seed_chilean_universities()
        except Exception as exc:
            db.session.rollback()
            app.logger.warning("Institution registry seed failed: %s", exc)

        try:
            from .services.background_jobs import recover_interrupted_jobs
            recover_interrupted_jobs(app.config["JOB_STALE_MINUTES"])
        except Exception as exc:
            db.session.rollback()
            app.logger.warning("Interrupted job recovery failed: %s", exc)

        from .blueprints.main import bp_main
        from .blueprints.cache_control import bp_cache
        from .blueprints.admin import bp_admin
        from .blueprints.auth import bp_auth
        from .blueprints.works import bp_works
        from .blueprints.export import bp_export
        from .blueprints.dashboard import bp_dash
        from .blueprints.api_misc import bp_api
        from .blueprints.duplicates import bp_duplicates
        from .blueprints.help import bp_help

        app.register_blueprint(bp_main)
        app.register_blueprint(bp_cache)
        app.register_blueprint(bp_admin)
        app.register_blueprint(bp_auth)
        app.register_blueprint(bp_works)
        app.register_blueprint(bp_export)
        app.register_blueprint(bp_dash)
        app.register_blueprint(bp_api)
        app.register_blueprint(bp_duplicates)
        app.register_blueprint(bp_help)

        try:
            from .commands import register_commands
            register_commands(app)
        except ImportError:
            app.logger.warning("CLI commands module not found.")

    return app
