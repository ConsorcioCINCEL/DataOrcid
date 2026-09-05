"""Render localized transactional messages with the shared DataORCID identity."""

from datetime import datetime, timezone
from pathlib import Path

from flask import current_app, render_template, url_for
from flask_babel import _, force_locale, get_locale

USER_MANUAL_LANGUAGES = ("en", "es", "fr", "pt", "de")


def email_locale(value: str | None) -> str:
    """Use the recipient's preference, with a supported language fallback."""
    language = (value or current_app.config.get("BABEL_DEFAULT_LOCALE", "en")).replace("_", "-").split("-")[0].lower()
    return language if language in USER_MANUAL_LANGUAGES else "en"


def user_manual_path(language: str) -> Path:
    """Resolve only published language editions, never arbitrary documentation files."""
    if language not in USER_MANUAL_LANGUAGES:
        raise ValueError("Unsupported user manual language.")
    version = current_app.config.get("APP_VERSION", "2.1")
    directory = Path(current_app.config.get("USER_MANUAL_DIRECTORY") or Path(__file__).resolve().parents[2] / "manuals")
    path = directory / f"dataorcid-chile-user-manual-v{version}-{language}.pdf"
    with path.open("rb") as source:
        if source.read(5) != b"%PDF-":
            raise ValueError("The configured user manual is not a PDF document.")
    return path


def user_manual_url(language: str) -> str:
    """Build a stable public download URL for the current language edition."""
    manual_language = email_locale(language)
    user_manual_path(manual_language)
    base = (current_app.config.get("APP_BASE_URL") or "").rstrip("/")
    path = url_for("main.user_manual", language=manual_language)
    return f"{base}{path}" if base else url_for("main.user_manual", language=manual_language, _external=True)


def _render(kind: str, subject: str, *, preview: bool = False, **context) -> dict:
    values = dict(
        subject=subject, language=str(get_locale()), preview=preview,
        year=datetime.now(timezone.utc).year, **context,
    )
    return {
        "subject": subject,
        "html": render_template(f"emails/{kind}.html", **values),
        "text": render_template(f"emails/{kind}.txt", **values).strip(),
    }


def render_credentials_email(user, password: str, login_url: str, *, welcome: bool = False, preview: bool = False) -> dict:
    """Build welcome/resend content with a language-matched public guide link."""
    language = email_locale(user.locale)
    manual_url = user_manual_url(language)
    with force_locale(language):
        subject = _("Welcome to Data ORCID-Chile") if welcome else _("Access to Data ORCID-Chile (credentials)")
        manual_labels = {
            "en": _("User manual (English)"), "es": _("User manual (Spanish)"),
            "fr": _("User manual (French)"), "pt": _("User manual (Portuguese)"),
            "de": _("User manual (German)"),
        }
        result = _render(
            "credentials", subject, user=user, password=password, login_url=login_url,
            welcome=welcome, preview=preview,
            manual_label=manual_labels[language],
            manual_url=manual_url,
        )
    return result


def render_password_reset_email(user, reset_url: str, *, preview: bool = False) -> dict:
    """Use the account language rather than the password-request browser locale."""
    with force_locale(email_locale(user.locale)):
        return _render(
            "password_reset", _("Recover your password — Data ORCID-Chile"),
            user=user, reset_url=reset_url, preview=preview,
        )


def render_contact_email(inquiry, topic_label: str, *, language: str | None = None, preview: bool = False) -> dict:
    """Render escaped visitor content while retaining a readable text version."""
    with force_locale(email_locale(language or str(get_locale()))):
        return _render(
            "contact", _("New inquiry from Data ORCID-Chile"),
            inquiry=inquiry, topic_label=topic_label, preview=preview,
        )
