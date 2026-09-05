"""Persistent module visibility and route-level access enforcement."""

from __future__ import annotations

from flask import Response, current_app, g, jsonify, render_template, request
from flask_babel import _

from ..models import SystemModule


MODULE_DEFINITIONS = (
    {
        "key": "landing_page",
        "group": "Public site",
        "label": "Public landing page",
        "description": "Single-page service overview and public contact form.",
        "icon": "fas fa-bullhorn",
    },
    {
        "key": "researchers",
        "group": "Explore",
        "label": "Researchers",
        "description": "Researcher directory, ORCID profiles, and profile exports.",
        "icon": "fas fa-user-graduate",
    },
    {
        "key": "orcid_analytics",
        "group": "Explore",
        "label": "ORCID analytics",
        "description": "Institutional ORCID indicators, charts, and metric downloads.",
        "icon": "fas fa-chart-line",
    },
    {
        "key": "openalex_analytics",
        "group": "Explore",
        "label": "OpenAlex analytics",
        "description": "Institutional analytics and cross-institution comparisons.",
        "icon": "fas fa-project-diagram",
    },
    {
        "key": "synchronization",
        "group": "Manage data",
        "label": "Synchronization and datasets",
        "description": "ORCID cache updates and institutional dataset exports.",
        "icon": "fas fa-sync-alt",
    },
    {
        "key": "data_quality",
        "group": "Manage data",
        "label": "Data quality",
        "description": "Coverage, integrity, relationship, and canonical-work checks.",
        "icon": "fas fa-shield-alt",
    },
    {
        "key": "duplicates",
        "group": "Manage data",
        "label": "Quality and duplicates",
        "description": "Duplicate-profile analysis, review decisions, and exports.",
        "icon": "fas fa-user-check",
    },
    {
        "key": "openalex_enrichment",
        "group": "Manage data",
        "label": "OpenAlex enrichment",
        "description": "OpenAlex synchronization, enriched records, and downloads.",
        "icon": "fas fa-link",
    },
    {
        "key": "api_read",
        "group": "Integrate",
        "label": "Read from the API",
        "description": "API reading guide and cached or live ORCID data endpoint.",
        "icon": "fas fa-code",
    },
    {
        "key": "orcid_write",
        "group": "Integrate",
        "label": "Write to ORCID",
        "description": "Affiliation Manager integration guide and downloadable examples.",
        "icon": "fas fa-terminal",
    },
    {
        "key": "affiliation_manager",
        "group": "Integrate",
        "label": "Affiliation Manager identifier",
        "description": "Institutional Affiliation Manager client configuration.",
        "icon": "fas fa-key",
    },
    {
        "key": "oai_pmh",
        "group": "Integrate",
        "label": "OAI-PMH repository",
        "description": "Repository configuration, article selection, exports, and provider API.",
        "icon": "fas fa-broadcast-tower",
    },
    {
        "key": "resources",
        "group": "Support",
        "label": "ORCID resources",
        "description": "Curated external documentation and institutional resources.",
        "icon": "fas fa-book-open",
    },
    {
        "key": "help",
        "group": "Support",
        "label": "Help center",
        "description": "In-product functional, methodological, and technical documentation.",
        "icon": "far fa-question-circle",
    },
    {
        "key": "user_management",
        "group": "Administration",
        "label": "User management",
        "description": "User accounts, institutional assignments, roles, and credentials.",
        "icon": "fas fa-users-cog",
    },
    {
        "key": "system_activity",
        "group": "Administration",
        "label": "System activity",
        "description": "Request history, usage, performance, and access statistics.",
        "icon": "fas fa-history",
    },
    {
        "key": "background_jobs",
        "group": "Administration",
        "label": "Background jobs",
        "description": "Administrator monitor for durable background operations.",
        "icon": "fas fa-tasks",
    },
    {
        "key": "system_errors",
        "group": "Administration",
        "label": "System errors",
        "description": "Administrator diagnostics for sanitized runtime failures.",
        "icon": "fas fa-bug",
    },
)

MODULE_KEYS = frozenset(item["key"] for item in MODULE_DEFINITIONS)
MODULE_BY_KEY = {item["key"]: item for item in MODULE_DEFINITIONS}


def _module_description_translation_markers():
    """Keep data-driven module descriptions in the gettext catalog."""
    return (
        _("Public site"),
        _("Public landing page"),
        _("Single-page service overview and public contact form."),
        _("Researcher directory, ORCID profiles, and profile exports."),
        _("Institutional ORCID indicators, charts, and metric downloads."),
        _("Institutional analytics and cross-institution comparisons."),
        _("ORCID cache updates and institutional dataset exports."),
        _("Coverage, integrity, relationship, and canonical-work checks."),
        _("Duplicate-profile analysis, review decisions, and exports."),
        _("OpenAlex synchronization, enriched records, and downloads."),
        _("API reading guide and cached or live ORCID data endpoint."),
        _("Affiliation Manager integration guide and downloadable examples."),
        _("Institutional Affiliation Manager client configuration."),
        _("Repository configuration, article selection, exports, and provider API."),
        _("Curated external documentation and institutional resources."),
        _("In-product functional, methodological, and technical documentation."),
        _("User accounts, institutional assignments, roles, and credentials."),
        _("Request history, usage, performance, and access statistics."),
        _("Administrator monitor for durable background operations."),
        _("Administrator diagnostics for sanitized runtime failures."),
    )

_ENDPOINT_MODULES = {
    "main.contact": "landing_page",
    "admin.contact_inquiries": "landing_page",
    "admin.update_contact_inquiry": "landing_page",
    "main.researcher_list": "researchers",
    "main.researcher_list_export": "researchers",
    "main.orcid_profile": "researchers",
    "export.download_excel": "researchers",
    "export.download_section_excel": "researchers",
    "main.metrics_panel": "orcid_analytics",
    "main.download_metrics_data": "orcid_analytics",
    "works.openalex_analytics": "openalex_analytics",
    "works.openalex_analytics_export": "openalex_analytics",
    "works.openalex_global": "openalex_analytics",
    "works.openalex_global_export": "openalex_analytics",
    "main.cache_dashboard": "synchronization",
    "dashboard.cache_dashboard": "synchronization",
    "cache_control.rebuild_cache": "synchronization",
    "works.cache_works_status": "synchronization",
    "works.cache_full_build": "synchronization",
    "works.cache_full_build_all": "synchronization",
    "works.cache_fundings_build": "synchronization",
    "works.cache_profiles_build": "synchronization",
    "works.cache_staff_institution_build": "synchronization",
    "works.cache_works_build": "synchronization",
    "works.download_all_fundings_admin": "synchronization",
    "works.download_all_works_admin": "synchronization",
    "works.download_all_researchers_admin": "synchronization",
    "works.download_all_fundings_cache": "synchronization",
    "works.download_all_works_cache": "synchronization",
    "works.download_researchers_cache": "synchronization",
    "works.download_staff_institution_cache": "synchronization",
    "works.download_institution_cache_summary": "synchronization",
    "works.data_quality": "data_quality",
    "works.data_quality_backfill_associations": "data_quality",
    "works.data_quality_rebuild_canonical_works": "data_quality",
    "works.openalex_works": "openalex_enrichment",
    "works.openalex_works_export": "openalex_enrichment",
    "works.openalex_sync": "openalex_enrichment",
    "works.openalex_sync_system": "openalex_enrichment",
    "works.download_openalex_admin": "openalex_enrichment",
    "main.integration_pull": "api_read",
    "api_misc.download_orcid": "api_read",
    "main.integration_guide": "orcid_write",
    "auth.am_settings": "affiliation_manager",
    "main.resources": "resources",
    "admin.users_list": "user_management",
    "admin.users_new": "user_management",
    "admin.users_reset_password": "user_management",
    "admin.users_send_creds": "user_management",
    "admin.users_update": "user_management",
    "admin.users_delete": "user_management",
    "admin.statistics": "system_activity",
    "admin.jobs": "background_jobs",
    "admin.system_errors": "system_errors",
    "admin.update_system_error_status": "system_errors",
}

_EXPORT_KIND_MODULES = {
    "institution_works": "synchronization",
    "institution_fundings": "synchronization",
    "institution_researchers": "synchronization",
    "institution_summary": "synchronization",
    "staff_works": "synchronization",
    "staff_fundings": "synchronization",
    "staff_researchers": "synchronization",
    "researcher_directory": "researchers",
    "raw_openalex": "openalex_enrichment",
    "institution_openalex": "openalex_enrichment",
    "duplicate_profiles": "duplicates",
    "oai_article_audit": "oai_pmh",
}


def disabled_module_keys() -> frozenset[str]:
    cached = getattr(g, "disabled_module_keys", None)
    if cached is None:
        cached = frozenset(
            key
            for (key,) in SystemModule.query.with_entities(SystemModule.key).filter(
                SystemModule.is_enabled.is_(False),
                SystemModule.key.in_(MODULE_KEYS),
            )
        )
        g.disabled_module_keys = cached
    return cached


def is_module_enabled(module_key: str | None) -> bool:
    return not module_key or module_key not in disabled_module_keys()


def get_landing_default_locale() -> str:
    """Read the shared landing preference without overriding visitor choices."""
    from .. import DEFAULT_LANGUAGES

    supported = current_app.config.get("LANGUAGES", DEFAULT_LANGUAGES)
    configured = SystemModule.query.with_entities(SystemModule.default_locale).filter_by(
        key="landing_page",
    ).scalar()
    if configured in supported:
        return configured
    fallback = current_app.config.get("BABEL_DEFAULT_LOCALE", "en")
    return fallback if fallback in supported else next(iter(supported), "en")


def module_for_export_kind(kind: str | None) -> str | None:
    return _EXPORT_KIND_MODULES.get(kind or "")


def module_for_export_job(job) -> str | None:
    try:
        args = (job.payload_json or {}).get("args") or []
        return module_for_export_kind(args[0] if args else None)
    except (AttributeError, TypeError):
        return None


def module_for_endpoint(endpoint: str | None, view_args: dict | None = None) -> str | None:
    endpoint = endpoint or ""
    if endpoint.startswith("duplicates."):
        return "duplicates"
    if endpoint.startswith("help."):
        return "help"
    if endpoint.startswith("oai_pmh.") or endpoint in {
        "admin.oai_repositories",
        "admin.open_oai_repository",
    }:
        return "oai_pmh"
    if endpoint == "works.download_staff_institution_cache":
        if (view_args or {}).get("dataset_key") == "openalex":
            return "openalex_enrichment"
    return _ENDPOINT_MODULES.get(endpoint)


def module_disabled_response(module_key: str):
    definition = MODULE_BY_KEY[module_key]
    label = definition["label"]
    if request.endpoint == "oai_pmh.provider":
        return Response(
            _("This repository module is currently unavailable."),
            status=403,
            mimetype="text/plain",
        )
    wants_json = (
        request.is_json
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or request.accept_mimetypes.best == "application/json"
    )
    if wants_json:
        return jsonify({
            "error": _("This module is currently unavailable."),
            "module": module_key,
        }), 403
    return render_template(
        "module_disabled.html",
        module={
            **definition,
            "label": _(label),
        },
    ), 403


def enforce_current_module():
    module_key = module_for_endpoint(request.endpoint, request.view_args)
    if module_key and not is_module_enabled(module_key):
        return module_disabled_response(module_key)
    return None
