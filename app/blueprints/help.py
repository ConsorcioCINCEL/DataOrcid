"""User-facing help center routes and module-aware reference pages."""

from flask import Blueprint, abort, render_template

from ..decorators import login_required
from ..services.module_access import is_module_enabled


bp_help = Blueprint("help", __name__, url_prefix="/help")

HELP_TOPIC_MODULES = {
    "getting-started": (),
    "metadata-sources": (
        "researchers",
        "orcid_analytics",
        "openalex_analytics",
        "synchronization",
        "data_quality",
        "duplicates",
        "openalex_enrichment",
        "api_read",
        "orcid_write",
        "affiliation_manager",
        "oai_pmh",
    ),
    "data-flow": (
        "researchers",
        "orcid_analytics",
        "openalex_analytics",
        "synchronization",
        "data_quality",
        "duplicates",
        "openalex_enrichment",
        "oai_pmh",
    ),
    "metrics": (
        "researchers",
        "orcid_analytics",
        "openalex_analytics",
        "data_quality",
        "duplicates",
        "openalex_enrichment",
    ),
    "synchronization": (
        "synchronization",
        "openalex_enrichment",
        "researchers",
        "orcid_analytics",
        "openalex_analytics",
        "duplicates",
        "oai_pmh",
    ),
    "integrations": ("api_read", "orcid_write", "affiliation_manager", "oai_pmh"),
    "data-dictionary": (
        "researchers",
        "orcid_analytics",
        "openalex_analytics",
        "synchronization",
        "data_quality",
        "duplicates",
        "openalex_enrichment",
        "oai_pmh",
    ),
    "roles-governance": (),
    "architecture": (),
    "troubleshooting": (),
    "release-notes": (),
}

HELP_TOPICS = tuple(HELP_TOPIC_MODULES)


def available_help_topics() -> frozenset[str]:
    """Return topics that still contain content for globally enabled modules."""
    return frozenset(
        topic
        for topic, module_keys in HELP_TOPIC_MODULES.items()
        if not module_keys or any(is_module_enabled(key) for key in module_keys)
    )


def _render_help(active_topic: str | None):
    return render_template(
        "help/index.html",
        active_topic=active_topic,
        available_topics=available_help_topics(),
    )


@bp_help.route("/")
@login_required
def index():
    """Render the searchable help-center landing page."""
    return _render_help(None)


@bp_help.route("/topic/<topic>/")
@login_required
def topic(topic: str):
    """Render one focused help topic while retaining global search."""
    if topic not in available_help_topics():
        abort(404)
    return _render_help(topic)
