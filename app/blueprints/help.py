"""User-facing help center routes and technical reference pages."""

from flask import Blueprint, abort, render_template

from ..decorators import login_required


bp_help = Blueprint("help", __name__, url_prefix="/help")

HELP_TOPICS = {
    "getting-started",
    "metadata-sources",
    "data-flow",
    "metrics",
    "synchronization",
    "roles-governance",
    "data-dictionary",
    "architecture",
    "troubleshooting",
    "release-notes",
}


@bp_help.route("/")
@login_required
def index():
    """Render the searchable help-center landing page."""
    return render_template("help/index.html", active_topic=None)


@bp_help.route("/topic/<topic>/")
@login_required
def topic(topic: str):
    """Render one focused help topic while retaining global search."""
    if topic not in HELP_TOPICS:
        abort(404)
    return render_template("help/index.html", active_topic=topic)
