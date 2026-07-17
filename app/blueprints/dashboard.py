"""Legacy cached-data dashboard routes."""

from flask import Blueprint, redirect, url_for

from ..decorators import login_required


bp_dash = Blueprint("dashboard", __name__)


@bp_dash.route("/cache/dashboard")
@login_required
def cache_dashboard():
    """Redirect the retired dashboard to synchronization and datasets."""
    return redirect(url_for("works.cache_works_status"), code=302)
