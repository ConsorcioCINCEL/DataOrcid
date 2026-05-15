"""Small JSON API endpoints for cached and fresh ORCID datasets."""

import logging
from flask import Blueprint, request, jsonify
from flask_babel import _

from ..decorators import login_required
from ..orcid_cache import get_cached_data, save_cache

bp_api = Blueprint("api_misc", __name__)
logger = logging.getLogger(__name__)


@bp_api.route('/download_orcid', methods=['GET'])
@login_required
def download_orcid():
    """
    Return an aggregated ORCID dataset from cache or refresh it from ORCID.
    """
    year = request.args.get('year', type=int)
    mode = request.args.get('mode', 'cache').strip().lower()

    if not year:
        return jsonify({"error": _("Parameter 'year' must be specified.")}), 400

    if mode not in ('cache', 'fresh'):
        return jsonify({"error": _("Invalid mode. Use 'cache' or 'fresh'.")}), 400

    try:
        if mode == 'cache':
            cached = get_cached_data(year)
            
            if cached:
                return jsonify({
                    "source": "cache",
                    "year": year,
                    "data": cached.data,
                    "cached_at": cached.created_at.isoformat(),
                }), 200

            # Cache mode stays non-blocking; clients can opt into fresh mode.
            return jsonify({
                "error": _("No cache available for year %(y)s.", y=year)
            }), 404

        if mode == 'fresh':
            from ..orcid_queries import fetch_orcid_data

            live_data = fetch_orcid_data(year)
            
            if live_data is None:
                return jsonify({"error": _("Could not fetch data from ORCID.")}), 502

            save_cache(year, live_data)
            
            return jsonify({
                "source": "fresh",
                "year": year,
                "data": live_data
            }), 200

    except Exception as exc:
        logger.exception("CRITICAL: Error processing /download_orcid for year %s: %s", year, exc)
        return jsonify({
            "error": _("Internal server error."),
            "details": str(exc)
        }), 500
