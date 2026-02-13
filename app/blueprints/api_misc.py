"""
Module: api_misc.py
Author: Gastón Olivares
Project: DataOrcid-Chile
License: MIT
Description:
    Defines miscellaneous API endpoints for interacting with ORCID datasets.
    This blueprint handles data retrieval requests, offering a dual-strategy approach:
    1. 'Cache': Low-latency retrieval from the local database.
    2. 'Fresh': Real-time fetching from the external ORCID API (with auto-caching).

    Designed to be consumed by frontend dashboards or external data analysis tools.
"""

import logging
from flask import Blueprint, request, jsonify
from flask_babel import _  # Import translation function for localized error messages

# Internal module dependencies
from ..decorators import login_required
from ..orcid_cache import get_cached_data, save_cache

# --- Blueprint Configuration ---
# Registers the 'api_misc' blueprint.
# Note: Routes defined here are usually prefixed (e.g., /api) in the main app factory.
bp_api = Blueprint("api_misc", __name__)
logger = logging.getLogger(__name__)


@bp_api.route('/download_orcid', methods=['GET'])
@login_required
def download_orcid():
    """
    API Endpoint: /download_orcid
    Method: GET
    Access: Authenticated Users Only

    Retrieves an ORCID dataset for a specific year.
    Supports switching between local cached data and fetching fresh data from the source.

    Query Parameters:
        year (int): [Required] The target year for the dataset (e.g., 2024).
        mode (str): [Optional] Data retrieval strategy.
                    - 'cache' (default): Returns data from local storage. Fails if missing.
                    - 'fresh': Forces a call to the ORCID API and updates the local cache.

    Returns:
        JSON Response:
            - On Success (200): { "source": str, "year": int, "data": list/dict, ... }
            - On Error (4xx/5xx): { "error": str, "details": str (optional) }
    """
    # 1. Parameter Extraction & Validation
    # ---------------------------------------------------------
    year = request.args.get('year', type=int)
    # Default to 'cache' mode to minimize external API usage
    mode = request.args.get('mode', 'cache').strip().lower()

    if not year:
        return jsonify({"error": _("Parameter 'year' must be specified.")}), 400

    # Validate allowed modes to prevent unexpected behavior
    if mode not in ('cache', 'fresh'):
        return jsonify({"error": _("Invalid mode. Use 'cache' or 'fresh'.")}), 400

    try:
        # 2. Strategy: Cache Retrieval
        # ---------------------------------------------------------
        if mode == 'cache':
            # Attempt to retrieve the pre-stored JSON blob from the database
            cached = get_cached_data(year)
            
            if cached:
                return jsonify({
                    "source": "cache",
                    "year": year,
                    "data": cached.data,  # The actual ORCID dataset
                    "cached_at": cached.created_at.isoformat(),  # ISO timestamp for data age verification
                }), 200
            
            # If mode is cache but data is missing, we return 404 rather than auto-fetching
            # to give the client control over long-running operations.
            return jsonify({
                "error": _("No cache available for year %(y)s.", y=year)
            }), 404

        # 3. Strategy: Fresh Fetch (External API)
        # ---------------------------------------------------------
        if mode == 'fresh':
            # Lazy Import: 'fetch_orcid_data' is imported here inside the function scope.
            # This prevents circular dependency issues if 'orcid_queries' imports models
            # or blueprints that depend on this file.
            from ..orcid_queries import fetch_orcid_data
            
            # Execute the external API request (Warning: This may be slow)
            live_data = fetch_orcid_data(year)
            
            if live_data is None:
                # 502 Bad Gateway indicates the upstream service (ORCID) failed
                return jsonify({"error": _("Could not fetch data from ORCID.")}), 502

            # 4. Persistence
            # ---------------------------------------------------------
            # Automatically update the local cache with the fresh data for future requests
            save_cache(year, live_data)
            
            return jsonify({
                "source": "fresh",
                "year": year,
                "data": live_data
            }), 200

    except Exception as exc:
        # 5. Global Error Handler
        # ---------------------------------------------------------
        # Catch-all for unexpected server-side errors (DB connection lost, parsing errors, etc.)
        logger.exception("CRITICAL: Error processing /download_orcid for year %s: %s", year, exc)
        return jsonify({
            "error": _("Internal server error."),
            "details": str(exc)
        }), 500