"""
Module: commands.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Custom CLI Management Commands.
    
    This module registers custom commands with the Flask CLI, enabling system administrators
    to perform maintenance tasks directly from the terminal. 
    
    Primary Functions:
    1. 'rebuild-caches': High-performance synchronization of Works and Fundings 
       using the ORCID Member API.
    2. 'sync-researcher-names': Updates local profile metadata (names, bios) 
       for cached researchers.
"""

import logging
import datetime as dt
import click
from flask.cli import with_appcontext
from flask import current_app

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


def register_commands(app):
    """
    Registers custom CLI commands with the Flask application instance.
    
    Args:
        app (Flask): The active Flask application instance.
    """

    @app.cli.command("rebuild-caches")
    @click.option("--ror", default=None, help="Target specific ROR ID. If omitted, scans all active institutions.")
    @click.option("--target", default="both", type=click.Choice(['works', 'fundings', 'both']), help="Data type to synchronize.")
    @with_appcontext
    def rebuild_caches(ror, target):
        """
        Executes the high-performance cache rebuild process.
        
        This command is designed to be run via cron jobs or manually by admins.
        It uses the ORCID Member API (if configured) to maximize rate limits and speed.
        
        Process:
        1. Authenticates with ORCID using Client Credentials.
        2. Identifies target institutions (from args or database scan).
        3. Iterates through targets, fetching Works and/or Fundings.
        4. Logs the execution status (success/failure) to the database for auditing.
        """
        # --- Deferred Imports ---
        # Imported inside the function to prevent circular dependency errors 
        # during the initial application startup.
        from . import db
        from .models import User, WorkCacheRun, FundingCacheRun
        from .services.cache_service import build_works_cache_for_ror, build_fundings_cache_for_ror
        from .services.orcid_service import get_client_credentials_token
        # ------------------------

        click.echo("🚀 Starting high-performance cache rebuild (MEMBER API MODE)...")

        # 1. Configuration Check
        member_url = current_app.config.get('ORCID_MEMBER_URL')
        if not member_url:
            click.echo("❌ FATAL: 'ORCID_MEMBER_URL' missing in config.toml.")
            return

        click.echo(f"🌍 Target API Endpoint: {member_url}")

        # 2. Authentication
        # Obtaining a specialized token for the Member API
        click.echo("🔑 Authenticating with ORCID Member API...")
        token = get_client_credentials_token()
        
        if not token:
            click.echo("❌ FATAL: Authentication failed. Verify Client ID/Secret.")
            return

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        # 3. Target Discovery
        ror_list = []
        if ror:
            ror_list = [ror]
        else:
            click.echo("📋 Scanning database for active institutional records...")
            try:
                # Fetch distinct, non-null ROR IDs from the User table
                rows = db.session.query(User.ror_id)\
                    .filter(User.ror_id.isnot(None), User.ror_id != "")\
                    .distinct().all()
                ror_list = [r[0] for r in rows if r[0]] 
            except Exception as exc:
                click.echo(f"❌ Database Query Error: {exc}")
                return
        
        total_rors = len(ror_list)
        click.echo(f"Identified {total_rors} institutional record(s) for processing.")

        # Helper function to persist execution logs
        def _log_execution_run(model_class, ror_id, status, count, error_msg=None):
            """Persists the result of a cache run to the database."""
            try:
                if not ror_id: return

                execution_time = dt.datetime.utcnow()
                run_log = model_class(
                    ror_id=ror_id,
                    status=status,
                    rows_count=count,
                    error=error_msg,
                    started_at=execution_time,
                    finished_at=execution_time
                )
                db.session.add(run_log)
                db.session.commit()
            except Exception as exc:
                db.session.rollback()
                click.echo(f"⚠️ Warning: Failed to persist execution log: {exc}")

        # 4. Batch Processing
        for index, current_ror in enumerate(ror_list, start=1):
            current_ror = current_ror.strip()
            
            click.echo(f"\n{'='*50}")
            click.echo(f"Processing Institution [{index}/{total_rors}]: {current_ror}")
            click.echo(f"{'='*50}")

            # --- Sync Works (Publications) ---
            if target in ['works', 'both']:
                try:
                    click.echo(f"  > Initializing Works synchronization...")
                    # Calls the optimized service (Multithreaded + Bulk Insert)
                    count = build_works_cache_for_ror(current_ror, base_url=member_url, headers=headers)
                    
                    click.echo(f"  > [Works] Success: {count} records synchronized.")
                    _log_execution_run(WorkCacheRun, current_ror, 'success', count)
                except Exception as exc:
                    click.echo(f"  > [Works] Critical failure: {exc}")
                    _log_execution_run(WorkCacheRun, current_ror, 'failed', 0, str(exc))

            # --- Sync Fundings (Grants) ---
            if target in ['fundings', 'both']:
                try:
                    click.echo(f"  > Initializing Funding synchronization...")
                    count = build_fundings_cache_for_ror(current_ror, base_url=member_url, headers=headers)
                    
                    click.echo(f"  > [Fundings] Success: {count} records synchronized.")
                    _log_execution_run(FundingCacheRun, current_ror, 'success', count)
                except Exception as exc:
                    click.echo(f"  > [Fundings] Critical failure: {exc}")
                    _log_execution_run(FundingCacheRun, current_ror, 'failed', 0, str(exc))
        
        click.echo("\n✅ Institutional cache rebuild sequence completed.")

    @app.cli.command("sync-researcher-names")
    @click.option("--ror", default=None, help="Target specific ROR ID. If omitted, syncs ALL.")
    @with_appcontext
    def sync_researcher_names(ror):
        """
        Secondary synchronization routine.
        
        Updates the 'ResearcherCache' table with names and biographical info 
        fetched from ORCID profiles. This is usually run AFTER 'rebuild-caches'
        to ensure names are available for the dashboard.
        """
        from . import db
        from .models import User
        from .services.cache_service import build_researcher_names_cache

        click.echo("👤 Starting researcher profile synchronization...")

        ror_list = []
        if ror:
            ror_list = [ror]
        else:
            try:
                rows = db.session.query(User.ror_id)\
                    .filter(User.ror_id.isnot(None), User.ror_id != "")\
                    .distinct().all()
                ror_list = [r[0] for r in rows if r[0]]
            except Exception as exc:
                click.echo(f"❌ Database Query Error: {exc}")
                return

        for current_ror in ror_list:
            click.echo(f"🔄 Syncing profiles for ROR: {current_ror}")
            try:
                # Optimized call (Concurrent + Bulk Insert)
                count = build_researcher_names_cache(current_ror)
                click.echo(f"✅ Success: {count} profiles updated.")
            except Exception as e:
                click.echo(f"❌ Error syncing {current_ror}: {e}")

        click.echo("🏁 Profile synchronization finished.")