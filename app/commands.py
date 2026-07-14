"""Flask CLI commands for cache rebuilds and profile metadata sync."""

import logging
import datetime as dt
import click
from flask.cli import with_appcontext
from flask import current_app

logger = logging.getLogger(__name__)


def register_commands(app):
    """Register maintenance commands on the Flask application."""

    @app.cli.command("rebuild-caches")
    @click.option("--ror", default=None, help="Target specific ROR ID. If omitted, scans all active institutions.")
    @click.option("--start-at", default=None, help="Resume an all-institution rebuild at this ROR ID.")
    @click.option("--target", default="both", type=click.Choice(['works', 'fundings', 'both']), help="Data type to synchronize.")
    @with_appcontext
    def rebuild_caches(ror, start_at, target):
        """Rebuild works and/or funding caches for one or all institutions."""
        # Keep imports local so CLI registration does not trigger service imports early.
        from . import db
        from .models import WorkCacheRun, FundingCacheRun
        from .services.cache_service import (
            build_full_cache_for_ror,
            build_fundings_cache_for_ror,
            build_works_cache_for_ror,
        )
        from .services.institution_registry_service import get_institution_options
        from .services.orcid_service import get_client_credentials_token

        click.echo("🚀 Starting high-performance cache rebuild (MEMBER API MODE)...")

        member_url = current_app.config.get('ORCID_MEMBER_URL')
        if not member_url:
            click.echo("❌ FATAL: 'ORCID_MEMBER_URL' missing in config.toml.")
            return

        click.echo(f"🌍 Target API Endpoint: {member_url}")

        click.echo("🔑 Authenticating with ORCID Member API...")
        token = get_client_credentials_token()
        
        if not token:
            click.echo("❌ FATAL: Authentication failed. Verify Client ID/Secret.")
            return

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        ror_list = []
        if ror:
            ror_list = [ror]
        else:
            click.echo("📋 Scanning institutional registry and user-owned ROR records...")
            try:
                ror_list = [item["ror_id"] for item in get_institution_options() if item.get("ror_id")]
            except Exception as exc:
                click.echo(f"❌ Database Query Error: {exc}")
                return

            if start_at:
                clean_start = start_at.strip().rstrip("/").split("/")[-1].lower()
                try:
                    start_index = ror_list.index(clean_start)
                except ValueError:
                    click.echo(f"❌ Resume ROR ID not found in the active registry: {clean_start}")
                    return
                ror_list = ror_list[start_index:]
                click.echo(f"▶️ Resuming the rebuild at ROR {clean_start}.")
        
        total_rors = len(ror_list)
        click.echo(f"Identified {total_rors} institutional record(s) for processing.")

        def _log_execution_run(model_class, ror_id, status, count, error_msg=None):
            """Persist a cache-run audit record."""
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

        for index, current_ror in enumerate(ror_list, start=1):
            current_ror = current_ror.strip()
            
            click.echo(f"\n{'='*50}")
            click.echo(f"Processing Institution [{index}/{total_rors}]: {current_ror}")
            click.echo(f"{'='*50}")

            if target == 'both':
                try:
                    click.echo("  > Discovering researchers and synchronizing all metadata...")
                    result = build_full_cache_for_ror(current_ror, base_url=member_url, headers=headers)
                    click.echo(
                        "  > Success: "
                        f"{result['researchers']} researchers, {result['works']} works, "
                        f"{result['fundings']} fundings, and {result['profiles']} profiles."
                    )
                    _log_execution_run(WorkCacheRun, current_ror, 'success', result['works'])
                    _log_execution_run(FundingCacheRun, current_ror, 'success', result['fundings'])
                except Exception as exc:
                    db.session.rollback()
                    click.echo(f"  > Full metadata synchronization failed: {exc}")
                    _log_execution_run(WorkCacheRun, current_ror, 'failed', 0, str(exc))
                    _log_execution_run(FundingCacheRun, current_ror, 'failed', 0, str(exc))
                continue

            if target == 'works':
                try:
                    click.echo("  > Initializing Works synchronization...")
                    count = build_works_cache_for_ror(current_ror, base_url=member_url, headers=headers)
                    click.echo(f"  > [Works] Success: {count} records synchronized.")
                    _log_execution_run(WorkCacheRun, current_ror, 'success', count)
                except Exception as exc:
                    db.session.rollback()
                    click.echo(f"  > [Works] Critical failure: {exc}")
                    _log_execution_run(WorkCacheRun, current_ror, 'failed', 0, str(exc))

            if target == 'fundings':
                try:
                    click.echo(f"  > Initializing Funding synchronization...")
                    count = build_fundings_cache_for_ror(current_ror, base_url=member_url, headers=headers)
                    
                    click.echo(f"  > [Fundings] Success: {count} records synchronized.")
                    _log_execution_run(FundingCacheRun, current_ror, 'success', count)
                except Exception as exc:
                    db.session.rollback()
                    click.echo(f"  > [Fundings] Critical failure: {exc}")
                    _log_execution_run(FundingCacheRun, current_ror, 'failed', 0, str(exc))
        
        click.echo("\n✅ Institutional cache rebuild sequence completed.")

    @app.cli.command("sync-researcher-names")
    @click.option("--ror", default=None, help="Target specific ROR ID. If omitted, syncs ALL.")
    @with_appcontext
    def sync_researcher_names(ror):
        """Refresh cached researcher display names from ORCID profiles."""
        from . import db
        from .services.cache_service import build_researcher_names_cache
        from .services.institution_registry_service import get_institution_options

        click.echo("👤 Starting researcher profile synchronization...")

        ror_list = []
        if ror:
            ror_list = [ror]
        else:
            try:
                ror_list = [item["ror_id"] for item in get_institution_options() if item.get("ror_id")]
            except Exception as exc:
                click.echo(f"❌ Database Query Error: {exc}")
                return

        for current_ror in ror_list:
            click.echo(f"🔄 Syncing profiles for ROR: {current_ror}")
            try:
                count = build_researcher_names_cache(current_ror)
                click.echo(f"✅ Success: {count} profiles updated.")
            except Exception as e:
                click.echo(f"❌ Error syncing {current_ror}: {e}")

        click.echo("🏁 Profile synchronization finished.")
