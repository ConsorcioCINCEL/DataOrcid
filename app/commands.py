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
    @click.option("--target", default="both", type=click.Choice(['works', 'fundings', 'both']), help="Data type to synchronize.")
    @with_appcontext
    def rebuild_caches(ror, target):
        """Rebuild works and/or funding caches for one or all institutions."""
        # Keep imports local so CLI registration does not trigger service imports early.
        from . import db
        from .models import WorkCacheRun, FundingCacheRun
        from .services.cache_service import build_works_cache_for_ror, build_fundings_cache_for_ror
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

            if target in ['works', 'both']:
                try:
                    click.echo(f"  > Initializing Works synchronization...")
                    count = build_works_cache_for_ror(current_ror, base_url=member_url, headers=headers)
                    
                    click.echo(f"  > [Works] Success: {count} records synchronized.")
                    _log_execution_run(WorkCacheRun, current_ror, 'success', count)
                except Exception as exc:
                    db.session.rollback()
                    click.echo(f"  > [Works] Critical failure: {exc}")
                    _log_execution_run(WorkCacheRun, current_ror, 'failed', 0, str(exc))

            if target in ['fundings', 'both']:
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

    @app.cli.command("sync-openalex-works")
    @click.option("--ror", default=None, help="Target a specific institutional ROR ID.")
    @click.option("--all", "all_institutions", is_flag=True, help="Scan every known institution.")
    @click.option("--limit", default=None, type=int, help="Maximum DOI count to process per scope.")
    @click.option("--force", is_flag=True, help="Refresh records even when the local OpenAlex cache is fresh.")
    @click.option("--stale-days", default=None, type=int, help="Refresh cached records older than this many days.")
    @click.option("--include-all-types", is_flag=True, help="Include every ORCID work type, not only journal articles.")
    @click.option("--dry-run", is_flag=True, help="Count candidate DOI values without calling OpenAlex.")
    @with_appcontext
    def sync_openalex_works_command(ror, all_institutions, limit, force, stale_days, include_all_types, dry_run):
        """Enrich local DOI-backed works with OpenAlex metadata."""
        from .services.institution_registry_service import get_institution_options
        from .services.openalex_service import OpenAlexConfigError, sync_openalex_works

        if ror and all_institutions:
            click.echo("Use either --ror or --all, not both.")
            return
        if not ror and not all_institutions:
            click.echo("Choose a scope with --ror <ROR_ID> or --all.")
            return

        if all_institutions:
            try:
                scopes = [item["ror_id"] for item in get_institution_options() if item.get("ror_id")]
            except Exception as exc:
                click.echo(f"Database Query Error: {exc}")
                return
        else:
            scopes = [ror]

        articles_only = not include_all_types
        mode = "dry-run" if dry_run else "sync"
        click.echo(f"Starting OpenAlex {mode} for {len(scopes)} scope(s).")

        for current_ror in scopes:
            click.echo(f"\nProcessing ROR: {current_ror}")
            try:
                summary = sync_openalex_works(
                    ror_id=current_ror,
                    limit=limit,
                    force_refresh=force,
                    stale_days=stale_days,
                    articles_only=articles_only,
                    dry_run=dry_run,
                )
            except OpenAlexConfigError as exc:
                click.echo(f"OpenAlex configuration error: {exc}")
                return

            click.echo(
                "Works: {works_seen} | DOI candidates: {dois_found} | "
                "Fetched: {fetched_count} | Matched: {matched_count} | "
                "Not found: {not_found_count} | Skipped: {skipped_count} | "
                "Errors: {error_count} | Status: {status}".format(**summary)
            )
            if summary.get("error"):
                click.echo(f"Error: {summary['error']}")

        click.echo("\nOpenAlex synchronization finished.")
