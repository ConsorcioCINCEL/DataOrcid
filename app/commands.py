"""Flask CLI commands for cache rebuilds and profile metadata sync."""

import logging
import click
from flask.cli import with_appcontext
from flask import current_app

logger = logging.getLogger(__name__)


def register_commands(app):
    """Register maintenance commands on the Flask application."""

    @app.cli.command("seed-db")
    @with_appcontext
    def seed_db_command():
        """Seed the institution registry and create the initial admin account."""
        from .database import populate_users
        from .services.institution_registry_service import seed_chilean_universities

        institution_count = seed_chilean_universities()
        message, status = populate_users()
        if status >= 400:
            raise click.ClickException(message)
        click.echo(
            f"Database seed completed. Institutional changes: {institution_count}. {message}"
        )

    @app.cli.command("recover-interrupted-jobs")
    @click.option(
        "--minutes",
        default=None,
        type=click.IntRange(min=1),
        help="Override the configured stale heartbeat threshold.",
    )
    @with_appcontext
    def recover_interrupted_jobs_command(minutes):
        """Requeue recoverable jobs abandoned by a stopped worker."""
        from .services.background_jobs import recover_interrupted_jobs

        stale_minutes = minutes or int(current_app.config.get("JOB_STALE_MINUTES", 30))
        count = recover_interrupted_jobs(stale_minutes)
        click.echo(f"Recovered {count} stale background job(s).")

    @app.cli.command("cleanup-exports")
    @with_appcontext
    def cleanup_exports_command():
        """Delete private generated exports beyond their retention period."""
        from .services.export_jobs import cleanup_expired_exports

        result = cleanup_expired_exports(force=True)
        click.echo(
            "Removed "
            f"{result['files']} expired export file(s) and "
            f"{result['jobs']} completed job record(s)."
        )

    @app.cli.command("run-job-worker")
    @click.option("--once", is_flag=True, help="Process at most one queued job and exit.")
    @click.option(
        "--poll-seconds",
        default=2.0,
        type=click.FloatRange(min=0.1),
        show_default=True,
        help="Delay between empty queue checks.",
    )
    @with_appcontext
    def run_job_worker_command(once, poll_seconds):
        """Run the durable database-backed synchronization worker."""
        import time
        import uuid

        from .services.background_jobs import run_queued_job

        app_object = current_app._get_current_object()
        worker_id = f"cli:{uuid.uuid4().hex[:12]}"
        click.echo(f"Background job worker started ({worker_id}).")
        while True:
            job_id = run_queued_job(app_object, worker_id)
            if job_id:
                click.echo(f"Processed job {job_id}.")
            elif once:
                click.echo("No queued jobs found.")
                return
            if once:
                return
            time.sleep(poll_seconds)

    @app.cli.command("run-email-worker")
    @click.option("--once", is_flag=True, help="Process at most one due email and exit.")
    @click.option("--poll-seconds", default=2.0, type=click.FloatRange(min=0.1))
    @with_appcontext
    def run_email_worker_command(once, poll_seconds):
        """Deliver transactional account emails independently of long sync jobs."""
        import time
        from . import db
        from .services.email_outbox import deliver_next_email
        while True:
            try:
                email_id = deliver_next_email()
                if email_id:
                    click.echo(f"Processed email intent {email_id}.")
            finally:
                db.session.remove()
            if once:
                return
            time.sleep(poll_seconds)

    @app.cli.command("publish-oai-records")
    @click.option("--ror", default=None, help="Publish one institution, or initialize all repositories.")
    @with_appcontext
    def publish_oai_records_command(ror):
        """Initialize publication history before reopening upgraded repositories."""
        from . import db
        from .models import OaiPmhInstitutionConfig
        from .services.oai_publication_service import refresh_oai_publication
        query = OaiPmhInstitutionConfig.query
        if ror:
            query = query.filter_by(ror_id=ror)
        for config in query.all():
            refresh_oai_publication(config.ror_id)
            db.session.commit()
            click.echo(f"Published OAI records for {config.ror_id}.")

    @app.cli.command("rebuild-caches")
    @click.option("--ror", default=None, help="Target specific ROR ID. If omitted, scans all active institutions.")
    @click.option("--start-at", default=None, help="Resume an all-institution rebuild at this ROR ID.")
    @click.option("--target", default="both", type=click.Choice(['works', 'fundings', 'both']), help="Data type to synchronize.")
    @click.option("--limit-orcids", default=None, type=int, help="Testing only: limit profile fetches per institution.")
    @with_appcontext
    def rebuild_caches(ror, start_at, target, limit_orcids):
        """Rebuild works and/or funding caches for one or all institutions."""
        # Keep imports local so CLI registration does not trigger service imports early.
        from . import db
        from .models import FundingCacheRun, WorkCacheRun, utc_now
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
        if limit_orcids:
            click.echo(
                "⚠️  Testing mode: each selected ROR cache will be rebuilt from only "
                f"the first {limit_orcids} ORCID iDs found."
            )

        def _log_execution_run(model_class, ror_id, status, count, error_msg=None):
            """Persist a cache-run audit record."""
            try:
                if not ror_id: return

                execution_time = utc_now()
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
                    result = build_full_cache_for_ror(
                        current_ror,
                        base_url=member_url,
                        headers=headers,
                        max_orcids=limit_orcids,
                    )
                    status = "partial" if result.get("errors") else "success"
                    click.echo(
                        f"  > {status}: "
                        f"{result['researchers']} researchers, {result['works']} works, "
                        f"{result['fundings']} fundings, and {result['profiles']} profiles."
                    )
                    _log_execution_run(WorkCacheRun, current_ror, status, result['works'])
                    _log_execution_run(FundingCacheRun, current_ror, status, result['fundings'])
                except Exception as exc:
                    db.session.rollback()
                    click.echo(f"  > Full metadata synchronization failed: {exc}")
                    _log_execution_run(WorkCacheRun, current_ror, 'failed', 0, str(exc))
                    _log_execution_run(FundingCacheRun, current_ror, 'failed', 0, str(exc))
                continue

            if target == 'works':
                try:
                    click.echo("  > Initializing Works synchronization...")
                    result = build_works_cache_for_ror(
                        current_ror,
                        base_url=member_url,
                        headers=headers,
                        max_orcids=limit_orcids,
                        return_result=True,
                    )
                    count = result["works"]
                    status = "partial" if result.get("errors") else "success"
                    click.echo(f"  > [Works] {status}: {count} records synchronized.")
                    _log_execution_run(WorkCacheRun, current_ror, status, count)
                except Exception as exc:
                    db.session.rollback()
                    click.echo(f"  > [Works] Critical failure: {exc}")
                    _log_execution_run(WorkCacheRun, current_ror, 'failed', 0, str(exc))

            if target == 'fundings':
                try:
                    click.echo(f"  > Initializing Funding synchronization...")
                    result = build_fundings_cache_for_ror(
                        current_ror,
                        base_url=member_url,
                        headers=headers,
                        max_orcids=limit_orcids,
                        return_result=True,
                    )
                    
                    count = result["fundings"]
                    status = "partial" if result.get("errors") else "success"
                    click.echo(f"  > [Fundings] {status}: {count} records synchronized.")
                    _log_execution_run(FundingCacheRun, current_ror, status, count)
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
    @click.option("--system", "system_wide", is_flag=True, help="Scan all works in one system-wide run.")
    @click.option("--limit", default=None, type=int, help="Maximum DOI count to process per scope.")
    @click.option("--force", is_flag=True, help="Refresh records even when the local OpenAlex cache is fresh.")
    @click.option("--stale-days", default=None, type=int, help="Refresh cached records older than this many days.")
    @click.option("--include-all-types", is_flag=True, help="Include every ORCID work type, not only journal articles.")
    @click.option("--dry-run", is_flag=True, help="Count candidate DOI values without calling OpenAlex.")
    @click.option("--workers", default=None, type=int, help="Parallel DOI fetch workers. Defaults to openalex.workers.")
    @click.option("--title-fallback", is_flag=True, help="Search by title only for DOI misses and works without DOI.")
    @with_appcontext
    def sync_openalex_works_command(ror, all_institutions, system_wide, limit, force, stale_days, include_all_types, dry_run, workers, title_fallback):
        """Enrich local DOI-backed works with OpenAlex metadata."""
        from .services.institution_registry_service import get_institution_options
        from .services.openalex_service import (
            OpenAlexConfigError,
            sync_openalex_title_matches,
            sync_openalex_works,
        )

        selected_scopes = sum(bool(value) for value in (ror, all_institutions, system_wide))
        if selected_scopes > 1:
            click.echo("Use only one of --ror, --all, or --system.")
            return
        if not selected_scopes:
            click.echo("Choose a scope with --ror <ROR_ID>, --all, or --system.")
            return

        if system_wide:
            scopes = [None]
        elif all_institutions:
            try:
                scopes = [item["ror_id"] for item in get_institution_options() if item.get("ror_id")]
            except Exception as exc:
                click.echo(f"Database Query Error: {exc}")
                return
        else:
            scopes = [ror]

        articles_only = not include_all_types
        mode = "title fallback" if title_fallback else "DOI sync"
        if dry_run:
            mode = f"{mode} dry-run"
        click.echo(f"Starting OpenAlex {mode} for {len(scopes)} scope(s).")

        for current_ror in scopes:
            scope_label = current_ror or "system-wide"
            click.echo(f"\nProcessing scope: {scope_label}")
            try:
                sync_func = sync_openalex_title_matches if title_fallback else sync_openalex_works
                summary = sync_func(
                    ror_id=current_ror,
                    limit=limit,
                    force_refresh=force,
                    stale_days=stale_days,
                    articles_only=articles_only,
                    dry_run=dry_run,
                    workers=workers,
                )
            except OpenAlexConfigError as exc:
                click.echo(f"OpenAlex configuration error: {exc}")
                return

            click.echo(
                "Works: {works_seen} | Candidates: {dois_found} | "
                "Workers: {workers} | "
                "Fetched: {fetched_count} | Matched: {matched_count} | "
                "Not found: {not_found_count} | Skipped: {skipped_count} | "
                "Errors: {error_count} | Status: {status}".format(**summary)
            )
            if summary.get("error"):
                click.echo(f"Error: {summary['error']}")

        click.echo("\nOpenAlex synchronization finished.")

    @app.cli.command("rebuild-openalex-dimensions")
    @click.option("--limit", default=None, type=int, help="Maximum raw OpenAlex records to process.")
    @click.option("--batch-size", default=50, type=int, help="Raw records to process before each commit.")
    @click.option("--missing-only", is_flag=True, help="Only process raw records without author dimension rows.")
    @click.option("--reset", is_flag=True, help="Delete existing OpenAlex dimensions before rebuilding.")
    @with_appcontext
    def rebuild_openalex_dimensions_command(limit, batch_size, missing_only, reset):
        """Build author and institution dimensions from stored OpenAlex raw JSON."""
        from .services.analytics_service import refresh_openalex_facts
        from .services.openalex_service import rebuild_openalex_dimensions

        click.echo("Rebuilding OpenAlex author and institution dimensions from raw cache...")
        def _progress(processed, author_rows, institution_rows, last_id):
            click.echo(
                f"Processed {processed} raw records | "
                f"authors {author_rows} | institutions {institution_rows} | last raw id {last_id}"
            )

        summary = rebuild_openalex_dimensions(
            limit=limit,
            batch_size=batch_size,
            missing_only=missing_only,
            reset=reset,
            progress=_progress,
        )
        analytics_summary = refresh_openalex_facts()
        click.echo(
            "Processed: {processed} | Author rows: {author_rows} | "
            "Institution rows: {institution_rows} | Analytics rows: "
            "{analytics_rows}".format(
                **summary,
                analytics_rows=analytics_summary["rows"],
            )
        )

    @app.cli.command("rebuild-openalex-metadata")
    @click.option("--limit", default=None, type=int, help="Maximum raw OpenAlex records to process.")
    @click.option("--batch-size", default=500, type=int, help="Raw records to process before each commit.")
    @click.option("--start-after-id", default=0, type=int, help="Resume after a raw-cache primary key.")
    @with_appcontext
    def rebuild_openalex_metadata_command(limit, batch_size, start_after_id):
        """Rebuild exportable OpenAlex metadata from the local raw cache."""
        from .services.openalex_service import rebuild_openalex_metadata

        click.echo("Rebuilding queryable OpenAlex metadata from raw cache...")

        def _progress(processed, created, updated, last_id):
            click.echo(
                f"Processed {processed} raw records | created {created} | "
                f"updated {updated} | last raw id {last_id}"
            )

        summary = rebuild_openalex_metadata(
            limit=limit,
            batch_size=batch_size,
            start_after_id=start_after_id,
            progress=_progress,
        )
        click.echo(
            "Processed: {processed} | Created: {created} | Updated: {updated}".format(
                **summary
            )
        )

    @app.cli.command("rebuild-openalex-analytics")
    @click.option("--ror", default=None, help="Target one ROR ID; omit it to rebuild every institution.")
    @with_appcontext
    def rebuild_openalex_analytics_command(ror):
        """Rebuild the filterable OpenAlex analytics fact layer."""
        from .services.analytics_service import refresh_openalex_facts

        summary = refresh_openalex_facts(ror)
        if ror:
            click.echo(
                "ROR: {ror_id} | Source records: {source_records} | "
                "Analytics rows: {rows}".format(**summary)
            )
        else:
            click.echo(
                "Institutions: {institutions} | Analytics rows: {rows}".format(
                    **summary
                )
            )

    @app.cli.command("rebuild-data-trust")
    @click.option("--ror", default=None, help="Target one ROR ID; omit it to process every institution.")
    @click.option("--skip-associations", is_flag=True, help="Do not rebuild inferred researcher relationships.")
    @click.option("--skip-works", is_flag=True, help="Do not rebuild canonical scholarly outputs.")
    @with_appcontext
    def rebuild_data_trust_command(ror, skip_associations, skip_works):
        """Rebuild provenance-aware researcher links and canonical works."""
        from .services.canonical_work_service import rebuild_canonical_works
        from .services.data_trust_service import backfill_inferred_associations

        if skip_associations and skip_works:
            raise click.UsageError("At least one rebuild must remain enabled.")
        if not skip_associations:
            click.echo("Rebuilding inferred institutional researcher relationships...")
            summary = backfill_inferred_associations(ror)
            click.echo(
                "Relationships: {associations} | Created: {created} | Updated: {updated}".format(
                    **summary
                )
            )
        if not skip_works:
            click.echo("Rebuilding canonical scholarly outputs...")
            summary = rebuild_canonical_works(ror)
            click.echo(
                "Source records: {source_records} | Unique outputs: {unique_outputs} | "
                "DOI-backed: {doi_outputs}".format(**summary)
            )
        click.echo("Data trust layers rebuilt successfully.")

    @app.cli.command("cleanup-tracking-logs")
    @click.option("--days", default=None, type=click.IntRange(min=1), help="Override the configured retention period.")
    @click.option("--dry-run", is_flag=True, help="Count eligible rows without deleting them.")
    @with_appcontext
    def cleanup_tracking_logs_command(days, dry_run):
        """Delete usage logs older than the configured retention period."""
        from datetime import timedelta

        from . import db
        from .models import TrackingLog, utc_now

        retention_days = days or int(current_app.config.get("TRACKING_RETENTION_DAYS", 90))
        cutoff = utc_now() - timedelta(days=retention_days)
        query = TrackingLog.query.filter(TrackingLog.timestamp < cutoff)
        count = query.count()
        if not dry_run and count:
            query.delete(synchronize_session=False)
            db.session.commit()
        verb = "Would delete" if dry_run else "Deleted"
        click.echo(f"{verb} {count} tracking log row(s) older than {retention_days} days.")

    @app.cli.command("cleanup-system-errors")
    @click.option("--days", default=None, type=click.IntRange(min=1), help="Override the configured retention period.")
    @click.option("--dry-run", is_flag=True, help="Count eligible rows without deleting them.")
    @with_appcontext
    def cleanup_system_errors_command(days, dry_run):
        """Delete sanitized system errors beyond their retention period."""
        from datetime import timedelta

        from . import db
        from .models import SystemError, utc_now

        retention_days = days or int(current_app.config.get("ERROR_LOG_RETENTION_DAYS", 90))
        cutoff = utc_now() - timedelta(days=retention_days)
        query = SystemError.query.filter(SystemError.occurred_at < cutoff)
        count = query.count()
        if not dry_run and count:
            query.delete(synchronize_session=False)
            db.session.commit()
        verb = "Would delete" if dry_run else "Deleted"
        click.echo(f"{verb} {count} system error row(s) older than {retention_days} days.")

    @app.cli.command("repair-openalex-integrity")
    @with_appcontext
    def repair_openalex_integrity_command():
        """Remove orphaned and duplicate OpenAlex dimension rows."""
        from .services.analytics_service import refresh_openalex_facts
        from .services.openalex_service import repair_openalex_integrity

        summary = repair_openalex_integrity()
        analytics_summary = refresh_openalex_facts()
        click.echo(
            "Orphan authors: {orphan_authors_removed} | Orphan institutions: "
            "{orphan_institutions_removed} | Duplicate authors: "
            "{duplicate_authors_removed} | Duplicate institutions: "
            "{duplicate_institutions_removed} | Analytics rows: "
            "{analytics_rows}".format(
                **summary,
                analytics_rows=analytics_summary["rows"],
            )
        )
