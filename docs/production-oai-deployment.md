# Production OAI deployment — 2026-09-09

Production remains in `/var/www/orcid-cincel`, using its original PostgreSQL
database. `/var/www/DataOrcid` remains the separate testing installation.
The application release is `openalex-oai` at `fb7f997`, with production
integration changes maintained on `production/oai-20260909`.

## Database and configuration

The upgrade applied the 12 revisions from `b9e2f4a6c8d0` to `53e7ddc83bf3`.
The existing-database upgrade path omits `ix_tracking_logs_username`, although
the new model requires it. `scripts/ensure_tracking_username_index.py` creates
that index concurrently and verifies its definition. The final `flask db check`
reports no pending schema changes.

Production configuration remains private in `config/config.toml`. It sets
environment `production`, the public HTTPS base URL, Spanish as the default
language, one trusted Nginx proxy hop and job execution mode `queue`.
Signing secrets were rotated; stored account passwords were preserved.

## Services and scheduling

- `orcid-cincel.service`: existing Gunicorn web service using `wsgi:application`.
- `orcid-cincel-worker.service`: production synchronization/export worker.
- `orcid-cincel-email-worker.service`: production transactional-email worker.

All three use the production configuration. Export files are private in
`instance/exports`, writable by the `www-data` group.

The existing `gaston` crontab remains scheduled at 03:00 on Mondays and Thursdays
in `America/Santiago`. The production wrapper preserves the full sequence:
ORCID caches, system-wide OpenAlex enrichment, then integrity and analytics.
It retains the cross-process lock and propagates nonzero subprocess exits.
`python update_caches.py --check` validates command loading without refreshing
data. It does not replace monitoring the results of an actual scheduled run.

## Validation and recovery

The release passed 259 general tests and five PostgreSQL integration tests.
Four additional tests cover the cron pipeline, command failure propagation,
overlap prevention and check mode. A restored production copy passed all 12
migrations, schema validation, OAI screens and queued XLSX generation/download.

Before reopening production, all 26 original table counts and the digest of
account identifiers, passwords, email addresses, institutions and roles matched
the pre-upgrade snapshot. HTTPS, login page, OAI screens and XLSX downloads were
checked. Cron commands were checked as `gaston`; ORCID authentication and an
OpenAlex API request succeeded. A full institutional refresh was not triggered.

Root-only backups, configuration copies, verification logs and rollback notes
are in `/var/backups/orcid-oai-20260909`. The final database snapshot was taken
with the production web service stopped and its cron paused. For rollback after
reopening, reconcile subsequent user changes before using that snapshot.
