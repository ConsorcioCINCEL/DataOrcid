# Data ORCID-Chile 2.1 🇨🇱

**DataOrcid-Chile** is a scientific production management and monitoring platform designed specifically to meet the needs of the **Chilean Consortium**. This project was developed by **Gastón Olivares** at **Cincel** to enhance the visibility and tracking of research records linked to Chilean institutions.

The platform allows institutions to synchronize, cache, and export data (Researchers, Works, Fundings, and Profiles) directly from ORCID APIs. Institutional discovery combines verified ROR, GRID, and historical Ringgold identifiers and deduplicates matches by ORCID iD.

Ringgold identifiers in the bundled Chilean university dataset were validated against public ORCID affiliation records whose disambiguation source is `RINGGOLD`. ROR remains the canonical institutional identifier because ORCID no longer updates its Ringgold registry data.

## Version 2.1 highlights

- Durable synchronization and export jobs with progress, recovery, retention,
  per-user cleanup, and reuse of equivalent completed exports.
- Background CSV/XLSX generation with a persistent floating notification,
  incremental XLSX writing, and no former 100,000-row application limit.
- Institution-scoped OAI-PMH publishing with article selection, metadata
  crosswalks, auditable DOI imports, and `oai-user` permissions.
- OpenAlex analytical caches, data-quality and duplicate-profile views, module
  availability controls, and sanitized system-error monitoring.
- A task-oriented sidebar and a searchable Help center. Help remains visible to
  every signed-in role, documents only the User and OAI User experience, and
  automatically omits content and direct topic routes for disabled modules.
- A five-language interface with English as the source language and complete
  Spanish, French, Portuguese, and German Babel catalogs.

## Project structure

```text
app/
├── blueprints/        # Focused Flask routes, including split works_* areas
├── services/          # ORCID/OpenAlex, jobs, exports, quality, and access rules
├── templates/         # Jinja2 pages and shared interface components
├── static/            # CSS, JavaScript, schemas, and OAI presentation assets
├── translations/      # Babel source catalogs
├── models.py          # SQLAlchemy persistence model
├── decorators.py      # Authentication, role, and institution guards
└── __init__.py        # Application factory and shared extensions
migrations/            # Flask-Migrate/Alembic schema history
tests/                 # Unit and regression suite
config/config.toml.example
run.py
```

---

## 🚀 Installation & Setup

### 1. Prerequisites

- Python 3.9 or higher.
- ORCID API credentials (Public or Member API).
- PostgreSQL for production; SQLite is supported for isolated development and tests.

### 2. Clone and Prepare Environment

```bash
git clone https://github.com/ConsorcioCINCEL/DataOrcid.git
cd DataOrcid
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```


### 3. Configuration (config.toml)
The system uses a TOML file for settings. Create the file at config/config.toml.

```bash
cp config/config.toml.example config/config.toml
```

In production, set `[app].environment = "production"`,
`[flask].allow_insecure_dev_config = false`, HTTPS cookies, and an HTTPS
`base_url`. Credentials can override TOML through `ORCID_DATABASE_URI` (or
`DATABASE_URL`), `SECRET_KEY`, `SECURITY_PASSWORD_SALT`, `ORCID_CLIENT_ID`,
`ORCID_CLIENT_SECRET`, `OPENALEX_API_KEY`, and `MAIL_PASSWORD`.

The language selector is available on public and authenticated pages. Configure
the enabled choices and fallback locale as follows:

```toml
[languages]
supported = ["en", "es", "fr", "pt", "de"]
default = "en"
```

English is the source language used by Babel `msgid` entries. The repository
ships complete catalogs for Spanish, French, Portuguese, and German.


### 4. Initialize Database

```bash
export FLASK_APP=run.py
flask db upgrade
flask seed-db
```

---

## 🛠️ Execution

### Launch Development Server

```bash
python run.py
```


### Launch in Production (Gunicorn)

```bash
gunicorn --workers 4 --bind 0.0.0.0:5000 "run:app"
```


---

## 🔄 Cache Management (CLI)
The system utilizes a local cache to prevent ORCID API rate-limiting. CLI commands are optimized for **Member API Mode**. A full synchronization searches every verified institutional identifier and downloads each ORCID profile once:

```bash
# Sync ALL institutions
flask rebuild-caches

# Sync a specific institution (using ROR ID)
flask rebuild-caches --ror 02ap3w078

# Preview selected institutions without calling ORCID
flask rebuild-caches --dry-run

# Sync researcher profiles only (Names/Bio)
flask sync-researcher-names

# Rebuild the indexed OpenAlex analytics fact layer
flask rebuild-openalex-analytics

# Rebuild it for one institution
flask rebuild-openalex-analytics --ror 02ap3w078

# Populate extended export metadata from the existing raw OpenAlex cache
flask rebuild-openalex-metadata --batch-size 2000
# Resume after the last raw-cache ID reported in the log
flask rebuild-openalex-metadata --batch-size 2000 --start-after-id 150000
```

The analytics layer is refreshed automatically after Works or OpenAlex
synchronization. When upgrading an existing database, run `flask db upgrade`
and then `flask rebuild-openalex-analytics` so optimized filters are available
immediately.

Large cache and OpenAlex CSV/XLSX exports are submitted to the same durable job
system. A floating notification follows their queued/running state across page
navigation and exposes the private download only when the file is complete.
Generated files live outside the static tree, are restricted to the requesting
account (with administrator oversight), and expire after the configured
retention period. Expired files and their completed job records are pruned
automatically; users can also delete all of their finished exports from the
floating export center without interrupting queued or running work. Operators
can force the same retention cleanup with `flask cleanup-exports`. Identical
requests from the same account reuse a still-valid completed file; a source-data
revision automatically invalidates that reuse and queues a fresh export. XLSX
generation remains incremental and has no former
100,000-row application limit. If web and queue workers run on different hosts,
`exports.directory` must point to storage shared by both.

Raw DOI values remain complete in a `TEXT` column. Searches and joins use a
validated, normalized DOI key limited to 255 characters; invalid or oversized
values are neither truncated nor indexed as DOI keys, preventing failures and
identifier collisions.

Web-triggered long-running syncs keep the same request and result behavior in
the default `jobs.execution_mode = "thread"`. For multi-worker production use
the durable database queue instead:

```toml
[jobs]
execution_mode = "queue"
stale_minutes = 30
max_attempts = 3
heartbeat_seconds = 30

[exports]
directory = "instance/exports"
retention_hours = 24
max_active_per_user = 3
max_active_global = 20

[tracking]
retention_days = 90
error_monitoring_enabled = true
error_retention_days = 90
```

Run a separately supervised worker with `flask run-job-worker`. It atomically
claims queued work, persists its handler and arguments, refreshes an independent
execution heartbeat, and requeues recoverable jobs after an interrupted process.
`flask recover-interrupted-jobs` is also
available for manual recovery. Do not start the application before applying
`flask db upgrade`; schema creation and seed data are intentionally no longer
performed during web startup.

Administrators can review sanitized runtime failures at `/admin/errors`.
Events include the affected account and institution, endpoint, path, request or
background-job correlation ID, exception type, recurring fingerprint, and
traceback. Query strings, request bodies, cookies, authorization headers, and
credentials are intentionally excluded. Apply retention with
`flask cleanup-system-errors`; normal request activity continues to use
`flask cleanup-tracking-logs`.

Global availability for optional application areas is managed at
`/admin/modules`. Disabling a module removes it from navigation and makes the
server deny its pages, forms, APIs, downloads, and public endpoints. Overview,
authentication, personal settings, and the module control panel remain active
to preserve administrative recovery. Settings are stored in `system_module`.

The authenticated Help center is available at `/help/` to every role. Its
content deliberately covers only standard User and OAI User workflows; staff
and system operations stay in this repository documentation. Help topics,
blocks, links, search results, and direct topic URLs use the same global module
switches as the application, so documentation cannot advertise a disabled
area.

Compile localization catalogs during deployment with
`pybabel compile -d app/translations`; the generated `.mo` files are runtime
artifacts and are intentionally excluded from Git.


## Institutional OAI-PMH

The **OAI-PMH** module makes DataORCID-Chile an institutional metadata
provider. DSpace, DSpace-CRIS, or another external harvester queries the
generated endpoint and receives only the articles authorized for that ROR.

- Initial configuration: `/oai-pmh/`
- Article selection and audit: `/oai-pmh/articles/`
- Metadata formats and mapping: `/oai-pmh/metadata/`
- Bulk DOI activation and upload history: `/oai-pmh/doi-import/`
- Administrator and manager inventory: `/admin/oai-pmh`
- Public provider per institution: `/oai/<public_key>`
- Published formats: unqualified Dublin Core (`oai_dc`), OpenAIRE 4
  (`oai_openaire`), and an institution-mapped profile (`dataorcid`)
- Verbs: `Identify`, `ListMetadataFormats`, `ListSets`, `GetRecord`,
  `ListIdentifiers`, and `ListRecords`
- Data source: canonical, deduplicated DataORCID-Chile works
- Default policy: expose only articles whose active-ROR affiliation OpenAlex validates
- Optional policies: all public articles or explicitly selected articles only
- Individual quick actions and bulk include/exclude overrides
- Sorting by title, creator, year, type, affiliations, validation, and exposure
- Per-work OpenAlex affiliations with the active ROR explicitly highlighted
- Bulk DOI activation through a guided XLSX template, restricted to the institutional profile
- An auditable history for every Excel file, including totals and article-level details; the latest active batch remains reversible while preserving later manual edits
- Complete filtered CSV/XLSX audit exports with all affiliations and each publication decision source; XLSX uses write-only generation and skips XML metadata work that is not present in the spreadsheet
- Per-institution destination-name mapping for the `dataorcid` profile
- Configurable DataORCID/OpenAlex field catalog with optional add/hide controls
- Incremental DSpace harvesting with `resumptionToken` support
- Public lists contain only effectively exposed articles
- A custom XSL browser presentation while preserving the protocol XML

Configuration and selection remain bound to the account's ROR. Automatic
validation requires OpenAlex to link one article authorship to the active ROR;
manual include or exclude decisions take precedence. Administrators
may use the institution switcher; managers can modify only their assigned
institution. The public URL is generated automatically with a rotatable random
192-bit key, and no external OAI source is configured. The administration
inventory summarizes provider status, associated and exposed works, OpenAlex
validation, and manual decisions for each institution in scope.

Metadata formats and the custom crosswalk are managed separately at
`/oai-pmh/metadata/`. The `dataorcid` profile exposes only its active fields;
optional DataORCID and OpenAlex fields can be added, renamed, or hidden without
changing the fixed `oai_dc` and `oai_openaire` schemas.

The `oai-user` role inherits standard-user access and can manage metadata
mapping, article selection, and DOI uploads within its assigned institution. Provider
activation, global policy changes, and public-key rotation remain restricted
to managers and administrators; both roles retain full OAI management access.

The `oai_openaire` format is available by default as a starting point for
interoperability with ANID, Espacio Ciencia, and LA Referencia. It implements
the OpenAIRE 4 profile, COAR vocabularies, and the `openaire` OAI set. Standard `oai_dc` and
`oai_openaire` element names remain fixed; aliases such as `dc.titulo` belong
to the safe, explicitly advertised `dataorcid` profile.

Provider examples:

```text
/oai/<public_key>?verb=Identify
/oai/<public_key>?verb=ListRecords&metadataPrefix=oai_dc
/oai/<public_key>?verb=ListRecords&metadataPrefix=oai_openaire
/oai/<public_key>?verb=ListRecords&metadataPrefix=dataorcid
```

---

## 📝 License
This project is licensed under the **MIT** License.

**Developed by:** Gastón Olivares
**Institution:** Chilean Consortium, Cincel.
