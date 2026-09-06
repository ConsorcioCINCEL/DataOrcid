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
- A self-explanatory public landing page with calls to action and a contact form
  that stores inquiries in a private administrative inbox, with optional SMTP
  notifications, and can be enabled or disabled globally.
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
`jobs.execution_mode = "thread"` during development. Production defaults to
`queue` when the setting is omitted. For multi-worker production configure
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

Institutional writers share PostgreSQL locks across web workers, CLI tasks and
transaction commits. Duplicate active jobs reuse one queue entry. ORCID profiles
are downloaded into temporary version tables before the published cache changes;
source rows, canonical links, analytics facts and OAI publication events are then
promoted in one transaction. Failed derivation preserves the previous version.
Partial profile downloads retain existing data and report failed/retained counts;
staff can retry only failed profiles from synchronization controls.

Without a DOI, a title/year match is a review candidate rather than an automatic
merge. Staff can confirm or split groups under Data quality → Technical integrity.
Decisions retain their source identifiers and review reasons across rebuilds.
OAI publication selections are preserved during regrouping.

OAI-PMH retains immutable publication events and persistent withdrawal headers.
Resumption tokens pin a publication revision, so changes between pages do not
alter an ongoing harvest. A later harvest sees withdrawals and real metadata
changes. After upgrading an existing installation, initialize its publication
history with `flask publish-oai-records` before reopening OAI access. Harvests
using tokens issued before this upgrade must restart. Retain this
history in database backups; it supplies deletion notices and stable harvests.

Account creation and access requests queue email intents in the same transaction
as the account operation. Messages contain a password-setting link and, for
welcome/access messages, the localized PDF manual. They never contain a password.
Links expire after 24 hours and stop working once the password changes. Supervise
`flask run-email-worker` separately from the sync worker so long harvests cannot
delay mail. Delivery retries up to five times with increasing delays; account
lists show the latest email status, and the access action can queue a fresh
attempt. SMTP delivery can be repeated after a worker interruption; retries use
a stable Message-ID and never invalidate the existing password.

User-service examples are provided in `config/systemd/`. Adjust their checkout
and virtualenv paths if needed, configure the web service to use queue mode,
then install and enable the workers:

```bash
mkdir -p ~/.config/systemd/user
cp config/systemd/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now dataorcid-job-worker@1 dataorcid-job-worker@2 dataorcid-email-worker
```

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
The **Public landing page** switch controls both the visitor homepage and its
contact form. When disabled, `/` sends visitors directly to sign-in; signed-in
accounts always keep the institutional dashboard at `/`. Valid contact messages
are stored durably in the private inbox at `/admin/contact-inquiries` even when
SMTP is unavailable; email is an optional notification channel, not the system
of record.

Administrators choose **Default landing page language** in the same module
panel and save with **Save module settings**. The supported choices are English,
Spanish, French, Portuguese, and German, subject to the enabled language list.
This applies to the anonymous homepage and contact form when no visitor language
has been selected. URL, session, and account preferences retain priority. The
value is stored in `system_module.default_locale`, takes effect across workers
on the next request, and survives disabling and re-enabling the landing page.
An unset or no-longer-enabled preference falls back to the application language.

The authenticated Help center is available at `/help/` to every role. Its
content deliberately covers only standard User and OAI User workflows; staff
and system operations stay in this repository documentation. Help topics,
blocks, links, search results, and direct topic URLs use the same global module
switches as the application, so documentation cannot advertise a disabled
area.

Compile localization catalogs during deployment with
`pybabel compile -d app/translations`; the generated `.mo` files are runtime
artifacts and are intentionally excluded from Git.


## Transactional email

Welcome/credential, password-recovery, and contact-notification messages share
`app/templates/emails/base.html`: the site's charcoal header, DataORCID wordmark,
orange actions, blue details, and a readable plain-text alternative. Layout and
colors are inline and do not require remote images or stylesheets.

Creating an account queues a welcome message with a password-setting link and
the PDF user manual. Subsequent access emails include both links. The account locale
controls welcome and recovery messages, and each account receives the manual
link in its own language: English, Spanish, French, Portuguese, or German.
Deploy all five `manuals/dataorcid-chile-user-manual-v<APP_VERSION>-{en,es,fr,pt,de}.pdf` files.
The public routes `/manuals/user-guide/{en,es,fr,pt,de}.pdf` serve only these five files,
without requiring sign-in; downloads revalidate their cache and support ranges.
Signed-in users can also download the guide from the compact, localized
“User manual” link with a PDF icon beside their account options in the toolbar.
The link follows the active interface
language and remains available when optional modules such as Help are disabled.
If the guide is unavailable, account email is not sent with a broken link. A failed welcome preserves
the created account for retry, and access-link requests preserve the existing
password until the account holder chooses a replacement. Contact notification failures continue to preserve inquiries.

Use the configured SMTP service and a public `app.base_url` for working email
links. `MAIL_REPLY_TO` is used for account messages; contact notifications retain
the visitor's address as Reply-To. Samples contain fictional account details and
nonfunctional password-setting links, and never create or change accounts:

```bash
python tools/preview_emails.py
# Explicitly send four samples, including Spanish and English PDF links:
python tools/preview_emails.py --send-to recipient@example.org --base-url https://dataorcid.example.org
```

HTML/text previews and the SMTP acceptance report are written to
`/tmp/dataorcid-email-previews/`. SMTP acceptance does not confirm inbox delivery.

## Institutional OAI-PMH

The **OAI-PMH** module makes DataORCID-Chile an institutional metadata
provider. DSpace, DSpace-CRIS, or another external harvester queries the
generated endpoint and receives only the articles authorized for that ROR.

- Initial configuration: `/oai-pmh/`
- Article selection and audit: `/oai-pmh/articles/`
- Metadata formats and mapping: `/oai-pmh/metadata/`
- Bulk DOI activation and upload history: `/oai-pmh/doi-import/`
- Institutional harvesting access: `/oai-pmh/access/`
- Private provider per registered repository: `/oai/<public_key>/<harvester_key>`
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
mapping, article selection, DOI uploads, and harvesting access within its assigned institution. Provider
activation, global policy changes, and public-key rotation remain restricted
to managers and administrators; both roles retain full OAI management access.

At **Harvesting access**, OAI Users register up to 20 HTTP(S) repository URIs
and copy a separate, randomly generated 192-bit private URL for each harvester.
The URI labels the recipient; possession of the private URL authorizes a request.
It does not prove domain ownership or prevent a holder from sharing the URL.
There are no DNS, IP, Origin, or Referer checks, so Cloudflare proxy addresses
have no bearing on this authorization. Standard Users see repository status,
but cannot manage or view private credentials.

Existing institutional URLs remain available by default. After updating clients,
enable **Allow harvesting only through registered private URLs** to return HTTP
403 on the general endpoint. Invalid, revoked, removed, or foreign credentials
return HTTP 404 before metadata generation. Revoking the last credential keeps
restricted mode closed. Replacing one private URL leaves other harvesters intact;
rotating the institutional public key changes the parent path of every private URL.

In DSpace-CRIS, set **OAI Provider** to the complete private base URL (without
`?verb=...`), choose **Simple Dublin Core** (`oai_dc`) and **metadata only**, then
start or schedule the harvest. No interactive DataORCID login is needed. Custom
`dataorcid` and OpenAIRE formats require compatible ingestion mappings in the
receiving system. See the [DSpace-CRIS import documentation](https://wiki.lyrasis.org/spaces/DSPACECRIS/pages/403767433/Import%2Bvia%2BOAI-PMH).

Private URLs act as credentials, including in browsers. Serve them over HTTPS
and keep them out of public pages. The application sends `private, no-store`
and `Referrer-Policy: no-referrer`, and redacts OAI URL credentials in activity
and error records. Reverse-proxy access logs need equivalent redaction. Configure
any CDN cache overrides to bypass `/oai/*`, and purge previously cached provider
responses when enabling restrictions, so cached XML cannot bypass revocation.
Browser challenges on the provider path would interrupt server-to-server harvesting.

A real HTTP integration check using the optional **Sickle 0.7.0** client is
available in `tests/oai_harvester_smoke.py`. It creates an isolated SQLite fixture
and loopback server, exercises all six verbs, GET/POST, pagination, tenant isolation,
and revocation, and writes a report without credential URLs:

```bash
python tests/oai_harvester_smoke.py --report /tmp/oai-harvester-report.json
```

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

The PostgreSQL concurrency/publication checks require a disposable database:

```bash
DATABASE_URL=postgresql+psycopg://localhost/dataorcid_test_reliability flask db upgrade
DATAORCID_TEST_POSTGRES_URI=postgresql+psycopg://localhost/dataorcid_test_reliability python -m pytest -q tests/test_postgres_reliability.py
```

These integration tests clear application tables and reject database names that
do not start with `dataorcid_test_`. Ordinary tests use isolated SQLite databases.
