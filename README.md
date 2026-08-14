# DataOrcid-Chile 🇨🇱

**DataOrcid-Chile** is a scientific production management and monitoring platform designed specifically to meet the needs of the **Chilean Consortium**. This project was developed by **Gastón Olivares** at **Cincel** to enhance the visibility and tracking of research records linked to Chilean institutions.

The platform allows institutions to synchronize, cache, and export data (Researchers, Works, Fundings, and Profiles) directly from ORCID APIs. Institutional discovery combines verified ROR, GRID, and historical Ringgold identifiers and deduplicates matches by ORCID iD.

Ringgold identifiers in the bundled Chilean university dataset were validated against public ORCID affiliation records whose disambiguation source is `RINGGOLD`. ROR remains the canonical institutional identifier because ORCID no longer updates its Ringgold registry data.

---

## 🚀 Installation & Setup

### 1. Prerequisites
* Python 3.9 or higher.
* Access to ORCID API Keys (Public or Member API).
* Database (MySQL/MariaDB recommended, or SQLite for local dev).

### 2. Clone and Prepare Environment

# Clone the repository
git clone [https://github.com/your-user/dataorcid-chile.git](https://github.com/ConsorcioCINCEL/DataOrcid.git)
cd dataorcid-chile

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt


### 3. Configuration (config.toml)
The system uses a TOML file for settings. Create the file at config/config.toml.

cp config/config.toml.example config/config.toml


### 4. Initialize Database

# Create tables
flask db upgrade

# Seed the initial admin account
flask seed-db

---

## 🛠️ Execution

### Launch Development Server
python run.py


### Launch in Production (Gunicorn)
gunicorn --workers 4 --bind 0.0.0.0:5000 "run:app"


---

## 🔄 Cache Management (CLI)
The system utilizes a local cache to prevent ORCID API rate-limiting. CLI commands are optimized for **Member API Mode**. A full synchronization searches every verified institutional identifier and downloads each ORCID profile once:

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

The analytics layer is refreshed automatically after Works or OpenAlex
synchronization. When upgrading an existing database, run `flask db upgrade`
and then `flask rebuild-openalex-analytics` so optimized filters are available
immediately.

Institutional OpenAlex CSV exports stream rows as they are read, and XLSX
exports use a write-only workbook without the former 100,000-row application
limit. Extended bibliographic, authorship, affiliation, citation, topic, open
access, SDG, funding, and APC fields are read from the queryable metadata table
so downloads do not have to deserialize the full raw JSON cache.

Raw DOI values remain complete in a `TEXT` column. Searches and joins use a
validated, normalized DOI key limited to 255 characters; invalid or oversized
values are neither truncated nor indexed as DOI keys, preventing failures and
identifier collisions.

Web-triggered long-running syncs are started in a process-local background
runner to avoid request timeouts. For production multi-worker deployments,
prefer CLI/cron or a persistent job queue.


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
