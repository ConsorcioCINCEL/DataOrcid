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

Web-triggered long-running syncs are started in a process-local background
runner to avoid request timeouts. For production multi-worker deployments,
prefer CLI/cron or a persistent job queue.


---

## 📝 License
This project is licensed under the **MIT** License.

**Developed by:** Gastón Olivares
**Institution:** Chilean Consortium, Cincel.
