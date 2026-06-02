"""SQLAlchemy models for users, ORCID caches, sync runs, and audit logs."""

from datetime import datetime
import bcrypt
from . import db

class User(db.Model):
    """Application user with role and institution scope metadata."""
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(255), nullable=True)
    first_name = db.Column(db.String(120), nullable=True)
    last_name = db.Column(db.String(120), nullable=True)
    position = db.Column(db.String(180), nullable=True)
    password_hash = db.Column(db.String(200), nullable=False)

    is_admin = db.Column(db.Boolean, default=False)   # Superuser
    is_manager = db.Column(db.Boolean, default=False) # Institutional Manager

    institution_name = db.Column(db.String(120), nullable=True)
    ror_id = db.Column(db.String(20), nullable=True, index=True) # ROR Identifier (e.g., 02ap3w078)
    grid_id = db.Column(db.String(32), nullable=True)            # Legacy GRID Identifier

    # Used to identify records written by the institution's Affiliation Manager.
    am_client_id = db.Column(db.String(40), nullable=True)

    locale = db.Column(db.String(5), default='es', nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def set_password(self, password: str) -> None:
        """Hash and store a password using bcrypt."""
        self.password_hash = bcrypt.hashpw(
            password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

    def check_password(self, password: str) -> bool:
        """Return whether the provided password matches the stored hash."""
        return bcrypt.checkpw(
            password.encode("utf-8"), self.password_hash.encode("utf-8")
        )

    @property
    def full_name(self) -> str:
        """Display name for navigation and admin tables."""
        return f"{self.first_name or ''} {self.last_name or ''}".strip() or self.username


class WorkCache(db.Model):
    """Cached ORCID work summary scoped by institution ROR."""
    __tablename__ = "work_cache"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    orcid = db.Column(db.String(32), index=True, nullable=False)

    title = db.Column(db.Text)
    type = db.Column(db.String(64))      # e.g., journal-article, conference-paper
    put_code = db.Column(db.Integer)     # ORCID internal unique ID for the item
    journal_title = db.Column(db.Text)
    
    pub_year = db.Column(db.String(8))
    pub_month = db.Column(db.String(4))
    pub_day = db.Column(db.String(4))
    
    doi = db.Column(db.String(255), index=True)
    issn = db.Column(db.String(64))
    other_external_ids = db.Column(db.Text) # Serialized list of other IDs
    
    source = db.Column(db.Text)         # Who added this record to ORCID?
    url = db.Column(db.Text)
    visibility = db.Column(db.String(32)) # public, limited, registered-only
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class WorkCacheRun(db.Model):
    """Audit log for work-cache rebuild jobs."""
    __tablename__ = "work_cache_run"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    status = db.Column(db.String(16), default="pending", nullable=False) # running, success, failed
    rows_count = db.Column(db.Integer, default=0)
    error = db.Column(db.Text)
    started_at = db.Column(db.DateTime)
    finished_at = db.Column(db.DateTime)


class FundingCache(db.Model):
    """Cached ORCID funding summary scoped by institution ROR."""
    __tablename__ = "funding_cache"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    orcid = db.Column(db.String(32), index=True, nullable=False)

    title = db.Column(db.Text)
    type = db.Column(db.String(64)) # grant, contract, award
    org_name = db.Column(db.Text)   # Funding Agency
    city = db.Column(db.Text)
    country = db.Column(db.Text)
    
    start_y = db.Column(db.String(8))
    start_m = db.Column(db.String(4))
    start_d = db.Column(db.String(4))
    end_y = db.Column(db.String(8))
    end_m = db.Column(db.String(4))
    end_d = db.Column(db.String(4))
    
    grant_number = db.Column(db.String(255))
    currency = db.Column(db.String(8))
    amount = db.Column(db.String(64))
    
    source = db.Column(db.Text)
    visibility = db.Column(db.String(32))
    url = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class FundingCacheRun(db.Model):
    """Audit log for funding-cache rebuild jobs."""
    __tablename__ = "funding_cache_run"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    status = db.Column(db.String(16), default="pending", nullable=False)
    rows_count = db.Column(db.Integer, default=0)
    error = db.Column(db.Text)
    started_at = db.Column(db.DateTime)
    finished_at = db.Column(db.DateTime)


class ResearcherStatus(db.Model):
    """Whether an ORCID profile has records managed by an institution."""
    __tablename__ = 'researcher_status'

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(20), index=True, nullable=False)
    orcid = db.Column(db.String(20), index=True, nullable=False)
    
    is_managed_by_am = db.Column(db.Boolean, default=False)
    
    last_updated = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())

    __table_args__ = (
        db.UniqueConstraint('ror_id', 'orcid', name='_ror_orcid_uc'),
    )


class ResearcherCache(db.Model):
    """Cached researcher display metadata used by lists and charts."""
    __tablename__ = "researcher_cache"

    orcid = db.Column(db.String(32), primary_key=True, index=True)
    given_names = db.Column(db.String(150))
    family_name = db.Column(db.String(150))
    credit_name = db.Column(db.String(255))
    email = db.Column(db.String(255), nullable=True)
    
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class OrcidCache(db.Model):
    """Aggregated yearly JSON datasets served by API endpoints."""
    __tablename__ = 'orcid_cache'

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False, index=True)
    data = db.Column(db.JSON, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class DuplicateProfileCache(db.Model):
    """Cached duplicate-profile analysis derived from local metadata caches."""
    __tablename__ = "duplicate_profile_cache"

    id = db.Column(db.Integer, primary_key=True)
    scope_key = db.Column(db.String(255), unique=True, index=True, nullable=False)
    dependency_hash = db.Column(db.String(64), nullable=False)
    report_json = db.Column(db.JSON, nullable=False)
    source_summary = db.Column(db.JSON, nullable=True)
    generated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class OpenAlexWorkRawCache(db.Model):
    """Raw OpenAlex work payloads keyed by normalized DOI."""
    __tablename__ = "openalex_work_raw_cache"

    id = db.Column(db.Integer, primary_key=True)
    doi_normalized = db.Column(db.String(255), unique=True, index=True, nullable=False)
    source_doi = db.Column(db.String(255), nullable=True)
    openalex_id = db.Column(db.String(64), index=True, nullable=True)
    status = db.Column(db.String(16), default="pending", nullable=False)
    http_status = db.Column(db.Integer, nullable=True)
    raw_json = db.Column(db.JSON, nullable=True)
    oa_updated_date = db.Column(db.DateTime, nullable=True)
    error = db.Column(db.Text, nullable=True)
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class OpenAlexWorkMetadata(db.Model):
    """Queryable OpenAlex work metadata derived from raw payloads."""
    __tablename__ = "openalex_work_metadata"

    id = db.Column(db.Integer, primary_key=True)
    doi_normalized = db.Column(db.String(255), unique=True, index=True, nullable=False)
    openalex_id = db.Column(db.String(64), index=True, nullable=True)
    title = db.Column(db.Text, nullable=True)
    publication_year = db.Column(db.Integer, index=True, nullable=True)
    publication_date = db.Column(db.String(10), nullable=True)
    type = db.Column(db.String(64), index=True, nullable=True)
    language = db.Column(db.String(8), index=True, nullable=True)
    cited_by_count = db.Column(db.Integer, default=0, nullable=False)
    fwci = db.Column(db.Float, nullable=True)
    is_retracted = db.Column(db.Boolean, default=False, nullable=False)
    is_oa = db.Column(db.Boolean, default=False, nullable=False)
    oa_status = db.Column(db.String(32), index=True, nullable=True)
    oa_url = db.Column(db.Text, nullable=True)
    best_pdf_url = db.Column(db.Text, nullable=True)
    source_name = db.Column(db.Text, nullable=True)
    source_issn_l = db.Column(db.String(32), index=True, nullable=True)
    source_type = db.Column(db.String(64), nullable=True)
    source_is_in_doaj = db.Column(db.Boolean, nullable=True)
    primary_topic_name = db.Column(db.String(255), nullable=True)
    primary_topic_field = db.Column(db.String(255), nullable=True)
    primary_topic_domain = db.Column(db.String(255), nullable=True)
    raw_updated_date = db.Column(db.DateTime, nullable=True)
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class OpenAlexSyncRun(db.Model):
    """Audit log for OpenAlex enrichment runs."""
    __tablename__ = "openalex_sync_run"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=True)
    status = db.Column(db.String(16), default="running", nullable=False)
    works_seen = db.Column(db.Integer, default=0, nullable=False)
    dois_found = db.Column(db.Integer, default=0, nullable=False)
    fetched_count = db.Column(db.Integer, default=0, nullable=False)
    matched_count = db.Column(db.Integer, default=0, nullable=False)
    not_found_count = db.Column(db.Integer, default=0, nullable=False)
    error_count = db.Column(db.Integer, default=0, nullable=False)
    skipped_count = db.Column(db.Integer, default=0, nullable=False)
    error = db.Column(db.Text, nullable=True)
    started_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    finished_at = db.Column(db.DateTime, nullable=True)


class InstitutionRegistry(db.Model):
    """Institution records available for cache building even without users."""
    __tablename__ = "institution_registry"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), unique=True, index=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    display_name_en = db.Column(db.String(255), nullable=True)
    grid_id = db.Column(db.String(32), nullable=True)
    country_code = db.Column(db.String(2), default="CL", nullable=False)
    institution_type = db.Column(db.String(64), default="university", nullable=False)
    source = db.Column(db.String(64), default="ror", nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class TrackingLog(db.Model):
    """HTTP request audit log used by the admin statistics view."""
    __tablename__ = "tracking_logs"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=True, index=True)
    username = db.Column(db.String(80), nullable=True)

    method = db.Column(db.String(10))
    path = db.Column(db.String(300))
    status_code = db.Column(db.Integer)
    ip = db.Column(db.String(50))
    user_agent = db.Column(db.String(255))
    duration_ms = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
