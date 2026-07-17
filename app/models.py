"""SQLAlchemy models for users, ORCID caches, sync runs, and audit logs."""

from datetime import datetime, timezone
import bcrypt
from . import db


def utc_now() -> datetime:
    """Return a naive UTC timestamp for database columns without time zones."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

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

    locale = db.Column(db.String(5), default='en', nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utc_now)

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
    __table_args__ = (
        db.Index("ix_work_cache_ror_type", "ror_id", "type"),
    )

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
    
    doi = db.Column(db.Text)
    issn = db.Column(db.Text)
    other_external_ids = db.Column(db.Text) # Serialized list of other IDs
    
    source = db.Column(db.Text)         # Who added this record to ORCID?
    url = db.Column(db.Text)
    visibility = db.Column(db.String(32)) # public, limited, registered-only
    
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


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
    
    grant_number = db.Column(db.Text)
    currency = db.Column(db.String(8))
    amount = db.Column(db.String(64))
    
    source = db.Column(db.Text)
    visibility = db.Column(db.String(32))
    url = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


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
    
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now)


class OrcidCache(db.Model):
    """Aggregated yearly JSON datasets served by API endpoints."""
    __tablename__ = 'orcid_cache'

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False, index=True)
    data = db.Column(db.JSON, nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


class DuplicateProfileCache(db.Model):
    """Cached duplicate-profile analysis derived from local metadata caches."""
    __tablename__ = "duplicate_profile_cache"

    id = db.Column(db.Integer, primary_key=True)
    scope_key = db.Column(db.String(255), unique=True, index=True, nullable=False)
    dependency_hash = db.Column(db.String(64), nullable=False)
    report_json = db.Column(db.JSON, nullable=False)
    source_summary = db.Column(db.JSON, nullable=True)
    generated_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class DuplicateProfileReview(db.Model):
    """Persistent human decision attached to a stable duplicate candidate key."""
    __tablename__ = "duplicate_profile_review"

    id = db.Column(db.Integer, primary_key=True)
    group_key = db.Column(db.String(64), unique=True, index=True, nullable=False)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    normalized_name = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(24), default="pending", index=True, nullable=False)
    selected_orcid = db.Column(db.String(32), nullable=True)
    assigned_user_id = db.Column(db.Integer, nullable=True, index=True)
    reviewed_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    notes = db.Column(db.Text, nullable=True)
    candidate_snapshot = db.Column(db.JSON, nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


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
    attempt_count = db.Column(db.Integer, default=0, nullable=False)
    next_retry_at = db.Column(db.DateTime, index=True, nullable=True)
    fetched_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


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
    fetched_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


class OpenAlexWorkAuthor(db.Model):
    """Author-level metadata extracted from OpenAlex authorships."""
    __tablename__ = "openalex_work_author"
    __table_args__ = (
        db.UniqueConstraint(
            "doi_normalized",
            "author_id",
            name="uq_openalex_work_author_doi_author",
        ),
        db.Index("ix_openalex_work_author_chile_doi", "has_chile_affiliation", "doi_normalized"),
    )

    id = db.Column(db.Integer, primary_key=True)
    doi_normalized = db.Column(db.String(255), index=True, nullable=False)
    openalex_id = db.Column(db.String(64), index=True, nullable=True)
    author_id = db.Column(db.String(64), index=True, nullable=True)
    author_name = db.Column(db.Text, nullable=True)
    orcid = db.Column(db.String(32), index=True, nullable=True)
    raw_author_name = db.Column(db.Text, nullable=True)
    author_position = db.Column(db.String(32), nullable=True)
    is_corresponding = db.Column(db.Boolean, default=False, nullable=False)
    has_chile_affiliation = db.Column(db.Boolean, default=False, nullable=False)
    countries = db.Column(db.JSON, nullable=True)
    institution_rors = db.Column(db.JSON, nullable=True)
    institution_names = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


class OpenAlexWorkInstitution(db.Model):
    """Institution-level metadata extracted from OpenAlex authorships."""
    __tablename__ = "openalex_work_institution"
    __table_args__ = (
        db.UniqueConstraint(
            "doi_normalized",
            "institution_id",
            name="uq_openalex_work_institution_doi_institution",
        ),
        db.Index("ix_openalex_work_institution_doi_country", "doi_normalized", "country_code"),
        db.Index("ix_openalex_work_institution_country_doi", "country_code", "doi_normalized"),
        db.Index("ix_openalex_work_institution_doi_ror", "doi_normalized", "ror_id"),
        db.Index("ix_openalex_work_institution_ror_doi", "ror_id", "doi_normalized"),
    )

    id = db.Column(db.Integer, primary_key=True)
    doi_normalized = db.Column(db.String(255), index=True, nullable=False)
    openalex_id = db.Column(db.String(64), index=True, nullable=True)
    institution_id = db.Column(db.String(64), index=True, nullable=True)
    institution_name = db.Column(db.Text, nullable=True)
    ror_id = db.Column(db.String(32), index=True, nullable=True)
    country_code = db.Column(db.String(2), index=True, nullable=True)
    institution_type = db.Column(db.String(64), nullable=True)
    author_count = db.Column(db.Integer, default=0, nullable=False)
    has_corresponding_author = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


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
    started_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    finished_at = db.Column(db.DateTime, nullable=True)


class CanonicalWork(db.Model):
    """Source-independent scholarly output keyed by DOI or title/year fallback."""
    __tablename__ = "canonical_work"

    id = db.Column(db.Integer, primary_key=True)
    canonical_key = db.Column(db.String(80), unique=True, index=True, nullable=False)
    doi_normalized = db.Column(db.String(255), index=True, nullable=True)
    title = db.Column(db.Text, nullable=True)
    title_normalized = db.Column(db.Text, nullable=True)
    publication_year = db.Column(db.Integer, index=True, nullable=True)
    record_count = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class WorkRecordLink(db.Model):
    """Link one cached ORCID work record to its canonical scholarly output."""
    __tablename__ = "work_record_link"
    __table_args__ = (
        db.UniqueConstraint(
            "ror_id",
            "orcid",
            "source_record_key",
            name="uq_work_record_link_source",
        ),
        db.Index("ix_work_record_link_ror_canonical", "ror_id", "canonical_work_id"),
    )

    id = db.Column(db.Integer, primary_key=True)
    canonical_work_id = db.Column(
        db.Integer,
        db.ForeignKey("canonical_work.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    work_cache_id = db.Column(db.Integer, nullable=True, index=True)
    ror_id = db.Column(db.String(32), nullable=False, index=True)
    orcid = db.Column(db.String(32), nullable=False, index=True)
    source_record_key = db.Column(db.String(96), nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)


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
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class InstitutionIdentifier(db.Model):
    """Verified external identifiers assigned to an institution."""
    __tablename__ = "institution_identifier"

    id = db.Column(db.Integer, primary_key=True)
    institution_id = db.Column(
        db.Integer,
        db.ForeignKey("institution_registry.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scheme = db.Column(db.String(16), nullable=False, index=True)
    value = db.Column(db.String(255), nullable=False)
    source = db.Column(db.String(64), default="manual", nullable=False)
    is_verified = db.Column(db.Boolean, default=False, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    verified_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("scheme", "value", name="uq_institution_identifier_scheme_value"),
    )


class InstitutionResearcher(db.Model):
    """Search-backed association between an institution and an ORCID record."""
    __tablename__ = "institution_researcher"

    id = db.Column(db.Integer, primary_key=True)
    institution_id = db.Column(
        db.Integer,
        db.ForeignKey("institution_registry.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    orcid = db.Column(db.String(32), nullable=False, index=True)
    matched_by_ror = db.Column(db.Boolean, default=False, nullable=False)
    matched_by_grid = db.Column(db.Boolean, default=False, nullable=False)
    matched_by_ringgold = db.Column(db.Boolean, default=False, nullable=False)
    evidence_type = db.Column(db.String(24), default="verified_search", nullable=False, index=True)
    evidence_sources = db.Column(db.JSON, nullable=True)
    is_verified = db.Column(db.Boolean, default=True, nullable=False, index=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False, index=True)
    profile_status = db.Column(db.String(16), default="pending", nullable=False)
    profile_error = db.Column(db.Text, nullable=True)
    first_seen_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    last_seen_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    profile_updated_at = db.Column(db.DateTime, nullable=True)

    __table_args__ = (
        db.UniqueConstraint("institution_id", "orcid", name="uq_institution_researcher"),
    )


class ResearcherAffiliationEvidence(db.Model):
    """Normalized public ORCID affiliation evidence used by researcher views."""
    __tablename__ = "researcher_affiliation_evidence"
    __table_args__ = (
        db.Index(
            "ix_affiliation_evidence_institution_orcid",
            "institution_id",
            "orcid",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    institution_id = db.Column(
        db.Integer,
        db.ForeignKey("institution_registry.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    orcid = db.Column(db.String(32), nullable=False, index=True)
    source_section = db.Column(db.String(40), nullable=False)
    organization_name = db.Column(db.Text, nullable=True)
    role_title = db.Column(db.Text, nullable=True)
    department_name = db.Column(db.Text, nullable=True)
    start_year = db.Column(db.Integer, nullable=True)
    end_year = db.Column(db.Integer, nullable=True)
    source_client_id = db.Column(db.String(64), nullable=True)
    organization_identifiers = db.Column(db.JSON, nullable=True)
    evidence_type = db.Column(db.String(24), default="public_orcid", nullable=False)
    is_current = db.Column(db.Boolean, default=False, nullable=False, index=True)
    observed_at = db.Column(db.DateTime, default=utc_now, nullable=False)


class TrackingLog(db.Model):
    """HTTP request audit log used by the admin statistics view."""
    __tablename__ = "tracking_logs"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=True, index=True)
    username = db.Column(db.String(80), nullable=True)
    institution_ror = db.Column(db.String(32), nullable=True, index=True)
    role = db.Column(db.String(24), nullable=True, index=True)
    action = db.Column(db.String(80), nullable=True, index=True)
    job_id = db.Column(db.String(36), nullable=True, index=True)

    method = db.Column(db.String(10))
    path = db.Column(db.String(300))
    status_code = db.Column(db.Integer)
    ip = db.Column(db.String(50))
    user_agent = db.Column(db.String(255))
    duration_ms = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, default=utc_now, nullable=False)


class SyncJob(db.Model):
    """Durable status for a user-triggered background synchronization job."""
    __tablename__ = "sync_job"

    id = db.Column(db.String(36), primary_key=True)
    name = db.Column(db.String(160), nullable=False)
    job_type = db.Column(db.String(40), nullable=False, index=True)
    ror_id = db.Column(db.String(32), nullable=True, index=True)
    requested_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    status = db.Column(db.String(24), default="queued", nullable=False, index=True)
    progress_current = db.Column(db.Integer, default=0, nullable=False)
    progress_total = db.Column(db.Integer, default=0, nullable=False)
    message = db.Column(db.Text, nullable=True)
    result_json = db.Column(db.JSON, nullable=True)
    error = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)
    heartbeat_at = db.Column(db.DateTime, nullable=True)


class SyncJobStep(db.Model):
    """Durable progress step within a synchronization job."""
    __tablename__ = "sync_job_step"
    __table_args__ = (
        db.UniqueConstraint("sync_job_id", "name", name="uq_sync_job_step_name"),
    )

    id = db.Column(db.Integer, primary_key=True)
    sync_job_id = db.Column(
        db.String(36),
        db.ForeignKey("sync_job.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = db.Column(db.String(80), nullable=False)
    position = db.Column(db.Integer, default=0, nullable=False)
    status = db.Column(db.String(24), default="pending", nullable=False, index=True)
    records_count = db.Column(db.Integer, default=0, nullable=False)
    error = db.Column(db.Text, nullable=True)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)
