"""SQLAlchemy models for users, ORCID caches, sync runs, and audit logs."""

from datetime import datetime, timezone
import bcrypt
import secrets
from . import db
from sqlalchemy.orm import validates


BCRYPT_MAX_PASSWORD_BYTES = 72


def password_fits_bcrypt(password: str | None) -> bool:
    """Return whether a value can be processed by bcrypt without truncation."""
    return isinstance(password, str) and len(password.encode("utf-8")) <= BCRYPT_MAX_PASSWORD_BYTES


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
    is_oai_user = db.Column(db.Boolean, default=False, nullable=False)

    institution_name = db.Column(db.String(120), nullable=True)
    ror_id = db.Column(db.String(20), nullable=True, index=True) # ROR Identifier (e.g., 02ap3w078)
    grid_id = db.Column(db.String(32), nullable=True)            # Legacy GRID Identifier

    # Used to identify records written by the institution's Affiliation Manager.
    am_client_id = db.Column(db.String(40), nullable=True)

    locale = db.Column(db.String(5), default='en', nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utc_now)

    def set_password(self, password: str) -> None:
        """Hash and store a password using bcrypt."""
        if not password_fits_bcrypt(password):
            raise ValueError("Password must contain at most 72 UTF-8 bytes.")
        self.password_hash = bcrypt.hashpw(
            password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

    def check_password(self, password: str) -> bool:
        """Return whether the provided password matches the stored hash."""
        if not password_fits_bcrypt(password) or not self.password_hash:
            return False
        try:
            return bcrypt.checkpw(
                password.encode("utf-8"), self.password_hash.encode("utf-8")
            )
        except (TypeError, ValueError):
            # A malformed legacy hash must be treated as an authentication
            # failure, not as a server error exposed by the login route.
            return False

    @property
    def full_name(self) -> str:
        """Display name for navigation and admin tables."""
        return f"{self.first_name or ''} {self.last_name or ''}".strip() or self.username


class WorkCache(db.Model):
    """Cached ORCID work summary scoped by institution ROR."""
    __tablename__ = "work_cache"
    __table_args__ = (
        db.Index("ix_work_cache_ror_type", "ror_id", "type"),
        db.Index("ix_work_cache_ror_type_doi_normalized", "ror_id", "type", "doi_normalized"),
        db.Index("ix_work_cache_ror_year_type", "ror_id", "pub_year", "type"),
        db.Index("ix_work_cache_ror_orcid_year", "ror_id", "orcid", "pub_year"),
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
    doi_normalized = db.Column(db.String(255), nullable=True)
    issn = db.Column(db.Text)
    other_external_ids = db.Column(db.Text) # Serialized list of other IDs
    
    source = db.Column(db.Text)         # Who added this record to ORCID?
    url = db.Column(db.Text)
    visibility = db.Column(db.String(32)) # public, limited, registered-only
    
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)

    @validates("doi")
    def _normalize_doi(self, _key, value):
        from .services.doi_service import normalize_doi

        self.doi_normalized = normalize_doi(value)
        return value


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
    __table_args__ = (
        db.Index("ix_funding_cache_ror_year_type", "ror_id", "start_y", "type"),
        db.Index("ix_funding_cache_ror_orcid_year", "ror_id", "orcid", "start_y"),
    )

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
    # Migration 8e2f4a6c9d10 creates this constraint as well as the unique index.
    __table_args__ = (db.UniqueConstraint("group_key"),)

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
    pmid = db.Column(db.String(64), nullable=True)
    pmcid = db.Column(db.String(64), nullable=True)
    volume = db.Column(db.String(64), nullable=True)
    issue = db.Column(db.String(64), nullable=True)
    first_page = db.Column(db.String(64), nullable=True)
    last_page = db.Column(db.String(64), nullable=True)
    source_id = db.Column(db.String(64), nullable=True)
    source_issns = db.Column(db.Text, nullable=True)
    source_host_organization_name = db.Column(db.Text, nullable=True)
    primary_landing_page_url = db.Column(db.Text, nullable=True)
    primary_pdf_url = db.Column(db.Text, nullable=True)
    primary_license = db.Column(db.String(64), nullable=True)
    primary_version = db.Column(db.String(32), nullable=True)
    referenced_works_count = db.Column(db.Integer, nullable=True)
    citation_normalized_percentile = db.Column(db.Float, nullable=True)
    is_in_top_1_percent = db.Column(db.Boolean, nullable=True)
    is_in_top_10_percent = db.Column(db.Boolean, nullable=True)
    cited_by_percentile_min = db.Column(db.Integer, nullable=True)
    cited_by_percentile_max = db.Column(db.Integer, nullable=True)
    author_count = db.Column(db.Integer, nullable=True)
    institution_count = db.Column(db.Integer, nullable=True)
    country_count = db.Column(db.Integer, nullable=True)
    location_count = db.Column(db.Integer, nullable=True)
    has_abstract = db.Column(db.Boolean, nullable=True)
    has_fulltext = db.Column(db.Boolean, nullable=True)
    indexed_in = db.Column(db.Text, nullable=True)
    topics = db.Column(db.Text, nullable=True)
    keywords = db.Column(db.Text, nullable=True)
    sustainable_development_goals = db.Column(db.Text, nullable=True)
    funders = db.Column(db.Text, nullable=True)
    awards = db.Column(db.Text, nullable=True)
    apc_list_value = db.Column(db.Float, nullable=True)
    apc_list_currency = db.Column(db.String(8), nullable=True)
    apc_list_value_usd = db.Column(db.Float, nullable=True)
    apc_paid_value = db.Column(db.Float, nullable=True)
    apc_paid_currency = db.Column(db.String(8), nullable=True)
    apc_paid_value_usd = db.Column(db.Float, nullable=True)
    author_ids = db.Column(db.Text, nullable=True)
    author_names = db.Column(db.Text, nullable=True)
    author_orcids = db.Column(db.Text, nullable=True)
    corresponding_author_names = db.Column(db.Text, nullable=True)
    institution_names = db.Column(db.Text, nullable=True)
    institution_rors = db.Column(db.Text, nullable=True)
    countries = db.Column(db.Text, nullable=True)
    raw_affiliation_strings = db.Column(db.Text, nullable=True)
    raw_created_date = db.Column(db.String(32), nullable=True)
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


class AnalyticsDataVersion(db.Model):
    """Version marker used to invalidate derived analytics caches."""
    __tablename__ = "analytics_data_version"

    scope_key = db.Column(db.String(96), primary_key=True)
    version = db.Column(db.Integer, default=1, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class OpenAlexInstitutionWorkFact(db.Model):
    """One filterable OpenAlex work per local institutional scope."""
    __tablename__ = "openalex_institution_work_fact"
    __table_args__ = (
        db.UniqueConstraint(
            "ror_id",
            "openalex_cache_key",
            name="uq_openalex_institution_work_fact_scope_key",
        ),
        db.Index(
            "ix_openalex_fact_ror_year",
            "ror_id",
            "publication_year",
        ),
        db.Index(
            "ix_openalex_fact_ror_type",
            "ror_id",
            "document_type",
        ),
        db.Index(
            "ix_openalex_fact_ror_oa",
            "ror_id",
            "oa_status",
        ),
        db.Index(
            "ix_openalex_fact_ror_language",
            "ror_id",
            "language",
        ),
        db.Index(
            "ix_openalex_fact_ror_citations",
            "ror_id",
            "cited_by_count",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), nullable=False, index=True)
    openalex_cache_key = db.Column(db.String(255), nullable=False, index=True)
    representative_work_cache_id = db.Column(db.Integer, nullable=False)
    source_record_count = db.Column(db.Integer, default=1, nullable=False)
    has_valid_doi = db.Column(db.Boolean, default=False, nullable=False)
    has_local_title = db.Column(db.Boolean, default=False, nullable=False)
    raw_status = db.Column(db.String(16), nullable=True)
    raw_error = db.Column(db.Text, nullable=True)
    openalex_id = db.Column(db.String(64), nullable=True)
    title = db.Column(db.Text, nullable=True)
    publication_year = db.Column(db.Integer, nullable=True)
    document_type = db.Column(db.String(64), nullable=True)
    language = db.Column(db.String(8), nullable=True)
    cited_by_count = db.Column(db.Integer, default=0, nullable=False)
    fwci = db.Column(db.Float, nullable=True)
    is_oa = db.Column(db.Boolean, default=False, nullable=False)
    oa_status = db.Column(db.String(32), nullable=True)
    source_name = db.Column(db.Text, nullable=True)
    source_issn_l = db.Column(db.String(32), nullable=True)
    primary_topic_field = db.Column(db.String(255), nullable=True)
    primary_topic_domain = db.Column(db.String(255), nullable=True)
    has_selected_affiliation = db.Column(db.Boolean, default=False, nullable=False)
    has_chile_affiliation = db.Column(db.Boolean, default=False, nullable=False)
    has_non_chile_affiliation = db.Column(db.Boolean, default=False, nullable=False)
    has_international_collaboration = db.Column(db.Boolean, default=False, nullable=False)
    author_count = db.Column(db.Integer, default=0, nullable=False)
    institution_count = db.Column(db.Integer, default=0, nullable=False)
    refreshed_at = db.Column(db.DateTime, default=utc_now, nullable=False)


class CanonicalWork(db.Model):
    """Source-independent scholarly output keyed by DOI or title/year fallback."""
    __tablename__ = "canonical_work"
    # Keep metadata aligned with the existing constraint and unique index.
    __table_args__ = (db.UniqueConstraint("canonical_key"),)

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
    username = db.Column(db.String(80), nullable=True, index=True)
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


class SystemError(db.Model):
    """Sanitized application error available to the administrator console."""
    __tablename__ = "system_error"

    id = db.Column(db.Integer, primary_key=True)
    event_id = db.Column(
        db.String(36),
        default=lambda: secrets.token_hex(16),
        nullable=False,
        unique=True,
        index=True,
    )
    fingerprint = db.Column(db.String(64), nullable=False, index=True)
    source = db.Column(db.String(32), nullable=False, index=True)
    severity = db.Column(db.String(16), nullable=False)
    logger_name = db.Column(db.String(160), nullable=True)
    exception_type = db.Column(db.String(255), nullable=True, index=True)
    message = db.Column(db.Text, nullable=False)
    traceback = db.Column(db.Text, nullable=True)

    user_id = db.Column(db.Integer, nullable=True, index=True)
    username = db.Column(db.String(80), nullable=True)
    institution_ror = db.Column(db.String(32), nullable=True, index=True)
    role = db.Column(db.String(24), nullable=True)

    endpoint = db.Column(db.String(160), nullable=True, index=True)
    method = db.Column(db.String(10), nullable=True)
    path = db.Column(db.String(500), nullable=True)
    status_code = db.Column(db.Integer, nullable=True)
    request_id = db.Column(db.String(36), nullable=True, index=True)
    job_id = db.Column(db.String(36), nullable=True, index=True)
    ip = db.Column(db.String(50), nullable=True)
    user_agent = db.Column(db.String(255), nullable=True)
    process_id = db.Column(db.Integer, nullable=True)
    thread_name = db.Column(db.String(80), nullable=True)
    context_json = db.Column(db.JSON, nullable=True)

    occurred_at = db.Column(db.DateTime, default=utc_now, nullable=False, index=True)
    is_resolved = db.Column(db.Boolean, default=False, nullable=False, index=True)
    resolved_at = db.Column(db.DateTime, nullable=True)
    resolved_by_user_id = db.Column(db.Integer, nullable=True)
    resolved_by_username = db.Column(db.String(80), nullable=True)
    resolution_note = db.Column(db.Text, nullable=True)


class AuthRateLimitEvent(db.Model):
    """Shared authentication-attempt window used by every web worker."""
    __tablename__ = "auth_rate_limit_event"
    __table_args__ = (
        db.Index(
            "ix_auth_rate_limit_action_client_time",
            "action",
            "client_key",
            "occurred_at",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    action = db.Column(db.String(32), nullable=False)
    client_key = db.Column(db.String(64), nullable=False)
    occurred_at = db.Column(db.DateTime, default=utc_now, nullable=False)


class SystemModule(db.Model):
    """Global availability and presentation settings for an optional module."""
    __tablename__ = "system_module"

    key = db.Column(db.String(64), primary_key=True)
    is_enabled = db.Column(db.Boolean, default=True, nullable=False)
    default_locale = db.Column(db.String(8), nullable=True)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    updated_by_user_id = db.Column(db.Integer, nullable=True)
    updated_by_username = db.Column(db.String(80), nullable=True)


class ContactInquiry(db.Model):
    """Private, durable message submitted through the public landing page."""
    __tablename__ = "contact_inquiry"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(254), nullable=False, index=True)
    institution = db.Column(db.String(160), nullable=True)
    topic = db.Column(db.String(32), nullable=False, index=True)
    message = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False, index=True)

    notification_status = db.Column(
        db.String(32),
        default="pending",
        nullable=False,
        index=True,
    )
    is_resolved = db.Column(db.Boolean, default=False, nullable=False, index=True)
    resolved_at = db.Column(db.DateTime, nullable=True)
    resolved_by_user_id = db.Column(db.Integer, nullable=True)
    resolved_by_username = db.Column(db.String(80), nullable=True)


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
    items_current = db.Column(db.Integer, default=0, nullable=False)
    items_total = db.Column(db.Integer, default=0, nullable=False)
    progress_unit = db.Column(db.String(32), nullable=True)
    message = db.Column(db.Text, nullable=True)
    result_json = db.Column(db.JSON, nullable=True)
    error = db.Column(db.Text, nullable=True)
    handler = db.Column(db.String(255), nullable=True)
    payload_json = db.Column(db.JSON, nullable=True)
    attempt_count = db.Column(db.Integer, default=0, nullable=False)
    max_attempts = db.Column(db.Integer, default=3, nullable=False)
    claimed_by = db.Column(db.String(80), nullable=True, index=True)
    claimed_at = db.Column(db.DateTime, nullable=True)
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


class OaiPmhInstitutionConfig(db.Model):
    """Institution-scoped OAI-PMH provider and publication policy."""
    __tablename__ = "oai_pmh_institution_config"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), unique=True, index=True, nullable=False)
    public_key = db.Column(
        db.String(64),
        unique=True,
        index=True,
        nullable=False,
        default=lambda: secrets.token_hex(24),
    )
    provider_enabled = db.Column(db.Boolean, default=False, nullable=False)
    repository_name = db.Column(db.String(255), nullable=False)
    admin_email = db.Column(db.String(255), nullable=True)
    publication_policy = db.Column(db.String(16), default="validated", nullable=False)
    metadata_mapping = db.Column(db.JSON, nullable=True)
    harvester_access_restricted = db.Column(db.Boolean, default=False, nullable=False)
    policy_updated_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    created_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    updated_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class OaiPmhHarvester(db.Model):
    """A repository reference and revocable credential owned by one provider."""
    __tablename__ = "oai_pmh_harvester"
    __table_args__ = (
        db.UniqueConstraint("config_id", "base_uri", name="uq_oai_pmh_harvester_config_uri"),
    )

    id = db.Column(db.Integer, primary_key=True)
    config_id = db.Column(
        db.Integer, db.ForeignKey("oai_pmh_institution_config.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    base_uri = db.Column(db.String(2048), nullable=False)
    access_key = db.Column(db.String(64), unique=True, nullable=False, default=lambda: secrets.token_hex(24))
    is_enabled = db.Column(db.Boolean, default=True, nullable=False)
    created_by_user_id = db.Column(db.Integer, nullable=True)
    updated_by_user_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class OaiPmhDoiImportBatch(db.Model):
    """One reversible institutional XLSX import of DOI publication choices."""
    __tablename__ = "oai_pmh_doi_import_batch"
    __table_args__ = (
        db.Index(
            "ix_oai_pmh_doi_import_batch_ror_undone_created",
            "ror_id",
            "undone_at",
            "created_at",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), nullable=False, index=True)
    filename = db.Column(db.String(255), nullable=False)
    submitted_count = db.Column(db.Integer, default=0, nullable=False)
    matched_count = db.Column(db.Integer, default=0, nullable=False)
    article_count = db.Column(db.Integer, default=0, nullable=False)
    invalid_count = db.Column(db.Integer, default=0, nullable=False)
    duplicate_count = db.Column(db.Integer, default=0, nullable=False)
    unmatched_count = db.Column(db.Integer, default=0, nullable=False)
    imported_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    undone_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    undone_at = db.Column(db.DateTime, nullable=True)


class OaiPmhWorkSelection(db.Model):
    """Explicit publication override for one canonical institutional work."""
    __tablename__ = "oai_pmh_work_selection"
    __table_args__ = (
        db.UniqueConstraint(
            "ror_id",
            "canonical_work_id",
            name="uq_oai_pmh_work_selection_ror_work",
        ),
        db.Index(
            "ix_oai_pmh_work_selection_ror_included_updated",
            "ror_id",
            "is_included",
            "updated_at",
        ),
        db.Index(
            "ix_oai_pmh_work_selection_ror_decision_source",
            "ror_id",
            "decision_source",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    canonical_work_id = db.Column(
        db.Integer,
        db.ForeignKey("canonical_work.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ror_id = db.Column(db.String(32), nullable=False, index=True)
    is_included = db.Column(db.Boolean, nullable=False)
    decision_source = db.Column(
        db.String(16),
        default="manual",
        nullable=False,
    )
    doi_import_batch_id = db.Column(
        db.Integer,
        db.ForeignKey("oai_pmh_doi_import_batch.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    updated_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=utc_now, nullable=False)
    updated_at = db.Column(db.DateTime, default=utc_now, onupdate=utc_now, nullable=False)


class OaiPmhDoiImportChange(db.Model):
    """Previous state needed to undo one DOI import without crossing tenants."""
    __tablename__ = "oai_pmh_doi_import_change"
    __table_args__ = (
        db.UniqueConstraint(
            "batch_id",
            "canonical_work_id",
            name="uq_oai_pmh_doi_import_change_batch_work",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(
        db.Integer,
        db.ForeignKey("oai_pmh_doi_import_batch.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    canonical_work_id = db.Column(
        db.Integer,
        db.ForeignKey("canonical_work.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    previous_selection_existed = db.Column(db.Boolean, nullable=False)
    previous_is_included = db.Column(db.Boolean, nullable=True)
    previous_decision_source = db.Column(db.String(16), nullable=True)
    previous_import_batch_id = db.Column(
        db.Integer,
        db.ForeignKey("oai_pmh_doi_import_batch.id", ondelete="SET NULL"),
        nullable=True,
    )
