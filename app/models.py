"""
Module: models.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Data Persistence Layer.
    
    This module defines the SQLAlchemy Object-Relational Mapping (ORM) schema 
    for the application. It manages:
    1. User Authentication & Role-Based Access Control (User table).
    2. Institutional Data Caching (WorkCache, FundingCache).
    3. Synchronization State Tracking (CacheRun tables).
    4. Researcher Management Status (Affiliation Manager tracking).
    5. System Audit Logs.
"""

from datetime import datetime
import bcrypt
from . import db

# ============================================================
# USER MANAGEMENT & AUTHENTICATION
# ============================================================

class User(db.Model):
    """
    Represents a system user.
    
    This model handles authentication and stores institutional context.
    A user is typically associated with a specific ROR ID, which limits 
    their dashboard view to that institution's data.
    """
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(255), nullable=True)
    first_name = db.Column(db.String(120), nullable=True)
    last_name = db.Column(db.String(120), nullable=True)
    position = db.Column(db.String(180), nullable=True)
    password_hash = db.Column(db.String(200), nullable=False)

    # Role-Based Access Control (RBAC)
    is_admin = db.Column(db.Boolean, default=False)   # Superuser
    is_manager = db.Column(db.Boolean, default=False) # Institutional Manager

    # Institutional Context
    institution_name = db.Column(db.String(120), nullable=True)
    ror_id = db.Column(db.String(20), nullable=True, index=True) # ROR Identifier (e.g., 02ap3w078)
    grid_id = db.Column(db.String(32), nullable=True)            # Legacy GRID Identifier
    
    # Affiliation Manager (AM) Client ID
    # Used to identify if records in ORCID were written by this institution's credentials
    am_client_id = db.Column(db.String(40), nullable=True)

    # Preferences & Metadata
    locale = db.Column(db.String(5), default='es', nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def set_password(self, password: str) -> None:
        """
        Hashes and sets the user password using bcrypt.
        
        Args:
            password (str): The plain-text password.
        """
        self.password_hash = bcrypt.hashpw(
            password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

    def check_password(self, password: str) -> bool:
        """
        Verifies a plain-text password against the stored bcrypt hash.
        
        Args:
            password (str): The plain-text password to verify.
            
        Returns:
            bool: True if password matches, False otherwise.
        """
        return bcrypt.checkpw(
            password.encode("utf-8"), self.password_hash.encode("utf-8")
        )

    @property
    def full_name(self) -> str:
        """Helper property to get the user's display name."""
        return f"{self.first_name or ''} {self.last_name or ''}".strip() or self.username


# ============================================================
# SCHOLARLY WORKS CACHE (PUBLICATIONS)
# ============================================================

class WorkCache(db.Model):
    """
    Local cache of 'Work' (Publication) records fetched from ORCID.
    
    This table allows the application to generate reports and analytics 
    without querying the ORCID API in real-time. It is scoped by Institution (ROR).
    """
    __tablename__ = "work_cache"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    orcid = db.Column(db.String(32), index=True, nullable=False)

    title = db.Column(db.Text)
    type = db.Column(db.String(64))      # e.g., journal-article, conference-paper
    put_code = db.Column(db.Integer)     # ORCID internal unique ID for the item
    journal_title = db.Column(db.Text)
    
    # Publication Date Components
    pub_year = db.Column(db.String(8))
    pub_month = db.Column(db.String(4))
    pub_day = db.Column(db.String(4))
    
    # Identifiers
    doi = db.Column(db.String(255), index=True)
    issn = db.Column(db.String(64))
    other_external_ids = db.Column(db.Text) # Serialized list of other IDs
    
    source = db.Column(db.Text)         # Who added this record to ORCID?
    url = db.Column(db.Text)
    visibility = db.Column(db.String(32)) # public, limited, registered-only
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class WorkCacheRun(db.Model):
    """
    Audit log for Work Cache synchronization jobs.
    Tracks success, failure, and row counts of bulk update operations.
    """
    __tablename__ = "work_cache_run"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    status = db.Column(db.String(16), default="pending", nullable=False) # running, success, failed
    rows_count = db.Column(db.Integer, default=0)
    error = db.Column(db.Text)
    started_at = db.Column(db.DateTime)
    finished_at = db.Column(db.DateTime)


# ============================================================
# GRANT & FUNDING CACHE
# ============================================================

class FundingCache(db.Model):
    """
    Local cache of 'Funding' (Grant) records fetched from ORCID.
    Used for tracking research income and project grants per institution.
    """
    __tablename__ = "funding_cache"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    orcid = db.Column(db.String(32), index=True, nullable=False)

    title = db.Column(db.Text)
    type = db.Column(db.String(64)) # grant, contract, award
    org_name = db.Column(db.Text)   # Funding Agency
    city = db.Column(db.Text)
    country = db.Column(db.Text)
    
    # Timeline
    start_y = db.Column(db.String(8))
    start_m = db.Column(db.String(4))
    start_d = db.Column(db.String(4))
    end_y = db.Column(db.String(8))
    end_m = db.Column(db.String(4))
    end_d = db.Column(db.String(4))
    
    # Financials & IDs
    grant_number = db.Column(db.String(255))
    currency = db.Column(db.String(8))
    amount = db.Column(db.String(64))
    
    source = db.Column(db.Text)
    visibility = db.Column(db.String(32))
    url = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class FundingCacheRun(db.Model):
    """
    Audit log for Funding Cache synchronization jobs.
    """
    __tablename__ = "funding_cache_run"

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(32), index=True, nullable=False)
    status = db.Column(db.String(16), default="pending", nullable=False)
    rows_count = db.Column(db.Integer, default=0)
    error = db.Column(db.Text)
    started_at = db.Column(db.DateTime)
    finished_at = db.Column(db.DateTime)


# ============================================================
# RESEARCHER ADMINISTRATIVE STATUS
# ============================================================

class ResearcherStatus(db.Model):
    """
    Tracks the institutional management status of a researcher.
    
    Specifically used to identify if the researcher's profile has been written to
    by the institution's Affiliation Manager (AM) credentials.
    """
    __tablename__ = 'researcher_status'

    id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.String(20), index=True, nullable=False)
    orcid = db.Column(db.String(20), index=True, nullable=False)
    
    # If True, the profile contains an entry created by the institution's API Key
    is_managed_by_am = db.Column(db.Boolean, default=False)
    
    last_updated = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())

    __table_args__ = (
        db.UniqueConstraint('ror_id', 'orcid', name='_ror_orcid_uc'),
    )


# ============================================================
# RESEARCHER PROFILE CACHE (NAMES & BIO)
# ============================================================

class ResearcherCache(db.Model):
    """
    Lightweight cache for Researcher Profile Metadata.
    
    Stores Names and Emails to prevent N+1 API calls when rendering 
    lists or dashboards (e.g., 'Top Contributors' charts).
    """
    __tablename__ = "researcher_cache"

    orcid = db.Column(db.String(32), primary_key=True, index=True)
    given_names = db.Column(db.String(150))
    family_name = db.Column(db.String(150))
    credit_name = db.Column(db.String(255))
    email = db.Column(db.String(255), nullable=True)
    
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ============================================================
# ANALYTICS & MONITORING
# ============================================================

class OrcidCache(db.Model):
    """
    Storage for aggregated JSON datasets.
    Typically used for yearly snapshots or raw API dumps served via `api_misc`.
    """
    __tablename__ = 'orcid_cache'

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False, index=True)
    data = db.Column(db.JSON, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class TrackingLog(db.Model):
    """
    System Audit Trail.
    
    Logs every HTTP request processed by the application, including:
    - User Identity (Who)
    - Request Path & Method (What)
    - IP Address (Where)
    - Execution Time (Performance)
    """
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