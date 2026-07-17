"""Add data trust, canonical work, review, and durable job tables.

Revision ID: 8e2f4a6c9d10
Revises: 7c8d9e0f1a2b
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa


revision = "8e2f4a6c9d10"
down_revision = "7c8d9e0f1a2b"
branch_labels = None
depends_on = None


def upgrade():
    # The application historically calls ``create_all`` during startup. Keep this
    # migration safe when that startup has already created the new tables while
    # still adding columns to pre-existing tables.
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    institution_columns = {
        column["name"] for column in inspector.get_columns("institution_researcher")
    }
    if "evidence_type" not in institution_columns:
        op.add_column(
            "institution_researcher",
            sa.Column(
                "evidence_type",
                sa.String(length=24),
                nullable=False,
                server_default="verified_search",
            ),
        )
    if "evidence_sources" not in institution_columns:
        op.add_column(
            "institution_researcher",
            sa.Column("evidence_sources", sa.JSON(), nullable=True),
        )
    if "is_verified" not in institution_columns:
        op.add_column(
            "institution_researcher",
            sa.Column(
                "is_verified",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
            ),
        )
    institution_indexes = {
        index["name"] for index in sa.inspect(bind).get_indexes("institution_researcher")
    }
    if "ix_institution_researcher_evidence_type" not in institution_indexes:
        op.create_index(
            "ix_institution_researcher_evidence_type",
            "institution_researcher",
            ["evidence_type"],
            unique=False,
        )
    if "ix_institution_researcher_is_verified" not in institution_indexes:
        op.create_index(
            "ix_institution_researcher_is_verified",
            "institution_researcher",
            ["is_verified"],
            unique=False,
        )

    tracking_columns = {
        column["name"] for column in inspector.get_columns("tracking_logs")
    }
    for column in (
        sa.Column("institution_ror", sa.String(length=32), nullable=True),
        sa.Column("role", sa.String(length=24), nullable=True),
        sa.Column("action", sa.String(length=80), nullable=True),
        sa.Column("job_id", sa.String(length=36), nullable=True),
    ):
        if column.name not in tracking_columns:
            op.add_column("tracking_logs", column)
    tracking_indexes = {
        index["name"] for index in sa.inspect(bind).get_indexes("tracking_logs")
    }
    for index_name, column_name in (
        ("ix_tracking_logs_institution_ror", "institution_ror"),
        ("ix_tracking_logs_role", "role"),
        ("ix_tracking_logs_action", "action"),
        ("ix_tracking_logs_job_id", "job_id"),
    ):
        if index_name not in tracking_indexes:
            op.create_index(index_name, "tracking_logs", [column_name])

    new_tables = {
        "canonical_work",
        "work_record_link",
        "duplicate_profile_review",
        "researcher_affiliation_evidence",
        "sync_job",
        "sync_job_step",
    }
    if new_tables.issubset(tables):
        return

    op.create_table(
        "canonical_work",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("canonical_key", sa.String(length=80), nullable=False),
        sa.Column("doi_normalized", sa.String(length=255), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("title_normalized", sa.Text(), nullable=True),
        sa.Column("publication_year", sa.Integer(), nullable=True),
        sa.Column("record_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("canonical_key"),
    )
    op.create_index("ix_canonical_work_canonical_key", "canonical_work", ["canonical_key"], unique=True)
    op.create_index("ix_canonical_work_doi_normalized", "canonical_work", ["doi_normalized"])
    op.create_index("ix_canonical_work_publication_year", "canonical_work", ["publication_year"])

    op.create_table(
        "work_record_link",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("canonical_work_id", sa.Integer(), nullable=False),
        sa.Column("work_cache_id", sa.Integer(), nullable=True),
        sa.Column("ror_id", sa.String(length=32), nullable=False),
        sa.Column("orcid", sa.String(length=32), nullable=False),
        sa.Column("source_record_key", sa.String(length=96), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["canonical_work_id"], ["canonical_work.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ror_id", "orcid", "source_record_key", name="uq_work_record_link_source"),
    )
    op.create_index("ix_work_record_link_canonical_work_id", "work_record_link", ["canonical_work_id"])
    op.create_index("ix_work_record_link_work_cache_id", "work_record_link", ["work_cache_id"])
    op.create_index("ix_work_record_link_ror_id", "work_record_link", ["ror_id"])
    op.create_index("ix_work_record_link_orcid", "work_record_link", ["orcid"])
    op.create_index("ix_work_record_link_ror_canonical", "work_record_link", ["ror_id", "canonical_work_id"])

    op.create_table(
        "duplicate_profile_review",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("group_key", sa.String(length=64), nullable=False),
        sa.Column("ror_id", sa.String(length=32), nullable=False),
        sa.Column("normalized_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("selected_orcid", sa.String(length=32), nullable=True),
        sa.Column("assigned_user_id", sa.Integer(), nullable=True),
        sa.Column("reviewed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("candidate_snapshot", sa.JSON(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_key"),
    )
    op.create_index("ix_duplicate_profile_review_group_key", "duplicate_profile_review", ["group_key"], unique=True)
    op.create_index("ix_duplicate_profile_review_ror_id", "duplicate_profile_review", ["ror_id"])
    op.create_index("ix_duplicate_profile_review_status", "duplicate_profile_review", ["status"])
    op.create_index("ix_duplicate_profile_review_assigned_user_id", "duplicate_profile_review", ["assigned_user_id"])
    op.create_index("ix_duplicate_profile_review_reviewed_by_user_id", "duplicate_profile_review", ["reviewed_by_user_id"])

    op.create_table(
        "researcher_affiliation_evidence",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("institution_id", sa.Integer(), nullable=False),
        sa.Column("orcid", sa.String(length=32), nullable=False),
        sa.Column("source_section", sa.String(length=40), nullable=False),
        sa.Column("organization_name", sa.Text(), nullable=True),
        sa.Column("role_title", sa.Text(), nullable=True),
        sa.Column("department_name", sa.Text(), nullable=True),
        sa.Column("start_year", sa.Integer(), nullable=True),
        sa.Column("end_year", sa.Integer(), nullable=True),
        sa.Column("source_client_id", sa.String(length=64), nullable=True),
        sa.Column("organization_identifiers", sa.JSON(), nullable=True),
        sa.Column("evidence_type", sa.String(length=24), nullable=False, server_default="public_orcid"),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("observed_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["institution_id"], ["institution_registry.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_researcher_affiliation_evidence_institution_id", "researcher_affiliation_evidence", ["institution_id"])
    op.create_index("ix_researcher_affiliation_evidence_orcid", "researcher_affiliation_evidence", ["orcid"])
    op.create_index("ix_researcher_affiliation_evidence_is_current", "researcher_affiliation_evidence", ["is_current"])
    op.create_index("ix_affiliation_evidence_institution_orcid", "researcher_affiliation_evidence", ["institution_id", "orcid"])

    op.create_table(
        "sync_job",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("job_type", sa.String(length=40), nullable=False),
        sa.Column("ror_id", sa.String(length=32), nullable=True),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="queued"),
        sa.Column("progress_current", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("progress_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sync_job_job_type", "sync_job", ["job_type"])
    op.create_index("ix_sync_job_ror_id", "sync_job", ["ror_id"])
    op.create_index("ix_sync_job_requested_by_user_id", "sync_job", ["requested_by_user_id"])
    op.create_index("ix_sync_job_status", "sync_job", ["status"])

    op.create_table(
        "sync_job_step",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("sync_job_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=80), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("records_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["sync_job_id"], ["sync_job.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sync_job_id", "name", name="uq_sync_job_step_name"),
    )
    op.create_index("ix_sync_job_step_sync_job_id", "sync_job_step", ["sync_job_id"])
    op.create_index("ix_sync_job_step_status", "sync_job_step", ["status"])


def downgrade():
    op.drop_table("sync_job_step")
    op.drop_table("sync_job")
    op.drop_table("researcher_affiliation_evidence")
    op.drop_table("duplicate_profile_review")
    op.drop_table("work_record_link")
    op.drop_table("canonical_work")

    op.drop_index("ix_tracking_logs_job_id", table_name="tracking_logs")
    op.drop_index("ix_tracking_logs_action", table_name="tracking_logs")
    op.drop_index("ix_tracking_logs_role", table_name="tracking_logs")
    op.drop_index("ix_tracking_logs_institution_ror", table_name="tracking_logs")
    op.drop_column("tracking_logs", "job_id")
    op.drop_column("tracking_logs", "action")
    op.drop_column("tracking_logs", "role")
    op.drop_column("tracking_logs", "institution_ror")

    op.drop_index("ix_institution_researcher_is_verified", table_name="institution_researcher")
    op.drop_index("ix_institution_researcher_evidence_type", table_name="institution_researcher")
    op.drop_column("institution_researcher", "is_verified")
    op.drop_column("institution_researcher", "evidence_sources")
    op.drop_column("institution_researcher", "evidence_type")
