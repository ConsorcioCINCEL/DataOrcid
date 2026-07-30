"""Add OpenAlex cache tables

Revision ID: 7b4c2f9a8d10
Revises: 3c2b6d9a1f40
Create Date: 2026-06-02 16:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7b4c2f9a8d10"
down_revision = "3c2b6d9a1f40"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def upgrade():
    if not _table_exists("openalex_work_raw_cache"):
        op.create_table(
            "openalex_work_raw_cache",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("doi_normalized", sa.String(length=255), nullable=False),
            sa.Column("source_doi", sa.String(length=255), nullable=True),
            sa.Column("openalex_id", sa.String(length=64), nullable=True),
            sa.Column("status", sa.String(length=16), nullable=False),
            sa.Column("http_status", sa.Integer(), nullable=True),
            sa.Column("raw_json", sa.JSON(), nullable=True),
            sa.Column("oa_updated_date", sa.DateTime(), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("fetched_at", sa.DateTime(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        with op.batch_alter_table("openalex_work_raw_cache", schema=None) as batch_op:
            batch_op.create_index(
                batch_op.f("ix_openalex_work_raw_cache_doi_normalized"),
                ["doi_normalized"],
                unique=True,
            )
            batch_op.create_index(
                batch_op.f("ix_openalex_work_raw_cache_openalex_id"),
                ["openalex_id"],
                unique=False,
            )

    if not _table_exists("openalex_work_metadata"):
        op.create_table(
            "openalex_work_metadata",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("doi_normalized", sa.String(length=255), nullable=False),
            sa.Column("openalex_id", sa.String(length=64), nullable=True),
            sa.Column("title", sa.Text(), nullable=True),
            sa.Column("publication_year", sa.Integer(), nullable=True),
            sa.Column("publication_date", sa.String(length=10), nullable=True),
            sa.Column("type", sa.String(length=64), nullable=True),
            sa.Column("language", sa.String(length=8), nullable=True),
            sa.Column("cited_by_count", sa.Integer(), nullable=False),
            sa.Column("fwci", sa.Float(), nullable=True),
            sa.Column("is_retracted", sa.Boolean(), nullable=False),
            sa.Column("is_oa", sa.Boolean(), nullable=False),
            sa.Column("oa_status", sa.String(length=32), nullable=True),
            sa.Column("oa_url", sa.Text(), nullable=True),
            sa.Column("best_pdf_url", sa.Text(), nullable=True),
            sa.Column("source_name", sa.Text(), nullable=True),
            sa.Column("source_issn_l", sa.String(length=32), nullable=True),
            sa.Column("source_type", sa.String(length=64), nullable=True),
            sa.Column("source_is_in_doaj", sa.Boolean(), nullable=True),
            sa.Column("primary_topic_name", sa.String(length=255), nullable=True),
            sa.Column("primary_topic_field", sa.String(length=255), nullable=True),
            sa.Column("primary_topic_domain", sa.String(length=255), nullable=True),
            sa.Column("raw_updated_date", sa.DateTime(), nullable=True),
            sa.Column("fetched_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        with op.batch_alter_table("openalex_work_metadata", schema=None) as batch_op:
            batch_op.create_index(
                batch_op.f("ix_openalex_work_metadata_doi_normalized"),
                ["doi_normalized"],
                unique=True,
            )
            batch_op.create_index(
                batch_op.f("ix_openalex_work_metadata_openalex_id"),
                ["openalex_id"],
                unique=False,
            )
            batch_op.create_index(batch_op.f("ix_openalex_work_metadata_publication_year"), ["publication_year"])
            batch_op.create_index(batch_op.f("ix_openalex_work_metadata_type"), ["type"])
            batch_op.create_index(batch_op.f("ix_openalex_work_metadata_language"), ["language"])
            batch_op.create_index(batch_op.f("ix_openalex_work_metadata_oa_status"), ["oa_status"])
            batch_op.create_index(batch_op.f("ix_openalex_work_metadata_source_issn_l"), ["source_issn_l"])

    if not _table_exists("openalex_sync_run"):
        op.create_table(
            "openalex_sync_run",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=True),
            sa.Column("status", sa.String(length=16), nullable=False),
            sa.Column("works_seen", sa.Integer(), nullable=False),
            sa.Column("dois_found", sa.Integer(), nullable=False),
            sa.Column("fetched_count", sa.Integer(), nullable=False),
            sa.Column("matched_count", sa.Integer(), nullable=False),
            sa.Column("not_found_count", sa.Integer(), nullable=False),
            sa.Column("error_count", sa.Integer(), nullable=False),
            sa.Column("skipped_count", sa.Integer(), nullable=False),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("started_at", sa.DateTime(), nullable=False),
            sa.Column("finished_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        with op.batch_alter_table("openalex_sync_run", schema=None) as batch_op:
            batch_op.create_index(batch_op.f("ix_openalex_sync_run_ror_id"), ["ror_id"], unique=False)


def downgrade():
    if _table_exists("openalex_sync_run"):
        with op.batch_alter_table("openalex_sync_run", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_openalex_sync_run_ror_id"))
        op.drop_table("openalex_sync_run")

    if _table_exists("openalex_work_metadata"):
        with op.batch_alter_table("openalex_work_metadata", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_source_issn_l"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_oa_status"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_language"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_type"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_publication_year"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_openalex_id"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_metadata_doi_normalized"))
        op.drop_table("openalex_work_metadata")

    if _table_exists("openalex_work_raw_cache"):
        with op.batch_alter_table("openalex_work_raw_cache", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_openalex_work_raw_cache_openalex_id"))
            batch_op.drop_index(batch_op.f("ix_openalex_work_raw_cache_doi_normalized"))
        op.drop_table("openalex_work_raw_cache")
