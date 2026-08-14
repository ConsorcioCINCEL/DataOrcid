"""Add institution-scoped OAI-PMH provider and work selection.

Revision ID: c1d3e5f7a9b2
Revises: b9e2f4a6c8d0
Create Date: 2026-08-14 10:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "c1d3e5f7a9b2"
down_revision = "b9e2f4a6c8d0"
branch_labels = None
depends_on = None


def _table_exists(table_name):
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade():
    if not _table_exists("oai_pmh_institution_config"):
        op.create_table(
            "oai_pmh_institution_config",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("provider_enabled", sa.Boolean(), nullable=False),
            sa.Column("repository_name", sa.String(length=255), nullable=False),
            sa.Column("admin_email", sa.String(length=255), nullable=True),
            sa.Column("publication_policy", sa.String(length=16), nullable=False),
            sa.Column("policy_updated_at", sa.DateTime(), nullable=False),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index("ix_oai_pmh_institution_config_ror_id", "oai_pmh_institution_config", ["ror_id"], unique=True)
        op.create_index("ix_oai_pmh_institution_config_created_by_user_id", "oai_pmh_institution_config", ["created_by_user_id"], unique=False)
        op.create_index("ix_oai_pmh_institution_config_updated_by_user_id", "oai_pmh_institution_config", ["updated_by_user_id"], unique=False)

    if not _table_exists("oai_pmh_work_selection"):
        op.create_table(
            "oai_pmh_work_selection",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("canonical_work_id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("is_included", sa.Boolean(), nullable=False),
            sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["canonical_work_id"], ["canonical_work.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("ror_id", "canonical_work_id", name="uq_oai_pmh_work_selection_ror_work"),
        )
        op.create_index("ix_oai_pmh_work_selection_canonical_work_id", "oai_pmh_work_selection", ["canonical_work_id"], unique=False)
        op.create_index("ix_oai_pmh_work_selection_ror_id", "oai_pmh_work_selection", ["ror_id"], unique=False)
        op.create_index("ix_oai_pmh_work_selection_updated_by_user_id", "oai_pmh_work_selection", ["updated_by_user_id"], unique=False)
        op.create_index(
            "ix_oai_pmh_work_selection_ror_included_updated",
            "oai_pmh_work_selection",
            ["ror_id", "is_included", "updated_at"],
            unique=False,
        )


def downgrade():
    if _table_exists("oai_pmh_work_selection"):
        op.drop_table("oai_pmh_work_selection")
    # Remove the unpublished prototype table if this migration was applied
    # before DataORCID-Chile became the sole OAI-PMH metadata source.
    if _table_exists("oai_pmh_record"):
        op.drop_table("oai_pmh_record")
    if _table_exists("oai_pmh_institution_config"):
        op.drop_table("oai_pmh_institution_config")
