"""Add institution-owned private OAI-PMH harvesting URLs.

Revision ID: f0a2b4c6d8e1
Revises: e9f1a3c5d7b2
Create Date: 2026-09-04
"""

from alembic import op
import sqlalchemy as sa


revision = "f0a2b4c6d8e1"
down_revision = "e9f1a3c5d7b2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("oai_pmh_institution_config", sa.Column(
        "harvester_access_restricted", sa.Boolean(), nullable=False, server_default=sa.false(),
    ))
    op.create_table(
        "oai_pmh_harvester",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("config_id", sa.Integer(), sa.ForeignKey("oai_pmh_institution_config.id", ondelete="CASCADE"), nullable=False),
        sa.Column("base_uri", sa.String(2048), nullable=False),
        sa.Column("access_key", sa.String(64), nullable=False, unique=True),
        sa.Column("is_enabled", sa.Boolean(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("config_id", "base_uri", name="uq_oai_pmh_harvester_config_uri"),
    )
    op.create_index("ix_oai_pmh_harvester_config_id", "oai_pmh_harvester", ["config_id"])


def downgrade():
    op.drop_table("oai_pmh_harvester")
    op.drop_column("oai_pmh_institution_config", "harvester_access_restricted")
