"""Expand DOI storage for long ORCID external identifiers.

Revision ID: b73d19ea4c52
Revises: a8c4e2f71b90
Create Date: 2026-07-14 12:36:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "b73d19ea4c52"
down_revision = "a8c4e2f71b90"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_work_cache_doi"))
        batch_op.alter_column(
            "doi",
            existing_type=sa.String(length=255),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade():
    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "doi",
            existing_type=sa.Text(),
            type_=sa.String(length=255),
            existing_nullable=True,
        )
        batch_op.create_index(batch_op.f("ix_work_cache_doi"), ["doi"], unique=False)
