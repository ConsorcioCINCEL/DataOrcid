"""Widen the raw OpenAlex creation timestamp.

Revision ID: b9e2f4a6c8d0
Revises: b8d1e3f5a7c9
Create Date: 2026-08-06 12:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "b9e2f4a6c8d0"
down_revision = "b8d1e3f5a7c9"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "openalex_work_metadata",
        "raw_created_date",
        existing_type=sa.String(length=10),
        type_=sa.String(length=32),
        existing_nullable=True,
    )


def downgrade():
    op.alter_column(
        "openalex_work_metadata",
        "raw_created_date",
        existing_type=sa.String(length=32),
        type_=sa.String(length=10),
        existing_nullable=True,
    )
