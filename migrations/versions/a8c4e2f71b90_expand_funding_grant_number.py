"""Expand funding grant numbers for long ORCID external identifiers.

Revision ID: a8c4e2f71b90
Revises: 6f8d2a41c9b7
Create Date: 2026-07-14 12:21:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "a8c4e2f71b90"
down_revision = "6f8d2a41c9b7"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("funding_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "grant_number",
            existing_type=sa.String(length=255),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade():
    with op.batch_alter_table("funding_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "grant_number",
            existing_type=sa.Text(),
            type_=sa.String(length=255),
            existing_nullable=True,
        )
