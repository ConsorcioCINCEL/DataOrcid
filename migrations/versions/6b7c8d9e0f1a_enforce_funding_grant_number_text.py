"""Enforce unbounded funding grant number storage.

Revision ID: 6b7c8d9e0f1a
Revises: b73d19ea4c52
Create Date: 2026-07-13 15:32:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "6b7c8d9e0f1a"
down_revision = "b73d19ea4c52"
branch_labels = None
depends_on = None


def _grant_number_type():
    inspector = sa.inspect(op.get_bind())
    columns = inspector.get_columns("funding_cache")
    return next(column["type"] for column in columns if column["name"] == "grant_number")


def upgrade():
    if not isinstance(_grant_number_type(), sa.Text):
        with op.batch_alter_table("funding_cache", schema=None) as batch_op:
            batch_op.alter_column(
                "grant_number",
                existing_type=_grant_number_type(),
                type_=sa.Text(),
                existing_nullable=True,
            )


def downgrade():
    op.execute(
        "UPDATE funding_cache "
        "SET grant_number = substr(grant_number, 1, 255) "
        "WHERE length(grant_number) > 255"
    )
    with op.batch_alter_table("funding_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "grant_number",
            existing_type=sa.Text(),
            type_=sa.String(length=255),
            existing_nullable=True,
        )
