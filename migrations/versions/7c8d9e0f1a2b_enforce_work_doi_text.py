"""Enforce unbounded work DOI storage.

Revision ID: 7c8d9e0f1a2b
Revises: 6b7c8d9e0f1a
Create Date: 2026-07-14 14:28:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "7c8d9e0f1a2b"
down_revision = "6b7c8d9e0f1a"
branch_labels = None
depends_on = None


def _work_cache_schema():
    inspector = sa.inspect(op.get_bind())
    columns = {
        column["name"]: column["type"]
        for column in inspector.get_columns("work_cache")
    }
    indexes = {
        index["name"]
        for index in inspector.get_indexes("work_cache")
    }
    return columns, indexes


def upgrade():
    columns, indexes = _work_cache_schema()
    doi_type = columns["doi"]

    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        if "ix_work_cache_doi" in indexes:
            batch_op.drop_index("ix_work_cache_doi")
        if "ix_work_cache_ror_type_doi" in indexes:
            batch_op.drop_index("ix_work_cache_ror_type_doi")
        if not isinstance(doi_type, sa.Text):
            batch_op.alter_column(
                "doi",
                existing_type=doi_type,
                type_=sa.Text(),
                existing_nullable=True,
            )
        if "ix_work_cache_ror_type" not in indexes:
            batch_op.create_index(
                "ix_work_cache_ror_type",
                ["ror_id", "type"],
                unique=False,
            )


def downgrade():
    columns, indexes = _work_cache_schema()
    doi_type = columns["doi"]

    op.execute(
        "UPDATE work_cache "
        "SET doi = substr(doi, 1, 255) "
        "WHERE length(doi) > 255"
    )
    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        if "ix_work_cache_ror_type" in indexes:
            batch_op.drop_index("ix_work_cache_ror_type")
        if isinstance(doi_type, sa.Text):
            batch_op.alter_column(
                "doi",
                existing_type=doi_type,
                type_=sa.String(length=255),
                existing_nullable=True,
            )
        if "ix_work_cache_doi" not in indexes:
            batch_op.create_index("ix_work_cache_doi", ["doi"], unique=False)
        if "ix_work_cache_ror_type_doi" not in indexes:
            batch_op.create_index(
                "ix_work_cache_ror_type_doi",
                ["ror_id", "type", "doi"],
                unique=False,
            )
