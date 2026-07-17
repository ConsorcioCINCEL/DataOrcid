"""Harden OpenAlex synchronization retries and dimension integrity.

Revision ID: c4d8e1f2a3b4
Revises: 8e2f4a6c9d10
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa


revision = "c4d8e1f2a3b4"
down_revision = "8e2f4a6c9d10"
branch_labels = None
depends_on = None


def _remove_duplicate_dimensions(bind, table_name, key_column):
    table = sa.table(
        table_name,
        sa.column("id", sa.Integer()),
        sa.column("doi_normalized", sa.String()),
        sa.column(key_column, sa.String()),
    )
    ranked = (
        sa.select(
            table.c.id,
            sa.func.row_number().over(
                partition_by=(table.c.doi_normalized, table.c[key_column]),
                order_by=table.c.id.asc(),
            ).label("row_number"),
        )
        .where(table.c[key_column].is_not(None))
        .subquery()
    )
    duplicate_ids = [
        row[0]
        for row in bind.execute(
            sa.select(ranked.c.id).where(ranked.c.row_number > 1)
        )
    ]
    for start in range(0, len(duplicate_ids), 1000):
        bind.execute(
            table.delete().where(table.c.id.in_(duplicate_ids[start:start + 1000]))
        )


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    raw_columns = {
        column["name"] for column in inspector.get_columns("openalex_work_raw_cache")
    }
    with op.batch_alter_table("openalex_work_raw_cache") as batch_op:
        if "attempt_count" not in raw_columns:
            batch_op.add_column(
                sa.Column(
                    "attempt_count",
                    sa.Integer(),
                    nullable=False,
                    server_default="0",
                )
            )
        if "next_retry_at" not in raw_columns:
            batch_op.add_column(sa.Column("next_retry_at", sa.DateTime(), nullable=True))

    inspector = sa.inspect(bind)
    raw_indexes = {
        index["name"] for index in inspector.get_indexes("openalex_work_raw_cache")
    }
    if "ix_openalex_work_raw_cache_next_retry_at" not in raw_indexes:
        op.create_index(
            "ix_openalex_work_raw_cache_next_retry_at",
            "openalex_work_raw_cache",
            ["next_retry_at"],
        )

    _remove_duplicate_dimensions(bind, "openalex_work_author", "author_id")
    _remove_duplicate_dimensions(bind, "openalex_work_institution", "institution_id")

    inspector = sa.inspect(bind)
    author_indexes = {
        index["name"] for index in inspector.get_indexes("openalex_work_author")
    }
    author_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("openalex_work_author")
    }
    with op.batch_alter_table("openalex_work_author") as batch_op:
        if "ix_openalex_work_author_doi_author" in author_indexes:
            batch_op.drop_index("ix_openalex_work_author_doi_author")
        if "uq_openalex_work_author_doi_author" not in author_constraints:
            batch_op.create_unique_constraint(
                "uq_openalex_work_author_doi_author",
                ["doi_normalized", "author_id"],
            )

    inspector = sa.inspect(bind)
    institution_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("openalex_work_institution")
    }
    if "uq_openalex_work_institution_doi_institution" not in institution_constraints:
        with op.batch_alter_table("openalex_work_institution") as batch_op:
            batch_op.create_unique_constraint(
                "uq_openalex_work_institution_doi_institution",
                ["doi_normalized", "institution_id"],
            )


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    institution_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("openalex_work_institution")
    }
    if "uq_openalex_work_institution_doi_institution" in institution_constraints:
        with op.batch_alter_table("openalex_work_institution") as batch_op:
            batch_op.drop_constraint(
                "uq_openalex_work_institution_doi_institution",
                type_="unique",
            )

    inspector = sa.inspect(bind)
    author_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("openalex_work_author")
    }
    author_indexes = {
        index["name"] for index in inspector.get_indexes("openalex_work_author")
    }
    with op.batch_alter_table("openalex_work_author") as batch_op:
        if "uq_openalex_work_author_doi_author" in author_constraints:
            batch_op.drop_constraint("uq_openalex_work_author_doi_author", type_="unique")
        if "ix_openalex_work_author_doi_author" not in author_indexes:
            batch_op.create_index(
                "ix_openalex_work_author_doi_author",
                ["doi_normalized", "author_id"],
            )

    inspector = sa.inspect(bind)
    raw_indexes = {
        index["name"] for index in inspector.get_indexes("openalex_work_raw_cache")
    }
    if "ix_openalex_work_raw_cache_next_retry_at" in raw_indexes:
        op.drop_index(
            "ix_openalex_work_raw_cache_next_retry_at",
            table_name="openalex_work_raw_cache",
        )

    raw_columns = {
        column["name"] for column in sa.inspect(bind).get_columns("openalex_work_raw_cache")
    }
    with op.batch_alter_table("openalex_work_raw_cache") as batch_op:
        if "next_retry_at" in raw_columns:
            batch_op.drop_column("next_retry_at")
        if "attempt_count" in raw_columns:
            batch_op.drop_column("attempt_count")
