"""Track and undo institutional OAI DOI spreadsheet imports.

Revision ID: a5c7e9f1b3d4
Revises: f4a6c8d0e2b3
Create Date: 2026-08-14 21:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "a5c7e9f1b3d4"
down_revision = "f4a6c8d0e2b3"
branch_labels = None
depends_on = None


INDEX_NAME = "ix_oai_pmh_work_selection_ror_decision_source"


def _column_exists(table_name, column_name):
    columns = sa.inspect(op.get_bind()).get_columns(table_name)
    return any(column["name"] == column_name for column in columns)


def _index_exists(table_name, index_name):
    indexes = sa.inspect(op.get_bind()).get_indexes(table_name)
    return any(index["name"] == index_name for index in indexes)


def _table_exists(table_name):
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade():
    if not _table_exists("oai_pmh_doi_import_batch"):
        op.create_table(
            "oai_pmh_doi_import_batch",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("filename", sa.String(length=255), nullable=False),
            sa.Column("submitted_count", sa.Integer(), nullable=False),
            sa.Column("matched_count", sa.Integer(), nullable=False),
            sa.Column("article_count", sa.Integer(), nullable=False),
            sa.Column("invalid_count", sa.Integer(), nullable=False),
            sa.Column("duplicate_count", sa.Integer(), nullable=False),
            sa.Column("unmatched_count", sa.Integer(), nullable=False),
            sa.Column("imported_by_user_id", sa.Integer(), nullable=True),
            sa.Column("undone_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("undone_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(
            "ix_oai_pmh_doi_import_batch_ror_id",
            "oai_pmh_doi_import_batch",
            ["ror_id"],
            unique=False,
        )
        op.create_index(
            "ix_oai_pmh_doi_import_batch_imported_by_user_id",
            "oai_pmh_doi_import_batch",
            ["imported_by_user_id"],
            unique=False,
        )
        op.create_index(
            "ix_oai_pmh_doi_import_batch_undone_by_user_id",
            "oai_pmh_doi_import_batch",
            ["undone_by_user_id"],
            unique=False,
        )
        op.create_index(
            "ix_oai_pmh_doi_import_batch_ror_undone_created",
            "oai_pmh_doi_import_batch",
            ["ror_id", "undone_at", "created_at"],
            unique=False,
        )

    if not _column_exists("oai_pmh_work_selection", "decision_source"):
        with op.batch_alter_table("oai_pmh_work_selection") as batch_op:
            batch_op.add_column(
                sa.Column(
                    "decision_source",
                    sa.String(length=16),
                    nullable=False,
                    server_default="manual",
                )
            )
    if not _column_exists("oai_pmh_work_selection", "doi_import_batch_id"):
        with op.batch_alter_table("oai_pmh_work_selection") as batch_op:
            batch_op.add_column(
                sa.Column("doi_import_batch_id", sa.Integer(), nullable=True)
            )
            batch_op.create_foreign_key(
                "fk_oai_pmh_work_selection_doi_import_batch",
                "oai_pmh_doi_import_batch",
                ["doi_import_batch_id"],
                ["id"],
                ondelete="SET NULL",
            )
            batch_op.create_index(
                "ix_oai_pmh_work_selection_doi_import_batch_id",
                ["doi_import_batch_id"],
                unique=False,
            )
    if not _index_exists("oai_pmh_work_selection", INDEX_NAME):
        op.create_index(
            INDEX_NAME,
            "oai_pmh_work_selection",
            ["ror_id", "decision_source"],
            unique=False,
        )

    if not _table_exists("oai_pmh_doi_import_change"):
        op.create_table(
            "oai_pmh_doi_import_change",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("batch_id", sa.Integer(), nullable=False),
            sa.Column("canonical_work_id", sa.Integer(), nullable=False),
            sa.Column("previous_selection_existed", sa.Boolean(), nullable=False),
            sa.Column("previous_is_included", sa.Boolean(), nullable=True),
            sa.Column("previous_decision_source", sa.String(length=16), nullable=True),
            sa.Column("previous_import_batch_id", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["batch_id"],
                ["oai_pmh_doi_import_batch.id"],
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["canonical_work_id"],
                ["canonical_work.id"],
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["previous_import_batch_id"],
                ["oai_pmh_doi_import_batch.id"],
                ondelete="SET NULL",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "batch_id",
                "canonical_work_id",
                name="uq_oai_pmh_doi_import_change_batch_work",
            ),
        )
        op.create_index(
            "ix_oai_pmh_doi_import_change_batch_id",
            "oai_pmh_doi_import_change",
            ["batch_id"],
            unique=False,
        )
        op.create_index(
            "ix_oai_pmh_doi_import_change_canonical_work_id",
            "oai_pmh_doi_import_change",
            ["canonical_work_id"],
            unique=False,
        )


def downgrade():
    if _table_exists("oai_pmh_doi_import_change"):
        op.drop_table("oai_pmh_doi_import_change")
    if _index_exists("oai_pmh_work_selection", INDEX_NAME):
        op.drop_index(INDEX_NAME, table_name="oai_pmh_work_selection")
    if _column_exists("oai_pmh_work_selection", "doi_import_batch_id"):
        with op.batch_alter_table("oai_pmh_work_selection") as batch_op:
            batch_op.drop_index("ix_oai_pmh_work_selection_doi_import_batch_id")
            batch_op.drop_constraint(
                "fk_oai_pmh_work_selection_doi_import_batch",
                type_="foreignkey",
            )
            batch_op.drop_column("doi_import_batch_id")
    if _column_exists("oai_pmh_work_selection", "decision_source"):
        with op.batch_alter_table("oai_pmh_work_selection") as batch_op:
            batch_op.drop_column("decision_source")
    if _table_exists("oai_pmh_doi_import_batch"):
        op.drop_table("oai_pmh_doi_import_batch")
