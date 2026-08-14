"""Add institution-specific OAI metadata mappings.

Revision ID: e3f5a7b9c1d2
Revises: d2e4f6a8b0c1
Create Date: 2026-08-14 17:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "e3f5a7b9c1d2"
down_revision = "d2e4f6a8b0c1"
branch_labels = None
depends_on = None


def _column_exists(table_name, column_name):
    columns = sa.inspect(op.get_bind()).get_columns(table_name)
    return any(column["name"] == column_name for column in columns)


def upgrade():
    if not _column_exists("oai_pmh_institution_config", "metadata_mapping"):
        with op.batch_alter_table("oai_pmh_institution_config") as batch_op:
            batch_op.add_column(sa.Column("metadata_mapping", sa.JSON(), nullable=True))


def downgrade():
    if _column_exists("oai_pmh_institution_config", "metadata_mapping"):
        with op.batch_alter_table("oai_pmh_institution_config") as batch_op:
            batch_op.drop_column("metadata_mapping")
