"""Add the institution-scoped OAI editor role.

Revision ID: f4a6c8d0e2b3
Revises: e3f5a7b9c1d2
Create Date: 2026-08-14 20:05:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "f4a6c8d0e2b3"
down_revision = "e3f5a7b9c1d2"
branch_labels = None
depends_on = None


def _column_exists(table_name, column_name):
    columns = sa.inspect(op.get_bind()).get_columns(table_name)
    return any(column["name"] == column_name for column in columns)


def upgrade():
    if not _column_exists("user", "is_oai_user"):
        with op.batch_alter_table("user") as batch_op:
            batch_op.add_column(
                sa.Column(
                    "is_oai_user",
                    sa.Boolean(),
                    nullable=False,
                    server_default=sa.false(),
                )
            )


def downgrade():
    if _column_exists("user", "is_oai_user"):
        with op.batch_alter_table("user") as batch_op:
            batch_op.drop_column("is_oai_user")
