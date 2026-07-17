"""Add item-level progress to durable background jobs.

Revision ID: d5e9f2a3b4c5
Revises: c4d8e1f2a3b4
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa


revision = "d5e9f2a3b4c5"
down_revision = "c4d8e1f2a3b4"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    columns = {
        column["name"] for column in sa.inspect(bind).get_columns("sync_job")
    }
    with op.batch_alter_table("sync_job") as batch_op:
        if "items_current" not in columns:
            batch_op.add_column(
                sa.Column(
                    "items_current",
                    sa.Integer(),
                    nullable=False,
                    server_default="0",
                )
            )
        if "items_total" not in columns:
            batch_op.add_column(
                sa.Column(
                    "items_total",
                    sa.Integer(),
                    nullable=False,
                    server_default="0",
                )
            )
        if "progress_unit" not in columns:
            batch_op.add_column(sa.Column("progress_unit", sa.String(length=32), nullable=True))


def downgrade():
    bind = op.get_bind()
    columns = {
        column["name"] for column in sa.inspect(bind).get_columns("sync_job")
    }
    with op.batch_alter_table("sync_job") as batch_op:
        if "progress_unit" in columns:
            batch_op.drop_column("progress_unit")
        if "items_total" in columns:
            batch_op.drop_column("items_total")
        if "items_current" in columns:
            batch_op.drop_column("items_current")
