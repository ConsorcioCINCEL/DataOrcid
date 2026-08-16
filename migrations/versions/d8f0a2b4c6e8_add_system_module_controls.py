"""Add global application-module availability controls.

Revision ID: d8f0a2b4c6e8
Revises: c7e9a1b3d5f7
Create Date: 2026-08-15
"""

from alembic import op
import sqlalchemy as sa


revision = "d8f0a2b4c6e8"
down_revision = "c7e9a1b3d5f7"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade():
    if _table_exists("system_module"):
        return
    op.create_table(
        "system_module",
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column(
            "is_enabled",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_username", sa.String(length=80), nullable=True),
        sa.PrimaryKeyConstraint("key"),
    )


def downgrade():
    if _table_exists("system_module"):
        op.drop_table("system_module")
