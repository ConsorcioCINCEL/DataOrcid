"""Widen funding cache grant number storage

Revision ID: 3e4f5a6b7c8d
Revises: 2d3f4a5b6c7d
Create Date: 2026-06-02 22:35:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "3e4f5a6b7c8d"
down_revision = "2d3f4a5b6c7d"
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
    op.execute(
        "UPDATE funding_cache "
        "SET grant_number = left(grant_number, 255) "
        "WHERE length(grant_number) > 255"
    )
    with op.batch_alter_table("funding_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "grant_number",
            existing_type=sa.Text(),
            type_=sa.String(length=255),
            existing_nullable=True,
        )
