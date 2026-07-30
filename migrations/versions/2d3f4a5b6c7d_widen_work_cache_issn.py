"""Widen work cache ISSN storage

Revision ID: 2d3f4a5b6c7d
Revises: 7b4c2f9a8d10
Create Date: 2026-06-02 22:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2d3f4a5b6c7d"
down_revision = "7b4c2f9a8d10"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "issn",
            existing_type=sa.String(length=64),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade():
    op.execute("UPDATE work_cache SET issn = left(issn, 64) WHERE length(issn) > 64")
    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        batch_op.alter_column(
            "issn",
            existing_type=sa.Text(),
            type_=sa.String(length=64),
            existing_nullable=True,
        )
