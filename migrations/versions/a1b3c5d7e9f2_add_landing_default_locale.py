"""Add an administrator-controlled default language for the public landing page.

Revision ID: a1b3c5d7e9f2
Revises: f0a2b4c6d8e1
Create Date: 2026-09-04
"""

from alembic import op
import sqlalchemy as sa


revision = "a1b3c5d7e9f2"
down_revision = "f0a2b4c6d8e1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("system_module", sa.Column("default_locale", sa.String(8), nullable=True))


def downgrade():
    op.drop_column("system_module", "default_locale")
