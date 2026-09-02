"""Add durable public contact inquiries.

Revision ID: e9f1a3c5d7b2
Revises: d8f0a2b4c6e8
Create Date: 2026-08-26
"""

from alembic import op
import sqlalchemy as sa


revision = "e9f1a3c5d7b2"
down_revision = "d8f0a2b4c6e8"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade():
    if _table_exists("contact_inquiry"):
        return
    op.create_table(
        "contact_inquiry",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("email", sa.String(length=254), nullable=False),
        sa.Column("institution", sa.String(length=160), nullable=True),
        sa.Column("topic", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "notification_status",
            sa.String(length=32),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "is_resolved",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("resolved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("resolved_by_username", sa.String(length=80), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_contact_inquiry_email", "contact_inquiry", ["email"])
    op.create_index("ix_contact_inquiry_topic", "contact_inquiry", ["topic"])
    op.create_index("ix_contact_inquiry_created_at", "contact_inquiry", ["created_at"])
    op.create_index(
        "ix_contact_inquiry_notification_status",
        "contact_inquiry",
        ["notification_status"],
    )
    op.create_index(
        "ix_contact_inquiry_is_resolved",
        "contact_inquiry",
        ["is_resolved"],
    )


def downgrade():
    if _table_exists("contact_inquiry"):
        op.drop_table("contact_inquiry")
