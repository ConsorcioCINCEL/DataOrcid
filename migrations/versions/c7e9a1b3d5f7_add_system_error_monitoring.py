"""Add persistent sanitized system error monitoring.

Revision ID: c7e9a1b3d5f7
Revises: b6d8f0a2c4e6
Create Date: 2026-08-15
"""

from alembic import op
import sqlalchemy as sa


revision = "c7e9a1b3d5f7"
down_revision = "b6d8f0a2c4e6"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade():
    if _table_exists("system_error"):
        return
    op.create_table(
        "system_error",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("logger_name", sa.String(length=160), nullable=True),
        sa.Column("exception_type", sa.String(length=255), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("traceback", sa.Text(), nullable=True),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("username", sa.String(length=80), nullable=True),
        sa.Column("institution_ror", sa.String(length=32), nullable=True),
        sa.Column("role", sa.String(length=24), nullable=True),
        sa.Column("endpoint", sa.String(length=160), nullable=True),
        sa.Column("method", sa.String(length=10), nullable=True),
        sa.Column("path", sa.String(length=500), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.String(length=36), nullable=True),
        sa.Column("job_id", sa.String(length=36), nullable=True),
        sa.Column("ip", sa.String(length=50), nullable=True),
        sa.Column("user_agent", sa.String(length=255), nullable=True),
        sa.Column("process_id", sa.Integer(), nullable=True),
        sa.Column("thread_name", sa.String(length=80), nullable=True),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
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
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_system_error_event_id", "system_error", ["event_id"], unique=True)
    op.create_index("ix_system_error_fingerprint", "system_error", ["fingerprint"], unique=False)
    op.create_index("ix_system_error_source", "system_error", ["source"], unique=False)
    op.create_index("ix_system_error_exception_type", "system_error", ["exception_type"], unique=False)
    op.create_index("ix_system_error_user_id", "system_error", ["user_id"], unique=False)
    op.create_index("ix_system_error_institution_ror", "system_error", ["institution_ror"], unique=False)
    op.create_index("ix_system_error_endpoint", "system_error", ["endpoint"], unique=False)
    op.create_index("ix_system_error_request_id", "system_error", ["request_id"], unique=False)
    op.create_index("ix_system_error_job_id", "system_error", ["job_id"], unique=False)
    op.create_index("ix_system_error_occurred_at", "system_error", ["occurred_at"], unique=False)
    op.create_index("ix_system_error_is_resolved", "system_error", ["is_resolved"], unique=False)


def downgrade():
    if _table_exists("system_error"):
        op.drop_table("system_error")
