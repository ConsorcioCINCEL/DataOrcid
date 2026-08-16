"""Harden authentication throttling and durable background jobs.

Revision ID: b6d8f0a2c4e6
Revises: a5c7e9f1b3d4
Create Date: 2026-08-15
"""

from alembic import op
import sqlalchemy as sa


revision = "b6d8f0a2c4e6"
down_revision = "a5c7e9f1b3d4"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def _column_names(table_name: str) -> set[str]:
    return {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def _index_exists(table_name: str, index_name: str) -> bool:
    return any(
        index["name"] == index_name
        for index in sa.inspect(op.get_bind()).get_indexes(table_name)
    )


def upgrade():
    if not _table_exists("auth_rate_limit_event"):
        op.create_table(
            "auth_rate_limit_event",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("action", sa.String(length=32), nullable=False),
            sa.Column("client_key", sa.String(length=64), nullable=False),
            sa.Column("occurred_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(
            "ix_auth_rate_limit_action_client_time",
            "auth_rate_limit_event",
            ["action", "client_key", "occurred_at"],
            unique=False,
        )

    columns = _column_names("sync_job")
    additions = (
        sa.Column("handler", sa.String(length=255), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=True),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "max_attempts",
            sa.Integer(),
            nullable=False,
            server_default="3",
        ),
        sa.Column("claimed_by", sa.String(length=80), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
    )
    with op.batch_alter_table("sync_job") as batch_op:
        for column in additions:
            if column.name not in columns:
                batch_op.add_column(column)

    if not _index_exists("sync_job", "ix_sync_job_claimed_by"):
        op.create_index(
            "ix_sync_job_claimed_by",
            "sync_job",
            ["claimed_by"],
            unique=False,
        )


def downgrade():
    if _table_exists("sync_job"):
        if _index_exists("sync_job", "ix_sync_job_claimed_by"):
            op.drop_index("ix_sync_job_claimed_by", table_name="sync_job")
        columns = _column_names("sync_job")
        with op.batch_alter_table("sync_job") as batch_op:
            for column_name in (
                "claimed_at",
                "claimed_by",
                "max_attempts",
                "attempt_count",
                "payload_json",
                "handler",
            ):
                if column_name in columns:
                    batch_op.drop_column(column_name)

    if _table_exists("auth_rate_limit_event"):
        op.drop_table("auth_rate_limit_event")
