"""Add opaque OAI URLs and OpenAlex-validated publication defaults.

Revision ID: d2e4f6a8b0c1
Revises: c1d3e5f7a9b2
Create Date: 2026-08-14 14:35:00.000000

"""
import secrets

from alembic import op
import sqlalchemy as sa


revision = "d2e4f6a8b0c1"
down_revision = "c1d3e5f7a9b2"
branch_labels = None
depends_on = None


def _column_exists(table_name, column_name):
    columns = sa.inspect(op.get_bind()).get_columns(table_name)
    return any(column["name"] == column_name for column in columns)


def upgrade():
    if not _column_exists("oai_pmh_institution_config", "public_key"):
        with op.batch_alter_table("oai_pmh_institution_config") as batch_op:
            batch_op.add_column(sa.Column("public_key", sa.String(length=64), nullable=True))

        connection = op.get_bind()
        identifiers = connection.execute(
            sa.text("SELECT id FROM oai_pmh_institution_config ORDER BY id")
        ).scalars().all()
        for config_id in identifiers:
            connection.execute(
                sa.text(
                    "UPDATE oai_pmh_institution_config "
                    "SET public_key = :public_key WHERE id = :config_id"
                ),
                {"public_key": secrets.token_hex(24), "config_id": config_id},
            )

        with op.batch_alter_table("oai_pmh_institution_config") as batch_op:
            batch_op.alter_column("public_key", existing_type=sa.String(length=64), nullable=False)
            batch_op.create_index(
                "ix_oai_pmh_institution_config_public_key",
                ["public_key"],
                unique=True,
            )

    op.execute(
        "UPDATE oai_pmh_institution_config "
        "SET publication_policy = 'validated', policy_updated_at = CURRENT_TIMESTAMP "
        "WHERE publication_policy = 'all'"
    )


def downgrade():
    op.execute(
        "UPDATE oai_pmh_institution_config "
        "SET publication_policy = 'all', policy_updated_at = CURRENT_TIMESTAMP "
        "WHERE publication_policy = 'validated'"
    )
    if _column_exists("oai_pmh_institution_config", "public_key"):
        with op.batch_alter_table("oai_pmh_institution_config") as batch_op:
            batch_op.drop_index("ix_oai_pmh_institution_config_public_key")
            batch_op.drop_column("public_key")
