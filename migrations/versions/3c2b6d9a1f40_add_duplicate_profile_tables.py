"""Add duplicate profile cache and institution registry tables

Revision ID: 3c2b6d9a1f40
Revises: 1f5c3eb504eb
Create Date: 2026-05-15 13:25:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "3c2b6d9a1f40"
down_revision = "1f5c3eb504eb"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def upgrade():
    if not _table_exists("duplicate_profile_cache"):
        op.create_table(
            "duplicate_profile_cache",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("scope_key", sa.String(length=255), nullable=False),
            sa.Column("dependency_hash", sa.String(length=64), nullable=False),
            sa.Column("report_json", sa.JSON(), nullable=False),
            sa.Column("source_summary", sa.JSON(), nullable=True),
            sa.Column("generated_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        with op.batch_alter_table("duplicate_profile_cache", schema=None) as batch_op:
            batch_op.create_index(
                batch_op.f("ix_duplicate_profile_cache_scope_key"),
                ["scope_key"],
                unique=True,
            )

    if not _table_exists("institution_registry"):
        op.create_table(
            "institution_registry",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("display_name_en", sa.String(length=255), nullable=True),
            sa.Column("grid_id", sa.String(length=32), nullable=True),
            sa.Column("country_code", sa.String(length=2), nullable=False),
            sa.Column("institution_type", sa.String(length=64), nullable=False),
            sa.Column("source", sa.String(length=64), nullable=False),
            sa.Column("is_active", sa.Boolean(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        with op.batch_alter_table("institution_registry", schema=None) as batch_op:
            batch_op.create_index(
                batch_op.f("ix_institution_registry_ror_id"),
                ["ror_id"],
                unique=True,
            )


def downgrade():
    if _table_exists("institution_registry"):
        with op.batch_alter_table("institution_registry", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_institution_registry_ror_id"))
        op.drop_table("institution_registry")

    if _table_exists("duplicate_profile_cache"):
        with op.batch_alter_table("duplicate_profile_cache", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_duplicate_profile_cache_scope_key"))
        op.drop_table("duplicate_profile_cache")
