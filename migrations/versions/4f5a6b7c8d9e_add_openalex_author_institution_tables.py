"""Add OpenAlex author and institution dimensions

Revision ID: 4f5a6b7c8d9e
Revises: 3e4f5a6b7c8d
Create Date: 2026-06-03 09:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4f5a6b7c8d9e"
down_revision = "3e4f5a6b7c8d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "openalex_work_author",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("doi_normalized", sa.String(length=255), nullable=False),
        sa.Column("openalex_id", sa.String(length=64), nullable=True),
        sa.Column("author_id", sa.String(length=64), nullable=True),
        sa.Column("author_name", sa.Text(), nullable=True),
        sa.Column("orcid", sa.String(length=32), nullable=True),
        sa.Column("raw_author_name", sa.Text(), nullable=True),
        sa.Column("author_position", sa.String(length=32), nullable=True),
        sa.Column("is_corresponding", sa.Boolean(), nullable=False),
        sa.Column("has_chile_affiliation", sa.Boolean(), nullable=False),
        sa.Column("countries", sa.JSON(), nullable=True),
        sa.Column("institution_rors", sa.JSON(), nullable=True),
        sa.Column("institution_names", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("openalex_work_author", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_openalex_work_author_doi_normalized"), ["doi_normalized"])
        batch_op.create_index(batch_op.f("ix_openalex_work_author_openalex_id"), ["openalex_id"])
        batch_op.create_index(batch_op.f("ix_openalex_work_author_author_id"), ["author_id"])
        batch_op.create_index(batch_op.f("ix_openalex_work_author_orcid"), ["orcid"])

    op.create_table(
        "openalex_work_institution",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("doi_normalized", sa.String(length=255), nullable=False),
        sa.Column("openalex_id", sa.String(length=64), nullable=True),
        sa.Column("institution_id", sa.String(length=64), nullable=True),
        sa.Column("institution_name", sa.Text(), nullable=True),
        sa.Column("ror_id", sa.String(length=32), nullable=True),
        sa.Column("country_code", sa.String(length=2), nullable=True),
        sa.Column("institution_type", sa.String(length=64), nullable=True),
        sa.Column("author_count", sa.Integer(), nullable=False),
        sa.Column("has_corresponding_author", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("openalex_work_institution", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_openalex_work_institution_doi_normalized"), ["doi_normalized"])
        batch_op.create_index(batch_op.f("ix_openalex_work_institution_openalex_id"), ["openalex_id"])
        batch_op.create_index(batch_op.f("ix_openalex_work_institution_institution_id"), ["institution_id"])
        batch_op.create_index(batch_op.f("ix_openalex_work_institution_ror_id"), ["ror_id"])
        batch_op.create_index(batch_op.f("ix_openalex_work_institution_country_code"), ["country_code"])


def downgrade():
    with op.batch_alter_table("openalex_work_institution", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_openalex_work_institution_country_code"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_institution_ror_id"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_institution_institution_id"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_institution_openalex_id"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_institution_doi_normalized"))
    op.drop_table("openalex_work_institution")

    with op.batch_alter_table("openalex_work_author", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_openalex_work_author_orcid"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_author_author_id"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_author_openalex_id"))
        batch_op.drop_index(batch_op.f("ix_openalex_work_author_doi_normalized"))
    op.drop_table("openalex_work_author")
