"""Add OpenAlex analytics performance indexes

Revision ID: 5a6b7c8d9e0f
Revises: 4f5a6b7c8d9e
Create Date: 2026-06-03 10:35:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "5a6b7c8d9e0f"
down_revision = "4f5a6b7c8d9e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_work_cache_ror_type_doi",
        "work_cache",
        ["ror_id", "type", "doi"],
        unique=False,
    )
    op.create_index(
        "ix_openalex_work_author_doi_author",
        "openalex_work_author",
        ["doi_normalized", "author_id"],
        unique=False,
    )
    op.create_index(
        "ix_openalex_work_author_chile_doi",
        "openalex_work_author",
        ["has_chile_affiliation", "doi_normalized"],
        unique=False,
    )
    op.create_index(
        "ix_openalex_work_institution_doi_country",
        "openalex_work_institution",
        ["doi_normalized", "country_code"],
        unique=False,
    )
    op.create_index(
        "ix_openalex_work_institution_country_doi",
        "openalex_work_institution",
        ["country_code", "doi_normalized"],
        unique=False,
    )
    op.create_index(
        "ix_openalex_work_institution_doi_ror",
        "openalex_work_institution",
        ["doi_normalized", "ror_id"],
        unique=False,
    )
    op.create_index(
        "ix_openalex_work_institution_ror_doi",
        "openalex_work_institution",
        ["ror_id", "doi_normalized"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_openalex_work_institution_ror_doi", table_name="openalex_work_institution")
    op.drop_index("ix_openalex_work_institution_doi_ror", table_name="openalex_work_institution")
    op.drop_index("ix_openalex_work_institution_country_doi", table_name="openalex_work_institution")
    op.drop_index("ix_openalex_work_institution_doi_country", table_name="openalex_work_institution")
    op.drop_index("ix_openalex_work_author_chile_doi", table_name="openalex_work_author")
    op.drop_index("ix_openalex_work_author_doi_author", table_name="openalex_work_author")
    op.drop_index("ix_work_cache_ror_type_doi", table_name="work_cache")
