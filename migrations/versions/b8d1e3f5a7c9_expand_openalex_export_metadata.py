"""Expand OpenAlex metadata available to fast exports.

Revision ID: b8d1e3f5a7c9
Revises: a7c9e2f4b6d8
Create Date: 2026-08-06 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "b8d1e3f5a7c9"
down_revision = "a7c9e2f4b6d8"
branch_labels = None
depends_on = None


COLUMNS = (
    sa.Column("pmid", sa.String(length=64), nullable=True),
    sa.Column("pmcid", sa.String(length=64), nullable=True),
    sa.Column("volume", sa.String(length=64), nullable=True),
    sa.Column("issue", sa.String(length=64), nullable=True),
    sa.Column("first_page", sa.String(length=64), nullable=True),
    sa.Column("last_page", sa.String(length=64), nullable=True),
    sa.Column("source_id", sa.String(length=64), nullable=True),
    sa.Column("source_issns", sa.Text(), nullable=True),
    sa.Column("source_host_organization_name", sa.Text(), nullable=True),
    sa.Column("primary_landing_page_url", sa.Text(), nullable=True),
    sa.Column("primary_pdf_url", sa.Text(), nullable=True),
    sa.Column("primary_license", sa.String(length=64), nullable=True),
    sa.Column("primary_version", sa.String(length=32), nullable=True),
    sa.Column("referenced_works_count", sa.Integer(), nullable=True),
    sa.Column("citation_normalized_percentile", sa.Float(), nullable=True),
    sa.Column("is_in_top_1_percent", sa.Boolean(), nullable=True),
    sa.Column("is_in_top_10_percent", sa.Boolean(), nullable=True),
    sa.Column("cited_by_percentile_min", sa.Integer(), nullable=True),
    sa.Column("cited_by_percentile_max", sa.Integer(), nullable=True),
    sa.Column("author_count", sa.Integer(), nullable=True),
    sa.Column("institution_count", sa.Integer(), nullable=True),
    sa.Column("country_count", sa.Integer(), nullable=True),
    sa.Column("location_count", sa.Integer(), nullable=True),
    sa.Column("has_abstract", sa.Boolean(), nullable=True),
    sa.Column("has_fulltext", sa.Boolean(), nullable=True),
    sa.Column("indexed_in", sa.Text(), nullable=True),
    sa.Column("topics", sa.Text(), nullable=True),
    sa.Column("keywords", sa.Text(), nullable=True),
    sa.Column("sustainable_development_goals", sa.Text(), nullable=True),
    sa.Column("funders", sa.Text(), nullable=True),
    sa.Column("awards", sa.Text(), nullable=True),
    sa.Column("apc_list_value", sa.Float(), nullable=True),
    sa.Column("apc_list_currency", sa.String(length=8), nullable=True),
    sa.Column("apc_list_value_usd", sa.Float(), nullable=True),
    sa.Column("apc_paid_value", sa.Float(), nullable=True),
    sa.Column("apc_paid_currency", sa.String(length=8), nullable=True),
    sa.Column("apc_paid_value_usd", sa.Float(), nullable=True),
    sa.Column("author_ids", sa.Text(), nullable=True),
    sa.Column("author_names", sa.Text(), nullable=True),
    sa.Column("author_orcids", sa.Text(), nullable=True),
    sa.Column("corresponding_author_names", sa.Text(), nullable=True),
    sa.Column("institution_names", sa.Text(), nullable=True),
    sa.Column("institution_rors", sa.Text(), nullable=True),
    sa.Column("countries", sa.Text(), nullable=True),
    sa.Column("raw_affiliation_strings", sa.Text(), nullable=True),
    sa.Column("raw_created_date", sa.String(length=10), nullable=True),
)


def _column_names():
    return {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("openalex_work_metadata")
    }


def upgrade():
    existing = _column_names()
    for column in COLUMNS:
        if column.name not in existing:
            op.add_column("openalex_work_metadata", column)


def downgrade():
    existing = _column_names()
    for column in reversed(COLUMNS):
        if column.name in existing:
            op.drop_column("openalex_work_metadata", column.name)
