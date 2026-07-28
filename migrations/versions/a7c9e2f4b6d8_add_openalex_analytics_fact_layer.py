"""Add the OpenAlex analytics fact layer.

Revision ID: a7c9e2f4b6d8
Revises: d5e9f2a3b4c5
Create Date: 2026-07-28 15:10:00.000000

"""
import re

from alembic import op
import sqlalchemy as sa


revision = "a7c9e2f4b6d8"
down_revision = "d5e9f2a3b4c5"
branch_labels = None
depends_on = None

DOI_PATTERN = re.compile(
    r"10\.\d{4,9}/[-._;()/:A-Z0-9]+",
    re.IGNORECASE,
)
MAX_DOI_LENGTH = 255


def _table_exists(table_name):
    return sa.inspect(op.get_bind()).has_table(table_name)


def _column_exists(table_name, column_name):
    return column_name in {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def _index_exists(table_name, index_name):
    return index_name in {
        index["name"]
        for index in sa.inspect(op.get_bind()).get_indexes(table_name)
    }


def _create_index(index_name, table_name, columns):
    if op.get_bind().dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.create_index(
                index_name,
                table_name,
                columns,
                unique=False,
                postgresql_concurrently=True,
            )
        return
    op.create_index(index_name, table_name, columns, unique=False)


def _normalize_doi(value):
    text = str(value or "").strip()
    match = DOI_PATTERN.search(text)
    if not match:
        return None

    doi = match.group(0).lower().rstrip(".,;:")
    while doi.endswith(")") and doi.count(")") > doi.count("("):
        doi = doi[:-1]
    return doi if 0 < len(doi) <= MAX_DOI_LENGTH else None


def _backfill_normalized_dois():
    connection = op.get_bind()
    work_cache = sa.table(
        "work_cache",
        sa.column("id", sa.Integer()),
        sa.column("doi", sa.Text()),
        sa.column("doi_normalized", sa.String(length=255)),
    )
    last_id = 0
    while True:
        rows = connection.execute(
            sa.select(work_cache.c.id, work_cache.c.doi)
            .where(
                work_cache.c.id > last_id,
                work_cache.c.doi.isnot(None),
                work_cache.c.doi != "",
                work_cache.c.doi_normalized.is_(None),
            )
            .order_by(work_cache.c.id)
            .limit(5000)
        ).all()
        if not rows:
            break

        updates = []
        for row in rows:
            normalized_doi = _normalize_doi(row.doi)
            if normalized_doi:
                updates.append({
                    "row_id": row.id,
                    "doi_normalized": normalized_doi,
                })
        if updates:
            connection.execute(
                sa.text(
                    "UPDATE work_cache "
                    "SET doi_normalized = :doi_normalized "
                    "WHERE id = :row_id"
                ),
                updates,
            )
        last_id = rows[-1].id


def _seed_analytics_versions():
    connection = op.get_bind()
    versions = sa.table(
        "analytics_data_version",
        sa.column("scope_key", sa.String(length=96)),
        sa.column("version", sa.Integer()),
        sa.column("updated_at", sa.DateTime()),
    )
    work_cache = sa.table(
        "work_cache",
        sa.column("ror_id", sa.String(length=32)),
    )
    scope_keys = {"openalex:global"}
    scope_keys.update(
        f"openalex:ror:{ror_id}"
        for (ror_id,) in connection.execute(
            sa.select(work_cache.c.ror_id)
            .where(
                work_cache.c.ror_id.isnot(None),
                work_cache.c.ror_id != "",
            )
            .distinct()
        )
        if ror_id
    )
    existing_scope_keys = set(
        connection.execute(sa.select(versions.c.scope_key)).scalars()
    )
    rows = [
        {"scope_key": scope_key, "version": 1}
        for scope_key in sorted(scope_keys - existing_scope_keys)
    ]
    if rows:
        connection.execute(
            sa.insert(versions).values(updated_at=sa.func.current_timestamp()),
            rows,
        )


def upgrade():
    if not _column_exists("work_cache", "doi_normalized"):
        with op.batch_alter_table("work_cache", schema=None) as batch_op:
            batch_op.add_column(
                sa.Column("doi_normalized", sa.String(length=255), nullable=True)
            )

    _backfill_normalized_dois()

    work_cache_indexes = (
        (
            "ix_work_cache_ror_type_doi_normalized",
            ["ror_id", "type", "doi_normalized"],
        ),
        ("ix_work_cache_ror_year_type", ["ror_id", "pub_year", "type"]),
        ("ix_work_cache_ror_orcid_year", ["ror_id", "orcid", "pub_year"]),
    )
    for index_name, columns in work_cache_indexes:
        if not _index_exists("work_cache", index_name):
            _create_index(index_name, "work_cache", columns)

    funding_cache_indexes = (
        ("ix_funding_cache_ror_year_type", ["ror_id", "start_y", "type"]),
        ("ix_funding_cache_ror_orcid_year", ["ror_id", "orcid", "start_y"]),
    )
    for index_name, columns in funding_cache_indexes:
        if not _index_exists("funding_cache", index_name):
            _create_index(index_name, "funding_cache", columns)

    if not _table_exists("analytics_data_version"):
        op.create_table(
            "analytics_data_version",
            sa.Column("scope_key", sa.String(length=96), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("scope_key"),
        )
    if not _table_exists("openalex_institution_work_fact"):
        op.create_table(
            "openalex_institution_work_fact",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("openalex_cache_key", sa.String(length=255), nullable=False),
            sa.Column("representative_work_cache_id", sa.Integer(), nullable=False),
            sa.Column("source_record_count", sa.Integer(), nullable=False),
            sa.Column("has_valid_doi", sa.Boolean(), nullable=False),
            sa.Column("has_local_title", sa.Boolean(), nullable=False),
            sa.Column("raw_status", sa.String(length=16), nullable=True),
            sa.Column("raw_error", sa.Text(), nullable=True),
            sa.Column("openalex_id", sa.String(length=64), nullable=True),
            sa.Column("title", sa.Text(), nullable=True),
            sa.Column("publication_year", sa.Integer(), nullable=True),
            sa.Column("document_type", sa.String(length=64), nullable=True),
            sa.Column("language", sa.String(length=8), nullable=True),
            sa.Column("cited_by_count", sa.Integer(), nullable=False),
            sa.Column("fwci", sa.Float(), nullable=True),
            sa.Column("is_oa", sa.Boolean(), nullable=False),
            sa.Column("oa_status", sa.String(length=32), nullable=True),
            sa.Column("source_name", sa.Text(), nullable=True),
            sa.Column("source_issn_l", sa.String(length=32), nullable=True),
            sa.Column("primary_topic_field", sa.String(length=255), nullable=True),
            sa.Column("primary_topic_domain", sa.String(length=255), nullable=True),
            sa.Column("has_selected_affiliation", sa.Boolean(), nullable=False),
            sa.Column("has_chile_affiliation", sa.Boolean(), nullable=False),
            sa.Column("has_non_chile_affiliation", sa.Boolean(), nullable=False),
            sa.Column("has_international_collaboration", sa.Boolean(), nullable=False),
            sa.Column("author_count", sa.Integer(), nullable=False),
            sa.Column("institution_count", sa.Integer(), nullable=False),
            sa.Column("refreshed_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "ror_id",
                "openalex_cache_key",
                name="uq_openalex_institution_work_fact_scope_key",
            ),
        )

    fact_indexes = (
        ("ix_openalex_institution_work_fact_ror_id", ["ror_id"]),
        (
            "ix_openalex_institution_work_fact_openalex_cache_key",
            ["openalex_cache_key"],
        ),
        ("ix_openalex_fact_ror_year", ["ror_id", "publication_year"]),
        ("ix_openalex_fact_ror_type", ["ror_id", "document_type"]),
        ("ix_openalex_fact_ror_oa", ["ror_id", "oa_status"]),
        ("ix_openalex_fact_ror_language", ["ror_id", "language"]),
        ("ix_openalex_fact_ror_citations", ["ror_id", "cited_by_count"]),
    )
    for index_name, columns in fact_indexes:
        if not _index_exists("openalex_institution_work_fact", index_name):
            _create_index(
                index_name,
                "openalex_institution_work_fact",
                columns,
            )

    _seed_analytics_versions()


def downgrade():
    if _table_exists("openalex_institution_work_fact"):
        op.drop_table("openalex_institution_work_fact")
    if _table_exists("analytics_data_version"):
        op.drop_table("analytics_data_version")

    with op.batch_alter_table("funding_cache", schema=None) as batch_op:
        if _index_exists("funding_cache", "ix_funding_cache_ror_orcid_year"):
            batch_op.drop_index("ix_funding_cache_ror_orcid_year")
        if _index_exists("funding_cache", "ix_funding_cache_ror_year_type"):
            batch_op.drop_index("ix_funding_cache_ror_year_type")

    with op.batch_alter_table("work_cache", schema=None) as batch_op:
        if _index_exists("work_cache", "ix_work_cache_ror_orcid_year"):
            batch_op.drop_index("ix_work_cache_ror_orcid_year")
        if _index_exists("work_cache", "ix_work_cache_ror_year_type"):
            batch_op.drop_index("ix_work_cache_ror_year_type")
        if _index_exists("work_cache", "ix_work_cache_ror_type_doi_normalized"):
            batch_op.drop_index("ix_work_cache_ror_type_doi_normalized")
        if _column_exists("work_cache", "doi_normalized"):
            batch_op.drop_column("doi_normalized")
