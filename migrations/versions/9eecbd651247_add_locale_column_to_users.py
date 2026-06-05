"""Add locale column to users and initialize the base schema when needed

Revision ID: 9eecbd651247
Revises:
Create Date: 2026-02-04 21:18:54.442819

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = '9eecbd651247'
down_revision = None
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _column_exists(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def _index_exists(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return index_name in {index["name"] for index in inspector.get_indexes(table_name)}


def _create_index_if_missing(table_name: str, index_name: str, columns: list[str], unique: bool = False) -> None:
    if _table_exists(table_name) and not _index_exists(table_name, index_name):
        op.create_index(index_name, table_name, columns, unique=unique)


def _create_base_tables_if_missing() -> None:
    if not _table_exists("user"):
        op.create_table(
            "user",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("username", sa.String(length=80), nullable=False),
            sa.Column("email", sa.String(length=255), nullable=False, server_default=""),
            sa.Column("first_name", sa.String(length=100), nullable=True),
            sa.Column("last_name", sa.String(length=100), nullable=True),
            sa.Column("position", sa.String(length=150), nullable=True),
            sa.Column("password_hash", sa.String(length=200), nullable=False),
            sa.Column("is_admin", sa.Boolean(), nullable=True),
            sa.Column("is_manager", sa.Boolean(), nullable=True),
            sa.Column("institution_name", sa.String(length=120), nullable=True),
            sa.Column("ror_id", sa.String(length=20), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("username"),
        )

    if not _table_exists("work_cache"):
        op.create_table(
            "work_cache",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("orcid", sa.String(length=32), nullable=False),
            sa.Column("title", sa.Text(), nullable=True),
            sa.Column("type", sa.String(length=64), nullable=True),
            sa.Column("put_code", sa.Integer(), nullable=True),
            sa.Column("journal_title", sa.Text(), nullable=True),
            sa.Column("pub_year", sa.String(length=8), nullable=True),
            sa.Column("pub_month", sa.String(length=4), nullable=True),
            sa.Column("pub_day", sa.String(length=4), nullable=True),
            sa.Column("doi", sa.String(length=255), nullable=True),
            sa.Column("issn", sa.Text(), nullable=True),
            sa.Column("other_external_ids", sa.Text(), nullable=True),
            sa.Column("source", sa.Text(), nullable=True),
            sa.Column("url", sa.Text(), nullable=True),
            sa.Column("visibility", sa.String(length=32), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        _create_index_if_missing("work_cache", "ix_work_cache_ror_id", ["ror_id"])
        _create_index_if_missing("work_cache", "ix_work_cache_orcid", ["orcid"])

    if not _table_exists("work_cache_run"):
        op.create_table(
            "work_cache_run",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("status", sa.String(length=16), nullable=False),
            sa.Column("rows_count", sa.Integer(), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("started_at", sa.DateTime(), nullable=True),
            sa.Column("finished_at", sa.DateTime(), nullable=True),
            sa.Column("total_orcids", sa.Integer(), nullable=True),
            sa.Column("processed_orcids", sa.Integer(), nullable=True),
            sa.Column("processed_works", sa.Integer(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        _create_index_if_missing("work_cache_run", "ix_work_cache_run_ror_id", ["ror_id"])

    if not _table_exists("funding_cache"):
        op.create_table(
            "funding_cache",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("orcid", sa.String(length=32), nullable=False),
            sa.Column("title", sa.Text(), nullable=True),
            sa.Column("type", sa.String(length=64), nullable=True),
            sa.Column("org_name", sa.Text(), nullable=True),
            sa.Column("city", sa.Text(), nullable=True),
            sa.Column("country", sa.Text(), nullable=True),
            sa.Column("start_y", sa.String(length=8), nullable=True),
            sa.Column("start_m", sa.String(length=4), nullable=True),
            sa.Column("start_d", sa.String(length=4), nullable=True),
            sa.Column("end_y", sa.String(length=8), nullable=True),
            sa.Column("end_m", sa.String(length=4), nullable=True),
            sa.Column("end_d", sa.String(length=4), nullable=True),
            sa.Column("grant_number", sa.Text(), nullable=True),
            sa.Column("currency", sa.String(length=8), nullable=True),
            sa.Column("amount", sa.String(length=64), nullable=True),
            sa.Column("source", sa.Text(), nullable=True),
            sa.Column("visibility", sa.String(length=32), nullable=True),
            sa.Column("url", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        _create_index_if_missing("funding_cache", "ix_funding_cache_ror_id", ["ror_id"])
        _create_index_if_missing("funding_cache", "ix_funding_cache_orcid", ["orcid"])

    if not _table_exists("funding_cache_run"):
        op.create_table(
            "funding_cache_run",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=32), nullable=False),
            sa.Column("status", sa.String(length=16), nullable=False),
            sa.Column("rows_count", sa.Integer(), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("started_at", sa.DateTime(), nullable=True),
            sa.Column("finished_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        _create_index_if_missing("funding_cache_run", "ix_funding_cache_run_ror_id", ["ror_id"])

    if not _table_exists("researcher_status"):
        op.create_table(
            "researcher_status",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("ror_id", sa.String(length=20), nullable=False),
            sa.Column("orcid", sa.String(length=20), nullable=False),
            sa.Column("is_managed_by_am", sa.Boolean(), nullable=True),
            sa.Column("last_updated", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("ror_id", "orcid", name="_ror_orcid_uc"),
        )
        _create_index_if_missing("researcher_status", "ix_researcher_status_ror_id", ["ror_id"])
        _create_index_if_missing("researcher_status", "ix_researcher_status_orcid", ["orcid"])

    if not _table_exists("orcid_cache"):
        op.create_table(
            "orcid_cache",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("year", sa.Integer(), nullable=False),
            sa.Column("data", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )

    if not _table_exists("tracking_logs"):
        op.create_table(
            "tracking_logs",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), nullable=True),
            sa.Column("username", sa.String(length=80), nullable=True),
            sa.Column("method", sa.String(length=10), nullable=True),
            sa.Column("path", sa.String(length=300), nullable=True),
            sa.Column("status_code", sa.Integer(), nullable=True),
            sa.Column("ip", sa.String(length=50), nullable=True),
            sa.Column("user_agent", sa.String(length=255), nullable=True),
            sa.Column("duration_ms", sa.Float(), nullable=True),
            sa.Column("timestamp", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        _create_index_if_missing("tracking_logs", "ix_tracking_logs_user_id", ["user_id"])
        _create_index_if_missing("tracking_logs", "ix_tracking_logs_username", ["username"])


def upgrade():
    _create_base_tables_if_missing()

    if _table_exists("orcid_cache") and _column_exists("orcid_cache", "created_at"):
        with op.batch_alter_table('orcid_cache', schema=None) as batch_op:
            batch_op.alter_column('created_at',
                   existing_type=mysql.DATETIME(),
                   nullable=False)

    if _table_exists("user"):
        with op.batch_alter_table('user', schema=None) as batch_op:
            if not _column_exists("user", "locale"):
                batch_op.add_column(sa.Column('locale', sa.String(length=5), nullable=True))
            if _column_exists("user", "email"):
                batch_op.alter_column('email',
                       existing_type=mysql.VARCHAR(length=255),
                       nullable=True,
                       existing_server_default=sa.text("''"))
            if _column_exists("user", "first_name"):
                batch_op.alter_column('first_name',
                       existing_type=mysql.VARCHAR(length=100),
                       type_=sa.String(length=120),
                       existing_nullable=True)
            if _column_exists("user", "last_name"):
                batch_op.alter_column('last_name',
                       existing_type=mysql.VARCHAR(length=100),
                       type_=sa.String(length=120),
                       existing_nullable=True)
            if _column_exists("user", "position"):
                batch_op.alter_column('position',
                       existing_type=mysql.VARCHAR(length=150),
                       type_=sa.String(length=180),
                       existing_nullable=True)


def downgrade():
    if _table_exists("user"):
        with op.batch_alter_table('user', schema=None) as batch_op:
            if _column_exists("user", "position"):
                batch_op.alter_column('position',
                       existing_type=sa.String(length=180),
                       type_=mysql.VARCHAR(length=150),
                       existing_nullable=True)
            if _column_exists("user", "last_name"):
                batch_op.alter_column('last_name',
                       existing_type=sa.String(length=120),
                       type_=mysql.VARCHAR(length=100),
                       existing_nullable=True)
            if _column_exists("user", "first_name"):
                batch_op.alter_column('first_name',
                       existing_type=sa.String(length=120),
                       type_=mysql.VARCHAR(length=100),
                       existing_nullable=True)
            if _column_exists("user", "email"):
                batch_op.alter_column('email',
                       existing_type=mysql.VARCHAR(length=255),
                       nullable=False,
                       existing_server_default=sa.text("''"))
            if _column_exists("user", "locale"):
                batch_op.drop_column('locale')

    if _table_exists("orcid_cache") and _column_exists("orcid_cache", "created_at"):
        with op.batch_alter_table('orcid_cache', schema=None) as batch_op:
            batch_op.alter_column('created_at',
                   existing_type=mysql.DATETIME(),
                   nullable=True)
