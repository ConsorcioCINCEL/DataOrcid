"""Add institution identifiers and researcher associations

Revision ID: 6f8d2a41c9b7
Revises: 3c2b6d9a1f40
Create Date: 2026-07-14 12:00:00.000000

"""
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa


revision = "6f8d2a41c9b7"
down_revision = "3c2b6d9a1f40"
branch_labels = None
depends_on = None


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for database columns without time zones."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _table_exists(table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names()


def _backfill_user_institutions() -> None:
    if not _table_exists("user") or not _table_exists("institution_registry"):
        return

    bind = op.get_bind()
    metadata = sa.MetaData()
    users = sa.Table("user", metadata, autoload_with=bind)
    institutions = sa.Table("institution_registry", metadata, autoload_with=bind)

    existing_rors = {
        value
        for value in bind.execute(sa.select(institutions.c.ror_id)).scalars()
        if value
    }
    rows = bind.execute(
        sa.select(users.c.ror_id, users.c.institution_name).where(
            users.c.ror_id.is_not(None),
            users.c.ror_id != "",
        )
    ).all()

    now = _utc_now()
    for ror_id, institution_name in rows:
        if ror_id in existing_rors:
            continue
        bind.execute(
            institutions.insert().values(
                ror_id=ror_id,
                name=institution_name or ror_id,
                display_name_en=institution_name or ror_id,
                grid_id=None,
                country_code="CL",
                institution_type="university",
                source="users",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
        )
        existing_rors.add(ror_id)


def _backfill_identifiers() -> None:
    if not _table_exists("institution_identifier"):
        return

    bind = op.get_bind()
    metadata = sa.MetaData()
    institutions = sa.Table("institution_registry", metadata, autoload_with=bind)
    identifiers = sa.Table("institution_identifier", metadata, autoload_with=bind)
    users = sa.Table("user", metadata, autoload_with=bind) if _table_exists("user") else None

    existing = {
        (scheme, value)
        for scheme, value in bind.execute(
            sa.select(identifiers.c.scheme, identifiers.c.value)
        ).all()
    }
    now = _utc_now()

    institution_rows = bind.execute(
        sa.select(institutions.c.id, institutions.c.ror_id, institutions.c.grid_id)
    ).all()
    institution_by_ror = {ror_id: institution_id for institution_id, ror_id, _ in institution_rows}

    candidates = []
    for institution_id, ror_id, grid_id in institution_rows:
        candidates.append((institution_id, "ror", ror_id, "ror"))
        if grid_id:
            candidates.append((institution_id, "grid", grid_id, "ror"))

    if users is not None and "grid_id" in users.c:
        user_rows = bind.execute(
            sa.select(users.c.ror_id, users.c.grid_id).where(
                users.c.ror_id.is_not(None),
                users.c.ror_id != "",
                users.c.grid_id.is_not(None),
                users.c.grid_id != "",
            )
        ).all()
        for ror_id, grid_id in user_rows:
            institution_id = institution_by_ror.get(ror_id)
            if institution_id:
                candidates.append((institution_id, "grid", grid_id, "users"))

    for institution_id, scheme, value, source in candidates:
        key = (scheme, value)
        if not value or key in existing:
            continue
        bind.execute(
            identifiers.insert().values(
                institution_id=institution_id,
                scheme=scheme,
                value=value,
                source=source,
                is_verified=True,
                is_active=True,
                verified_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        existing.add(key)


def upgrade():
    if not _table_exists("institution_identifier"):
        op.create_table(
            "institution_identifier",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("institution_id", sa.Integer(), nullable=False),
            sa.Column("scheme", sa.String(length=16), nullable=False),
            sa.Column("value", sa.String(length=255), nullable=False),
            sa.Column("source", sa.String(length=64), nullable=False),
            sa.Column("is_verified", sa.Boolean(), nullable=False),
            sa.Column("is_active", sa.Boolean(), nullable=False),
            sa.Column("verified_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(
                ["institution_id"],
                ["institution_registry.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "scheme",
                "value",
                name="uq_institution_identifier_scheme_value",
            ),
        )
        with op.batch_alter_table("institution_identifier", schema=None) as batch_op:
            batch_op.create_index(
                batch_op.f("ix_institution_identifier_institution_id"),
                ["institution_id"],
                unique=False,
            )
            batch_op.create_index(
                batch_op.f("ix_institution_identifier_scheme"),
                ["scheme"],
                unique=False,
            )

    if not _table_exists("institution_researcher"):
        op.create_table(
            "institution_researcher",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("institution_id", sa.Integer(), nullable=False),
            sa.Column("orcid", sa.String(length=32), nullable=False),
            sa.Column("matched_by_ror", sa.Boolean(), nullable=False),
            sa.Column("matched_by_grid", sa.Boolean(), nullable=False),
            sa.Column("matched_by_ringgold", sa.Boolean(), nullable=False),
            sa.Column("is_active", sa.Boolean(), nullable=False),
            sa.Column("profile_status", sa.String(length=16), nullable=False),
            sa.Column("profile_error", sa.Text(), nullable=True),
            sa.Column("first_seen_at", sa.DateTime(), nullable=False),
            sa.Column("last_seen_at", sa.DateTime(), nullable=False),
            sa.Column("profile_updated_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(
                ["institution_id"],
                ["institution_registry.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "institution_id",
                "orcid",
                name="uq_institution_researcher",
            ),
        )
        with op.batch_alter_table("institution_researcher", schema=None) as batch_op:
            batch_op.create_index(
                batch_op.f("ix_institution_researcher_institution_id"),
                ["institution_id"],
                unique=False,
            )
            batch_op.create_index(
                batch_op.f("ix_institution_researcher_is_active"),
                ["is_active"],
                unique=False,
            )
            batch_op.create_index(
                batch_op.f("ix_institution_researcher_orcid"),
                ["orcid"],
                unique=False,
            )

    _backfill_user_institutions()
    _backfill_identifiers()


def downgrade():
    if _table_exists("institution_researcher"):
        with op.batch_alter_table("institution_researcher", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_institution_researcher_orcid"))
            batch_op.drop_index(batch_op.f("ix_institution_researcher_is_active"))
            batch_op.drop_index(batch_op.f("ix_institution_researcher_institution_id"))
        op.drop_table("institution_researcher")

    if _table_exists("institution_identifier"):
        with op.batch_alter_table("institution_identifier", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("ix_institution_identifier_scheme"))
            batch_op.drop_index(batch_op.f("ix_institution_identifier_institution_id"))
        op.drop_table("institution_identifier")
