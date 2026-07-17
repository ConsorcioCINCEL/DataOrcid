"""Institution registry helpers for ROR-backed institutional context."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from .. import db
from ..models import InstitutionIdentifier, InstitutionRegistry, User

logger = logging.getLogger(__name__)
DATASET_PATH = Path(__file__).resolve().parent.parent / "datasets" / "chilean_universities_ror.json"
ADDITIONAL_RINGGOLD_PATH = (
    Path(__file__).resolve().parent.parent
    / "datasets"
    / "additional_validated_ringgold_identifiers.json"
)
SUPPORTED_IDENTIFIER_SCHEMES = ("ror", "grid", "ringgold")


def seed_chilean_universities() -> int:
    """Upsert bundled Chilean universities and their verified identifiers."""
    if not DATASET_PATH.exists():
        logger.warning("Institution registry dataset not found: %s", DATASET_PATH)
        return 0

    payload = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
    try:
        return _seed_institution_payload(payload)
    except IntegrityError:
        # Another process may have seeded the registry during startup.
        db.session.rollback()
        return _seed_institution_payload(payload)


def _seed_institution_payload(payload: dict) -> int:
    ror_ids = sorted({
        _clean_ror_id(item.get("ror_id"))
        for item in payload.get("institutions", [])
        if _clean_ror_id(item.get("ror_id"))
    })
    existing = {
        row.ror_id: row
        for row in InstitutionRegistry.query.filter(InstitutionRegistry.ror_id.in_(ror_ids)).all()
    }

    validation = payload.get("ringgold_validation") or {}
    ringgold_verified_at = _parse_datetime(validation.get("validated_at"))
    count = 0

    for item in payload.get("institutions", []):
        ror_id = _clean_ror_id(item.get("ror_id"))
        if not ror_id:
            continue

        record = existing.get(ror_id)
        if not record:
            record = InstitutionRegistry(ror_id=ror_id)
            db.session.add(record)
            existing[ror_id] = record

        record.name = item.get("name") or ror_id
        record.display_name_en = item.get("display_name_en") or record.name
        record.grid_id = item.get("grid_id") or None
        record.country_code = item.get("country_code") or "CL"
        record.institution_type = item.get("institution_type") or "university"
        record.source = item.get("source") or "ror"
        record.is_active = True
        db.session.flush()

        _upsert_identifier(
            record,
            "ror",
            ror_id,
            source="ror",
            is_verified=True,
        )
        _upsert_identifier(
            record,
            "grid",
            item.get("grid_id"),
            source="ror",
            is_verified=True,
        )
        _upsert_identifier(
            record,
            "ringgold",
            item.get("ringgold_id"),
            source="orcid-public-api",
            is_verified=True,
            verified_at=ringgold_verified_at,
        )
        count += 1

    _seed_additional_ringgold_identifiers()
    db.session.commit()
    return count


def _seed_additional_ringgold_identifiers() -> None:
    """Attach verified Ringgold IDs to matching registry rows when they exist."""
    if not ADDITIONAL_RINGGOLD_PATH.exists():
        return

    payload = json.loads(ADDITIONAL_RINGGOLD_PATH.read_text(encoding="utf-8"))
    verified_at = _parse_datetime(payload.get("validated_at"))
    for item in payload.get("identifiers", []):
        ror_id = _clean_ror_id(item.get("ror_id"))
        record = InstitutionRegistry.query.filter_by(ror_id=ror_id).first()
        if not record:
            continue
        _upsert_identifier(
            record,
            "ringgold",
            item.get("ringgold_id"),
            source="orcid-public-api",
            is_verified=True,
            verified_at=verified_at,
        )


def get_institution_options() -> list[dict]:
    """Return selectable institutions from the registry and user-owned RORs."""
    options = {}
    registry_rows = InstitutionRegistry.query.filter_by(is_active=True).all()
    identifier_map = _identifier_map_for_institutions([row.id for row in registry_rows])

    for row in registry_rows:
        identifiers = identifier_map.get(row.id, {})
        grid_ids = _merge_values(identifiers.get("grid", []), [row.grid_id])
        ringgold_ids = identifiers.get("ringgold", [])
        options[row.ror_id] = {
            "institution_id": row.id,
            "ror_id": row.ror_id,
            "name": row.name or row.ror_id,
            "grid_id": grid_ids[0] if grid_ids else "",
            "grid_ids": grid_ids,
            "ringgold_id": ringgold_ids[0] if ringgold_ids else "",
            "ringgold_ids": ringgold_ids,
            "source": "registry",
        }

    user_rows = (
        db.session.query(User.ror_id, User.institution_name, User.grid_id)
        .filter(User.ror_id.isnot(None), User.ror_id != "")
        .distinct()
        .all()
    )
    for ror_id, institution_name, grid_id in user_rows:
        current = options.setdefault(
            ror_id,
            {
                "institution_id": None,
                "ror_id": ror_id,
                "name": institution_name or ror_id,
                "grid_id": grid_id or "",
                "grid_ids": [grid_id] if grid_id else [],
                "ringgold_id": "",
                "ringgold_ids": [],
                "source": "users",
            },
        )
        # Keep the canonical registry name when the ROR already exists there.
        # User-owned names are only authoritative for institutions that are not
        # yet represented in the registry.
        if institution_name and current.get("source") == "users":
            current["name"] = institution_name
        if grid_id:
            current["grid_ids"] = _merge_values(current.get("grid_ids", []), [grid_id])
            current["grid_id"] = current["grid_ids"][0]

    return sorted(options.values(), key=lambda item: item["name"].lower())


def get_institution_by_ror(ror_id: str) -> dict | None:
    """Resolve an institution and all active identifiers from its canonical ROR ID."""
    clean_ror = _clean_ror_id(ror_id)
    if not clean_ror:
        return None

    registry_row = InstitutionRegistry.query.filter_by(ror_id=clean_ror, is_active=True).first()
    if registry_row:
        identifiers = get_institution_identifiers(clean_ror)
        return {
            "institution_id": registry_row.id,
            "ror_id": registry_row.ror_id,
            "name": registry_row.name,
            "grid_id": _first(identifiers["grid"]),
            "grid_ids": identifiers["grid"],
            "ringgold_id": _first(identifiers["ringgold"]),
            "ringgold_ids": identifiers["ringgold"],
            "source": "registry",
        }

    user_row = (
        User.query.filter(User.ror_id == clean_ror)
        .filter(User.institution_name != "")
        .first()
    )
    if user_row:
        grid_ids = [user_row.grid_id] if user_row.grid_id else []
        return {
            "institution_id": None,
            "ror_id": user_row.ror_id,
            "name": user_row.institution_name or user_row.ror_id,
            "grid_id": _first(grid_ids),
            "grid_ids": grid_ids,
            "ringgold_id": "",
            "ringgold_ids": [],
            "source": "users",
        }

    return None


def get_institution_identifiers(ror_id: str) -> dict[str, list[str]]:
    """Return active ROR, GRID, and Ringgold values for one institution."""
    clean_ror = _clean_ror_id(ror_id)
    identifiers = {scheme: [] for scheme in SUPPORTED_IDENTIFIER_SCHEMES}
    if not clean_ror:
        return identifiers

    identifiers["ror"] = [clean_ror]
    record = InstitutionRegistry.query.filter_by(ror_id=clean_ror, is_active=True).first()
    if record:
        rows = (
            InstitutionIdentifier.query.filter_by(institution_id=record.id, is_active=True)
            .order_by(InstitutionIdentifier.scheme, InstitutionIdentifier.id)
            .all()
        )
        for row in rows:
            if row.scheme in identifiers and row.value not in identifiers[row.scheme]:
                identifiers[row.scheme].append(row.value)
        user_grid_ids = [
            grid_id
            for (grid_id,) in db.session.query(User.grid_id)
            .filter(
                User.ror_id == clean_ror,
                User.grid_id.isnot(None),
                User.grid_id != "",
            )
            .distinct()
            .all()
        ]
        identifiers["grid"] = _merge_values(
            identifiers["grid"],
            [record.grid_id] + user_grid_ids,
        )
        return identifiers

    user_grid_ids = [
        grid_id
        for (grid_id,) in db.session.query(User.grid_id)
        .filter(User.ror_id == clean_ror, User.grid_id.isnot(None), User.grid_id != "")
        .distinct()
        .all()
    ]
    identifiers["grid"] = _merge_values([], user_grid_ids)
    return identifiers


def ensure_institution_registry(ror_id: str, name: str | None = None) -> InstitutionRegistry:
    """Return a registry row, creating one for a valid user-owned ROR when needed."""
    clean_ror = _clean_ror_id(ror_id)
    if not clean_ror:
        raise ValueError("A valid ROR ID is required.")

    record = InstitutionRegistry.query.filter_by(ror_id=clean_ror).first()
    if record:
        return record

    user = User.query.filter_by(ror_id=clean_ror).first()
    record = InstitutionRegistry(
        ror_id=clean_ror,
        name=name or (user.institution_name if user else None) or clean_ror,
        display_name_en=name or (user.institution_name if user else None) or clean_ror,
        grid_id=user.grid_id if user else None,
        country_code="CL",
        institution_type="university",
        source="users" if user else "manual",
        is_active=True,
    )
    db.session.add(record)
    db.session.flush()
    _upsert_identifier(record, "ror", clean_ror, source=record.source, is_verified=True)
    if record.grid_id:
        _upsert_identifier(record, "grid", record.grid_id, source=record.source, is_verified=True)
    return record


def upsert_institution_identifier(
    ror_id: str,
    scheme: str,
    value: str | None,
    *,
    source: str,
    is_verified: bool = False,
    verified_at: datetime | None = None,
) -> InstitutionIdentifier | None:
    """Persist an external identifier under the institution represented by a ROR ID."""
    record = ensure_institution_registry(ror_id)
    return _upsert_identifier(
        record,
        scheme,
        value,
        source=source,
        is_verified=is_verified,
        verified_at=verified_at,
    )


def _upsert_identifier(
    institution: InstitutionRegistry,
    scheme: str,
    value: str | None,
    *,
    source: str,
    is_verified: bool,
    verified_at: datetime | None = None,
) -> InstitutionIdentifier | None:
    clean_scheme = (scheme or "").strip().lower()
    clean_value = (value or "").strip()
    if clean_scheme not in SUPPORTED_IDENTIFIER_SCHEMES or not clean_value:
        return None

    identifier = InstitutionIdentifier.query.filter_by(
        scheme=clean_scheme,
        value=clean_value,
    ).first()
    if identifier and identifier.institution_id != institution.id:
        logger.error(
            "Identifier %s:%s is already assigned to institution ID %s; refusing reassignment to %s.",
            clean_scheme,
            clean_value,
            identifier.institution_id,
            institution.id,
        )
        return None

    if not identifier:
        identifier = InstitutionIdentifier(
            institution_id=institution.id,
            scheme=clean_scheme,
            value=clean_value,
        )
        db.session.add(identifier)

    identifier.source = source or identifier.source or "manual"
    identifier.is_verified = bool(is_verified or identifier.is_verified)
    identifier.is_active = True
    if verified_at:
        identifier.verified_at = verified_at
    elif identifier.is_verified and not identifier.verified_at:
        identifier.verified_at = _utc_now()
    return identifier


def _identifier_map_for_institutions(institution_ids: list[int]) -> dict[int, dict[str, list[str]]]:
    result = {}
    if not institution_ids:
        return result

    rows = (
        InstitutionIdentifier.query.filter(
            InstitutionIdentifier.institution_id.in_(institution_ids),
            InstitutionIdentifier.is_active.is_(True),
        )
        .order_by(InstitutionIdentifier.institution_id, InstitutionIdentifier.scheme, InstitutionIdentifier.id)
        .all()
    )
    for row in rows:
        schemes = result.setdefault(row.institution_id, {})
        values = schemes.setdefault(row.scheme, [])
        if row.value not in values:
            values.append(row.value)
    return result


def _merge_values(current: list[str], additions: list[str | None]) -> list[str]:
    values = list(current)
    for value in additions:
        clean_value = (value or "").strip()
        if clean_value and clean_value not in values:
            values.append(clean_value)
    return values


def _first(values: list[str]) -> str:
    return values[0] if values else ""


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        logger.warning("Invalid dataset verification date: %s", value)
        return None


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for database columns without time zones."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _clean_ror_id(value: str | None) -> str:
    return (value or "").strip().rstrip("/").split("/")[-1].lower()
