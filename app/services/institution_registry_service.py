"""Institution registry helpers for ROR-backed institutional context."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from .. import db
from ..models import InstitutionRegistry, User

logger = logging.getLogger(__name__)
DATASET_PATH = Path(__file__).resolve().parent.parent / "datasets" / "chilean_universities_ror.json"


def seed_chilean_universities() -> int:
    """Upsert the bundled Chilean university ROR records."""
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
        count += 1

    db.session.commit()
    return count


def get_institution_options() -> list[dict]:
    """Return selectable institutions from the registry and user-owned RORs."""
    options = {}

    registry_rows = InstitutionRegistry.query.filter_by(is_active=True).all()
    for row in registry_rows:
        options[row.ror_id] = {
            "ror_id": row.ror_id,
            "name": row.name or row.ror_id,
            "grid_id": row.grid_id or "",
            "source": "registry",
        }

    user_rows = (
        db.session.query(User.ror_id, User.institution_name, User.grid_id)
        .filter(User.ror_id.isnot(None), User.ror_id != "")
        .distinct()
        .all()
    )
    for ror_id, institution_name, grid_id in user_rows:
        current = options.setdefault(ror_id, {
            "ror_id": ror_id,
            "name": institution_name or ror_id,
            "grid_id": grid_id or "",
            "source": "users",
        })
        if institution_name:
            current["name"] = institution_name
        if grid_id:
            current["grid_id"] = grid_id
        current["source"] = "users" if current["source"] == "registry" else current["source"]

    return sorted(options.values(), key=lambda item: item["name"].lower())


def get_institution_by_ror(ror_id: str) -> dict | None:
    """Resolve an institution from the registry or user-owned institution data."""
    clean_ror = _clean_ror_id(ror_id)
    if not clean_ror:
        return None

    registry_row = InstitutionRegistry.query.filter_by(ror_id=clean_ror, is_active=True).first()
    if registry_row:
        return {
            "ror_id": registry_row.ror_id,
            "name": registry_row.name,
            "grid_id": registry_row.grid_id or "",
            "source": "registry",
        }

    user_row = (
        User.query
        .filter(User.ror_id == clean_ror)
        .filter(User.institution_name != "")
        .first()
    )
    if user_row:
        return {
            "ror_id": user_row.ror_id,
            "name": user_row.institution_name or user_row.ror_id,
            "grid_id": user_row.grid_id or "",
            "source": "users",
        }

    return None


def _clean_ror_id(value: str | None) -> str:
    return (value or "").strip().rstrip("/").split("/")[-1].lower()
