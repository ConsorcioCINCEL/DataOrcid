"""Publish immutable OAI records and retain deletions for incremental harvests."""

from dataclasses import asdict
import hashlib
from xml.etree import ElementTree as ET

from sqlalchemy import func, text

from .. import db
from ..models import OaiPmhInstitutionConfig, OaiPmhRecordVersion, utc_now
from .institution_lock import institutional_writer


def lock_oai_publication(ror_id, *, shared=False):
    """Keep harvest timestamps ordered with publication commits, not API fetches."""
    if db.engine.dialect.name == "postgresql":
        suffix = "_shared" if shared else ""
        db.session.execute(
            text(f"SELECT pg_advisory_xact_lock{suffix}(hashtextextended(:scope, 3))"),
            {"scope": f"dataorcid:oai:{ror_id}"},
        )


def published_record_query(ror_id, revision=None):
    """Read the latest event for each identifier at a fixed publication revision."""
    latest = db.session.query(func.max(OaiPmhRecordVersion.id).label("id")).filter_by(ror_id=ror_id)
    if revision is not None:
        latest = latest.filter(OaiPmhRecordVersion.id <= revision)
    latest = latest.group_by(OaiPmhRecordVersion.canonical_key).subquery()
    return OaiPmhRecordVersion.query.join(latest, latest.c.id == OaiPmhRecordVersion.id)


@institutional_writer
def refresh_oai_publication(ror_id):
    """Append only actual content changes inside the caller's publication commit."""
    from .oai_pmh_service import (institutional_work_query, work_from_query_row,
                                  _append_oai_dc_metadata, _append_openaire_metadata, _append_mapped_metadata)

    db.session.flush()
    lock_oai_publication(ror_id)
    config = OaiPmhInstitutionConfig.query.filter_by(ror_id=ror_id).first()
    if not config:
        return
    previous = {
        row.canonical_key: row
        for row in published_record_query(ror_id).with_entities(
            OaiPmhRecordVersion.id, OaiPmhRecordVersion.canonical_key,
            OaiPmhRecordVersion.is_deleted, OaiPmhRecordVersion.content_hash,
        ).all()
    }
    query, included, _ = institutional_work_query(config)
    now = utc_now()
    seen = set()
    events = []
    event_ids = []

    def flush_events():
        if events:
            db.session.add_all(events)
            db.session.flush()
            event_ids.extend(event.id for event in events)
            events.clear()
    for row in query.filter(included.is_(True)).yield_per(500):
        record = work_from_query_row(row, config)
        payload = asdict(record)
        # These fields describe processing/review state, not published metadata.
        for key in ("datestamp", "openalex_validation_at", "public_key", "event_id", "is_deleted"):
            payload.pop(key, None)
        metadata = ET.Element("metadata")
        for renderer in (_append_oai_dc_metadata, _append_openaire_metadata, _append_mapped_metadata):
            renderer(metadata, record)
        digest = hashlib.sha256(ET.tostring(metadata, encoding="utf-8")).hexdigest()
        old = previous.get(record.canonical_key)
        seen.add(record.canonical_key)
        if old and not old.is_deleted and old.content_hash == digest:
            continue
        events.append(OaiPmhRecordVersion(
            ror_id=ror_id, canonical_key=record.canonical_key,
            datestamp=now, is_deleted=False, content_hash=digest, payload_json=payload,
        ))
        if len(events) >= 500:
            flush_events()
    for key, old in previous.items():
        if key not in seen and not old.is_deleted:
            events.append(OaiPmhRecordVersion(
                ror_id=ror_id, canonical_key=key, datestamp=now, is_deleted=True,
                content_hash=old.content_hash,
                payload_json=db.session.get(OaiPmhRecordVersion, old.id).payload_json,
            ))
            if len(events) >= 500:
                flush_events()
    flush_events()
    published_at = utc_now()
    for start in range(0, len(event_ids), 500):
        OaiPmhRecordVersion.query.filter(OaiPmhRecordVersion.id.in_(event_ids[start:start + 500])).update(
            {OaiPmhRecordVersion.datestamp: published_at}, synchronize_session=False,
        )
    config.publication_initialized_at = published_at
    db.session.flush()


def ensure_oai_publication(config):
    """Bootstrap existing repositories once; normal harvests perform only reads."""
    if config.publication_initialized_at is None:
        from .institution_lock import institution_write_lock
        with institution_write_lock(config.ror_id):
            db.session.refresh(config)
            if config.publication_initialized_at is None:
                refresh_oai_publication(config.ror_id)
                db.session.commit()


def record_from_publication(event, config):
    from .oai_pmh_service import InstitutionalWork

    return InstitutionalWork(
        **event.payload_json, public_key=config.public_key, datestamp=event.datestamp,
        openalex_validation_at=None, is_deleted=event.is_deleted, event_id=event.id,
    )
