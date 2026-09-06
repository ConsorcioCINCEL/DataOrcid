"""Build and query a canonical scholarly-output layer above ORCID records."""

from __future__ import annotations

import hashlib
import unicodedata
import uuid

from sqlalchemy import func, text

from .. import db
from ..models import CanonicalWork, CanonicalWorkOverride, OaiPmhWorkSelection, WorkCache, WorkRecordLink, utc_now
from .doi_service import normalize_doi
from .institution_lock import institutional_writer


def normalize_title(value: str | None) -> str:
    """Return a conservative title key for records without DOI."""
    text = unicodedata.normalize("NFKD", (value or "").strip().lower())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join("".join(char if char.isalnum() else " " for char in text).split())


def canonical_key_for_work(doi: str | None, title: str | None, year) -> tuple[str, str | None, str, int | None]:
    """Return canonical key, normalized DOI/title, and parsed year."""
    normalized_doi = normalize_doi(doi)
    normalized_title = normalize_title(title)
    publication_year = _safe_year(year)
    if normalized_doi:
        digest = hashlib.sha256(normalized_doi.encode("utf-8")).hexdigest()
        return f"doi:{digest}", normalized_doi, normalized_title, publication_year

    fallback_value = f"{normalized_title}|{publication_year or ''}"
    if not normalized_title:
        fallback_value = f"untitled|{publication_year or ''}"
    digest = hashlib.sha256(fallback_value.encode("utf-8")).hexdigest()
    return f"title:{digest}", None, normalized_title, publication_year


def source_record_key(row: WorkCache) -> str:
    """Preserve ORCID put-code identities; unidentifiable legacy rows stay separate."""
    if row.put_code is not None:
        return f"put:{row.put_code}"
    return f"cache:{row.id}"


@institutional_writer
def rebuild_canonical_works(ror_id: str | None = None, *, commit: bool = True) -> dict:
    """Rebuild canonical links for one institution or for the whole cache."""
    if not ror_id:
        # Keep memory bounded on large installations. Each institutional slice is
        # committed independently and canonical DOI keys are still shared.
        ror_ids = [
            value
            for (value,) in db.session.query(WorkCache.ror_id)
            .distinct()
            .order_by(WorkCache.ror_id.asc())
            .all()
            if value
        ]
        for institutional_ror in ror_ids:
            rebuild_canonical_works(institutional_ror, commit=commit)
        return canonical_work_counts()

    if db.engine.dialect.name == "postgresql":
        db.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('dataorcid:canonical', 2))"))
    query = WorkCache.query
    query = query.filter_by(ror_id=ror_id)
    rows = query.order_by(WorkCache.id.asc()).all()
    previous_ids = {
        (link.orcid, link.source_record_key): link.canonical_work_id
        for link in WorkRecordLink.query.filter_by(ror_id=ror_id).all()
    }
    selections = {item.canonical_work_id: item for item in OaiPmhWorkSelection.query.filter_by(ror_id=ror_id).all()}
    overrides = {
        (item.orcid, item.source_record_key): item.canonical_key
        for item in CanonicalWorkOverride.query.filter_by(ror_id=ror_id).all()
    }

    WorkRecordLink.query.filter_by(ror_id=ror_id).delete(synchronize_session=False)
    db.session.flush()

    descriptors: dict[str, dict] = {}
    row_keys: list[tuple[WorkCache, str]] = []
    for row in rows:
        canonical_key, doi_normalized, title_normalized, publication_year = canonical_key_for_work(
            row.doi,
            row.title,
            row.pub_year,
        )
        override = overrides.get((row.orcid, source_record_key(row)))
        if override:
            canonical_key = override
        elif not doi_normalized:
            # Title/year is candidate evidence, never a definitive identifier.
            # ORCID summaries do not contain a complete author list, so preserve
            # distinct sources until a reviewer confirms their identity.
            digest = hashlib.sha256(
                f"{row.ror_id}|{row.orcid}|{source_record_key(row)}".encode("utf-8")
            ).hexdigest()
            canonical_key = f"record:{digest}"
        descriptors.setdefault(
            canonical_key,
            {
                "doi_normalized": doi_normalized,
                "title": row.title,
                "title_normalized": title_normalized,
                "publication_year": publication_year,
            },
        )
        row_keys.append((row, canonical_key))

    works_by_key: dict[str, CanonicalWork] = {}
    keys = sorted(descriptors)
    for chunk in _chunks(keys, 500):
        works_by_key.update({
            work.canonical_key: work
            for work in CanonicalWork.query.filter(CanonicalWork.canonical_key.in_(chunk)).all()
        })

    now = utc_now()
    for canonical_key, descriptor in descriptors.items():
        work = works_by_key.get(canonical_key)
        if not work:
            work = CanonicalWork(canonical_key=canonical_key, created_at=now)
            db.session.add(work)
            works_by_key[canonical_key] = work
        # Retain an established representative across institutions. Enrich empty
        # fields without letting the last institutional refresh win conflicts.
        for field, value in descriptor.items():
            if (not descriptor["doi_normalized"] or not getattr(work, field)) and value:
                setattr(work, field, value)
    db.session.flush()

    link_buffer = []
    for row, canonical_key in row_keys:
        link_buffer.append(
            WorkRecordLink(
                canonical_work_id=works_by_key[canonical_key].id,
                work_cache_id=row.id,
                ror_id=row.ror_id,
                orcid=row.orcid,
                source_record_key=source_record_key(row),
                created_at=now,
            )
        )
        if len(link_buffer) >= 2000:
            db.session.bulk_save_objects(link_buffer)
            link_buffer.clear()
    if link_buffer:
        db.session.bulk_save_objects(link_buffer)
    db.session.flush()

    # A regrouping changes canonical membership, never the user's publication
    # decision. Where merged sources disagree, preserve the explicit exclusion.
    inherited = {}
    for row, key in row_keys:
        previous_id = previous_ids.get((row.orcid, source_record_key(row)))
        selection = selections.get(previous_id)
        new_id = works_by_key[key].id
        if selection and new_id not in selections:
            current = inherited.get(new_id)
            if current is None or (current.is_included and not selection.is_included):
                inherited[new_id] = selection
    for new_id, selection in inherited.items():
        db.session.add(OaiPmhWorkSelection(
            canonical_work_id=new_id, ror_id=ror_id, is_included=selection.is_included,
            decision_source=selection.decision_source, doi_import_batch_id=selection.doi_import_batch_id,
            updated_by_user_id=selection.updated_by_user_id, created_at=selection.created_at,
            updated_at=selection.updated_at,
        ))

    affected_ids = [work.id for work in works_by_key.values()]
    for chunk in _chunks(affected_ids, 500):
        counts = dict(
            db.session.query(
                WorkRecordLink.canonical_work_id,
                func.count(WorkRecordLink.id),
            )
            .filter(WorkRecordLink.canonical_work_id.in_(chunk))
            .group_by(WorkRecordLink.canonical_work_id)
            .all()
        )
        for work in CanonicalWork.query.filter(CanonicalWork.id.in_(chunk)).all():
            work.record_count = int(counts.get(work.id, 0))

    CanonicalWork.query.filter(
        ~db.session.query(WorkRecordLink.id)
        .filter(WorkRecordLink.canonical_work_id == CanonicalWork.id)
        .exists()
    ).update({CanonicalWork.record_count: 0}, synchronize_session=False)
    db.session.flush()
    if commit:
        from .oai_publication_service import refresh_oai_publication
        refresh_oai_publication(ror_id)
        db.session.commit()

    return canonical_work_counts(ror_id)


def canonical_review_groups(ror_id: str, page: int = 1, limit: int = 20) -> list[dict]:
    """Present title/year candidates with the evidence needed for human review."""
    groups = {}
    rows = WorkCache.query.filter_by(ror_id=ror_id, doi_normalized=None).order_by(WorkCache.id).yield_per(500)
    for row in rows:
        key, _, title, year = canonical_key_for_work(None, row.title, row.pub_year)
        if not title:
            continue
        group = groups.setdefault(key, {"title": row.title, "year": year, "records": []})
        group["records"].append({"id": row.id, "orcid": row.orcid, "journal": row.journal_title, "type": row.type})
    return [group for group in groups.values() if len(group["records"]) > 1][(page - 1) * limit:page * limit]


@institutional_writer
def review_canonical_records(ror_id: str, record_ids: list[int], *, merge: bool, reason: str, user_id: int) -> None:
    """Confirm a grouping or split selected sources, retaining the decision."""
    if not reason.strip() or not record_ids or len(record_ids) > 100:
        raise ValueError("Select records and provide a review reason.")
    rows = WorkCache.query.filter(WorkCache.ror_id == ror_id, WorkCache.id.in_(set(record_ids))).all()
    if len(rows) != len(set(record_ids)) or (merge and len(rows) < 2):
        raise ValueError("The selected records are not available in this institution.")
    if merge and len({normalize_doi(row.doi) for row in rows if normalize_doi(row.doi)}) > 1:
        raise ValueError("Records with different DOIs cannot be merged.")
    group_key = "record:" + hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    for row in rows:
        source_key = source_record_key(row)
        decision = CanonicalWorkOverride.query.filter_by(ror_id=ror_id, orcid=row.orcid, source_record_key=source_key).first()
        if not decision:
            decision = CanonicalWorkOverride(ror_id=ror_id, orcid=row.orcid, source_record_key=source_key)
            db.session.add(decision)
        decision.canonical_key = group_key if merge else "record:" + hashlib.sha256(f"{ror_id}|{row.orcid}|{source_key}".encode()).hexdigest()
        decision.reason = reason.strip()
        decision.updated_by_user_id = user_id
        decision.updated_at = utc_now()
    db.session.flush()
    rebuild_canonical_works(ror_id)


def canonical_work_counts(ror_id: str | None = None) -> dict:
    """Return source-record and unique-output counts for one scope."""
    link_query = db.session.query(WorkRecordLink)
    if ror_id:
        link_query = link_query.filter(WorkRecordLink.ror_id == ror_id)
    source_records = link_query.count()

    unique_query = db.session.query(
        func.count(func.distinct(WorkRecordLink.canonical_work_id))
    )
    if ror_id:
        unique_query = unique_query.filter(WorkRecordLink.ror_id == ror_id)
    unique_outputs = int(unique_query.scalar() or 0)

    doi_query = db.session.query(
        func.count(func.distinct(WorkRecordLink.canonical_work_id))
    ).join(CanonicalWork, CanonicalWork.id == WorkRecordLink.canonical_work_id).filter(
        CanonicalWork.doi_normalized.isnot(None)
    )
    if ror_id:
        doi_query = doi_query.filter(WorkRecordLink.ror_id == ror_id)
    doi_outputs = int(doi_query.scalar() or 0)

    return {
        "source_records": int(source_records),
        "unique_outputs": unique_outputs,
        "doi_outputs": doi_outputs,
        "fallback_outputs": max(unique_outputs - doi_outputs, 0),
        "excess_records": max(int(source_records) - unique_outputs, 0),
    }


def _safe_year(value) -> int | None:
    try:
        year = int(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return year if 1000 <= year <= 9999 else None


def _chunks(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]
