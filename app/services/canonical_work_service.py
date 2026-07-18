"""Build and query a canonical scholarly-output layer above ORCID records."""

from __future__ import annotations

import hashlib
import re
import unicodedata

from sqlalchemy import func

from .. import db
from ..models import CanonicalWork, WorkCache, WorkRecordLink, utc_now
from .doi_service import normalize_doi


def normalize_title(value: str | None) -> str:
    """Return a conservative title key for records without DOI."""
    text = unicodedata.normalize("NFKD", (value or "").strip().lower())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


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


def rebuild_canonical_works(ror_id: str | None = None) -> dict:
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
            rebuild_canonical_works(institutional_ror)
        return canonical_work_counts()

    query = WorkCache.query
    query = query.filter_by(ror_id=ror_id)
    rows = query.order_by(WorkCache.id.asc()).all()

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
        if not doi_normalized and not title_normalized:
            # Records without a DOI or title have no defensible matching key.
            # Keep them separate instead of creating a false canonical cluster.
            record_identity = row.put_code if row.put_code is not None else row.id
            digest = hashlib.sha256(
                f"{row.ror_id}|{row.orcid}|{record_identity}".encode("utf-8")
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
        work.doi_normalized = descriptor["doi_normalized"]
        work.title = descriptor["title"] or work.title
        work.title_normalized = descriptor["title_normalized"]
        work.publication_year = descriptor["publication_year"]
        work.updated_at = now
    db.session.flush()

    link_buffer = []
    for row, canonical_key in row_keys:
        source_record_key = f"put:{row.put_code}" if row.put_code is not None else f"cache:{row.id}"
        link_buffer.append(
            WorkRecordLink(
                canonical_work_id=works_by_key[canonical_key].id,
                work_cache_id=row.id,
                ror_id=row.ror_id,
                orcid=row.orcid,
                source_record_key=source_record_key,
                created_at=now,
            )
        )
        if len(link_buffer) >= 2000:
            db.session.bulk_save_objects(link_buffer)
            link_buffer.clear()
    if link_buffer:
        db.session.bulk_save_objects(link_buffer)
    db.session.flush()

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
    ).delete(synchronize_session=False)
    db.session.commit()

    return canonical_work_counts(ror_id)


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
