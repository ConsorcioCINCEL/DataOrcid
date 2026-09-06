"""Prepare durable profile batches without changing the published institution."""

import uuid

from .. import db
from ..models import InstitutionSyncProfile, InstitutionSyncVersion, utc_now


def prepare_sync_version(ror_id, researchers, orcid_ids, fetch_profiles, batch_size):
    # The caller holds the institutional write lock, so unfinished generations
    # belong to an interrupted process and can no longer be promoted.
    abandoned = InstitutionSyncVersion.query.filter(
        InstitutionSyncVersion.ror_id == ror_id,
        InstitutionSyncVersion.status.in_(("preparing", "ready")),
    ).all()
    for previous in abandoned:
        previous.status = "failed"
        previous.error = "The previous attempt was interrupted before publication."
        InstitutionSyncProfile.query.filter_by(version_id=previous.id).delete(synchronize_session=False)
    version = InstitutionSyncVersion(
        id=str(uuid.uuid4()), ror_id=ror_id, researchers_json=researchers,
        status="preparing",
    )
    db.session.add(version)
    db.session.commit()
    version_id = version.id
    try:
        for start in range(0, len(orcid_ids), batch_size):
            batch = orcid_ids[start:start + batch_size]
            profiles = fetch_profiles(batch, max_workers=10)
            db.session.add_all([
                InstitutionSyncProfile(version_id=version_id, orcid=orcid, payload_json=profiles.get(orcid) or None)
                for orcid in batch
            ])
            db.session.commit()
        version.status = "ready"
        db.session.commit()
    except Exception as exc:
        fail_sync_version(version_id, exc)
        raise
    return version_id


def fail_sync_version(version_id, error):
    """Roll back publication while retaining an inspectable attempt summary."""
    db.session.rollback()
    version = db.session.get(InstitutionSyncVersion, version_id)
    version.status = "failed"
    version.error = str(error)
    InstitutionSyncProfile.query.filter_by(version_id=version_id).delete(synchronize_session=False)
    db.session.commit()


def publish_sync_version(version_id, result):
    """Join the caller's transaction; no intermediate state becomes public."""
    version = db.session.get(InstitutionSyncVersion, version_id)
    version.status = "partial" if result.get("failed_profiles") else "published"
    version.result_json = result
    version.published_at = utc_now()
    InstitutionSyncProfile.query.filter_by(version_id=version_id).delete(synchronize_session=False)
    db.session.commit()
