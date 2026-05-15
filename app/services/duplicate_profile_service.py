"""Duplicate ORCID profile detection based on cached institutional data."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import hashlib
import json
import re
import unicodedata
from typing import Iterable

from sqlalchemy import func

from .. import db
from ..models import (
    DuplicateProfileCache,
    FundingCache,
    InstitutionRegistry,
    ResearcherCache,
    ResearcherStatus,
    User,
    WorkCache,
)


def normalize_name(value: str | None) -> str:
    """Return a stable comparison key for researcher names."""
    text = (value or "").strip().lower()
    if not text:
        return ""

    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_duplicate_report(
    ror_ids: Iterable[str] | None = None,
    search: str | None = None,
    min_confidence: int = 0,
    force_refresh: bool = False,
) -> dict:
    """Build duplicate candidate groups and summary metrics."""
    allowed_rors = {ror for ror in (ror_ids or []) if ror}
    population = _load_researcher_population(allowed_rors or None)
    if not population:
        return _empty_report()

    scope_key = _scope_key(allowed_rors or None)
    dependency = _dependency_signature(allowed_rors or None, population)
    cached_report = _load_cached_report(scope_key, dependency["hash"], force_refresh)
    if cached_report:
        return _apply_report_filters(cached_report, search, min_confidence)

    report = _build_uncached_report(allowed_rors or None, population)
    _store_cached_report(scope_key, dependency, report)
    return _apply_report_filters(report, search, min_confidence)


def clear_duplicate_report_cache(ror_ids: Iterable[str] | None = None) -> int:
    """Clear cached duplicate reports for one scope or for every scope."""
    allowed_rors = {ror for ror in (ror_ids or []) if ror}
    query = DuplicateProfileCache.query
    if allowed_rors:
        query = query.filter_by(scope_key=_scope_key(allowed_rors))
    count = query.delete(synchronize_session=False)
    db.session.commit()
    return count


def _build_uncached_report(allowed_rors: set[str] | None, population: set[tuple[str, str]]) -> dict:
    all_orcids = sorted({orcid for _, orcid in population})
    institution_names = _load_institution_names()
    researcher_names = _load_researcher_names(all_orcids)
    work_stats = _load_work_stats(allowed_rors)
    funding_stats = _load_funding_stats(allowed_rors)
    statuses = _load_statuses(allowed_rors)
    dois = _load_dois(allowed_rors)

    grouped = defaultdict(list)
    for ror_id, orcid in sorted(population):
        name_data = researcher_names.get(orcid, {})
        display_name = _display_name(name_data, orcid)
        name_key = normalize_name(display_name)
        if not _is_candidate_name(name_key):
            continue

        key = (ror_id, name_key)
        works = work_stats.get((ror_id, orcid), {})
        fundings = funding_stats.get((ror_id, orcid), {})
        year_range = _merge_year_ranges(works.get("years", []), fundings.get("years", []))

        grouped[key].append({
            "orcid": orcid,
            "display_name": display_name,
            "given_names": name_data.get("given_names") or "",
            "family_name": name_data.get("family_name") or "",
            "credit_name": name_data.get("credit_name") or "",
            "works_count": works.get("count", 0),
            "fundings_count": fundings.get("count", 0),
            "year_range": year_range,
            "managed_by_am": statuses.get((ror_id, orcid), False),
            "orcid_url": f"https://orcid.org/{orcid}",
        })

    groups = []
    for index, ((ror_id, name_key), profiles) in enumerate(sorted(grouped.items()), start=1):
        if len(profiles) < 2:
            continue

        shared_dois = _shared_dois_for_group(ror_id, profiles, dois)
        confidence = _confidence_score(name_key, profiles, shared_dois)

        institution = institution_names.get(ror_id, ror_id)
        display_name = _best_group_name(profiles)

        evidence_keys = ["exact_name_match"]
        if shared_dois:
            evidence_keys.append("shared_doi")
        if any(profile["managed_by_am"] for profile in profiles):
            evidence_keys.append("managed_profile")

        groups.append({
            "group_id": f"DUP-{index:04d}",
            "ror_id": ror_id,
            "institution_name": institution,
            "normalized_name": name_key,
            "display_name": display_name,
            "orcid_count": len(profiles),
            "extra_profiles": len(profiles) - 1,
            "confidence": confidence,
            "confidence_level": _confidence_level(confidence),
            "evidence_keys": evidence_keys,
            "shared_dois": shared_dois[:5],
            "profiles": sorted(profiles, key=lambda item: item["orcid"]),
            "works_count": sum(profile["works_count"] for profile in profiles),
            "fundings_count": sum(profile["fundings_count"] for profile in profiles),
        })

    groups.sort(key=lambda item: (-item["confidence"], item["institution_name"], item["display_name"]))
    return _assemble_report(groups)


def _apply_report_filters(report: dict, search: str | None, min_confidence: int) -> dict:
    query = (search or "").strip().lower()
    query_key = normalize_name(query)
    groups = []
    for group in report.get("groups", []):
        if group["confidence"] < min_confidence:
            continue

        haystack = " ".join(
            [group["institution_name"], group["display_name"], group["normalized_name"]]
            + [profile["orcid"] for profile in group["profiles"]]
        ).lower()
        if query and query not in haystack and query_key not in haystack:
            continue

        groups.append(group)

    filtered = _assemble_report(groups)
    filtered["cache"] = report.get("cache", {})
    return filtered


def _assemble_report(groups: list[dict]) -> dict:
    institutions = _summarize_institutions(groups)
    profile_activity = _summarize_profile_activity(groups)
    return {
        "groups": groups,
        "institutions": institutions,
        "profile_activity": profile_activity,
        "cache": {},
        "summary": {
            "candidate_groups": len(groups),
            "duplicate_profiles": sum(group["orcid_count"] for group in groups),
            "extra_profiles": sum(group["extra_profiles"] for group in groups),
            "institutions": len(institutions),
            "candidate_works": sum(profile["works_count"] for profile in profile_activity),
            "candidate_fundings": sum(profile["fundings_count"] for profile in profile_activity),
        },
    }


def flatten_duplicate_rows(groups: list[dict]) -> list[dict]:
    """Flatten candidate groups into one export row per ORCID profile."""
    rows = []
    for group in groups:
        evidence = ", ".join(group["evidence_keys"])
        shared_dois = ", ".join(group["shared_dois"])
        for profile in group["profiles"]:
            rows.append({
                "group_id": group["group_id"],
                "institution_name": group["institution_name"],
                "ror_id": group["ror_id"],
                "candidate_name": group["display_name"],
                "normalized_name": group["normalized_name"],
                "confidence": group["confidence"],
                "confidence_level": group["confidence_level"],
                "orcid": profile["orcid"],
                "orcid_url": profile["orcid_url"],
                "profile_display_name": profile["display_name"],
                "given_names": profile["given_names"],
                "family_name": profile["family_name"],
                "credit_name": profile["credit_name"],
                "works_count": profile["works_count"],
                "fundings_count": profile["fundings_count"],
                "year_range": profile["year_range"],
                "managed_by_am": profile["managed_by_am"],
                "evidence": evidence,
                "shared_dois": shared_dois,
            })
    return rows


def _empty_report() -> dict:
    return {
        "groups": [],
        "institutions": [],
        "profile_activity": [],
        "cache": {},
        "summary": {
            "candidate_groups": 0,
            "duplicate_profiles": 0,
            "extra_profiles": 0,
            "institutions": 0,
            "candidate_works": 0,
            "candidate_fundings": 0,
        },
    }


def _load_researcher_population(allowed_rors: set[str] | None) -> set[tuple[str, str]]:
    population = set()
    for model in (WorkCache, FundingCache):
        query = db.session.query(model.ror_id, model.orcid).filter(
            model.ror_id.isnot(None),
            model.ror_id != "",
            model.orcid.isnot(None),
            model.orcid != "",
        )
        if allowed_rors:
            query = query.filter(model.ror_id.in_(allowed_rors))
        for ror_id, orcid in query.distinct().all():
            population.add((ror_id, orcid))
    return population


def _scope_key(allowed_rors: set[str] | None) -> str:
    if not allowed_rors:
        return "all"
    return "ror:" + ",".join(sorted(allowed_rors))


def _dependency_signature(allowed_rors: set[str] | None, population: set[tuple[str, str]]) -> dict:
    rors = sorted({ror_id for ror_id, _ in population})
    orcids = sorted({orcid for _, orcid in population})
    summary = {
        "rors": rors,
        "orcid_count": len(orcids),
        "works": _table_signature(WorkCache, allowed_rors, "created_at"),
        "fundings": _table_signature(FundingCache, allowed_rors, "created_at"),
        "statuses": _table_signature(ResearcherStatus, allowed_rors, "last_updated"),
        "researchers": _researcher_signature(orcids),
    }
    raw = json.dumps(summary, sort_keys=True, default=str)
    return {
        "hash": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "summary": summary,
    }


def _table_signature(model, allowed_rors: set[str] | None, timestamp_field: str) -> dict:
    timestamp_column = getattr(model, timestamp_field)
    query = db.session.query(func.count(model.id), func.max(timestamp_column))
    if allowed_rors:
        query = query.filter(model.ror_id.in_(allowed_rors))
    count, last_updated = query.one()
    return {
        "count": int(count or 0),
        "last_updated": _datetime_value(last_updated),
    }


def _researcher_signature(orcids: list[str]) -> dict:
    total_count = 0
    last_updated = None
    for chunk in _chunks(orcids, 500):
        count, max_updated = db.session.query(
            func.count(ResearcherCache.orcid),
            func.max(ResearcherCache.updated_at),
        ).filter(ResearcherCache.orcid.in_(chunk)).one()
        total_count += int(count or 0)
        if max_updated and (not last_updated or max_updated > last_updated):
            last_updated = max_updated
    return {
        "count": total_count,
        "last_updated": _datetime_value(last_updated),
    }


def _load_cached_report(scope_key: str, dependency_hash: str, force_refresh: bool) -> dict | None:
    if force_refresh:
        return None

    cache_row = DuplicateProfileCache.query.filter_by(scope_key=scope_key).first()
    if not cache_row or cache_row.dependency_hash != dependency_hash:
        return None

    report = cache_row.report_json or _empty_report()
    report["cache"] = {
        "hit": True,
        "scope_key": scope_key,
        "generated_at": _datetime_value(cache_row.generated_at),
        "dependency_hash": dependency_hash,
    }
    return report


def _store_cached_report(scope_key: str, dependency: dict, report: dict) -> None:
    cache_row = DuplicateProfileCache.query.filter_by(scope_key=scope_key).first()
    if not cache_row:
        cache_row = DuplicateProfileCache(scope_key=scope_key)
        db.session.add(cache_row)

    stored_report = json.loads(json.dumps(report, default=str))
    stored_report["cache"] = {
        "hit": False,
        "scope_key": scope_key,
        "generated_at": _datetime_value(datetime.utcnow()),
        "dependency_hash": dependency["hash"],
    }

    cache_row.dependency_hash = dependency["hash"]
    cache_row.report_json = stored_report
    cache_row.source_summary = dependency["summary"]
    cache_row.generated_at = datetime.utcnow()
    db.session.commit()
    report["cache"] = stored_report["cache"]


def _datetime_value(value) -> str:
    if not value:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat(timespec="seconds")
    return str(value)


def _load_institution_names() -> dict[str, str]:
    names = {
        row.ror_id: row.name or row.ror_id
        for row in InstitutionRegistry.query.filter_by(is_active=True).all()
    }
    rows = (
        db.session.query(User.ror_id, User.institution_name)
        .filter(User.ror_id.isnot(None), User.ror_id != "")
        .all()
    )
    for ror_id, institution_name in rows:
        if institution_name:
            names[ror_id] = institution_name
        elif ror_id not in names:
            names[ror_id] = ror_id
    return names


def _load_researcher_names(orcids: list[str]) -> dict[str, dict]:
    names = {}
    for chunk in _chunks(orcids, 500):
        rows = ResearcherCache.query.filter(ResearcherCache.orcid.in_(chunk)).all()
        for row in rows:
            names[row.orcid] = {
                "given_names": row.given_names,
                "family_name": row.family_name,
                "credit_name": row.credit_name,
            }
    return names


def _load_work_stats(allowed_rors: set[str] | None) -> dict[tuple[str, str], dict]:
    query = db.session.query(
        WorkCache.ror_id,
        WorkCache.orcid,
        func.count(WorkCache.id),
        func.min(WorkCache.pub_year),
        func.max(WorkCache.pub_year),
    ).filter(WorkCache.orcid.isnot(None), WorkCache.orcid != "")
    if allowed_rors:
        query = query.filter(WorkCache.ror_id.in_(allowed_rors))

    stats = {}
    for ror_id, orcid, count, min_year, max_year in query.group_by(WorkCache.ror_id, WorkCache.orcid).all():
        stats[(ror_id, orcid)] = {"count": count, "years": [min_year, max_year]}
    return stats


def _load_funding_stats(allowed_rors: set[str] | None) -> dict[tuple[str, str], dict]:
    query = db.session.query(
        FundingCache.ror_id,
        FundingCache.orcid,
        func.count(FundingCache.id),
        func.min(FundingCache.start_y),
        func.max(FundingCache.start_y),
    ).filter(FundingCache.orcid.isnot(None), FundingCache.orcid != "")
    if allowed_rors:
        query = query.filter(FundingCache.ror_id.in_(allowed_rors))

    stats = {}
    for ror_id, orcid, count, min_year, max_year in query.group_by(FundingCache.ror_id, FundingCache.orcid).all():
        stats[(ror_id, orcid)] = {"count": count, "years": [min_year, max_year]}
    return stats


def _load_statuses(allowed_rors: set[str] | None) -> dict[tuple[str, str], bool]:
    query = ResearcherStatus.query
    if allowed_rors:
        query = query.filter(ResearcherStatus.ror_id.in_(allowed_rors))
    return {
        (row.ror_id, row.orcid): bool(row.is_managed_by_am)
        for row in query.all()
    }


def _load_dois(allowed_rors: set[str] | None) -> dict[tuple[str, str], set[str]]:
    query = db.session.query(WorkCache.ror_id, WorkCache.orcid, WorkCache.doi).filter(
        WorkCache.orcid.isnot(None),
        WorkCache.orcid != "",
        WorkCache.doi.isnot(None),
        WorkCache.doi != "",
    )
    if allowed_rors:
        query = query.filter(WorkCache.ror_id.in_(allowed_rors))

    dois = defaultdict(set)
    for ror_id, orcid, doi in query.all():
        normalized = _normalize_doi(doi)
        if normalized:
            dois[(ror_id, orcid)].add(normalized)
    return dois


def _display_name(name_data: dict, orcid: str) -> str:
    credit_name = (name_data.get("credit_name") or "").strip()
    if credit_name:
        return credit_name

    full_name = " ".join(
        part.strip()
        for part in [name_data.get("given_names") or "", name_data.get("family_name") or ""]
        if part and part.strip()
    )
    return full_name or orcid


def _best_group_name(profiles: list[dict]) -> str:
    return sorted(
        (profile["display_name"] for profile in profiles if profile["display_name"]),
        key=lambda value: (-len(value), value.lower()),
    )[0]


def _is_candidate_name(name_key: str) -> bool:
    tokens = name_key.split()
    if len(tokens) < 2:
        return False
    if name_key in {"unknown researcher", "sin nombre", "no name"}:
        return False
    return not all(token.isdigit() for token in tokens)


def _confidence_score(name_key: str, profiles: list[dict], shared_dois: list[str]) -> int:
    score = 70
    token_count = len(name_key.split())
    if token_count >= 3:
        score += 10
    if shared_dois:
        score += 15
    if min((profile["works_count"] + profile["fundings_count"]) for profile in profiles) > 0:
        score += 5
    if token_count == 2 and any(len(token) <= 2 for token in name_key.split()):
        score -= 10
    return max(0, min(score, 95))


def _confidence_level(score: int) -> str:
    if score >= 85:
        return "high"
    if score >= 70:
        return "medium"
    return "low"


def _shared_dois_for_group(ror_id: str, profiles: list[dict], dois: dict[tuple[str, str], set[str]]) -> list[str]:
    owners = defaultdict(set)
    for profile in profiles:
        for doi in dois.get((ror_id, profile["orcid"]), set()):
            owners[doi].add(profile["orcid"])
    return sorted(doi for doi, orcids in owners.items() if len(orcids) > 1)


def _merge_year_ranges(*year_lists: list[str | None]) -> str:
    years = []
    for year_list in year_lists:
        for year in year_list:
            value = _year_as_int(year)
            if value:
                years.append(value)
    if not years:
        return ""
    first, last = min(years), max(years)
    return str(first) if first == last else f"{first}-{last}"


def _year_as_int(value: str | None) -> int | None:
    try:
        year = int(str(value or "").strip())
    except ValueError:
        return None
    return year if 1800 <= year <= 2200 else None


def _normalize_doi(value: str | None) -> str:
    doi = (value or "").strip().lower()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi)
    return doi.strip()


def _summarize_institutions(groups: list[dict]) -> list[dict]:
    summary = {}
    for group in groups:
        item = summary.setdefault(group["ror_id"], {
            "ror_id": group["ror_id"],
            "institution_name": group["institution_name"],
            "candidate_groups": 0,
            "duplicate_profiles": 0,
            "extra_profiles": 0,
            "highest_confidence": 0,
        })
        item["candidate_groups"] += 1
        item["duplicate_profiles"] += group["orcid_count"]
        item["extra_profiles"] += group["extra_profiles"]
        item["highest_confidence"] = max(item["highest_confidence"], group["confidence"])
    return sorted(summary.values(), key=lambda item: (-item["candidate_groups"], item["institution_name"]))


def _summarize_profile_activity(groups: list[dict]) -> list[dict]:
    summary = {}
    for group in groups:
        for profile in group["profiles"]:
            key = (group["ror_id"], profile["orcid"])
            item = summary.setdefault(key, {
                "ror_id": group["ror_id"],
                "institution_name": group["institution_name"],
                "orcid": profile["orcid"],
                "display_name": profile["display_name"],
                "works_count": profile["works_count"],
                "fundings_count": profile["fundings_count"],
                "total_activity": profile["works_count"] + profile["fundings_count"],
                "candidate_groups": 0,
                "orcid_url": profile["orcid_url"],
            })
            item["candidate_groups"] += 1
    return sorted(
        summary.values(),
        key=lambda item: (-item["total_activity"], item["institution_name"], item["display_name"], item["orcid"]),
    )


def _chunks(values: list[str], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]
