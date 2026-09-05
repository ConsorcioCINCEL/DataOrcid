"""Institution-scoped OAI-PMH provider backed by DataORCID works."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
import secrets
from xml.etree import ElementTree as ET

from flask import current_app
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import and_, case, func, literal, or_

from .. import db
from ..models import (
    CanonicalWork,
    OaiPmhInstitutionConfig,
    OaiPmhWorkSelection,
    OpenAlexInstitutionWorkFact,
    OpenAlexWorkInstitution,
    OpenAlexWorkMetadata,
    ResearcherCache,
    WorkCache,
    WorkRecordLink,
    utc_now,
)


OAI_NS = "http://www.openarchives.org/OAI/2.0/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"
DATACITE_NS = "http://datacite.org/schema/kernel-4"
OAIRE_NS = "http://namespace.openaire.eu/schema/oaire/"
MAPPED_NS = "https://dataorcid.cl/ns/mapped-metadata/1.0/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XML_NS = "http://www.w3.org/XML/1998/namespace"
OAI_SCHEMA = "http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
OAI_DC_SCHEMA = "http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
OAIRE_SCHEMA = "https://www.openaire.eu/schema/repo-lit/4.0/openaire.xsd"
SECOND_GRANULARITY = "YYYY-MM-DDThh:mm:ssZ"
DAY_GRANULARITY = "YYYY-MM-DD"
AFFILIATION_DISPLAY_LIMIT = 5
DC_FIELDS = (
    "title",
    "creator",
    "subject",
    "description",
    "publisher",
    "contributor",
    "date",
    "type",
    "format",
    "identifier",
    "source",
    "language",
    "relation",
    "coverage",
    "rights",
)
# The custom DataORCID profile can expose every field below. Dublin Core and
# OpenAIRE remain fixed protocol formats; this catalog only controls the
# institution-specific ``dataorcid`` crosswalk.
METADATA_FIELD_CATALOG = (
    ("title", "dc.title", "core", True, True),
    ("creator", "dc.creator", "core", True, False),
    ("subject", "dc.subject", "core", True, False),
    ("description", "dc.description", "core", True, False),
    ("publisher", "dc.publisher", "core", True, False),
    ("contributor", "dc.contributor", "core", True, False),
    ("date", "dc.date", "core", True, False),
    ("type", "dc.type", "core", True, False),
    ("format", "dc.format", "core", True, False),
    ("identifier", "dc.identifier", "core", True, True),
    ("source", "dc.source", "core", True, False),
    ("language", "dc.language", "core", True, False),
    ("relation", "dc.relation", "core", True, False),
    ("coverage", "dc.coverage", "core", True, False),
    ("rights", "dc.rights", "core", True, False),
    ("publication_year", "dc.date.issued", "publication", False, False),
    ("journal_title", "dc.relation.ispartof", "publication", False, False),
    ("volume", "dc.bibliographicCitation.volume", "publication", False, False),
    ("issue", "dc.bibliographicCitation.issue", "publication", False, False),
    ("first_page", "dc.bibliographicCitation.startPage", "publication", False, False),
    ("last_page", "dc.bibliographicCitation.endPage", "publication", False, False),
    ("corresponding_creator", "dc.contributor.author.corresponding", "publication", False, False),
    ("doi", "dc.identifier.doi", "identifier", False, False),
    ("openalex_id", "openalex.id", "identifier", False, False),
    ("creator_orcid", "dc.contributor.author.orcid", "identifier", False, False),
    ("issn", "dc.identifier.issn", "identifier", False, False),
    ("pmid", "dc.identifier.pmid", "identifier", False, False),
    ("pmcid", "dc.identifier.pmcid", "identifier", False, False),
    ("source_id", "openalex.source.id", "identifier", False, False),
    ("landing_page_url", "dc.identifier.uri", "identifier", False, False),
    ("pdf_url", "dc.relation.hasformat", "identifier", False, False),
    ("license", "dc.rights.license", "access", False, False),
    ("access_right", "dc.rights.access", "access", False, False),
    ("oa_status", "openalex.oa_status", "access", False, False),
    ("is_open_access", "openalex.is_open_access", "access", False, False),
    ("source_type", "openalex.source.type", "access", False, False),
    ("indexed_in", "openalex.indexed_in", "access", False, False),
    ("institution", "dc.contributor.institution", "context", False, False),
    ("institution_ror", "dc.contributor.institution.ror", "context", False, False),
    ("country", "dc.coverage.country", "context", False, False),
    ("funder", "dc.description.sponsorship", "context", False, False),
    ("award", "dc.relation.award", "context", False, False),
    ("sdg", "dc.subject.sdg", "context", False, False),
    ("keyword", "dc.subject.keyword", "enrichment", False, False),
    ("topic", "dc.subject.topic", "enrichment", False, False),
    ("primary_topic", "openalex.primary_topic", "enrichment", False, False),
    ("topic_field", "openalex.topic.field", "enrichment", False, False),
    ("topic_domain", "openalex.topic.domain", "enrichment", False, False),
    ("cited_by_count", "openalex.cited_by_count", "enrichment", False, False),
    ("fwci", "openalex.fwci", "enrichment", False, False),
)
METADATA_SOURCE_FIELDS = tuple(item[0] for item in METADATA_FIELD_CATALOG)
DEFAULT_METADATA_MAPPING = {
    field: target
    for field, target, _group, enabled_by_default, _required in METADATA_FIELD_CATALOG
    if enabled_by_default
}
METADATA_PREFIXES = {"oai_dc", "oai_openaire", "dataorcid"}
MAPPING_TARGET_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,95}$")
OAI_VERBS = {
    "Identify",
    "ListMetadataFormats",
    "ListSets",
    "GetRecord",
    "ListIdentifiers",
    "ListRecords",
}
CANONICAL_KEY_RE = re.compile(r"^(?:doi|title|record):[0-9a-f]{64}$")
XML_FORBIDDEN_RE = re.compile(
    "[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]"
)


ET.register_namespace("", OAI_NS)
ET.register_namespace("oai_dc", OAI_DC_NS)
ET.register_namespace("dc", DC_NS)
ET.register_namespace("datacite", DATACITE_NS)
ET.register_namespace("oaire", OAIRE_NS)
ET.register_namespace("mapped", MAPPED_NS)
ET.register_namespace("xsi", XSI_NS)


class OaiProtocolError(ValueError):
    """Represent one protocol-level error returned in an OAI-PMH response."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class InstitutionalWork:
    """One deduplicated institutional output prepared for OAI dissemination."""

    canonical_work_id: int
    canonical_key: str
    ror_id: str
    public_key: str
    title: str | None
    doi_normalized: str | None
    publication_year: int | None
    document_type: str | None
    journal_title: str | None
    creators: list[str]
    orcid: str | None
    language: str | None
    landing_page_url: str | None
    is_open_access: bool
    license_condition: str | None
    is_openalex_validated: bool
    openalex_validation_at: datetime | None
    has_manual_override: bool
    is_included: bool
    publication_source: str | None
    datestamp: datetime
    metadata_json: dict[str, list[str]]
    metadata_mapping: dict[str, str]
    affiliations: list[dict]

    @property
    def displayed_affiliations(self) -> list[dict]:
        """Return a compact list that always preserves the active institution."""
        if len(self.affiliations) <= AFFILIATION_DISPLAY_LIMIT:
            return self.affiliations

        displayed = list(self.affiliations[:AFFILIATION_DISPLAY_LIMIT])
        selected = next(
            (item for item in self.affiliations if item.get("is_selected")),
            None,
        )
        if selected is not None and selected not in displayed:
            displayed[-1] = selected
        return displayed

    @property
    def additional_affiliation_count(self) -> int:
        """Count affiliations hidden by the compact institutional listing."""
        return max(len(self.affiliations) - AFFILIATION_DISPLAY_LIMIT, 0)


def institutional_work_query(config: OaiPmhInstitutionConfig):
    """Return the deduplicated work query and its effective OAI expressions."""
    representative = (
        db.session.query(
            WorkRecordLink.canonical_work_id.label("canonical_work_id"),
            func.min(WorkRecordLink.work_cache_id).label("work_cache_id"),
        )
        .join(WorkCache, WorkCache.id == WorkRecordLink.work_cache_id)
        .filter(WorkRecordLink.ror_id == config.ror_id)
        .filter(WorkCache.visibility == "public")
        .group_by(WorkRecordLink.canonical_work_id)
        .subquery()
    )
    validation_key = func.coalesce(CanonicalWork.doi_normalized, CanonicalWork.canonical_key)
    validated_expr = func.coalesce(
        OpenAlexInstitutionWorkFact.has_selected_affiliation,
        literal(False),
    )
    if config.publication_policy == "all":
        default_included = literal(True)
    elif config.publication_policy == "selected":
        default_included = literal(False)
    else:
        default_included = validated_expr
    included_expr = case(
        (OaiPmhWorkSelection.id.isnot(None), OaiPmhWorkSelection.is_included),
        else_=default_included,
    )
    policy_datestamp = literal(config.policy_updated_at or config.updated_at or utc_now())
    validation_datestamp = case(
        (
            OpenAlexInstitutionWorkFact.refreshed_at > policy_datestamp,
            OpenAlexInstitutionWorkFact.refreshed_at,
        ),
        else_=policy_datestamp,
    )
    work_datestamp = case(
        (CanonicalWork.updated_at > validation_datestamp, CanonicalWork.updated_at),
        else_=validation_datestamp,
    )
    datestamp_expr = case(
        (OaiPmhWorkSelection.updated_at > work_datestamp, OaiPmhWorkSelection.updated_at),
        else_=work_datestamp,
    )
    query = (
        db.session.query(
            CanonicalWork,
            WorkCache,
            OpenAlexWorkMetadata,
            ResearcherCache,
            OpenAlexInstitutionWorkFact,
            OaiPmhWorkSelection,
            included_expr.label("oai_included"),
            datestamp_expr.label("oai_datestamp"),
            validated_expr.label("oai_openalex_validated"),
        )
        .join(representative, representative.c.canonical_work_id == CanonicalWork.id)
        .outerjoin(WorkCache, WorkCache.id == representative.c.work_cache_id)
        .outerjoin(
            OpenAlexWorkMetadata,
            OpenAlexWorkMetadata.doi_normalized == CanonicalWork.doi_normalized,
        )
        .outerjoin(ResearcherCache, ResearcherCache.orcid == WorkCache.orcid)
        .outerjoin(
            OpenAlexInstitutionWorkFact,
            and_(
                OpenAlexInstitutionWorkFact.ror_id == config.ror_id,
                OpenAlexInstitutionWorkFact.openalex_cache_key == validation_key,
            ),
        )
        .outerjoin(
            OaiPmhWorkSelection,
            and_(
                OaiPmhWorkSelection.ror_id == config.ror_id,
                OaiPmhWorkSelection.canonical_work_id == CanonicalWork.id,
            ),
        )
    )
    return query, included_expr, datestamp_expr


def work_from_query_row(
    row,
    config: OaiPmhInstitutionConfig,
    *,
    include_metadata: bool = True,
) -> InstitutionalWork:
    """Convert an institutional query row into its OAI representation."""
    (
        canonical,
        local,
        openalex,
        researcher,
        validation,
        _selection,
        included,
        datestamp,
        openalex_validated,
    ) = row
    title = (getattr(openalex, "title", None) or canonical.title or getattr(local, "title", None))
    creators = _split_values(getattr(openalex, "author_names", None))
    if not creators and researcher:
        display_name = (
            researcher.credit_name
            or " ".join(
                part for part in (researcher.given_names, researcher.family_name) if part
            ).strip()
        )
        if display_name:
            creators = [display_name]
    publication_year = (
        getattr(openalex, "publication_year", None)
        or canonical.publication_year
        or _safe_year(getattr(local, "pub_year", None))
    )
    document_type = getattr(openalex, "type", None) or getattr(local, "type", None)
    journal_title = (
        getattr(openalex, "source_name", None)
        or getattr(local, "journal_title", None)
        or getattr(local, "source", None)
    )
    landing_page_url = _first_url(
        getattr(openalex, "primary_landing_page_url", None),
        getattr(openalex, "oa_url", None),
        getattr(local, "url", None),
        f"https://doi.org/{canonical.doi_normalized}" if canonical.doi_normalized else None,
    )
    if include_metadata:
        metadata = _work_dc_metadata(
            canonical=canonical,
            local=local,
            openalex=openalex,
            title=title,
            creators=creators,
            publication_year=publication_year,
            document_type=document_type,
            journal_title=journal_title,
            landing_page_url=landing_page_url,
        )
        metadata_mapping = effective_metadata_mapping(config)
    else:
        metadata = {
            "institution": _split_values(
                getattr(openalex, "institution_names", None)
            ),
        }
        metadata_mapping = {}
    return InstitutionalWork(
        canonical_work_id=canonical.id,
        canonical_key=canonical.canonical_key,
        ror_id=config.ror_id,
        public_key=config.public_key,
        title=title,
        doi_normalized=canonical.doi_normalized,
        publication_year=publication_year,
        document_type=document_type,
        journal_title=journal_title,
        creators=creators,
        orcid=getattr(local, "orcid", None),
        language=getattr(openalex, "language", None),
        landing_page_url=landing_page_url,
        is_open_access=bool(getattr(openalex, "is_oa", False)),
        license_condition=getattr(openalex, "primary_license", None),
        is_openalex_validated=bool(openalex_validated),
        openalex_validation_at=getattr(validation, "refreshed_at", None),
        has_manual_override=_selection is not None,
        is_included=bool(included),
        publication_source=(
            getattr(_selection, "decision_source", None)
            if _selection is not None
            else None
        ),
        # Keep the exact database value for keyset pagination. XML output still
        # uses the protocol's second-level granularity.
        datestamp=datestamp or canonical.updated_at or canonical.created_at or utc_now(),
        metadata_json=metadata,
        metadata_mapping=metadata_mapping,
        affiliations=[],
    )


def attach_institutional_affiliations(
    records: list[InstitutionalWork],
    selected_ror: str,
) -> None:
    """Attach normalized OpenAlex affiliations without expanding the work query."""
    dois = {record.doi_normalized for record in records if record.doi_normalized}
    rows = []
    if dois:
        rows = (
            OpenAlexWorkInstitution.query
            .filter(OpenAlexWorkInstitution.doi_normalized.in_(dois))
            .order_by(
                OpenAlexWorkInstitution.doi_normalized.asc(),
                OpenAlexWorkInstitution.institution_name.asc(),
            )
            .all()
        )

    selected_suffix = _ror_suffix(selected_ror)
    by_doi: dict[str, list[dict]] = {}
    identities_by_doi: dict[str, set[tuple[str, str | None]]] = {}
    for row in rows:
        ror_suffix = _ror_suffix(row.ror_id)
        affiliation = {
            "name": row.institution_name or row.institution_id or ror_suffix,
            "ror_id": ror_suffix or None,
            "country_code": row.country_code or None,
            "is_selected": bool(ror_suffix and ror_suffix == selected_suffix),
        }
        current = by_doi.setdefault(row.doi_normalized, [])
        identities = identities_by_doi.setdefault(row.doi_normalized, set())
        identity = (affiliation["name"], affiliation["ror_id"])
        if identity not in identities:
            current.append(affiliation)
            identities.add(identity)

    for record in records:
        record.affiliations = list(by_doi.get(record.doi_normalized, []))
        if not record.affiliations:
            record.affiliations = [
                {
                    "name": name,
                    "ror_id": None,
                    "country_code": None,
                    "is_selected": False,
                }
                for name in record.metadata_json.get("institution", [])
            ]


def build_oai_response(
    config: OaiPmhInstitutionConfig, params, base_url: str,
    *, stylesheet_url: str = "/oai-pmh/stylesheet/en.xsl",
) -> bytes:
    """Build a complete OAI-PMH response for one institutional provider."""
    root = ET.Element(_oai("OAI-PMH"), {_xsi("schemaLocation"): f"{OAI_NS} {OAI_SCHEMA}"})
    ET.SubElement(root, _oai("responseDate")).text = _format_datestamp(
        utc_now(), SECOND_GRANULARITY
    )
    request_element = ET.SubElement(root, _oai("request"))
    request_element.text = base_url

    try:
        verb = _validate_request(params, request_element)
        if verb == "Identify":
            _build_identify(root, config, base_url)
        elif verb == "ListMetadataFormats":
            _build_metadata_formats(root, config, params, base_url)
        elif verb == "ListSets":
            _build_sets(root, config, params)
        elif verb == "GetRecord":
            _build_get_record(root, config, params)
        else:
            _build_record_list(root, config, params, verb)
    except OaiProtocolError as exc:
        if exc.code in {"badVerb", "badArgument"}:
            request_element.attrib.clear()
        ET.SubElement(root, _oai("error"), {"code": exc.code}).text = exc.message

    payload = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    declaration_end = payload.find(b"?>") + 2
    stylesheet = f'\n<?xml-stylesheet type="text/xsl" href="{stylesheet_url}"?>'.encode("utf-8")
    return payload[:declaration_end] + stylesheet + payload[declaration_end:]


def oai_repository_summaries(allowed_rors: list[str] | None = None) -> list[dict]:
    """Aggregate effective exposure metrics for configured OAI repositories."""
    candidates = (
        db.session.query(
            WorkRecordLink.ror_id.label("ror_id"),
            WorkRecordLink.canonical_work_id.label("canonical_work_id"),
        )
        .join(WorkCache, WorkCache.id == WorkRecordLink.work_cache_id)
        .filter(WorkCache.visibility == "public")
        .group_by(WorkRecordLink.ror_id, WorkRecordLink.canonical_work_id)
        .subquery()
    )
    validation_key = func.coalesce(CanonicalWork.doi_normalized, CanonicalWork.canonical_key)
    validated_expr = func.coalesce(
        OpenAlexInstitutionWorkFact.has_selected_affiliation,
        literal(False),
    )
    default_included = case(
        (OaiPmhInstitutionConfig.publication_policy == "all", literal(True)),
        (OaiPmhInstitutionConfig.publication_policy == "selected", literal(False)),
        else_=validated_expr,
    )
    included_expr = case(
        (OaiPmhWorkSelection.id.isnot(None), OaiPmhWorkSelection.is_included),
        else_=default_included,
    )
    is_candidate = candidates.c.canonical_work_id.isnot(None)
    query = (
        db.session.query(
            OaiPmhInstitutionConfig,
            func.count(candidates.c.canonical_work_id).label("available_count"),
            func.coalesce(func.sum(case(
                (and_(is_candidate, validated_expr.is_(True)), 1),
                else_=0,
            )), 0).label("validated_count"),
            func.coalesce(func.sum(case(
                (and_(is_candidate, included_expr.is_(True)), 1),
                else_=0,
            )), 0).label("exposed_count"),
            func.coalesce(func.sum(case(
                (and_(is_candidate, OaiPmhWorkSelection.id.isnot(None)), 1),
                else_=0,
            )), 0).label("manual_count"),
        )
        .outerjoin(candidates, candidates.c.ror_id == OaiPmhInstitutionConfig.ror_id)
        .outerjoin(CanonicalWork, CanonicalWork.id == candidates.c.canonical_work_id)
        .outerjoin(
            OpenAlexInstitutionWorkFact,
            and_(
                OpenAlexInstitutionWorkFact.ror_id == OaiPmhInstitutionConfig.ror_id,
                OpenAlexInstitutionWorkFact.openalex_cache_key == validation_key,
            ),
        )
        .outerjoin(
            OaiPmhWorkSelection,
            and_(
                OaiPmhWorkSelection.ror_id == OaiPmhInstitutionConfig.ror_id,
                OaiPmhWorkSelection.canonical_work_id == candidates.c.canonical_work_id,
            ),
        )
    )
    if allowed_rors is not None:
        query = query.filter(OaiPmhInstitutionConfig.ror_id.in_(allowed_rors))
    rows = query.group_by(OaiPmhInstitutionConfig.id).order_by(
        OaiPmhInstitutionConfig.repository_name.asc(),
        OaiPmhInstitutionConfig.ror_id.asc(),
    ).all()
    return [
        {
            "config": config,
            "available": int(available or 0),
            "validated": int(validated or 0),
            "exposed": int(exposed or 0),
            "manual": int(manual or 0),
        }
        for config, available, validated, exposed, manual in rows
    ]


def provider_identifier(record: InstitutionalWork) -> str:
    """Return the stable OAI identifier inside one opaque provider scope."""
    return f"oai:dataorcid-chile:{record.public_key}:{record.canonical_key}"


def provider_set_spec(public_key: str) -> str:
    """Return the opaque institutional set exposed by one provider endpoint."""
    return f"institution:{public_key}"


def generate_public_key() -> str:
    """Return an unguessable 192-bit public provider identifier."""
    return secrets.token_hex(24)


def effective_metadata_mapping(config: OaiPmhInstitutionConfig) -> dict[str, str]:
    """Return the validated custom field aliases for one repository."""
    stored = config.metadata_mapping
    if stored is None:
        return dict(DEFAULT_METADATA_MAPPING)
    if not isinstance(stored, dict):
        return {}
    return {
        field: target.strip()
        for field in METADATA_SOURCE_FIELDS
        if isinstance((target := stored.get(field)), str)
        and target.strip()
        and MAPPING_TARGET_RE.fullmatch(target.strip())
    }


def _validate_request(params, request_element: ET.Element) -> str:
    verb_values = params.getlist("verb")
    if len(verb_values) != 1 or verb_values[0] not in OAI_VERBS:
        raise OaiProtocolError("badVerb", "The request must contain one valid OAI-PMH verb.")
    verb = verb_values[0]
    request_element.set("verb", verb)
    request_attributes = {
        "verb", "identifier", "metadataPrefix", "from", "until", "set", "resumptionToken"
    }
    for key in params.keys():
        values = params.getlist(key)
        if len(values) != 1:
            raise OaiProtocolError("badArgument", "Repeated OAI-PMH arguments are not allowed.")
        if key not in {"verb", "resumptionToken"} and values[0] == "":
            raise OaiProtocolError("badArgument", "Empty OAI-PMH arguments are not allowed.")
        if key != "verb" and key in request_attributes:
            request_element.set(key, values[0])

    allowed = {
        "Identify": {"verb"},
        "ListMetadataFormats": {"verb", "identifier"},
        "ListSets": {"verb", "resumptionToken"},
        "GetRecord": {"verb", "identifier", "metadataPrefix"},
        "ListIdentifiers": {"verb", "from", "until", "set", "metadataPrefix", "resumptionToken"},
        "ListRecords": {"verb", "from", "until", "set", "metadataPrefix", "resumptionToken"},
    }[verb]
    if set(params.keys()) - allowed:
        raise OaiProtocolError("badArgument", "The request contains an argument not allowed for this verb.")
    return verb


def _build_identify(root, config, base_url) -> None:
    identify = ET.SubElement(root, _oai("Identify"))
    ET.SubElement(identify, _oai("repositoryName")).text = _xml_text(config.repository_name)
    ET.SubElement(identify, _oai("baseURL")).text = base_url
    ET.SubElement(identify, _oai("protocolVersion")).text = "2.0"
    ET.SubElement(identify, _oai("adminEmail")).text = _xml_text(
        config.admin_email or "noreply@localhost"
    )
    query, included_expr, datestamp_expr = institutional_work_query(config)
    query = query.filter(included_expr.is_(True))
    earliest = query.with_entities(func.min(datestamp_expr)).scalar() or config.created_at or utc_now()
    ET.SubElement(identify, _oai("earliestDatestamp")).text = _format_datestamp(
        earliest, SECOND_GRANULARITY
    )
    ET.SubElement(identify, _oai("deletedRecord")).text = "no"
    ET.SubElement(identify, _oai("granularity")).text = SECOND_GRANULARITY


def _build_metadata_formats(root, config, params, base_url: str) -> None:
    identifier = params.get("identifier")
    if identifier:
        _provider_record(config, identifier)
    formats = ET.SubElement(root, _oai("ListMetadataFormats"))
    base_url = base_url.rstrip("/")
    custom_schema = (
        f"{base_url.rsplit('/oai/', 1)[0]}/static/xsd/dataorcid-mapped.xsd"
        if "/oai/" in base_url
        else "/static/xsd/dataorcid-mapped.xsd"
    )
    for prefix, schema, namespace in (
        ("oai_dc", OAI_DC_SCHEMA, OAI_DC_NS),
        ("oai_openaire", OAIRE_SCHEMA, OAIRE_NS),
        ("dataorcid", custom_schema, MAPPED_NS),
    ):
        metadata_format = ET.SubElement(formats, _oai("metadataFormat"))
        ET.SubElement(metadata_format, _oai("metadataPrefix")).text = prefix
        ET.SubElement(metadata_format, _oai("schema")).text = schema
        ET.SubElement(metadata_format, _oai("metadataNamespace")).text = namespace


def _build_sets(root, config, params) -> None:
    if "resumptionToken" in params:
        raise OaiProtocolError("badResumptionToken", "This repository has no additional set pages.")
    list_sets = ET.SubElement(root, _oai("ListSets"))
    set_element = ET.SubElement(list_sets, _oai("set"))
    ET.SubElement(set_element, _oai("setSpec")).text = provider_set_spec(config.public_key)
    ET.SubElement(set_element, _oai("setName")).text = _xml_text(config.repository_name)
    openaire_set = ET.SubElement(list_sets, _oai("set"))
    ET.SubElement(openaire_set, _oai("setSpec")).text = "openaire"
    ET.SubElement(openaire_set, _oai("setName")).text = "OpenAIRE 4"


def _build_get_record(root, config, params) -> None:
    identifier = params.get("identifier")
    prefix = params.get("metadataPrefix")
    if not identifier or not prefix:
        raise OaiProtocolError("badArgument", "GetRecord requires identifier and metadataPrefix.")
    if prefix not in METADATA_PREFIXES:
        raise OaiProtocolError("cannotDisseminateFormat", "The requested metadata format is not available.")
    record = _provider_record(config, identifier)
    get_record = ET.SubElement(root, _oai("GetRecord"))
    _append_record(get_record, record, metadata_prefix=prefix)


def _build_record_list(root, config, params, verb: str) -> None:
    token_present = "resumptionToken" in params
    token_value = params.get("resumptionToken")
    if token_present:
        if set(params.keys()) != {"verb", "resumptionToken"}:
            raise OaiProtocolError(
                "badArgument", "resumptionToken must be the exclusive argument for this verb."
            )
        if not token_value:
            raise OaiProtocolError("badResumptionToken", "The resumption token is empty.")
        state = _load_resumption_token(token_value, config, verb)
    else:
        prefix = params.get("metadataPrefix")
        if not prefix:
            raise OaiProtocolError("badArgument", f"{verb} requires metadataPrefix.")
        if prefix not in METADATA_PREFIXES:
            raise OaiProtocolError(
                "cannotDisseminateFormat",
                "The requested metadata format is not available.",
            )
        requested_set = params.get("set")
        if requested_set and requested_set not in {
            provider_set_spec(config.public_key),
            "openaire",
        }:
            raise OaiProtocolError("noRecordsMatch", "The requested set does not exist in this repository.")
        from_dt, from_raw = _optional_datestamp(params.get("from"), lower=True)
        until_dt, until_raw = _optional_datestamp(params.get("until"), lower=False)
        if from_raw and until_raw and len(from_raw) != len(until_raw):
            raise OaiProtocolError("badArgument", "The from and until arguments must use the same granularity.")
        if from_dt and until_dt and from_dt > until_dt:
            raise OaiProtocolError("badArgument", "The from datestamp must not be later than until.")
        state = {
            "verb": verb,
            "ror_id": config.ror_id,
            "metadata_prefix": prefix,
            "set": requested_set,
            "from": from_raw,
            "until": until_raw or _format_datestamp(utc_now(), SECOND_GRANULARITY),
            "after_datestamp": None,
            "after_id": 0,
            "cursor": 0,
            "total": None,
        }

    from_dt = _optional_datestamp(state.get("from"), lower=True)[0]
    until_dt = _optional_datestamp(state.get("until"), lower=False)[0]
    query, included_expr, datestamp_expr = institutional_work_query(config)
    query = query.filter(included_expr.is_(True))
    if from_dt:
        query = query.filter(datestamp_expr >= from_dt)
    if until_dt:
        query = query.filter(datestamp_expr <= until_dt)

    after_datestamp = state.get("after_datestamp")
    if after_datestamp:
        try:
            after_dt = datetime.fromisoformat(after_datestamp)
            after_id = int(state.get("after_id") or 0)
        except (TypeError, ValueError) as exc:
            raise OaiProtocolError("badResumptionToken", "The resumption token contains an invalid cursor.") from exc
        query = query.filter(or_(
            datestamp_expr > after_dt,
            and_(datestamp_expr == after_dt, CanonicalWork.id > after_id),
        ))

    total = state.get("total")
    if total is None:
        total_query, total_included, total_datestamp = institutional_work_query(config)
        total_query = total_query.filter(total_included.is_(True))
        if from_dt:
            total_query = total_query.filter(total_datestamp >= from_dt)
        if until_dt:
            total_query = total_query.filter(total_datestamp <= until_dt)
        total = total_query.count()
    if not total:
        raise OaiProtocolError("noRecordsMatch", "No records match the requested criteria.")

    page_size = max(min(int(current_app.config.get("OAI_PROVIDER_PAGE_SIZE", 100)), 500), 1)
    rows = query.order_by(datestamp_expr.asc(), CanonicalWork.id.asc()).limit(page_size + 1).all()
    page_rows = rows[:page_size]
    has_more = len(rows) > page_size
    if not page_rows:
        if token_value:
            raise OaiProtocolError("badResumptionToken", "The resumption token no longer identifies a record page.")
        raise OaiProtocolError("noRecordsMatch", "No records match the requested criteria.")

    page = [work_from_query_row(row, config) for row in page_rows]
    metadata_prefix = state.get("metadata_prefix") or "oai_dc"
    container = ET.SubElement(root, _oai(verb))
    for record in page:
        if verb == "ListRecords":
            _append_record(container, record, metadata_prefix=metadata_prefix)
        else:
            _append_header(container, record)

    if has_more or token_value:
        token_element = ET.SubElement(
            container,
            _oai("resumptionToken"),
            {
                "expirationDate": _format_datestamp(
                    utc_now() + timedelta(seconds=_token_max_age()), SECOND_GRANULARITY
                ),
                "completeListSize": str(total),
                "cursor": str(int(state.get("cursor") or 0)),
            },
        )
        if has_more:
            last = page[-1]
            next_state = dict(state)
            next_state.update({
                "after_datestamp": last.datestamp.isoformat(timespec="microseconds"),
                "after_id": last.canonical_work_id,
                "cursor": int(state.get("cursor") or 0) + len(page),
                "total": total,
            })
            token_element.text = _token_serializer().dumps(next_state)


def _append_record(parent, record: InstitutionalWork, metadata_prefix: str) -> None:
    record_element = ET.SubElement(parent, _oai("record"))
    _append_header(record_element, record)
    metadata_element = ET.SubElement(record_element, _oai("metadata"))
    if metadata_prefix == "oai_dc":
        _append_oai_dc_metadata(metadata_element, record)
    elif metadata_prefix == "oai_openaire":
        _append_openaire_metadata(metadata_element, record)
    else:
        _append_mapped_metadata(metadata_element, record)


def _append_oai_dc_metadata(parent, record: InstitutionalWork) -> None:
    dc_element = ET.SubElement(
        parent,
        f"{{{OAI_DC_NS}}}dc",
        {_xsi("schemaLocation"): f"{OAI_DC_NS} {OAI_DC_SCHEMA}"},
    )
    for field in DC_FIELDS:
        for value in record.metadata_json.get(field, []):
            ET.SubElement(dc_element, f"{{{DC_NS}}}{field}").text = _xml_text(value)


def _append_openaire_metadata(parent, record: InstitutionalWork) -> None:
    resource = ET.SubElement(
        parent,
        f"{{{OAIRE_NS}}}resource",
        {_xsi("schemaLocation"): f"{OAIRE_NS} {OAIRE_SCHEMA}"},
    )
    titles = ET.SubElement(resource, f"{{{DATACITE_NS}}}titles")
    title_element = ET.SubElement(titles, f"{{{DATACITE_NS}}}title")
    title_element.text = _xml_text(record.title or record.canonical_key)
    if record.language:
        title_element.set(f"{{{XML_NS}}}lang", record.language)

    creators = ET.SubElement(resource, f"{{{DATACITE_NS}}}creators")
    creator_values = record.creators or ([record.orcid] if record.orcid else ["Unknown"])
    for value in creator_values:
        creator = ET.SubElement(creators, f"{{{DATACITE_NS}}}creator")
        ET.SubElement(
            creator,
            f"{{{DATACITE_NS}}}creatorName",
            {"nameType": "Personal"},
        ).text = _xml_text(value)

    dates = record.metadata_json.get("date", [])
    if dates:
        date_container = ET.SubElement(resource, f"{{{DATACITE_NS}}}dates")
        ET.SubElement(
            date_container,
            f"{{{DATACITE_NS}}}date",
            {"dateType": "Issued"},
        ).text = _xml_text(dates[0])

    ET.SubElement(resource, f"{{{DC_NS}}}language").text = _xml_text(
        record.language or "und"
    )
    publishers = record.metadata_json.get("publisher", [])
    if publishers:
        ET.SubElement(resource, f"{{{DC_NS}}}publisher").text = _xml_text(publishers[0])

    type_label, type_uri, type_general = _openaire_resource_type(record.document_type)
    ET.SubElement(
        resource,
        f"{{{OAIRE_NS}}}resourceType",
        {"resourceTypeGeneral": type_general, "uri": type_uri},
    ).text = type_label

    primary_identifier, identifier_type = _openaire_identifier(record)
    ET.SubElement(
        resource,
        f"{{{DATACITE_NS}}}identifier",
        {"identifierType": identifier_type},
    ).text = _xml_text(primary_identifier)

    rights_uri = (
        "http://purl.org/coar/access_right/c_abf2"
        if record.is_open_access
        else "http://purl.org/coar/access_right/c_14cb"
    )
    rights_label = "open access" if record.is_open_access else "metadata only access"
    ET.SubElement(
        resource,
        f"{{{DATACITE_NS}}}rights",
        {"rightsURI": rights_uri},
    ).text = rights_label

    subjects = record.metadata_json.get("subject", [])
    if subjects:
        subject_container = ET.SubElement(resource, f"{{{DATACITE_NS}}}subjects")
        for value in subjects:
            ET.SubElement(
                subject_container,
                f"{{{DATACITE_NS}}}subject",
            ).text = _xml_text(value)

    for value in record.metadata_json.get("description", []):
        ET.SubElement(resource, f"{{{DC_NS}}}description").text = _xml_text(value)
    for value in record.metadata_json.get("source", []):
        ET.SubElement(resource, f"{{{DC_NS}}}source").text = _xml_text(value)
    if record.journal_title:
        ET.SubElement(
            resource,
            f"{{{OAIRE_NS}}}citationTitle",
        ).text = _xml_text(record.journal_title)
    if record.license_condition:
        ET.SubElement(
            resource,
            f"{{{OAIRE_NS}}}licenseCondition",
        ).text = _xml_text(record.license_condition)


def _append_mapped_metadata(parent, record: InstitutionalWork) -> None:
    mapped = ET.SubElement(
        parent,
        f"{{{MAPPED_NS}}}record",
        {
            _xsi("schemaLocation"): (
                f"{MAPPED_NS} /static/xsd/dataorcid-mapped.xsd"
            ),
            "schemaVersion": "1.0",
        },
    )
    for source_field in METADATA_SOURCE_FIELDS:
        target = record.metadata_mapping.get(source_field)
        if not target:
            continue
        for value in record.metadata_json.get(source_field, []):
            ET.SubElement(
                mapped,
                f"{{{MAPPED_NS}}}field",
                {"name": target, "source": source_field},
            ).text = _xml_text(value)


def _append_header(parent, record: InstitutionalWork) -> None:
    header = ET.SubElement(parent, _oai("header"))
    ET.SubElement(header, _oai("identifier")).text = provider_identifier(record)
    ET.SubElement(header, _oai("datestamp")).text = _format_datestamp(
        record.datestamp, SECOND_GRANULARITY
    )
    ET.SubElement(header, _oai("setSpec")).text = provider_set_spec(record.public_key)
    ET.SubElement(header, _oai("setSpec")).text = "openaire"


def _provider_record(config, identifier: str) -> InstitutionalWork:
    prefix = f"oai:dataorcid-chile:{config.public_key}:"
    if not identifier or not identifier.startswith(prefix):
        raise OaiProtocolError("idDoesNotExist", "The requested identifier does not exist.")
    canonical_key = identifier[len(prefix):]
    if not CANONICAL_KEY_RE.fullmatch(canonical_key):
        raise OaiProtocolError("idDoesNotExist", "The requested identifier does not exist.")
    query, included_expr, _datestamp_expr = institutional_work_query(config)
    row = query.filter(
        CanonicalWork.canonical_key == canonical_key,
        included_expr.is_(True),
    ).first()
    if not row:
        raise OaiProtocolError("idDoesNotExist", "The requested identifier does not exist.")
    return work_from_query_row(row, config)


def _openaire_resource_type(document_type: str | None) -> tuple[str, str, str]:
    """Map local/OpenAlex types to the closest COAR resource type."""
    normalized = re.sub(r"[^a-z0-9]+", "-", str(document_type or "").lower()).strip("-")
    mappings = {
        "article": ("journal article", "http://purl.org/coar/resource_type/c_6501", "literature"),
        "journal-article": ("journal article", "http://purl.org/coar/resource_type/c_6501", "literature"),
        "review": ("review article", "http://purl.org/coar/resource_type/c_dcae04bc", "literature"),
        "book": ("book", "http://purl.org/coar/resource_type/c_2f33", "literature"),
        "book-chapter": ("book part", "http://purl.org/coar/resource_type/c_3248", "literature"),
        "proceedings-article": ("conference paper", "http://purl.org/coar/resource_type/c_5794", "literature"),
        "conference-paper": ("conference paper", "http://purl.org/coar/resource_type/c_5794", "literature"),
        "report": ("report", "http://purl.org/coar/resource_type/c_93fc", "literature"),
        "doctoral-thesis": ("doctoral thesis", "http://purl.org/coar/resource_type/c_db06", "literature"),
        "dissertation": ("doctoral thesis", "http://purl.org/coar/resource_type/c_db06", "literature"),
        "dataset": ("dataset", "http://purl.org/coar/resource_type/c_ddb1", "dataset"),
        "software": ("software", "http://purl.org/coar/resource_type/c_5ce6", "software"),
    }
    return mappings.get(
        normalized,
        ("other", "http://purl.org/coar/resource_type/c_1843", "other research product"),
    )


def _openaire_identifier(record: InstitutionalWork) -> tuple[str, str]:
    if record.doi_normalized:
        return f"https://doi.org/{record.doi_normalized}", "DOI"
    if record.landing_page_url:
        return record.landing_page_url, "URL"
    return f"urn:dataorcid:{record.canonical_key}", "URN"


def _work_dc_metadata(
    *, canonical, local, openalex, title, creators, publication_year,
    document_type, journal_title, landing_page_url,
) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    _add(values, "title", title)
    for creator in creators:
        _add(values, "creator", creator)
    for subject in _scored_labels(getattr(openalex, "keywords", None)) + _scored_labels(
        getattr(openalex, "topics", None)
    ):
        _add(values, "subject", subject)
    _add(values, "publisher", getattr(openalex, "source_host_organization_name", None))
    if getattr(local, "orcid", None):
        _add(values, "contributor", f"https://orcid.org/{local.orcid}")
    publication_date = getattr(openalex, "publication_date", None)
    if not publication_date and publication_year:
        month = str(getattr(local, "pub_month", None) or "").strip()
        day = str(getattr(local, "pub_day", None) or "").strip()
        publication_date = str(publication_year)
        if month.isdigit() and 1 <= int(month) <= 12:
            publication_date += f"-{int(month):02d}"
            if day.isdigit() and 1 <= int(day) <= 31:
                publication_date += f"-{int(day):02d}"
    _add(values, "date", publication_date or publication_year)
    _add(values, "type", document_type)
    if canonical.doi_normalized:
        _add(values, "identifier", canonical.doi_normalized)
        _add(values, "identifier", f"https://doi.org/{canonical.doi_normalized}")
    _add(values, "identifier", landing_page_url)
    if getattr(openalex, "openalex_id", None):
        _add(values, "identifier", f"https://openalex.org/{openalex.openalex_id}")
    _add(values, "source", journal_title)
    _add(values, "source", getattr(local, "issn", None))
    _add(values, "language", getattr(openalex, "language", None))
    _add(values, "relation", getattr(openalex, "primary_pdf_url", None))
    _add(values, "relation", getattr(openalex, "best_pdf_url", None))
    _add(values, "rights", getattr(openalex, "primary_license", None))

    # Additional source fields are available only to the configurable
    # ``dataorcid`` profile. Keeping them separate prevents accidental
    # changes to the fixed oai_dc and OpenAIRE schemas.
    _add(values, "publication_year", publication_year)
    _add(values, "journal_title", journal_title)
    _add(values, "volume", getattr(openalex, "volume", None))
    _add(values, "issue", getattr(openalex, "issue", None))
    _add(values, "first_page", getattr(openalex, "first_page", None))
    _add(values, "last_page", getattr(openalex, "last_page", None))
    _add_split(
        values,
        "corresponding_creator",
        getattr(openalex, "corresponding_author_names", None),
    )

    _add(values, "doi", canonical.doi_normalized)
    if getattr(openalex, "openalex_id", None):
        _add(values, "openalex_id", f"https://openalex.org/{openalex.openalex_id}")
    for orcid in _split_values(getattr(openalex, "author_orcids", None)):
        suffix = orcid.rstrip("/").split("/")[-1]
        _add(values, "creator_orcid", f"https://orcid.org/{suffix}")
    _add_split(values, "issn", getattr(local, "issn", None))
    _add(values, "issn", getattr(openalex, "source_issn_l", None))
    _add_split(values, "issn", getattr(openalex, "source_issns", None))
    _add(values, "pmid", getattr(openalex, "pmid", None))
    _add(values, "pmcid", getattr(openalex, "pmcid", None))
    if getattr(openalex, "source_id", None):
        _add(values, "source_id", f"https://openalex.org/{openalex.source_id}")
    _add(values, "landing_page_url", landing_page_url)
    _add(values, "pdf_url", getattr(openalex, "primary_pdf_url", None))
    _add(values, "pdf_url", getattr(openalex, "best_pdf_url", None))

    _add(values, "license", getattr(openalex, "primary_license", None))
    if openalex is not None:
        _add(
            values,
            "access_right",
            "http://purl.org/coar/access_right/c_abf2"
            if openalex.is_oa
            else "http://purl.org/coar/access_right/c_14cb",
        )
        _add(values, "is_open_access", str(bool(openalex.is_oa)).lower())
    _add(values, "oa_status", getattr(openalex, "oa_status", None))
    _add(values, "source_type", getattr(openalex, "source_type", None))
    _add_split(values, "indexed_in", getattr(openalex, "indexed_in", None))

    _add_split(values, "institution", getattr(openalex, "institution_names", None))
    for ror_id in _split_values(getattr(openalex, "institution_rors", None)):
        suffix = ror_id.rstrip("/").split("/")[-1]
        _add(values, "institution_ror", f"https://ror.org/{suffix}")
    _add_split(values, "country", getattr(openalex, "countries", None))
    _add_structured_labels(values, "funder", getattr(openalex, "funders", None))
    _add_split(values, "award", getattr(openalex, "awards", None), separator=" || ")
    for label in _scored_labels(
        getattr(openalex, "sustainable_development_goals", None)
    ):
        _add(values, "sdg", label)

    for label in _scored_labels(getattr(openalex, "keywords", None)):
        _add(values, "keyword", label)
    for label in _scored_labels(getattr(openalex, "topics", None)):
        _add(values, "topic", label)
    _add(values, "primary_topic", getattr(openalex, "primary_topic_name", None))
    _add(values, "topic_field", getattr(openalex, "primary_topic_field", None))
    _add(values, "topic_domain", getattr(openalex, "primary_topic_domain", None))
    if openalex is not None:
        _add(values, "cited_by_count", openalex.cited_by_count)
    _add(values, "fwci", getattr(openalex, "fwci", None))

    # Country codes are also useful as Dublin Core coverage in the standard
    # profile, while the custom format can emit them under a richer alias.
    for country in values.get("country", []):
        _add(values, "coverage", country)
    return values


def _add(values: dict[str, list[str]], field: str, raw_value) -> None:
    if raw_value is None:
        return
    value = " ".join(str(raw_value).split())
    if value and value not in values.setdefault(field, []):
        values[field].append(value)
    if not values.get(field):
        values.pop(field, None)


def _add_split(
    values: dict[str, list[str]],
    field: str,
    raw_value,
    *,
    separator: str | None = None,
) -> None:
    candidates = (
        str(raw_value or "").split(separator)
        if separator
        else _split_values(raw_value)
    )
    for candidate in candidates:
        _add(values, field, candidate)


def _add_structured_labels(values: dict[str, list[str]], field: str, raw_value) -> None:
    for item in str(raw_value or "").split(" || "):
        parts = [" ".join(part.split()) for part in item.split(" | ") if part.strip()]
        if parts:
            _add(values, field, parts[1] if len(parts) > 1 else parts[0])


def _split_values(raw_value) -> list[str]:
    if not raw_value:
        return []
    if isinstance(raw_value, (list, tuple)):
        candidates = raw_value
    else:
        candidates = re.split(r"\s*;\s*|\s*\|\s*", str(raw_value))
    return [" ".join(str(value).split()) for value in candidates if str(value).strip()]


def _scored_labels(raw_value) -> list[str]:
    """Extract display labels from OpenAlex's flattened scored summaries."""
    labels = []
    for item in str(raw_value or "").split(" || "):
        parts = [" ".join(part.split()) for part in item.split(" | ") if part.strip()]
        if not parts:
            continue
        label = parts[1] if len(parts) > 1 else parts[0]
        if label and label not in labels:
            labels.append(label)
    return labels


def _first_url(*values) -> str | None:
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned.lower().startswith(("https://", "http://")):
            return cleaned
    return None


def _ror_suffix(value: str | None) -> str:
    """Return a comparable ROR suffix from either an ID or a full URL."""
    return str(value or "").strip().rstrip("/").split("/")[-1].lower()


def _safe_year(value) -> int | None:
    try:
        year = int(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return year if 1000 <= year <= 9999 else None


def _optional_datestamp(value: str | None, *, lower: bool):
    if not value:
        return None, None
    try:
        parsed, granularity = _parse_datestamp(value)
    except ValueError as exc:
        raise OaiProtocolError("badArgument", "The request contains an invalid datestamp.") from exc
    if granularity == DAY_GRANULARITY and not lower:
        parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif granularity == SECOND_GRANULARITY and not lower:
        # Database timestamps may carry microseconds while OAI-PMH exposes
        # second-level granularity. Include the complete requested second.
        parsed = parsed.replace(microsecond=999999)
    return parsed, value


def _parse_datestamp(value: str) -> tuple[datetime, str]:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
        return datetime.strptime(value, "%Y-%m-%d"), DAY_GRANULARITY
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value or ""):
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ"), SECOND_GRANULARITY
    raise ValueError("invalid OAI-PMH datestamp")


def _format_datestamp(value: datetime, granularity: str) -> str:
    value = (value or utc_now()).replace(tzinfo=None)
    if granularity == DAY_GRANULARITY:
        return value.strftime("%Y-%m-%d")
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(
        current_app.config["SECRET_KEY"], salt="dataorcid-oai-pmh-resumption-v1"
    )


def _token_max_age() -> int:
    return max(min(int(current_app.config.get("OAI_RESUMPTION_TOKEN_MAX_AGE", 3600)), 86400), 60)


def _load_resumption_token(token: str, config, verb: str) -> dict:
    try:
        state = _token_serializer().loads(token, max_age=_token_max_age())
    except (BadSignature, SignatureExpired) as exc:
        raise OaiProtocolError("badResumptionToken", "The resumption token is invalid or expired.") from exc
    if not isinstance(state, dict) or state.get("ror_id") != config.ror_id or state.get("verb") != verb:
        raise OaiProtocolError("badResumptionToken", "The resumption token does not belong to this request.")
    return state


def _oai(local_name: str) -> str:
    return f"{{{OAI_NS}}}{local_name}"


def _xsi(local_name: str) -> str:
    return f"{{{XSI_NS}}}{local_name}"


def _xml_text(value) -> str:
    return XML_FORBIDDEN_RE.sub("", str(value or ""))
