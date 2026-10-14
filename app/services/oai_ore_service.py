"""Describe reusable OpenAlex PDFs as DSpace-compatible OAI-ORE resource maps."""

from __future__ import annotations

import ipaddress
import re
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from xml.etree import ElementTree as ET


ATOM_NS = "http://www.w3.org/2005/Atom"
ORE_NS = "http://www.openarchives.org/ore/terms/"
ORE_ATOM_NS = "http://www.openarchives.org/ore/atom/"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
DCTERMS_NS = "http://purl.org/dc/terms/"
DSPACE_NS = "http://www.dspace.org/objectModel/"
REUSABLE_PDF_LICENSES = {"cc-by", "cc-by-sa", "cc0", "public-domain"}
PDF_VERSION_ORDER = {"publishedVersion": 0, "acceptedVersion": 1, "submittedVersion": 2}


ET.register_namespace("atom", ATOM_NS)
ET.register_namespace("oreatom", ORE_ATOM_NS)
ET.register_namespace("rdf", RDF_NS)
ET.register_namespace("dcterms", DCTERMS_NS)


def select_ore_files(locations) -> list[dict]:
    """Choose one reusable, public PDF without downloading external content."""
    if not isinstance(locations, list):
        return []
    candidates = [location for location in locations if isinstance(location, dict)]
    candidates.sort(key=lambda location: PDF_VERSION_ORDER.get(str(location.get("version")), 3))
    for location in candidates:
        license_condition = location.get("license")
        if (
            location.get("is_oa") is not True
            or not isinstance(license_condition, str)
            or license_condition not in REUSABLE_PDF_LICENSES
        ):
            continue
        url = _public_document_url(location.get("pdf_url"))
        if url:
            return [{
                "url": url,
                "license": license_condition,
                "version": location.get("version") if location.get("version") in (
                    "publishedVersion", "acceptedVersion", "submittedVersion",
                ) else None,
            }]
    return []


def ore_schema_url(base_url: str) -> str:
    """Keep the advertised schema on the provider's configured origin."""
    origin = base_url.rstrip("/").rsplit("/oai/", 1)[0] if "/oai/" in base_url else ""
    return f"{origin}/static/xsd/ore-atom.xsd"


def ore_resource_url(base_url: str, canonical_key: str) -> str:
    """Preserve the authorized harvesting path in resource-map links."""
    return f"{base_url.rstrip('/')}/ore/{quote(canonical_key, safe=':')}"


def append_ore_metadata(parent, record, base_url: str) -> None:
    """Emit Atom links and bundle metadata understood by DSpace's ORE importer."""
    from .oai_pmh_service import (
        SECOND_GRANULARITY, XSI_NS, _format_datestamp, _xml_text, provider_identifier,
    )

    resource_url = ore_resource_url(base_url, record.canonical_key)
    aggregation_url = f"{resource_url}#aggregation"
    datestamp = _format_datestamp(record.datestamp, SECOND_GRANULARITY)
    entry = ET.SubElement(
        parent, f"{{{ATOM_NS}}}entry",
        {f"{{{XSI_NS}}}schemaLocation": f"{ATOM_NS} {ore_schema_url(base_url)}"},
    )
    ET.SubElement(entry, f"{{{ATOM_NS}}}id").text = (
        f"urn:dataorcid:ore:{record.public_key}:{record.canonical_key}"
    )
    ET.SubElement(entry, f"{{{ATOM_NS}}}title").text = _xml_text(record.title or record.canonical_key)
    ET.SubElement(entry, f"{{{ATOM_NS}}}updated").text = datestamp
    ET.SubElement(entry, f"{{{ATOM_NS}}}published").text = datestamp
    for name in record.creators or ["Unknown"]:
        author = ET.SubElement(entry, f"{{{ATOM_NS}}}author")
        ET.SubElement(author, f"{{{ATOM_NS}}}name").text = _xml_text(name)
    source = ET.SubElement(entry, f"{{{ATOM_NS}}}source")
    author = ET.SubElement(source, f"{{{ATOM_NS}}}author")
    ET.SubElement(author, f"{{{ATOM_NS}}}name").text = "Data ORCID-Chile"

    metadata_params = {
        "verb": "GetRecord", "metadataPrefix": "oai_dc", "identifier": provider_identifier(record),
    }
    metadata_url = f"{base_url}?{urlencode(metadata_params)}"
    landing_page_url = _public_document_url(record.landing_page_url)
    for relation, url, mime_type in (
        ("self", resource_url, "application/atom+xml"),
        (ORE_NS + "describes", aggregation_url, None),
        ("alternate", landing_page_url or metadata_url, "text/html" if landing_page_url else "text/xml"),
    ):
        attributes = {"rel": relation, "href": url}
        if mime_type:
            attributes["type"] = mime_type
        ET.SubElement(entry, f"{{{ATOM_NS}}}link", attributes)
    ET.SubElement(entry, f"{{{ATOM_NS}}}category", {
        "scheme": ORE_NS, "term": ORE_NS + "Aggregation", "label": "Aggregation",
    })

    triples = ET.Element(f"{{{ORE_ATOM_NS}}}triples")
    for document in record.ore_files:
        if not isinstance(document, dict):
            continue
        # Validate snapshots too: old or imported payloads must not expose keys.
        files = select_ore_files([{
            "pdf_url": document.get("url"), "is_oa": True,
            "license": document.get("license"), "version": document.get("version"),
        }])
        if not files:
            continue
        document = files[0]
        filename = f"{record.canonical_key.replace(':', '-')}.pdf"
        ET.SubElement(entry, f"{{{ATOM_NS}}}link", {
            "rel": ORE_NS + "aggregates", "href": document["url"],
            "title": filename, "type": "application/pdf",
        })
        description = ET.SubElement(triples, f"{{{RDF_NS}}}Description", {
            f"{{{RDF_NS}}}about": document["url"],
        })
        ET.SubElement(description, f"{{{RDF_NS}}}type", {
            f"{{{RDF_NS}}}resource": DSPACE_NS + "DSpaceBitstream",
        })
        ET.SubElement(description, f"{{{DCTERMS_NS}}}description").text = "ORIGINAL"
        ET.SubElement(description, f"{{{DCTERMS_NS}}}license").text = document["license"]
        if document.get("version"):
            ET.SubElement(description, f"{{{DCTERMS_NS}}}hasVersion").text = _xml_text(document["version"])
        # A single selected copy avoids importing repository/publisher mirrors twice.
        break
    entry.append(triples)


def _public_document_url(value) -> str | None:
    """Reject private addresses and credentials; never resolve or fetch a URL."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    if (
        not value or len(value) > 4096 or "\\" in value
        or any(char.isspace() or ord(char) < 32 for char in value)
        or re.search("[\ud800-\udfff\ufffe\uffff]", value)
    ):
        return None
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").rstrip(".").encode("idna").decode("ascii").lower()
        if parsed.scheme not in {"http", "https"} or not host or parsed.fragment:
            return None
        if parsed.username is not None or parsed.password is not None:
            return None
        if parsed.port not in {None, 80, 443}:
            return None
        if host == "content.openalex.org" or host.endswith((".localhost", ".local", ".internal", ".lan")):
            return None
        try:
            address = ipaddress.ip_address(host)
            if not address.is_global:
                return None
        except ValueError:
            if "." not in host or host.rsplit(".", 1)[-1].isdigit() or any(
                not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                for label in host.split(".")
            ):
                return None
        for name, _value in parse_qsl(parsed.query, keep_blank_values=True):
            normalized = re.sub(r"[^a-z0-9]", "", name.lower())
            if any(secret in normalized for secret in ("apikey", "token", "password", "secret", "signature", "credential", "authorization")):
                return None
        return value
    except (ValueError, UnicodeError):
        return None
