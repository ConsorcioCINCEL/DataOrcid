"""DSpace-CRIS date compatibility and inclusive OAI harvesting windows."""

from dataclasses import asdict
from datetime import datetime
from xml.etree import ElementTree as ET

from flask import Flask
import pytest

from app import babel, csrf, db
from app.blueprints.oai_pmh import bp_oai_pmh
from app.models import OaiPmhInstitutionConfig, OaiPmhRecordVersion
from app.services.oai_pmh_service import (
    InstitutionalWork,
    OAI_NS,
    _token_serializer,
)


PUBLIC_KEY = "a" * 48
PROVIDER_PATH = f"/oai/{PUBLIC_KEY}"
SET_SPEC = f"institution:{PUBLIC_KEY}"
FROM_SECOND = "2026-10-09T18:58:49"
UNTIL_SECOND = "2026-10-09T19:06:47"
NS = {"oai": OAI_NS}


@pytest.fixture
def provider(monkeypatch):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SECRET_KEY="datestamp-test-key",
        SQLALCHEMY_DATABASE_URI="sqlite://",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        OAI_PROVIDER_PAGE_SIZE=2,
        WTF_CSRF_ENABLED=False,
    )
    db.init_app(app)
    babel.init_app(app)
    csrf.init_app(app)
    app.register_blueprint(bp_oai_pmh)
    monkeypatch.setattr(
        "app.services.oai_pmh_service.utc_now", lambda: datetime(2026, 10, 10, 12)
    )
    stamps = [
        datetime(2026, 10, 9, 18, 58, 48, 999999),  # Immediately before from.
        datetime(2026, 10, 9, 18, 58, 49),
        datetime(2026, 10, 9, 18, 58, 49, 999999),
        datetime(2026, 10, 9, 19, 0, 25, 702936),
        datetime(2026, 10, 9, 19, 6, 47),
        datetime(2026, 10, 9, 19, 6, 47, 999999),
        datetime(2026, 10, 9, 19, 6, 48),  # Immediately after until.
    ]
    identifiers = []
    with app.app_context():
        db.create_all()
        db.session.add(OaiPmhInstitutionConfig(
            ror_id="01aaa1111", public_key=PUBLIC_KEY, provider_enabled=True,
            repository_name="Date compatibility fixture",
            publication_initialized_at=stamps[0],
        ))
        for index, stamp in enumerate(stamps):
            key = f"doi:{index:064x}"
            record = InstitutionalWork(
                canonical_work_id=index + 1, canonical_key=key,
                ror_id="01aaa1111", public_key=PUBLIC_KEY,
                title=f"Article {index}", doi_normalized=f"10.1234/date-{index}",
                publication_year=2026, document_type="article", journal_title=None,
                creators=["Test Author"], orcid=None, language="en",
                landing_page_url=None, is_open_access=False, license_condition=None,
                is_openalex_validated=True, openalex_validation_at=None,
                has_manual_override=False, is_included=True,
                publication_source="test", datestamp=stamp,
                metadata_json={"title": [f"Article {index}"]},
                metadata_mapping={}, affiliations=[],
            )
            payload = asdict(record)
            for field in ("datestamp", "openalex_validation_at", "public_key", "event_id", "is_deleted"):
                payload.pop(field)
            db.session.add(OaiPmhRecordVersion(
                ror_id=record.ror_id, canonical_key=key, datestamp=stamp,
                content_hash=f"{index:064x}", payload_json=payload,
            ))
            identifiers.append(f"oai:dataorcid-chile:{PUBLIC_KEY}:{key}")
        db.session.commit()
    try:
        yield app, app.test_client(), identifiers
    finally:
        with app.app_context():
            db.session.remove()
            db.drop_all()
            db.engine.dispose()


def request_page(client, verb, **params):
    response = client.get(PROVIDER_PATH, query_string={"verb": verb, **params})
    assert response.status_code == 200
    return ET.fromstring(response.data)


def harvest(client, verb, **bounds):
    pages = []
    identifiers = []
    params = (
        bounds if "resumptionToken" in bounds
        else {"metadataPrefix": "oai_dc", "set": SET_SPEC, **bounds}
    )
    while True:
        root = request_page(client, verb, **params)
        assert root.find("oai:error", NS) is None, ET.tostring(root)
        pages.append(root)
        identifiers.extend(node.text for node in root.findall(".//oai:header/oai:identifier", NS))
        token = root.find(f"oai:{verb}/oai:resumptionToken", NS)
        if token is None or not token.text:
            break
        assert len(pages) < 10, "Pagination did not finish"
        params = {"resumptionToken": token.text}
    assert len(identifiers) == len(set(identifiers))
    return identifiers, pages


@pytest.mark.parametrize("verb", ["ListRecords", "ListIdentifiers"])
@pytest.mark.parametrize("from_fraction,until_fraction", [
    ("817301", None), ("817301420", None),
    (None, "126996"), (None, "126996420"),
    ("817301", "126996"), ("817301420", "126996420"),
    ("817301", "126996420"), ("817301420", "126996"),
    ("817301", ""), ("", "126996420"),
])
def test_fractional_from_until_and_mixed_precision(provider, verb, from_fraction, until_fraction):
    _, client, expected = provider
    bounds = {}
    if from_fraction is not None:
        bounds["from"] = FROM_SECOND + (f".{from_fraction}" if from_fraction else "") + "Z"
    if until_fraction is not None:
        bounds["until"] = UNTIL_SECOND + (f".{until_fraction}" if until_fraction else "") + "Z"
    identifiers, pages = harvest(client, verb, **bounds)
    assert identifiers == expected[1 if "from" in bounds else 0:6 if "until" in bounds else 7]
    # Echoed bounds, response/header dates and token expiration stay standard OAI.
    request = pages[0].find("oai:request", NS)
    for bound, second in (("from", FROM_SECOND), ("until", UNTIL_SECOND)):
        if bound in bounds:
            assert request.get(bound) == second + "Z"
    for page in pages:
        for stamp in page.findall("oai:responseDate", NS) + page.findall(".//oai:header/oai:datestamp", NS):
            datetime.strptime(stamp.text, "%Y-%m-%dT%H:%M:%SZ")
        token = page.find(f"oai:{verb}/oai:resumptionToken", NS)
        if token is not None:
            datetime.strptime(token.get("expirationDate"), "%Y-%m-%dT%H:%M:%SZ")


@pytest.mark.parametrize("verb", ["ListRecords", "ListIdentifiers"])
def test_fractional_pagination_keeps_normalized_bounds_and_revision(provider, verb):
    app, client, expected = provider
    first = request_page(
        client, verb, metadataPrefix="oai_dc", set=SET_SPEC,
        **{"from": FROM_SECOND + ".817301Z", "until": UNTIL_SECOND + ".126996420Z"},
    )
    assert first.find("oai:error", NS) is None
    token = first.find(f"oai:{verb}/oai:resumptionToken", NS)
    assert token is not None and token.text
    assert token.get("completeListSize") == "5"
    assert token.get("cursor") == "0"
    with app.app_context():
        state = _token_serializer().loads(token.text)
        assert state["from"] == FROM_SECOND + "Z"
        assert state["until"] == UNTIL_SECOND + "Z"
        source = OaiPmhRecordVersion.query.filter_by(canonical_key="doi:" + f"{3:064x}").one()
        payload = dict(source.payload_json, canonical_key="doi:" + "f" * 64)
        db.session.add(OaiPmhRecordVersion(
            ror_id=source.ror_id, canonical_key=payload["canonical_key"],
            datestamp=source.datestamp, content_hash="f" * 64, payload_json=payload,
        ))
        db.session.commit()
    remaining, pages = harvest(client, verb, resumptionToken=token.text)
    first_ids = [node.text for node in first.findall(".//oai:header/oai:identifier", NS)]
    assert first_ids + remaining == expected[1:6]
    for cursor, page in zip((2, 4), pages, strict=True):
        next_token = page.find(f"oai:{verb}/oai:resumptionToken", NS)
        assert next_token.get("completeListSize") == "5"
        assert next_token.get("cursor") == str(cursor)
    assert not pages[-1].find(f"oai:{verb}/oai:resumptionToken", NS).text
    replay = request_page(client, verb, resumptionToken=token.text)
    assert [node.text for node in replay.findall(".//oai:header/oai:identifier", NS)] == remaining[:2]


@pytest.mark.parametrize("verb", ["ListRecords", "ListIdentifiers"])
def test_interval_is_validated_after_discarding_fractions(provider, verb):
    _, client, expected = provider
    identifiers, _ = harvest(
        client, verb,
        **{"from": UNTIL_SECOND + ".900000000Z", "until": UNTIL_SECOND + ".100000Z"},
    )
    assert identifiers == expected[4:6]
    invalid = request_page(
        client, verb, metadataPrefix="oai_dc",
        **{"from": "2026-10-09T19:06:48.000001Z", "until": UNTIL_SECOND + ".999999999Z"},
    )
    assert invalid.find("oai:error", NS).get("code") == "badArgument"


@pytest.mark.parametrize("verb", ["ListRecords", "ListIdentifiers"])
@pytest.mark.parametrize("bound", ["from", "until"])
@pytest.mark.parametrize("value", [
    "2026-02-30T19:00:25.817301Z", "2026-13-09T19:00:25.126996420Z",
    "2026-10-09T24:00:25.817301Z", "2026-10-09T19:60:25.126996420Z",
    "2026-10-09T19:00:60.817301Z", "2026-10-09T19:00:25.Z",
    "2026-10-09T19:00:25.123xZ", "2026-10-09T19:00:25.817301",
    "2026-10-09T19:00:25.817301+00:00", "2026-10-09T19:00:25,817301Z",
    "2026-10-09T19:00:25.817301z", "2026-10-09.817301Z",
])
def test_invalid_dates_still_return_bad_argument(provider, verb, bound, value):
    _, client, _ = provider
    root = request_page(client, verb, metadataPrefix="oai_dc", **{bound: value})
    assert root.find("oai:error", NS).get("code") == "badArgument"
    assert root.find("oai:request", NS).attrib == {}


@pytest.mark.parametrize("verb", ["ListRecords", "ListIdentifiers"])
@pytest.mark.parametrize("granularity", ["day", "second"])
def test_standard_date_formats_remain_supported(provider, verb, granularity):
    _, client, expected = provider
    bounds = (
        {"from": "2026-10-09", "until": "2026-10-09"}
        if granularity == "day"
        else {"from": FROM_SECOND + "Z", "until": UNTIL_SECOND + "Z"}
    )
    identifiers, _ = harvest(client, verb, **bounds)
    assert identifiers == (expected if granularity == "day" else expected[1:6])
    identify = request_page(client, "Identify")
    assert identify.findtext("oai:Identify/oai:granularity", namespaces=NS) == "YYYY-MM-DDThh:mm:ssZ"


@pytest.mark.parametrize("verb", ["ListRecords", "ListIdentifiers"])
@pytest.mark.parametrize("fractional_bound", ["from", "until"])
def test_mixed_day_and_fractional_second_granularity_is_rejected(provider, verb, fractional_bound):
    _, client, _ = provider
    bounds = {"from": "2026-10-09", "until": "2026-10-09"}
    bounds[fractional_bound] = FROM_SECOND + ".817301420Z"
    root = request_page(client, verb, metadataPrefix="oai_dc", **bounds)
    assert root.find("oai:error", NS).get("code") == "badArgument"
