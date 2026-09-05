"""Capture the current interface using an isolated, synthetic SQLite dataset.

Run from the repository root with application dependencies and Playwright.
No production account, database, mail service, or API credentials are used.
"""

from __future__ import annotations

import argparse
import hashlib
from io import BytesIO
import json
import logging
import os
from pathlib import Path
import secrets
import sys
import tempfile
import threading

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
WORK = Path(tempfile.mkdtemp(prefix="dataorcid-manual-"))
(WORK / "config.toml").write_text("", encoding="utf-8")
os.environ["ORCID_APP_CONFIG"] = str(WORK / "config.toml")
os.environ["ORCID_DATABASE_URI"] = "sqlite:///" + str(WORK / "demo.sqlite")
os.environ["ORCID_APP_ENV"] = "development"
# Do not inherit source-service credentials from the caller's environment.
for credential_name in ("ORCID_CLIENT_ID", "ORCID_CLIENT_SECRET", "OPENALEX_API_KEY",
                        "MAIL_PASSWORD", "SECRET_KEY", "ORCID_SECRET_KEY",
                        "SECURITY_PASSWORD_SALT", "ORCID_PASSWORD_SALT"):
    os.environ.pop(credential_name, None)

from app import create_app, db
from app.models import (
    CanonicalWork, FundingCache, FundingCacheRun, InstitutionRegistry,
    InstitutionResearcher, OpenAlexInstitutionWorkFact, OpenAlexWorkAuthor,
    OpenAlexWorkInstitution, OpenAlexWorkMetadata, OpenAlexWorkRawCache,
    OaiPmhInstitutionConfig, OaiPmhHarvester, ResearcherCache, ResearcherStatus,
    SystemModule, User, WorkCache, WorkCacheRun, WorkRecordLink, utc_now,
)
from babel.messages.mofile import write_mo
from babel.messages.pofile import read_po
from flask import redirect, session
from playwright.sync_api import sync_playwright
from openpyxl import Workbook
from werkzeug.serving import make_server


def make_app():
    """Create a demonstration application without loading deployed settings."""
    translations = WORK / "translations"
    for language in ("en", "es", "fr", "pt", "de"):
        target = translations / language / "LC_MESSAGES" / "messages.mo"
        target.parent.mkdir(parents=True, exist_ok=True)
        source = ROOT / "app/translations" / language / "LC_MESSAGES/messages.po"
        with source.open("rb") as handle, target.open("wb") as output:
            write_mo(output, read_po(handle))
    app = create_app({
        "TESTING": True, "SECRET_KEY": secrets.token_hex(32),
        "WTF_CSRF_ENABLED": False, "SESSION_COOKIE_SECURE": False,
        "BABEL_TRANSLATION_DIRECTORIES": str(translations),
        "APP_BASE_URL": "https://dataorcid.example.org", "MAIL_ENABLED": False,
        "JOB_EXECUTION_MODE": "queue", "EXPORT_DIRECTORY": str(WORK / "exports"),
    })
    # Keep any disk caches inside this temporary demonstration instance.
    app.instance_path = str(WORK)
    return app


ROR = "01demo001"
NAMES = ["Ana Demo", "Bruno Sample", "Camila Example", "Diego Demo",
         "Elena Sample", "Felipe Example", "Gabriela Demo", "Hugo Sample"]
TITLES = ["Open research metadata and institutional discovery",
          "Sustainable water management in local communities",
          "Collaborative science and public knowledge",
          "Biodiversity observation with reusable datasets",
          "Digital repositories and metadata quality",
          "Environmental monitoring and open infrastructure"]
ORCIDS = [f"0000-0000-0000-{index:04d}" for index in range(1, 9)]


def seed(app):
    """Populate synthetic records, never copied from institutional accounts."""
    with app.app_context():
        assert db.engine.url.drivername == "sqlite"
        db.create_all()
        db.session.add(SystemModule(key="landing_page", is_enabled=False))
        now = utc_now().replace(microsecond=0)
        institution = InstitutionRegistry(ror_id=ROR, name="Universidad de Demostración",
                                          display_name_en="Demonstration University")
        db.session.add(institution)
        accounts = {}
        for role in ("user", "oai"):
            account = User(username=f"{role}.demo", email=f"{role}@example.org",
                           first_name="Alex", last_name="Demo", position="Research information",
                           institution_name=institution.name, ror_id=ROR,
                           is_oai_user=role == "oai", am_client_id="APP-DEMONSTRATION")
            account.set_password(secrets.token_urlsafe(24))
            db.session.add(account)
            accounts[role] = account
        db.session.flush()
        for index, (orcid, name) in enumerate(zip(ORCIDS, NAMES)):
            given, family = name.split()
            db.session.add(ResearcherCache(orcid=orcid, given_names=given,
                family_name=family, credit_name=name, email=f"researcher{index+1}@example.org"))
            db.session.add(InstitutionResearcher(institution_id=institution.id, orcid=orcid,
                matched_by_ror=True, is_verified=True, profile_status="success",
                evidence_sources=["ror"], profile_updated_at=now))
            db.session.add(ResearcherStatus(ror_id=ROR, orcid=orcid, is_managed_by_am=index%2 == 0))
            db.session.add(FundingCache(ror_id=ROR, orcid=orcid, title=f"Demo research project {index+1}",
                type="grant", org_name="Demonstration Research Fund", country="CL",
                start_y=str(2021 + index%5), end_y="2026", grant_number=f"DEMO-{index+1:03d}",
                currency="CLP", amount="1000000", visibility="public"))
        for index in range(48):
            number = index + 1
            orcid = ORCIDS[index % len(ORCIDS)]
            doi = f"10.0000/demo.{number:03d}"
            title = TITLES[index%len(TITLES)] + f" — study {number:02d}"
            year = 2021 + index%6
            oa = ["diamond", "green", "gold", "hybrid", "closed"][index%5]
            source = ["Demonstration Science", "Open Knowledge Review", "Research Data Journal"][index%3]
            field = ["Environmental Science", "Computer Science", "Social Sciences"][index%3]
            local = WorkCache(ror_id=ROR, orcid=orcid, title=title, doi=doi, put_code=number,
                pub_year=str(year), type="journal-article", journal_title=source, visibility="public")
            db.session.add(local)
            db.session.flush()
            canonical = CanonicalWork(canonical_key="doi:"+hashlib.sha256(doi.encode()).hexdigest(),
                doi_normalized=doi, title=title, title_normalized=title.lower(),
                publication_year=year, record_count=1)
            db.session.add(canonical)
            db.session.flush()
            db.session.add(WorkRecordLink(canonical_work_id=canonical.id, work_cache_id=local.id,
                ror_id=ROR, orcid=orcid, source_record_key=f"put:{number}"))
            db.session.add(OpenAlexWorkMetadata(doi_normalized=doi, openalex_id=f"W900000{number}",
                title=title, publication_year=year, type="article", language="en" if index%3 else "es",
                cited_by_count=number*3, fwci=round(.4 + (index%10)*.18, 2),
                is_oa=oa != "closed", oa_status=oa, source_name=source,
                primary_topic_field=field, primary_topic_domain="Physical Sciences",
                author_names=NAMES[index%8], author_orcids=orcid,
                institution_names=institution.name, institution_rors=ROR, countries="CL; US",
                author_count=2, institution_count=2, country_count=2))
            db.session.add(OpenAlexInstitutionWorkFact(ror_id=ROR, openalex_cache_key=doi,
                representative_work_cache_id=local.id, source_record_count=1, has_valid_doi=True,
                has_local_title=True, raw_status="success", openalex_id=f"W900000{number}",
                title=title, publication_year=year, document_type="article",
                language="en" if index%3 else "es", cited_by_count=number*3,
                fwci=round(.4+(index%10)*.18, 2), is_oa=oa != "closed", oa_status=oa,
                source_name=source, primary_topic_field=field, primary_topic_domain="Physical Sciences",
                has_selected_affiliation=index%4 != 0, has_chile_affiliation=True,
                has_non_chile_affiliation=True, has_international_collaboration=True,
                author_count=2, institution_count=2))
            payload = {"id": f"https://openalex.org/W900000{number}", "doi": f"https://doi.org/{doi}",
                "title": title, "display_name": title, "publication_year": year, "type": "article",
                "language": "en" if index%3 else "es", "cited_by_count": number*3,
                "fwci": round(.4+(index%10)*.18, 2),
                "open_access": {"is_oa": oa != "closed", "oa_status": oa},
                "primary_location": {"source": {"display_name": source, "type": "journal"}},
                "primary_topic": {"display_name": field, "field": {"display_name": field},
                                  "domain": {"display_name": "Physical Sciences"}},
                "authorships": []}
            db.session.add(OpenAlexWorkRawCache(doi_normalized=doi, source_doi=doi,
                status="success", http_status=200, openalex_id=f"W900000{number}", raw_json=payload))
            for country, name, ror in [("CL", institution.name, ROR), ("US", "Partner Demo University", "02demo002")]:
                db.session.add(OpenAlexWorkInstitution(doi_normalized=doi, openalex_id=f"W900000{number}",
                    institution_id="I"+ror, institution_name=name, ror_id=ror,
                    country_code=country, author_count=1))
            db.session.add(OpenAlexWorkAuthor(doi_normalized=doi, openalex_id=f"W900000{number}",
                author_id=f"A900000{index%8}", author_name=NAMES[index%8], orcid=orcid,
                has_chile_affiliation=True, countries=["CL"], institution_rors=[ROR]))
        db.session.add(WorkCacheRun(ror_id=ROR, status="success", rows_count=48, started_at=now, finished_at=now))
        db.session.add(FundingCacheRun(ror_id=ROR, status="success", rows_count=8, started_at=now, finished_at=now))
        oai_config = OaiPmhInstitutionConfig(ror_id=ROR, public_key="demonstration-key-not-a-live-provider",
            provider_enabled=True, repository_name="DataORCID — Demonstration University",
            admin_email="repository@example.org", publication_policy="validated")
        db.session.add(oai_config)
        db.session.flush()
        db.session.add(OaiPmhHarvester(config_id=oai_config.id,
            base_uri="https://repository.example.edu", access_key="demonstration-only-not-a-valid-access-key"))
        db.session.commit()
        return {role: account.id for role, account in accounts.items()}


def demo_profile(orcid, **kwargs):
    """Return a public-profile-shaped illustration without an ORCID API call."""
    works = [{"work-summary": [{"title": {"title": {"value": title}},
              "type": "journal-article", "publication-date": {"year": {"value": "2025"}},
              "external-ids": {"external-id": [{"external-id-type": "doi",
                                   "external-id-value": f"10.0000/demo.{index:03d}"}]}}]}
             for index, title in enumerate(TITLES, 1)]
    return {"orcid-identifier": {"path": orcid}, "person": {
        "name": {"given-names": {"value": "Ana"}, "family-name": {"value": "Demo"}},
        "biography": {"content": "Demonstration profile for the Data ORCID-Chile user manual."}},
        "activities": {"works": {"group": works}}}


PAGES = [
    ("overview", "/"), ("directory", "/researcher-list"),
    ("portfolio", "/orcid-profile/"+ORCIDS[0]),
    ("orcid-overview", "/metrics-panel"),
    ("orcid-publications", "/metrics-panel?section=publications"),
    ("orcid-funding", "/metrics-panel?section=funding"),
    ("orcid-researchers", "/metrics-panel?section=researchers"),
    ("downloads", "/cache/works/status"), ("quality", "/data-quality"),
    ("duplicates", "/duplicates/"), ("enrichment", "/openalex/works"),
    ("openalex-overview", "/openalex/analytics"),
    ("openalex-oa", "/openalex/analytics?section=open_access"),
    ("openalex-collaboration", "/openalex/analytics?section=collaboration"),
    ("openalex-topics", "/openalex/analytics?section=topics"),
    ("openalex-impact", "/openalex/analytics?section=impact"),
    ("read-api", "/integration-orcid/pull"), ("write-orcid", "/integration_guide"),
    ("affiliation-manager", "/auth/am-settings"), ("resources", "/resources"),
    ("help", "/help/"), ("profile", "/auth/profile"), ("password", "/auth/change-password"),
    ("oai-overview", "/oai-pmh/"), ("oai-articles", "/oai-pmh/articles/"),
    ("oai-metadata", "/oai-pmh/metadata/"), ("oai-import", "/oai-pmh/doi-import/"),
    ("oai-access", "/oai-pmh/access/"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", help="Comma-separated screenshot names to refresh")
    parser.add_argument("--languages", default="en,es,fr,pt,de", help="Comma-separated edition languages to capture")
    args = parser.parse_args()
    selected = set(args.only.split(",")) if args.only else None
    languages = args.languages.split(",")
    if not languages or any(language not in {"en", "es", "fr", "pt", "de"} for language in languages):
        parser.error("Supported languages: en, es, fr, pt, de")
    app = make_app()
    account_ids = seed(app)
    import app.blueprints.main as main_views
    main_views.get_full_orcid_profile = demo_profile
    token = secrets.token_urlsafe(24)

    @app.get("/__manual/<provided>/<language>/<role>")
    def open_demo_session(provided, language, role):
        if provided != token:
            return "Not found", 404
        session.clear()
        session["locale"] = language
        if role in account_ids:
            session.update(logged_in=True, user_id=account_ids[role], username=role+".demo",
                email=role+"@example.org", ror_id=ROR, is_oai_user=role == "oai",
                institution_name={"en": "Demonstration University", "es": "Universidad de Demostración",
                                  "fr": "Université de démonstration", "pt": "Universidade de demonstração",
                                  "de": "Demonstrationsuniversität"}[language])
        return redirect("/")

    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    server = make_server("127.0.0.1", 0, app, threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    report = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path="/usr/bin/google-chrome", headless=True,
                                             args=["--no-sandbox"])
        for language in languages:
            context = browser.new_context(viewport={"width": 1440, "height": 1000},
                                          device_scale_factor=1.5, ignore_https_errors=True)
            page = context.new_page()
            output = ROOT / "manuals/assets/screenshots" / language
            output.mkdir(parents=True, exist_ok=True)

            def capture(name, path, scroll=None):
                if selected and name not in selected:
                    return
                response = page.goto(base + path, wait_until="networkidle", timeout=60000)
                if response is not None and response.status != 200:
                    raise RuntimeError(f"{name}: HTTP {response.status}: {path}")
                page.evaluate("document.fonts.ready")
                page.wait_for_timeout(900)
                if scroll:
                    target = page.locator(scroll).first
                    if name == "oai-metadata":
                        target.evaluate("element => element.scrollIntoView({block: 'start'})")
                    else:
                        target.scroll_into_view_if_needed()
                # Stop animation before freezing the real application view.
                page.add_style_tag(content="* {animation: none !important; transition: none !important;}")
                page.screenshot(path=str(output / (name+".png")), full_page=False)
                report.append({"language": language, "name": name, "path": path,
                               "title": page.title(), "status": response.status if response else 200})
                print(language, name, response.status if response else 200, flush=True)

            page.goto(f"{base}/__manual/{token}/{language}/public")
            capture("login", "/auth/login")
            page.goto(f"{base}/__manual/{token}/{language}/user")
            for name, path in PAGES:
                if name == "oai-overview":
                    page.goto(f"{base}/__manual/{token}/{language}/oai")
                if name == "oai-import":
                    workbook = Workbook()
                    worksheet = workbook.active
                    worksheet.append(["DOI"])
                    for number in (1, 2, 3):
                        worksheet.append([f"10.0000/demo.{number:03d}"])
                    buffer = BytesIO()
                    workbook.save(buffer)
                    response = page.request.post(base+"/oai-pmh/works/doi-import", multipart={
                        "doi_file": {"name": "demo-doi-import.xlsx",
                                     "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                     "buffer": buffer.getvalue()}})
                    assert response.ok, response.status
                capture(name, path, scroll={
                    "oai-access": ".oai-harvester",
                    "oai-metadata": "#oaiMappingTitle",
                }.get(name))
                if name == "downloads":
                    response = page.request.get(base+"/researcher-list/export?format=excel&background=1")
                    assert response.ok, response.text()
                    from app.services.background_jobs import run_queued_job
                    run_queued_job(app, worker_id="manual-demonstration")
                    capture("exports", "/researcher-list")
                    response = page.request.post(base+"/exports/jobs/clear")
                    assert response.ok, response.status
                if name == "openalex-oa":
                    capture("openalex-oa-tables", path, scroll=".table")
                if name == "oai-import":
                    from app.models import OaiPmhDoiImportBatch
                    with app.app_context():
                        batch_id = db.session.query(db.func.max(OaiPmhDoiImportBatch.id)).scalar()
                    assert batch_id
                    capture("oai-audit", f"/oai-pmh/doi-import/{batch_id}/")
            context.close()
        browser.close()
    server.shutdown()
    report_path = ROOT / "manuals/source/capture-report.json"
    if report_path.exists():
        previous = json.loads(report_path.read_text())
        replacements = {(item["language"], item["name"]): item for item in report}
        report = [replacements.pop((item["language"], item["name"]), item) for item in previous] + list(replacements.values())
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
