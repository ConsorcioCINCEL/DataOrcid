"""Exercise private OAI URLs over real HTTP with the optional Sickle client.

Run with application dependencies, pytest, and Sickle 0.7.0 installed:
    python tests/oai_harvester_smoke.py --report /tmp/oai-harvester-report.json

Only a temporary SQLite fixture and a loopback server are used. No configured
institutional provider or external repository is contacted or modified.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
from sickle import Sickle
from werkzeug.serving import make_server

from app import db
from app.models import OaiPmhHarvester, OaiPmhInstitutionConfig
from test_oai_pmh import OaiPmhModuleTest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("/tmp/oai-harvester-report.json"))
    args = parser.parse_args()
    fixture = OaiPmhModuleTest()
    fixture.setUp()
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    server = make_server("127.0.0.1", 0, fixture.app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    fixture.app.config["APP_BASE_URL"] = base
    results = []

    def passed(scenario, **details):
        results.append({"scenario": scenario, "passed": True, **details})
        print(f"PASS {scenario}: {details}", flush=True)

    def denied(url, expected, **options):
        try:
            Sickle(url, timeout=10, **options).Identify()
        except requests.HTTPError as exc:
            assert exc.response.status_code == expected
            assert "no-store" in exc.response.headers["Cache-Control"]
        else:
            raise AssertionError("The harvester unexpectedly obtained metadata")

    try:
        fixture._login(fixture.oai_editor_id, is_oai_user=True)
        editor = requests.Session()
        editor.cookies.set("session", fixture.client.get_cookie("session").value)

        def change(path, **data):
            response = editor.post(base + path, data=data, timeout=10, allow_redirects=False)
            assert response.status_code == 302

        change("/oai-pmh/access/", action="register", base_uri="https://repository.university-a.example")
        with fixture.app.app_context():
            config = OaiPmhInstitutionConfig.query.filter_by(ror_id="01aaa1111").one()
            config.publication_policy = "all"
            item = OaiPmhHarvester.query.filter_by(config_id=config.id).one()
            item_id, key = item.id, item.access_key
            db.session.commit()
        public = base + "/oai/" + "a" * 48
        private = public + "/" + key
        assert Sickle(public, timeout=10).Identify().repositoryName
        change("/oai-pmh/access/", action="policy", restrict_access="on")
        denied(public, 403)
        passed("General URL blocked in restricted mode", http_status=403)
        denied(private + "invalid", 404)
        passed("Incorrect credential rejected", http_status=404)

        client = Sickle(private, timeout=10)
        assert client.Identify().baseURL == private
        assert {item.metadataPrefix for item in client.ListMetadataFormats()} == {"oai_dc", "oai_openaire", "dataorcid"}
        sets = list(client.ListSets())
        assert sets
        records = list(client.ListRecords(metadataPrefix="oai_dc"))
        assert len(records) == 2
        assert {item.metadata["title"][0] for item in records} == {"Article A", "Article A2"}
        assert len(list(client.ListIdentifiers(metadataPrefix="oai_dc"))) == 2
        assert client.GetRecord(identifier=records[0].header.identifier, metadataPrefix="oai_dc").metadata
        passed("All six OAI verbs and automatic pagination", records=2, page_size=1)
        assert len(list(Sickle(private, http_method="POST", timeout=10).ListRecords(metadataPrefix="oai_dc"))) == 2
        passed("Form POST harvesting without a DataORCID session", records=2)

        denied(base + "/oai/" + "b" * 48 + "/" + key, 404)
        assert len(list(Sickle(base + "/oai/" + "b" * 48, timeout=10).ListRecords(metadataPrefix="oai_dc"))) == 1
        passed("University A credential rejected by university B", http_status=404)
        denied(public, 403, headers={"Origin": "https://repository.university-a.example",
                                    "Referer": "https://repository.university-a.example",
                                    "CF-Connecting-IP": "203.0.113.10"})
        passed("Claimed repository URI and proxy headers do not bypass restriction", http_status=403)
        assert Sickle(private, timeout=10, headers={"CF-Connecting-IP": "198.51.100.20"}).Identify().baseURL == private
        passed("Valid URL does not depend on proxy headers")

        pending = Sickle(private, timeout=10).ListRecords(metadataPrefix="oai_dc")
        next(pending)
        change(f"/oai-pmh/access/{item_id}/", action="revoke")
        try:
            next(pending)
        except requests.HTTPError as exc:
            assert exc.response.status_code == 404
        else:
            raise AssertionError("Revoked harvester retrieved the next page")
        denied(public, 403)
        passed("Revocation blocks the next page and keeps the general URL closed", http_status=404)
        change(f"/oai-pmh/access/{item_id}/", action="rotate")
        with fixture.app.app_context():
            new_key = db.session.get(OaiPmhHarvester, item_id).access_key
        denied(private, 404)
        new_private = public + "/" + new_key
        assert len(list(Sickle(new_private, timeout=10).ListRecords(metadataPrefix="oai_dc"))) == 2
        passed("Replacement URL works; the old URL remains revoked", records=2)
        change(f"/oai-pmh/access/{item_id}/", action="delete")
        denied(new_private, 404)
        denied(public, 403)
        passed("Removing the repository closes its access", http_status=404)

        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "client": "Sickle 0.7.0", "transport": "HTTP on loopback",
            "dataset": "Isolated SQLite; two universities; three synthetic articles",
            "production_data_modified": False, "results": results,
        }, indent=2) + "\n")
        print(f"Report: {args.report}")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        fixture.tearDown()


if __name__ == "__main__":
    main()
