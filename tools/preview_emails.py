"""Render safe email samples and optionally send them to one explicit recipient.

Examples:
    python tools/preview_emails.py
    python tools/preview_emails.py --send-to recipient@example.org

Messages contain demonstration data and nonfunctional credentials/reset links.
No account is created or modified. SMTP settings come from the application.
"""

import argparse
from email.headerregistry import Address
import json
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.services.transactional_email import (
    render_contact_email, render_credentials_email, render_password_reset_email,
)
from app.utils.emailer import send_email


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--send-to", help="Send sample messages to this single address")
    parser.add_argument("--base-url", help="Site URL for links; defaults to app.base_url or the local test service")
    parser.add_argument("--output-directory", type=Path, default=Path("/tmp/dataorcid-email-previews"))
    parser.add_argument("--only", help="Comma-separated sample numbers, e.g. 01,02 to retry only welcomes")
    args = parser.parse_args()
    if args.send_to:
        recipient = Address(addr_spec=args.send_to)
        if not recipient.username or not recipient.domain:
            parser.error("Provide one complete recipient email address")
    args.output_directory.mkdir(parents=True, exist_ok=True)
    app = create_app()
    demo_base = (args.base_url or app.config.get("APP_BASE_URL") or "http://127.0.0.1:5000").rstrip("/")
    app.config["APP_BASE_URL"] = demo_base
    results = []
    with app.test_request_context(base_url=demo_base):
        spanish_user = SimpleNamespace(
            username="usuario.demo@example.org", first_name="Gastón", locale="es",
            institution_name="Universidad de Demostración",
        )
        english_user = SimpleNamespace(
            username="user.demo@example.org", first_name="Gastón", locale="en",
            institution_name="Demonstration University",
        )
        samples = [
            ("01-welcome-es", render_credentials_email(spanish_user, "DEMO-No-es-una-clave-real", demo_base + "/auth/login", welcome=True, preview=True)),
            ("02-welcome-en", render_credentials_email(english_user, "DEMO-Not-a-real-password", demo_base + "/auth/login", welcome=True, preview=True)),
            ("03-password-reset-es", render_password_reset_email(spanish_user, demo_base + "/auth/reset-password/demonstration-only", preview=True)),
            ("04-contact-es", render_contact_email(SimpleNamespace(
                name="Alex Demo", email="persona.demo@example.org", institution="Universidad de Demostración",
                message="Hola, quisiera conocer las opciones de integración con nuestro repositorio institucional.\n\nEste mensaje contiene únicamente datos ficticios para revisar el diseño del correo.",
            ), "Integración OAI-PMH", language="es", preview=True)),
        ]
        for name, message in samples:
            if args.only and name[:2] not in args.only.split(","):
                continue
            message["subject"] = f"[PRUEBA DataORCID · enlace PDF · {name[:2]}/04] {message['subject']}"
            (args.output_directory / f"{name}.html").write_text(message["html"], encoding="utf-8")
            (args.output_directory / f"{name}.txt").write_text(message["text"], encoding="utf-8")
            result = {
                "sample": name, "subject": message["subject"],
                "attachments": [],
                "manual_link": demo_base + ("/manuals/user-guide/es.pdf" if name.endswith("es") else "/manuals/user-guide/en.pdf") if "welcome" in name else None,
            }
            if args.send_to:
                success, error = send_email(to_email=args.send_to, reply_to=args.send_to, **message)
                result.update(smtp_accepted=success, error=error)
                print(name, "SMTP accepted" if success else "SEND FAILED", flush=True)
            else:
                print(name, "preview saved", flush=True)
            results.append(result)
            report_path = args.output_directory / "report.json"
            reported_results = results
            if args.only and report_path.exists():
                previous = json.loads(report_path.read_text())
                replacements = {item["sample"]: item for item in results}
                reported_results = [replacements.pop(item["sample"], item) for item in previous] + list(replacements.values())
            report_path.write_text(json.dumps(reported_results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if any(item.get("smtp_accepted") is False for item in results):
        raise SystemExit("At least one email was not accepted by SMTP; inspect report.json.")


if __name__ == "__main__":
    main()
