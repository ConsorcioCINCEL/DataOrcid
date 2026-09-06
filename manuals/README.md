# Data ORCID-Chile user manuals

These matching English, Spanish, French, Portuguese, and German editions document **Data ORCID-Chile 2.1**
for **User** and **OAI User** accounts only. Each PDF has 41 US Letter pages,
33 numbered figures, a linked contents page, and PDF bookmarks.

| Language | Published PDF | Editable text |
| --- | --- | --- |
| English | [User manual](dataorcid-chile-user-manual-v2.1-en.pdf) | [Markdown](dataorcid-chile-user-manual-v2.1-en.md) |
| Español | [Manual de usuario](dataorcid-chile-user-manual-v2.1-es.pdf) | [Markdown](dataorcid-chile-user-manual-v2.1-es.md) |
| Français | [Manuel d’utilisation](dataorcid-chile-user-manual-v2.1-fr.pdf) | [Markdown](dataorcid-chile-user-manual-v2.1-fr.md) |
| Português | [Manual do usuário](dataorcid-chile-user-manual-v2.1-pt.pdf) | [Markdown](dataorcid-chile-user-manual-v2.1-pt.md) |
| Deutsch | [Benutzerhandbuch](dataorcid-chile-user-manual-v2.1-de.pdf) | [Markdown](dataorcid-chile-user-manual-v2.1-de.md) |

All five editions follow the supplied **manual-data-orcid-chile-v1.pdf** reference:
CINCEL branding, Barlow typography, orange rules and callouts, framed screenshots,
dark table headers, numbered sections, and a contact back page. They retain the
reference's explanatory style and eleven main chapters. The public landing page
and contact form are deliberately outside the manual's scope.

New coverage includes background exports, file expiry and cleanup, OAI-PMH
article selection, metadata mapping, DOI activation, import auditing and undo, private harvesting URLs,
and help filtered by enabled modules. No administrator or institutional manager
operating instructions are included.

Account welcome emails and password-setting link emails reference these PDFs at
`/manuals/user-guide/<language>.pdf`, where the language is `en`, `es`, `fr`, `pt`,
or `de`. Each account receives the link in its preferred language. Regional
variants use their base language; unsupported preferences fall back to English.
Downloads work without signing in and expose only these five PDFs. Include this
folder when deploying the application. The stable URLs resolve the configured
application version and revalidate cached downloads when the files change.
Signed-in users also have a compact, localized “User manual” link with a PDF icon
beside their account options in the toolbar. It follows the current interface
language and is described in section 2.2. On mobile, the institution has a separate
row so the manual label and account controls remain readable.

## Sources and review

- Behavior: application source based on commit `d216e4d`, including the private
  harvesting access update, version `2.1`, reviewed on
  September 4, 2026. Primary references are `app/blueprints/`,
  `app/templates/help/index.html`, `app/templates/oai_pmh/`,
  `app/services/export_jobs.py`, `app/services/module_access.py`, and
  `app/decorators.py`.
- Design, CINCEL logo, and contact details: the user-supplied v1 PDF. The logo was
  extracted with its transparency mask and composited on white, without redesign.
- Fonts: Barlow Regular, Bold, and Italic from the Google Fonts Barlow package;
  the SIL Open Font License is included in `assets/fonts/OFL.txt`.
- Screenshots: the actual application templates and styles, rendered separately
  in each of the five languages against a temporary SQLite database with fictional
  accounts, researchers, identifiers, and publications. No production records,
  account credentials, or live provider URLs are reproduced.
- Demonstrated export generation and DOI import use the real application routes
  against the temporary database. `source/capture-report.json` records the
  successful screenshot routes. All OAI screenshots use the OAI User role.
- All five PDFs were rendered and visually reviewed. `source/build-report.json`
  records page and figure counts and the bottom position of each content page;
  the generator refuses content that overlaps the footer.

The PDF binaries are intentionally allowed by this directory's `.gitignore`;
the repository's general exclusion of research-data PDFs remains in place.

## Rebuild the manuals

The shared page structure and English/Spanish text live in `source/content.py`.
French, Portuguese, and German text live in `source/locales/{fr,pt,de}.json`;
cover, contents, and contact labels live in `source/labels.py`. Update all five
editions together and regenerate the Markdown and PDFs. The generator requires
every page, paragraph, caption, step, note, and table cell to have a translation
with the same structure, and refuses incomplete editions. Direct changes to
generated Markdown will be overwritten. Identifiers, filenames, and code
comments remain in English throughout the documentation tooling.

Create a separate documentation environment from the repository root:

```bash
python3 -m venv /tmp/dataorcid-manual-env
/tmp/dataorcid-manual-env/bin/pip install -r manuals/source/requirements.txt
/tmp/dataorcid-manual-env/bin/python manuals/source/build_manuals.py
```

Existing screenshots and fonts are sufficient for rebuilding the documents.
The output uses US Letter paper (612 × 792 pt) and embeds the fonts.

## Refresh screenshots

Install the application's dependencies into the same separate environment:

```bash
/tmp/dataorcid-manual-env/bin/pip install -r requirements.txt
/tmp/dataorcid-manual-env/bin/python manuals/source/capture_screenshots.py
# Optionally refresh specific languages or screens:
/tmp/dataorcid-manual-env/bin/python manuals/source/capture_screenshots.py --languages fr,pt,de --only oai-access
/tmp/dataorcid-manual-env/bin/python manuals/source/build_manuals.py
```

The capture script expects Google Chrome at `/usr/bin/google-chrome`. It starts
a temporary local server bound to `127.0.0.1` on an automatically selected port.
Its SQLite database, configuration, exports, and compiled translation catalogs
are created in a temporary directory. Application CSS, scripts, and webfonts
referenced by the templates require network access during capture. Neither the
deployed application nor its database is used.

Keep screenshot filenames and page keys in English. Review the full PDFs after
editing content or refreshing screenshots, especially page breaks, figure
captions, translated button labels, and the distinction between User and OAI User.
