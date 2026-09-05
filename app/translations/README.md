# Localization maintenance

The application supports English, Spanish, French, Brazilian Portuguese, and
German. Keep source messages, identifiers, filenames, and code in English.
Translate user-facing messages through Babel, including email text and the
browser presentation of OAI-PMH XML.

The September 2026 contextual review covered all 2,427 current source messages
in the five catalogs. It checked meaning against the calling page, consistent
controls, grammatical agreement, technical identifiers, and metadata terminology.
Catalog coverage alone is not a linguistic quality guarantee. Review each new
message in context and check its rendered layout before releasing it.

## Shared terminology

| Concept | English | Spanish | French | Portuguese | German |
| --- | --- | --- | --- | --- | --- |
| Manual link | User manual | Manual de usuario | Manuel d’utilisation | Manual do usuário | Benutzerhandbuch |
| Sign-in action | Sign in | Iniciar sesión | Connexion | Entrar | Anmelden |
| Institutional scope | Institution | Institución | Établissement | Instituição | Einrichtung |
| Metadata mapping | Metadata mapping | Mapeo de metadatos | Correspondance des métadonnées | Mapeamento de metadados | Metadatenzuordnung |
| Harvesting | Harvesting | Recolección / cosecha | Moissonnage | Coleta | Metadatenernte |
| Expose an OAI article | Expose | Exponer | Exposer | Expor | Freigeben |
| Matching records | Matches | Coincidencias | Correspondances | Correspondências | Übereinstimmungen |
| Citation metric | Citations | Citas | Citations | Citações | Zitationen |

Use direct, concise wording. Spanish addresses the reader as `tú`; French uses
`vous`; German uses `Sie`; Portuguese follows Brazilian usage. Context can require
inflection or a more specific noun. A public ORCID funding record means a publicly
visible record, not necessarily government funding. An institutional affiliation
describes a researcher's association with an institution, not a subscription.

Preserve brands and technical values such as `Data ORCID-Chile`, `OpenAlex`,
`ORCID`, `ISSN`, `PMID`, `oa_status`, `cited_by_count`, `oai_dc`, and `oai_openaire`.
Translate category labels, but retain literal API values when describing fields
(for example, `diamond`, `green`, and `is_oa=true`). Preserve placeholders, URLs,
filenames used in instructions, and HTML formatting. Do not translate research
titles, author names, or metadata supplied by external sources.

## Validation and documentation

Run from the repository root:

```sh
venv/bin/pybabel extract -F config/babel.cfg -k lazy_gettext -o /tmp/dataorcid-messages.pot .
venv/bin/pybabel compile -d app/translations
venv/bin/python -m unittest tests.test_interface_helpers tests.test_oai_pmh tests.test_transactional_email
```

The interface test extracts the current source rather than comparing one catalog
with another. It checks coverage, empty and fuzzy translations, placeholders,
technical tokens, formatting tags, and the visible manual labels. The OAI tests
transform responses with each localized stylesheet while retaining protocol data.

Check the header in all five languages at narrow mobile and desktop widths.
Its manual link uses the active interface language; the institution moves to a
second row on small screens. Keep keyboard focus visible and labels readable.

When a control name or user workflow changes, update the corresponding content
in `manuals/source/content.py` and `manuals/source/locales/`. Regenerate the
synthetic screenshots and all five PDF editions using the instructions in
`manuals/README.md`. Inspect the resulting page layouts and download links.
The manuals cover User and OAI User roles only.
