# OAI-ORE PDF harvesting

The institutional provider supports `ore` alongside `oai_dc`, `oai_openaire`, and
`dataorcid`. The format uses the Atom namespace expected by DSpace's ORE ingestion
crosswalk: `http://www.w3.org/2005/Atom`.

ORE describes files; it does not embed PDF bytes in the OAI-PMH response. A
compatible harvester reads each `atom:link` whose relation is
`http://www.openarchives.org/ore/terms/aggregates`, then downloads its HTTP(S)
target. Data ORCID-Chile neither downloads nor hosts those documents.

## File selection

The publication process selects at most one PDF from the locally cached OpenAlex
`locations` array. A candidate must have `is_oa: true`, an explicit `pdf_url`, and
one of these declared licenses: `cc-by`, `cc-by-sa`, `cc0`, or `public-domain`.
Published versions are preferred over accepted manuscripts and submitted
versions. The file's own license and version are retained; the publisher's
license is never substituted for a repository copy's license.

Unknown, noncommercial, no-derivatives, and publisher-specific licenses are
excluded from automatic file harvesting. This conservative default does not
replace the receiving institution's rights review or attribution obligations.
Existing PDF links in Dublin Core and the custom metadata profile are unchanged.

URLs with credentials, sensitive query parameters, private IP addresses, local
hostnames, or unsupported schemes are excluded. OpenAlex's authenticated content
API is not advertised, and no API key is included in resource maps. The provider
does not resolve external DNS names or follow redirects; harvesters must enforce
their own network, redirect, file-type, size, timeout, and malware checks.

An article without an eligible file still has a valid metadata record and an
ORE entry without file aggregation links. No landing page is advertised as a
PDF, and no remote request is made while generating an OAI response. A remote
PDF can still become unavailable; DSpace may reject an individual import if its
bitstream download fails. Test a dedicated collection before enabling bulk file
imports.

## DSpace configuration

Keep the existing institutional base URL, optional private credential, and set:

```text
OAI provider: https://your-host/oai/<public_key>[/<harvester_key>]
OAI set: institution:<public_key>
Descriptive metadata: Simple Dublin Core
Harvesting mode: metadata and bitstreams (type 3), not metadata only
```

`ore` is the file-resource format, not a replacement for the descriptive metadata
crosswalk. DSpace discovers it through `ListMetadataFormats` and retrieves it
using the same OAI identifier as the Dublin Core record. Its file links identify
the `ORIGINAL` bundle and `application/pdf` MIME type. DSpace performs the actual
download; compatibility and successful imports must be checked on the receiving
DSpace version.

```text
/oai/<public_key>?verb=ListMetadataFormats
/oai/<public_key>?verb=ListRecords&metadataPrefix=ore
/oai/<public_key>?verb=GetRecord&metadataPrefix=ore&identifier=<oai_identifier>
/oai/<public_key>/ore/<canonical_key>
```

The last URL serves the standalone Atom resource map referenced by `rel="self"`.
Private resource-map URLs include the same harvester credential as the private
OAI base URL. Both endpoints enforce repository scope, restriction, disablement,
and revocation, and return `Cache-Control: private, no-store` and
`Referrer-Policy: no-referrer`. These credentials must also be redacted from
reverse-proxy logs, as described in the main README.

## Upgrading an existing installation

There is no database schema migration. Published JSON snapshots are backward
compatible: older records without file descriptions remain metadata-only until
the next publication refresh. Populate the new descriptions from the existing
OpenAlex cache without making API requests:

```sh
flask publish-oai-records --ror <ror_id>
```

Omit `--ror` only when all configured repositories should be refreshed. The
upgrade advertises a new metadata format and advances publication datestamps;
identifiers remain unchanged. Subsequent refreshes create events only when
published metadata or the selected PDF URL, license, or version changes.
Pagination retains the original publication revision, including file metadata.

## Validation

```sh
python -m pytest -q tests/test_oai_ore.py tests/test_oai_pmh.py tests/test_oai_datestamps.py
python tests/oai_harvester_smoke.py
```

The tests cover file eligibility, DSpace's link and bundle XPath expressions,
the locally advertised Atom-profile XSD, standalone resource maps, private
access, withdrawal notices, immutable snapshots, and resumption tokens. The XSD
is a schema for the emitted profile, not a replacement for the complete Atom or
ORE specifications.

References: [ORE Atom implementation guide](https://www.openarchives.org/ore/1.0/atom),
[DSpace-CRIS ORE importer](https://github.com/4Science/DSpace/blob/main-cris/dspace-api/src/main/java/org/dspace/content/crosswalk/OREIngestionCrosswalk.java),
and [OpenAlex locations](https://help.openalex.org/data/locations/).
