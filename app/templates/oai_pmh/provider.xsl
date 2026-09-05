<?xml version="1.0" encoding="UTF-8"?>
{% autoescape true %}
<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:oai="http://www.openarchives.org/OAI/2.0/"
  xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
  xmlns:dc="http://purl.org/dc/elements/1.1/"
  xmlns:datacite="http://datacite.org/schema/kernel-4"
  xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
  xmlns:mapped="https://dataorcid.cl/ns/mapped-metadata/1.0/"
  exclude-result-prefixes="oai oai_dc dc datacite oaire mapped">
  <xsl:output method="html" encoding="UTF-8" indent="yes" doctype-system="about:legacy-compat"/>
  <xsl:variable name="base" select="normalize-space(/oai:OAI-PMH/oai:request)"/>

  <xsl:template match="/">
    <html lang="{{ language }}">
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>OAI-PMH · Data ORCID-Chile</title>
        <style>
          :root{--orange:#c14320;--orange-dark:#9f3218;--blue:#0069b0;--cyan:#087b8c;--ink:#212127;--muted:#657184;--line:#dfe5ec;--surface:#fff;--bg:#f4f6f9;--green:#18793a;--red:#b4233e;--shadow:0 8px 24px rgba(31,41,55,.08)}
          *{box-sizing:border-box}body{background:var(--bg);color:var(--ink);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;line-height:1.5;margin:0}a{color:var(--blue)}code{font-family:"SFMono-Regular",Consolas,monospace}.shell{margin:0 auto;max-width:1220px;padding:0 24px 48px}.brandbar{background:var(--ink);border-bottom:4px solid var(--orange);color:#fff}.brandbar-inner{align-items:center;display:flex;gap:16px;justify-content:space-between;margin:0 auto;max-width:1220px;padding:17px 24px}.brand{align-items:center;display:flex;gap:12px}.brand-mark{align-items:center;background:var(--orange);border-radius:9px;display:flex;font-size:20px;font-weight:800;height:42px;justify-content:center;letter-spacing:-1px;width:42px}.brand strong{display:block;font-size:17px}.brand small{color:#cbd1d8;display:block;font-size:12px}.protocol-badge{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.22);border-radius:999px;font-size:12px;font-weight:700;padding:7px 11px}.hero{align-items:flex-start;display:flex;gap:24px;justify-content:space-between;padding:30px 0 20px}.eyebrow{color:var(--orange);font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}.hero h1{font-size:30px;letter-spacing:-.03em;line-height:1.15;margin:7px 0}.hero p{color:var(--muted);margin:0;max-width:700px}.request-card{background:#eef6fb;border:1px solid #c9e0ef;border-radius:9px;color:#31546d;font-size:12px;max-width:460px;padding:10px 13px;overflow-wrap:anywhere}.request-card b{color:var(--blue);display:block;margin-bottom:2px}.nav{display:flex;flex-wrap:wrap;gap:7px;margin-bottom:20px}.nav a{background:var(--surface);border:1px solid var(--line);border-radius:7px;color:#465466;font-size:12px;font-weight:700;padding:8px 11px;text-decoration:none}.nav a:hover{border-color:var(--orange);color:var(--orange)}.panel{background:var(--surface);border:1px solid var(--line);border-radius:11px;box-shadow:var(--shadow);margin-bottom:18px;overflow:hidden}.panel-head{align-items:center;background:#fbfcfd;border-bottom:1px solid var(--line);display:flex;gap:12px;justify-content:space-between;padding:16px 19px}.panel-head h2{font-size:17px;margin:0}.panel-head span{color:var(--muted);font-size:12px}.panel-body{padding:19px}.identify-grid{display:grid;gap:1px;grid-template-columns:repeat(3,minmax(0,1fr));background:var(--line)}.fact{background:#fff;min-height:105px;padding:18px}.fact label{color:var(--muted);display:block;font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}.fact strong,.fact code{display:block;font-size:15px;margin-top:7px;overflow-wrap:anywhere}.fact.primary{border-top:3px solid var(--orange)}.fact.good{border-top:3px solid var(--green)}.notice{border-radius:8px;margin-bottom:18px;padding:15px 17px}.notice.error{background:#fff0f2;border:1px solid #f3c3cd;color:#8d1c33}.notice strong{display:block;margin-bottom:4px}.record{border:1px solid var(--line);border-radius:9px;margin-bottom:14px;overflow:hidden}.record:last-child{margin-bottom:0}.record-head{background:#f8fafc;border-bottom:1px solid var(--line);padding:14px 17px}.record-head.deleted{background:#fff3f4}.identifier{color:var(--blue);font-family:"SFMono-Regular",Consolas,monospace;font-size:12px;overflow-wrap:anywhere}.record-meta{color:var(--muted);display:flex;flex-wrap:wrap;font-size:12px;gap:10px;margin-top:7px}.pill{background:#eaf6f2;border-radius:999px;color:var(--green);font-size:10px;font-weight:800;padding:3px 7px;text-transform:uppercase}.pill.deleted{background:#fdecef;color:var(--red)}.metadata{display:grid;gap:0;padding:4px 17px 10px}.field{border-bottom:1px solid #edf0f3;display:grid;gap:18px;grid-template-columns:130px minmax(0,1fr);padding:10px 0}.field:last-child{border-bottom:0}.field dt{color:var(--muted);font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase}.field dd{margin:0;overflow-wrap:anywhere}.simple-list{display:grid;gap:10px}.simple-item{align-items:start;border:1px solid var(--line);border-radius:8px;display:grid;gap:14px;grid-template-columns:minmax(160px,.8fr) 2fr;padding:14px}.simple-item strong,.simple-item code{overflow-wrap:anywhere}.token{align-items:center;background:#fff8f4;border:1px solid #f1d0c6;border-radius:9px;display:flex;gap:14px;justify-content:space-between;margin-top:16px;padding:14px 16px}.token small{color:var(--muted);display:block}.button{background:var(--orange);border-radius:7px;color:#fff;font-size:12px;font-weight:800;padding:8px 12px;text-decoration:none;white-space:nowrap}.button:hover{background:var(--orange-dark);color:#fff}.raw-note{color:var(--muted);font-size:12px;margin-top:22px;text-align:center}.raw-note code{background:#e9edf2;border-radius:4px;padding:2px 5px}@media(max-width:800px){.hero{display:block}.request-card{margin-top:16px;max-width:none}.identify-grid{grid-template-columns:1fr}.simple-item,.field{grid-template-columns:1fr;gap:4px}.shell{padding-left:14px;padding-right:14px}.brandbar-inner{padding-left:14px;padding-right:14px}.protocol-badge{display:none}}
        </style>
      </head>
      <body>
        <header class="brandbar"><div class="brandbar-inner"><div class="brand"><span class="brand-mark">DO</span><span><strong>Data ORCID-Chile</strong><small>{{ _('Institutional research interoperability') }}</small></span></div><span class="protocol-badge">OAI-PMH 2.0 · oai_dc</span></div></header>
        <main class="shell">
          <section class="hero">
            <div><span class="eyebrow">{{ _('Repository for external harvesting') }}</span><h1><xsl:choose><xsl:when test="//oai:repositoryName"><xsl:value-of select="//oai:repositoryName"/></xsl:when><xsl:otherwise>{{ _('OAI-PMH provider') }}</xsl:otherwise></xsl:choose></h1><p>{{ _('Metadata published by DataORCID-Chile for DSpace and other compatible harvesters.') }}</p></div>
            <div class="request-card"><b>{{ _('Current request') }}</b><xsl:value-of select="/oai:OAI-PMH/oai:request"/></div>
          </section>
          <nav class="nav" aria-label="{{ _('OAI-PMH operations') }}">
            <a href="{$base}?verb=Identify">Identify</a>
            <a href="{$base}?verb=ListMetadataFormats">ListMetadataFormats</a>
            <a href="{$base}?verb=ListSets">ListSets</a>
            <a href="{$base}?verb=ListIdentifiers&amp;metadataPrefix=oai_dc">ListIdentifiers</a>
            <a href="{$base}?verb=ListRecords&amp;metadataPrefix=oai_dc">{{ _('Records · DC') }}</a>
            <a href="{$base}?verb=ListRecords&amp;metadataPrefix=oai_openaire">{{ _('Records · OpenAIRE') }}</a>
            <a href="{$base}?verb=ListRecords&amp;metadataPrefix=dataorcid">{{ _('Records · Mapped') }}</a>
          </nav>

          <xsl:apply-templates select="/oai:OAI-PMH/oai:error"/>
          <xsl:apply-templates select="/oai:OAI-PMH/oai:Identify"/>
          <xsl:apply-templates select="/oai:OAI-PMH/oai:ListMetadataFormats"/>
          <xsl:apply-templates select="/oai:OAI-PMH/oai:ListSets"/>
          <xsl:apply-templates select="/oai:OAI-PMH/oai:GetRecord"/>
          <xsl:apply-templates select="/oai:OAI-PMH/oai:ListIdentifiers"/>
          <xsl:apply-templates select="/oai:OAI-PMH/oai:ListRecords"/>

          <p class="raw-note">{{ _('This view is generated from the original OAI-PMH XML. Harvesters receive the same structured metadata.') }}</p>
        </main>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="oai:error">
    <div class="notice error"><strong>{{ _('OAI-PMH error') }} · <xsl:value-of select="@code"/></strong><xsl:value-of select="."/></div>
  </xsl:template>

  <xsl:template match="oai:Identify">
    <section class="panel"><div class="panel-head"><h2>{{ _('Repository identity') }}</h2><span>{{ _('Identify response') }}</span></div><div class="identify-grid">
      <div class="fact primary"><label>{{ _('Repository') }}</label><strong><xsl:value-of select="oai:repositoryName"/></strong></div>
      <div class="fact"><label>{{ _('Protocol version') }}</label><strong><xsl:value-of select="oai:protocolVersion"/></strong></div>
      <div class="fact good"><label>{{ _('Deleted records') }}</label><strong><xsl:value-of select="oai:deletedRecord"/></strong></div>
      <div class="fact"><label>{{ _('Base URL') }}</label><code><xsl:value-of select="oai:baseURL"/></code></div>
      <div class="fact"><label>{{ _('Administrative contact') }}</label><strong><xsl:value-of select="oai:adminEmail"/></strong></div>
      <div class="fact"><label>{{ _('Earliest record / granularity') }}</label><strong><xsl:value-of select="oai:earliestDatestamp"/></strong><code><xsl:value-of select="oai:granularity"/></code></div>
    </div></section>
  </xsl:template>

  <xsl:template match="oai:ListMetadataFormats">
    <section class="panel"><div class="panel-head"><h2>{{ _('Metadata formats') }}</h2><span>ListMetadataFormats</span></div><div class="panel-body simple-list"><xsl:for-each select="oai:metadataFormat"><div class="simple-item"><strong><xsl:value-of select="oai:metadataPrefix"/></strong><div><div><xsl:value-of select="oai:metadataNamespace"/></div><small><a href="{oai:schema}"><xsl:value-of select="oai:schema"/></a></small></div></div></xsl:for-each></div></section>
  </xsl:template>

  <xsl:template match="oai:ListSets">
    <section class="panel"><div class="panel-head"><h2>{{ _('Institutional sets') }}</h2><span>ListSets</span></div><div class="panel-body simple-list"><xsl:for-each select="oai:set"><div class="simple-item"><code><xsl:value-of select="oai:setSpec"/></code><strong><xsl:value-of select="oai:setName"/></strong></div></xsl:for-each></div></section>
  </xsl:template>

  <xsl:template match="oai:GetRecord">
    <section class="panel"><div class="panel-head"><h2>{{ _('Requested record') }}</h2><span>GetRecord</span></div><div class="panel-body"><xsl:apply-templates select="oai:record"/></div></section>
  </xsl:template>

  <xsl:template match="oai:ListRecords">
    <section class="panel"><div class="panel-head"><h2>{{ _('Exposed articles') }}</h2><span>{{ _('Records on this page') }}: <xsl:value-of select="count(oai:record)"/></span></div><div class="panel-body"><xsl:apply-templates select="oai:record"/><xsl:apply-templates select="oai:resumptionToken"/></div></section>
  </xsl:template>

  <xsl:template match="oai:ListIdentifiers">
    <section class="panel"><div class="panel-head"><h2>{{ _('Published identifiers') }}</h2><span>{{ _('Headers on this page') }}: <xsl:value-of select="count(oai:header)"/></span></div><div class="panel-body"><xsl:for-each select="oai:header"><div class="record"><xsl:call-template name="header"/></div></xsl:for-each><xsl:apply-templates select="oai:resumptionToken"/></div></section>
  </xsl:template>

  <xsl:template match="oai:record">
    <article class="record">
      <xsl:for-each select="oai:header"><xsl:call-template name="header"/></xsl:for-each>
      <xsl:if test="oai:metadata/oai_dc:dc">
        <dl class="metadata"><xsl:for-each select="oai:metadata/oai_dc:dc/dc:*"><xsl:call-template name="metadata-field"><xsl:with-param name="label" select="local-name()"/></xsl:call-template></xsl:for-each></dl>
      </xsl:if>
      <xsl:if test="oai:metadata/oaire:resource">
        <dl class="metadata"><xsl:for-each select="oai:metadata/oaire:resource//*[not(*) and normalize-space(.)]"><xsl:call-template name="metadata-field"><xsl:with-param name="label" select="local-name()"/></xsl:call-template></xsl:for-each></dl>
      </xsl:if>
      <xsl:if test="oai:metadata/mapped:record">
        <dl class="metadata"><xsl:for-each select="oai:metadata/mapped:record/mapped:field"><xsl:call-template name="metadata-field"><xsl:with-param name="label" select="@name"/></xsl:call-template></xsl:for-each></dl>
      </xsl:if>
    </article>
  </xsl:template>

  <xsl:template name="metadata-field">
    <xsl:param name="label"/>
    <div class="field"><dt><xsl:value-of select="translate($label, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/></dt><dd><xsl:choose><xsl:when test="starts-with(., 'http://') or starts-with(., 'https://')"><a href="{.}" target="_blank" rel="noopener"><xsl:value-of select="."/></a></xsl:when><xsl:otherwise><xsl:value-of select="."/></xsl:otherwise></xsl:choose></dd></div>
  </xsl:template>

  <xsl:template name="header">
    <div><xsl:attribute name="class"><xsl:text>record-head</xsl:text><xsl:if test="@status='deleted'"><xsl:text> deleted</xsl:text></xsl:if></xsl:attribute><div class="identifier"><xsl:value-of select="oai:identifier"/></div><div class="record-meta"><span><xsl:value-of select="oai:datestamp"/></span><xsl:for-each select="oai:setSpec"><span><xsl:value-of select="."/></span></xsl:for-each><xsl:choose><xsl:when test="@status='deleted'"><span class="pill deleted">{{ _('Not exposed') }}</span></xsl:when><xsl:otherwise><span class="pill">{{ _('Exposed') }}</span></xsl:otherwise></xsl:choose></div></div>
  </xsl:template>

  <xsl:template match="oai:resumptionToken">
    <xsl:if test="normalize-space(.)"><div class="token"><div><strong>{{ _('More results available') }}</strong><small>{{ _('Use the resumption token to continue harvesting.') }}</small></div><a class="button"><xsl:attribute name="href"><xsl:value-of select="$base"/><xsl:text>?verb=</xsl:text><xsl:value-of select="local-name(..)"/><xsl:text>&amp;resumptionToken=</xsl:text><xsl:value-of select="."/></xsl:attribute>{{ _('Next page') }}</a></div></xsl:if>
  </xsl:template>
</xsl:stylesheet>

{% endautoescape %}
