"""Build matching multilingual PDF and editable Markdown manuals.

Usage: python manuals/source/build_manuals.py
Dependencies are isolated from the application's requirements.
"""

from __future__ import annotations

from html import escape
import json
from pathlib import Path
import re

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import Paragraph, Table, TableStyle

from content import LANGUAGES, PAGES, SOFTWARE_VERSION, UPDATED
from labels import LABELS

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "assets"
WIDTH, HEIGHT = 612, 792
MARGIN = 52
TEXT_WIDTH = WIDTH - 2 * MARGIN
ORANGE = colors.HexColor("#fa6035")
DARK = colors.HexColor("#303030")
GRAY = colors.HexColor("#6c6c6c")
PALE = colors.HexColor("#fff0eb")
RULE = colors.HexColor("#d1d1d1")


def register_fonts():
    for suffix in ("Regular", "Bold", "Italic"):
        pdfmetrics.registerFont(TTFont("Barlow-"+suffix, str(ASSETS / "fonts" / f"Barlow-{suffix}.ttf")))
    pdfmetrics.registerFontFamily("Barlow", normal="Barlow-Regular", bold="Barlow-Bold", italic="Barlow-Italic")


def style(name="body", **kwargs):
    defaults = dict(fontName="Barlow-Regular", fontSize=10, leading=13.8, textColor=DARK,
                    spaceAfter=7, alignment=TA_LEFT)
    defaults.update(kwargs)
    return ParagraphStyle(name, **defaults)


def paragraph(text, kind="body"):
    settings = {
        "body": {},
        "subheading": dict(fontName="Barlow-Bold", fontSize=12.2, leading=16, spaceAfter=5),
        "caption": dict(fontSize=8.5, leading=11, textColor=GRAY),
        "small": dict(fontSize=9, leading=12),
    }
    return Paragraph(text, style(kind, **settings[kind]))


def is_subheading(text):
    return bool(re.match(r"^\d+\.\d+\s", text)) or (len(text) < 85 and not text.endswith("."))


def draw_paragraph(canvas, text, x, top, width=TEXT_WIDTH, kind="body"):
    item = paragraph(text, kind)
    _, height = item.wrap(width, HEIGHT)
    item.drawOn(canvas, x, top-height)
    return top-height-item.style.spaceAfter


def draw_title(canvas, title, top=727):
    item = Paragraph(escape(title), style("title", fontName="Barlow-Bold", fontSize=18,
                     leading=22, spaceAfter=0))
    _, height = item.wrap(TEXT_WIDTH, HEIGHT)
    item.drawOn(canvas, MARGIN, top-height)
    top -= height+8
    canvas.setStrokeColor(ORANGE)
    canvas.setLineWidth(1.6)
    canvas.line(MARGIN, top, WIDTH-MARGIN, top)
    return top-12


def draw_header_footer(canvas, language, page_number):
    labels = LABELS[language]
    logo = ImageReader(str(ASSETS / "cincel-logo.png"))
    canvas.drawImage(logo, MARGIN, 758, width=62, height=18.2, mask="auto")
    canvas.setFillColor(GRAY)
    canvas.setFont("Barlow-Regular", 7.4)
    canvas.drawString(MARGIN+69, 756, "DATA ORCID CHILE · "+labels["manual"]+" · "+labels["audience"])
    canvas.setLineWidth(.5)
    canvas.setStrokeColor(RULE)
    canvas.line(MARGIN, 745, WIDTH-MARGIN, 745)
    canvas.setStrokeColor(ORANGE)
    canvas.setLineWidth(1.5)
    canvas.line(MARGIN, 38, WIDTH-MARGIN, 38)
    canvas.setFont("Barlow-Regular", 7)
    canvas.drawCentredString(WIDTH/2, 24,
        "DATA ORCID CHILE · "+labels["audience"]+" · "+labels["page"]+f" {page_number}")


def draw_note(canvas, language, text, top):
    note = paragraph(f'<font color="#fa6035"><b>{LABELS[language]["note"]}</b></font><br/>'+escape(text), "small")
    _, height = note.wrap(TEXT_WIDTH-77, HEIGHT)
    height += 14
    x = MARGIN+20
    canvas.setFillColor(PALE)
    canvas.rect(x, top-height, TEXT_WIDTH-40, height, fill=1, stroke=0)
    canvas.setFillColor(ORANGE)
    canvas.rect(x, top-height, 29, height, fill=1, stroke=0)
    canvas.setFillColor(colors.white)
    canvas.setFont("Barlow-Bold", 16)
    canvas.drawCentredString(x+14.5, top-height/2-5, "i")
    note.drawOn(canvas, x+36, top-height+7)
    return top-height-12


def make_table(rows):
    widths = [TEXT_WIDTH*.3, TEXT_WIDTH*.7] if len(rows[0]) == 2 else [TEXT_WIDTH*.54, TEXT_WIDTH*.23, TEXT_WIDTH*.23]
    data = [[Paragraph(escape(cell), style("cell", fontName="Barlow-Bold" if r == 0 or c == 0 else "Barlow-Regular",
                    fontSize=8.8, leading=12, textColor=colors.white if r == 0 else DARK))
             for c, cell in enumerate(row)] for r, row in enumerate(rows)]
    table = Table(data, colWidths=widths)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), DARK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f7f7")]),
        ("GRID", (0, 0), (-1, -1), .5, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return table


def reserve_after_image(page, language):
    height = 0
    for text in page["steps"][language]:
        item = paragraph(escape(text), "small")
        height += item.wrap(TEXT_WIDTH-27, HEIGHT)[1]+8
    if page["note"][language]:
        item = paragraph(LABELS[language]["note"]+"<br/>"+escape(page["note"][language]), "small")
        height += item.wrap(TEXT_WIDTH-77, HEIGHT)[1]+26
    return height


def draw_figure(canvas, language, page, number, top, max_height):
    path = ASSETS / "screenshots" / language / (page["image"]+".png")
    image = ImageReader(str(path))
    iw, ih = image.getSize()
    image_width = TEXT_WIDTH-28
    image_height = image_width * ih / iw
    if image_height > max_height:
        image_height = max_height
        image_width = image_height * iw / ih
    caption = paragraph(f'<font color="#fa6035"><b>{LABELS[language]["figure"]} {number}</b></font> '+escape(page["captions"][language]), "caption")
    _, caption_height = caption.wrap(TEXT_WIDTH-28, HEIGHT)
    box_height = image_height + caption_height + 23
    canvas.setFillColor(colors.HexColor("#f8f8f8"))
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(.6)
    canvas.rect(MARGIN+7, top-box_height, TEXT_WIDTH-14, box_height, fill=1, stroke=1)
    canvas.drawImage(image, (WIDTH-image_width)/2, top-image_height-7,
                     width=image_width, height=image_height, mask="auto")
    caption.drawOn(canvas, MARGIN+14, top-box_height+7)
    return top-box_height-15


def draw_cover(canvas, language):
    labels = LABELS[language]
    canvas.setStrokeColor(ORANGE)
    canvas.setLineWidth(1.7)
    canvas.rect(MARGIN, 63, TEXT_WIDTH, 666)
    canvas.drawImage(str(ASSETS/"cincel-logo.png"), 216, 670, width=180, height=52.8, mask="auto")
    canvas.setFillColor(ORANGE)
    canvas.setFont("Barlow-Bold", 10)
    canvas.drawCentredString(WIDTH/2, 628, labels["manual"])
    canvas.setFillColor(DARK)
    canvas.setFont("Barlow-Regular", 43)
    canvas.drawCentredString(WIDTH/2, 575, "DATA ORCID CHILE")
    canvas.setFillColor(GRAY)
    canvas.setFont("Barlow-Regular", 11)
    canvas.drawCentredString(WIDTH/2, 548, labels["subtitle"])
    canvas.setFillColor(DARK)
    canvas.setFont("Barlow-Regular", 8)
    canvas.drawCentredString(WIDTH/2, 508, labels["topics"])
    shot = ImageReader(str(ASSETS / "screenshots" / language / "overview.png"))
    iw, ih = shot.getSize()
    image_width = 466
    image_height = image_width * ih / iw
    canvas.setFillColor(colors.HexColor("#f7f7f7"))
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(.7)
    canvas.rect(67, 93, 478, image_height+45, stroke=1, fill=1)
    canvas.drawImage(shot, 73, 132, width=image_width, height=image_height)
    fields = [(labels["version"], SOFTWARE_VERSION), (labels["update"], UPDATED[language]),
              (labels["for"], labels["roles"])]
    for index, (label, value) in enumerate(fields):
        x = 67+index*(478/3)
        canvas.setFillColor(colors.white)
        canvas.rect(x, 93, 478/3, 34, stroke=1, fill=1)
        canvas.setFillColor(ORANGE)
        canvas.setFont("Barlow-Bold", 7.5)
        canvas.drawCentredString(x+478/6, 113, label)
        canvas.setFillColor(GRAY)
        canvas.setFont("Barlow-Regular", 8)
        canvas.drawCentredString(x+478/6, 102, value)
    canvas.showPage()


def draw_contents(canvas, language):
    draw_header_footer(canvas, language, 2)
    top = draw_title(canvas, LABELS[language]["contents"])
    for number, page in enumerate(PAGES, 3):
        if page["chapter"]:
            canvas.setFillColor(ORANGE)
            canvas.circle(MARGIN+5, top-7, 2.1, fill=1, stroke=0)
            canvas.setFillColor(DARK)
            canvas.setFont("Barlow-Regular", 10.5)
            canvas.drawString(MARGIN+14, top-10, page["titles"][language])
            canvas.drawRightString(WIDTH-MARGIN, top-10, str(number))
            canvas.linkRect("", page["key"], (MARGIN, top-15, WIDTH-MARGIN, top+3), relative=0, thickness=0)
            top -= 22
    top -= 20
    top = draw_note(canvas, language, LABELS[language]["contents_note"], top)
    heading = LABELS[language]["quick_access"]
    top = draw_paragraph(canvas, escape(heading), MARGIN, top-7, kind="subheading")
    for key in ("exports", "oai-overview", "oai-articles", "oai-metadata", "oai-import", "oai-audit", "oai-access"):
        number = next(n for n, p in enumerate(PAGES, 3) if p["key"] == key)
        page = PAGES[number-3]
        title = page["titles"][language]
        canvas.setFillColor(DARK)
        canvas.setFont("Barlow-Regular", 9.5)
        canvas.drawString(MARGIN+8, top-10, title)
        canvas.drawRightString(WIDTH-MARGIN, top-10, str(number))
        canvas.linkRect("", key, (MARGIN, top-15, WIDTH-MARGIN, top+3), relative=0, thickness=0)
        top -= 18
    canvas.showPage()


def draw_contact(canvas, language):
    canvas.drawImage(str(ASSETS/"cincel-logo.png"), 206, 630, width=200, height=58.7, mask="auto")
    canvas.setFillColor(ORANGE)
    canvas.setFont("Barlow-Bold", 9)
    canvas.drawCentredString(WIDTH/2, 570, LABELS[language]["contact"])
    name = "Consorcio para el Acceso a la Información<br/>Científica Electrónica"
    item = Paragraph(name, style("contact-title", fontName="Barlow-Bold", fontSize=20, leading=25, alignment=TA_CENTER))
    _, height = item.wrap(TEXT_WIDTH, HEIGHT)
    item.drawOn(canvas, MARGIN, 551-height)
    canvas.setFillColor(ORANGE)
    canvas.rect(265, 463, 82, 11, fill=1, stroke=0)
    canvas.setFillColor(colors.HexColor("#f8f8f8"))
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(.7)
    canvas.rect(106, 302, 400, 141, fill=1, stroke=1)
    phone = LABELS[language]["phone"]
    address = LABELS[language]["address"]
    city = LABELS[language]["city"]
    for offset, line in enumerate([address, city, phone, "secretariaejecutiva@cincel.cl", "www.cincel.cl"]):
        canvas.setFillColor(ORANGE if offset == 4 else DARK)
        canvas.setFont("Barlow-Bold" if offset == 4 else "Barlow-Regular", 11)
        y = 416-offset*20
        canvas.drawCentredString(WIDTH/2, y, line)
        if offset >= 3:
            url = "mailto:"+line if offset == 3 else "https://"+line
            canvas.linkURL(url, (155, y-3, 457, y+12), relative=0, thickness=0)
    canvas.showPage()


def build_pdf(language):
    output = ROOT / f"dataorcid-chile-user-manual-v{SOFTWARE_VERSION}-{language}.pdf"
    canvas = Canvas(str(output), pagesize=(WIDTH, HEIGHT), pageCompression=1)
    canvas.setTitle("DATA ORCID CHILE · "+LABELS[language]["manual"])
    canvas.setAuthor("Consorcio CINCEL")
    canvas.setCreator("Data ORCID-Chile documentation generator")
    canvas.setSubject(LABELS[language]["audience"]+f" · Data ORCID-Chile {SOFTWARE_VERSION}")
    canvas.setKeywords("ORCID, OpenAlex, OAI-PMH, CINCEL, "+LABELS[language]["audience"])
    canvas.setCatalogEntry("Lang", language)
    draw_cover(canvas, language)
    draw_contents(canvas, language)
    figure_number = 0
    layout = []
    for number, page in enumerate(PAGES, 3):
        draw_header_footer(canvas, language, number)
        canvas.bookmarkPage(page["key"])
        canvas.addOutlineEntry(page["titles"][language], page["key"], level=0 if page["chapter"] else 1, closed=False)
        top = draw_title(canvas, page["titles"][language])
        for index, text in enumerate(page["paragraphs"][language]):
            kind = "subheading" if is_subheading(page["paragraphs"]["en"][index]) else "body"
            if kind == "subheading":
                top -= 2
            top = draw_paragraph(canvas, escape(text), MARGIN, top, kind=kind)
        if page["table"][language]:
            table = make_table(page["table"][language])
            _, height = table.wrap(TEXT_WIDTH, HEIGHT)
            table.drawOn(canvas, MARGIN, top-height)
            top -= height+16
        if page["image"]:
            figure_number += 1
            top -= 3
            max_height = top-74-reserve_after_image(page, language)-43
            if max_height < 150:
                raise ValueError(f"Insufficient figure space: {language} {page['key']}: {max_height}")
            top = draw_figure(canvas, language, page, figure_number, top, max_height)
        for index, text in enumerate(page["steps"][language], 1):
            canvas.setFillColor(ORANGE)
            canvas.circle(MARGIN+7, top-7, 7, fill=1, stroke=0)
            canvas.setFillColor(colors.white)
            canvas.setFont("Barlow-Bold", 8)
            canvas.drawCentredString(MARGIN+7, top-10, str(index))
            top = draw_paragraph(canvas, escape(text), MARGIN+25, top, TEXT_WIDTH-25, "small")-1
        if page["note"][language]:
            top = draw_note(canvas, language, page["note"][language], top)
        if top < 53:
            raise ValueError(f"Content overlaps footer: {language} {page['key']}: {top}")
        layout.append({"page": number, "key": page["key"], "content_bottom_pt": round(top, 2)})
        canvas.showPage()
    draw_contact(canvas, language)
    canvas.save()
    return {"file": output.name, "language": language, "pages": len(PAGES)+3,
            "figures": figure_number, "layout": layout}


def build_markdown(language):
    labels = LABELS[language]
    lines = ["# DATA ORCID CHILE", "", labels["subtitle"], "",
             f"**{labels['version']}:** {SOFTWARE_VERSION} · **{labels['update']}:** {UPDATED[language]}", ""]
    figure = 0
    for page in PAGES:
        lines.extend(["## "+page["titles"][language], ""])
        for index, text in enumerate(page["paragraphs"][language]):
            lines.extend([("### " if is_subheading(page["paragraphs"]["en"][index]) else "")+text, ""])
        if page["table"][language]:
            for index, row in enumerate(page["table"][language]):
                lines.append("| "+" | ".join(row)+" |")
                if index == 0:
                    lines.append("| "+" | ".join(["---"]*len(row))+" |")
            lines.append("")
        if page["image"]:
            figure += 1
            caption = f"{labels['figure']} {figure}. "+page["captions"][language]
            lines.extend([f"![{caption}](assets/screenshots/{language}/{page['image']}.png)", "", caption, ""])
        for number, text in enumerate(page["steps"][language], 1):
            lines.append(f"{number}. {text}")
        if page["steps"][language]:
            lines.append("")
        if page["note"][language]:
            lines.extend([f"> **{labels['note']}:** "+page["note"][language], ""])
    lines.extend(["## "+labels["contact"], "", "Consorcio para el Acceso a la Información Científica Electrónica", "",
                  labels["address"]+" · "+labels["city"]+" · +56 2 2365 4589", "",
                  "[secretariaejecutiva@cincel.cl](mailto:secretariaejecutiva@cincel.cl) · [www.cincel.cl](https://www.cincel.cl)", ""])
    (ROOT / f"dataorcid-chile-user-manual-v{SOFTWARE_VERSION}-{language}.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    register_fonts()
    results = []
    for language in LANGUAGES:
        build_markdown(language)
        result = build_pdf(language)
        results.append(result)
        print(f"Built {result['file']}: {result['pages']} pages, {result['figures']} figures")
    (ROOT / "source/build-report.json").write_text(json.dumps(results, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
