"""Regenerate the GSTR-3A return-defaulter notice PDF from the portal's
summary JSON, replicating the client-side pdfMake template in the GST
portal's `gstr3actrl.js` (downloadPdfGSTR3A).

The portal never serves a GSTR-3A PDF as bytes — its "View" opens an
Angular page that fetches:
    GET return.gst.gov.in/returns/auth/api/gstr3a/summary
        ?defaulter_id=<appDefId>&order_id=<noticeOrderId>
    -> {"status":1,"data":{gstin, ret_period, address, name, orderId, retTyp,
        canellationOrderId?, cancellationDate?, cancelArn?, arnDate?}}
and renders the notice in-browser with pdfMake. This module rebuilds the
same document with reportlab so the direct-portal fetcher
(app.features.fetch_gst_notices) can produce an identical PDF from the JSON
alone — no browser PDF, no new tab.

The notice body text is copied verbatim from gstr3actrl.js (the three
retTyp variants: normal / 9 annual / 10 final-return). AI prompts and
legal notice text are sacred — do not reword.

Ported verbatim from research/gst_integration_spec/gstr3a_pdf.py.
"""
from __future__ import annotations

import io

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, ListFlowable, ListItem,
)


_MONTHS = ["January", "February", "March", "April", "May", "June", "July",
           "August", "September", "October", "November", "December"]


def _tax_period(ret_period: str | None) -> str:
    """Mirror gstr3actrl.js: monthNames[ret_period[:2]-1] + ' ' +
    ret_period[2:]. The portal sometimes already returns a worded period
    (e.g. 'January, 2020-21'); pass those through unchanged.
    """
    if not ret_period:
        return "-"
    head = ret_period[:2]
    if head.isdigit() and 1 <= int(head) <= 12:
        return f"{_MONTHS[int(head) - 1]} {ret_period[2:]}"
    return ret_period


def _body_paragraphs(ret_typ: str | None) -> list[str]:
    """The numbered notice body, verbatim per retTyp (see gstr3actrl.js)."""
    if ret_typ == "9":
        return [
            "Being a registered taxpayer, you are required to furnish annual return for the supplies made or received and/or to include self-certified reconciliation statement for the aforesaid financial year by due date. The due date specified for filing annual return for the said financial year is over and it has been noticed that you have not filed the said return till date.",
            "You are, therefore, requested to furnish the said return within 15 days failing which appropriate action including imposition of penalty as per law will be taken.",
            "This notice shall be deemed to have been withdrawn in case the return referred above, is filed by you before issue of the show cause notice of penalty proceeding.",
            "This is a system generated notice and does not require signature.",
        ]
    if ret_typ == "10":
        return [
            "Consequent upon applying for surrender of registration or cancellation of your registration for the reasons specified in the order, you were required to submit a final return in form GSTR-10 as required under section 45 of the Act.",
            "It has been noticed that you have not filed the final return by the due date.",
            "You are, therefore, requested to furnish the final return as specified under section 45 of the Act within 15 days failing which your tax liability for the aforesaid tax period may be determined in accordance with the provisions of the Act based on the relevant material available with or gathered by this office. Please note that in addition to tax so assessed,  you will also be liable to pay interest as per provisions of the Act.",
            "This notice shall be deemed to be withdrawn in case the return is filed by you before issue of the assessment order.",
            "This is a system generated notice and does not require signature.",
        ]
    # default (normal return defaulter)
    return [
        "Being a registered taxpayer, you are required to furnish return for the supplies made or received and to discharge resultant tax liability for the aforesaid tax period by due date.  It has been noticed that you have not filed the said return till date.",
        "You are, therefore, requested to furnish the said return within 15 days failing which the tax liability may be assessed u/s 62 of the Act, based on the relevant material available with this office. Please note that in addition to tax so assessed, you will also be liable to pay interest and penalty as per provisions of the Act.",
        "Please note that no further communication will be issued for assessing the liability.",
        "The notice shall be deemed to have been withdrawn in case the return referred above, is filed by you before issue of the assessment order.",
        "This is a system generated notice and will not require signature.",
    ]


def _subheader(ret_typ: str | None) -> str:
    if ret_typ == "10":
        return ("Notice to return defaulter u/s 46 for not filing final "
                "return upon cancellation of registration")
    if ret_typ == "9":
        return "Notice to return defaulter u/s 46 for not filing annual return"
    return "Notice to return defaulter u/s 46 for not filing return"


def build_gstr3a_pdf(summary: dict, dt_of_issue: str) -> bytes:
    """Build the GSTR-3A notice PDF bytes from the summary `data` dict
    and the row's dtOfIssue. Returns the PDF as bytes.
    """
    d = summary or {}
    ret_typ = str(d.get("retTyp") or "")
    is_annual = ret_typ in ("4X", "9")
    is_final = ret_typ == "10"

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=40, rightMargin=40, topMargin=60, bottomMargin=60,
    )
    ss = getSampleStyleSheet()
    header = ParagraphStyle("h", parent=ss["Title"], fontSize=18, alignment=TA_CENTER)
    sub = ParagraphStyle("s", parent=ss["Heading2"], fontSize=16, alignment=TA_CENTER)
    normal = ss["Normal"]
    center = ParagraphStyle("c", parent=normal, alignment=TA_CENTER)

    story = []
    story.append(Paragraph("Form GSTR-3A", header))
    story.append(Paragraph("[See rule 68]", center))
    story.append(Spacer(1, 10))

    ref = d.get("orderId") or "-"
    story.append(Paragraph(f"Reference No: {ref}&nbsp;&nbsp;&nbsp;&nbsp;Date: {dt_of_issue}", normal))
    story.append(Spacer(1, 8))

    # To-block (gstin / name / address)
    story.append(Paragraph("To", normal))
    story.append(Paragraph(str(d.get("gstin") or "-"), normal))
    story.append(Paragraph(str(d.get("name") or "-"), normal))
    addr = str(d.get("address") or "-").replace("\n", "<br/>")
    story.append(Paragraph(addr, normal))
    story.append(Spacer(1, 12))

    story.append(Paragraph(_subheader(ret_typ), sub))
    story.append(Spacer(1, 6))

    if is_final:
        story.append(Paragraph(
            f"Cancellation order No.: {d.get('canellationOrderId', '')}&nbsp;&nbsp;&nbsp;"
            f"Date: {d.get('cancellationDate', '')}", normal))
        story.append(Paragraph(
            f"Application Reference Number, if any: {d.get('cancelArn', '')}&nbsp;&nbsp;&nbsp;"
            f"Date: {d.get('arnDate', '')}", normal))
    else:
        period_label = "Financial Year: " if is_annual else "Tax Period: "
        period_val = (d.get("ret_period", "-") if is_annual
                      else _tax_period(d.get("ret_period")))
        ret_label = "GSTR-" + ("4 (Annual)" if ret_typ == "4X" else ret_typ or "-")
        story.append(Paragraph(
            f"{period_label}{period_val}&nbsp;&nbsp;&nbsp;&nbsp;Type of Return: {ret_label}",
            normal))
    story.append(Spacer(1, 12))

    items = [ListItem(Paragraph(t, normal), leftIndent=12)
             for t in _body_paragraphs(ret_typ)]
    story.append(ListFlowable(items, bulletType="1", leftIndent=18))

    doc.build(story)
    return buf.getvalue()
