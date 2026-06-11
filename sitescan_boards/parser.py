"""Parse Charleston board agenda PDFs into structured items.

Agenda PDFs are text-native (no OCR needed) and rigidly templated.
Observed item formats:

BAR-L / BAR-S:
    3. 474 Meeting Street
    BAR2025-002335 | TMS #459-05-03-071 | Eastside | Council District 4
    New Construction | Height Districts 2.5-3 | Height District 4
    Old and Historic District, Old City District
    Requesting Conceptual Approval for a mixed-use, affordable housing building.
    Owner: Star Gospel Mission
    Applicant: Bryanna Dering-Weyrich (LS3P)

Planning Commission:
    2. (Harris St & Jackson St)
    Peninsula | TMS# 4611301035 | Council District 4 | Approx. 2.41 acres
    Request to rezone from Upper Peninsula (UP) & Diverse Residential (DR-2)
    to Mixed-Use/Workforce Housing (MU-3/WH).
    Owner: ...
    Applicant: ...

Text extraction is separated from item parsing so the parsing logic is
unit-testable without PDF fixtures.
"""
from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from datetime import date

# --- field regexes ---------------------------------------------------------

ITEM_START_RE = re.compile(r"^\s*(\d{1,2})\.\s+(\S.*)$")
CASE_NO_RE = re.compile(r"\b([A-Z]{2,4}\d{4}-\d{4,6})\b")
TMS_RE = re.compile(r"TMS\s*#?\s*([0-9][0-9\-/ ,&]*[0-9])")
COUNCIL_RE = re.compile(r"Council\s+District\s+(\d+)", re.IGNORECASE)
ACREAGE_RE = re.compile(r"Approx\.?\s*([\d.]+)\s*ac", re.IGNORECASE)
OWNER_RE = re.compile(r"Owner:\s*(.+)", re.IGNORECASE)
APPLICANT_RE = re.compile(r"Applicant:\s*(.+)", re.IGNORECASE)
REQUEST_RE = re.compile(r"\bRequest(?:ing|s)?\b", re.IGNORECASE)
SITE_VISIT_RE = re.compile(r"Site\s+visit\s+on", re.IGNORECASE)
ZONING_CODE_RE = re.compile(r"\(([A-Z]{1,3}(?:-\d)?(?:/WH)?)\)")

# Lines that are page furniture, not item content.
FURNITURE_RES = [
    re.compile(r"^\s*Board of Architectural Review", re.IGNORECASE),
    re.compile(r"^\s*Planning Commission\s*(·|\||$)", re.IGNORECASE),
    re.compile(r"^\s*Agenda\s*\|", re.IGNORECASE),
    re.compile(r"^\s*Page\s+\d+\s*$", re.IGNORECASE),
    re.compile(r"^\s*[A-Z]\.\s+[A-Z &/]+\s*$"),  # section headers: "B. REZONINGS"
]


@dataclass
class AgendaItem:
    item_number: int
    address: str
    case_number: str | None = None
    tms: str | None = None
    neighborhood: str | None = None
    council_district: int | None = None
    acreage: float | None = None
    request_text: str = ""
    owner: str | None = None
    applicant: str | None = None
    raw_text: str = ""
    section: str | None = None       # e.g. "REZONINGS" on PC agendas
    rezoning_to: list[str] = field(default_factory=list)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from an agenda PDF. Lazy import keeps tests light."""
    import pdfplumber

    pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return "\n".join(pages)


def _is_furniture(line: str) -> bool:
    return any(p.search(line) for p in FURNITURE_RES)


def _split_items(text: str) -> list[tuple[int, str, list[str], str | None]]:
    """Split agenda text into (item_number, address, body_lines, section)."""
    items: list[tuple[int, str, list[str], str | None]] = []
    current: list[str] | None = None
    current_meta: tuple[int, str] | None = None
    section: str | None = None
    section_re = re.compile(r"^\s*[A-Z]\.\s+([A-Z][A-Z &/]+?)\s*$")

    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip():
            continue
        sec = section_re.match(line)
        if sec:
            section = sec.group(1).strip()
            continue
        if _is_furniture(line):
            continue
        start = ITEM_START_RE.match(line)
        # Guard against numbered sentences inside request text: a real item
        # start is short-ish and not mid-paragraph (heuristic: previous item
        # already has an Owner/Applicant line, or no item open yet).
        if start:
            n = int(start.group(1))
            plausible = current is None or any(
                OWNER_RE.search(l) or APPLICANT_RE.search(l) for l in current
            ) or (current_meta and n == current_meta[0] + 1)
            if plausible:
                if current is not None and current_meta is not None:
                    items.append((*current_meta, current, section))
                current_meta = (n, start.group(2).strip())
                current = []
                continue
        if current is not None:
            current.append(line.strip())

    if current is not None and current_meta is not None:
        items.append((*current_meta, current, section))
    return items


def _parse_body(item: AgendaItem, lines: list[str]) -> None:
    body = "\n".join(lines)
    item.raw_text = f"{item.address}\n{body}"

    if m := CASE_NO_RE.search(body):
        item.case_number = m.group(1)
    if m := TMS_RE.search(body):
        item.tms = re.sub(r"\s+", "", m.group(1))
    if m := COUNCIL_RE.search(body):
        item.council_district = int(m.group(1))
    if m := ACREAGE_RE.search(body):
        try:
            item.acreage = float(m.group(1))
        except ValueError:
            pass
    if m := OWNER_RE.search(body):
        item.owner = SITE_VISIT_RE.split(m.group(1))[0].strip(" ·|")
    if m := APPLICANT_RE.search(body):
        item.applicant = SITE_VISIT_RE.split(m.group(1))[0].strip(" ·|")

    # Neighborhood: in the pipe-delimited metadata line, it's the segment
    # that isn't a case number / TMS / council district / acreage / zoning.
    for line in lines:
        if "|" not in line:
            continue
        for seg in (s.strip() for s in line.split("|")):
            if not seg or CASE_NO_RE.search(seg) or TMS_RE.search(seg):
                continue
            if COUNCIL_RE.search(seg) or ACREAGE_RE.search(seg):
                continue
            if re.search(r"height|district\s+\d|construction|category",
                         seg, re.IGNORECASE):
                continue
            if re.fullmatch(r"[\d\s./-]+", seg):
                continue
            item.neighborhood = seg
            break
        if item.neighborhood:
            break

    # Request text: from the first "Request..." line up to Owner:/Applicant:.
    req_lines: list[str] = []
    in_request = False
    for line in lines:
        if OWNER_RE.match(line) or APPLICANT_RE.match(line):
            in_request = False
        if in_request:
            req_lines.append(line)
        elif REQUEST_RE.search(line) and not in_request:
            in_request = True
            req_lines.append(line[REQUEST_RE.search(line).start():])
    item.request_text = re.sub(r"\s+", " ", " ".join(req_lines)).strip()

    # Rezoning targets: zoning codes mentioned after "to" in rezonings.
    if re.search(r"\brezon", item.request_text, re.IGNORECASE):
        tail = re.split(r"\bto\b", item.request_text, flags=re.IGNORECASE)
        if len(tail) > 1:
            item.rezoning_to = ZONING_CODE_RE.findall(tail[-1])


def parse_agenda_text(text: str) -> list[AgendaItem]:
    """Parse extracted agenda text into structured items."""
    out: list[AgendaItem] = []
    for number, address, lines, section in _split_items(text):
        item = AgendaItem(item_number=number, address=address,
                          section=section)
        _parse_body(item, lines)
        # Drop obvious non-items (procedural motions with no parcel data).
        if item.tms or item.case_number or item.request_text:
            out.append(item)
    return out


def parse_agenda_pdf(pdf_bytes: bytes) -> list[AgendaItem]:
    return parse_agenda_text(extract_pdf_text(pdf_bytes))
