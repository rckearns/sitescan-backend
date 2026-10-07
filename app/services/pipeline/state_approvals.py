"""State capital-project approvals: JBRC agenda packages and SFAA minutes.

South Carolina permanent improvement projects (PIPs) pass through the Joint
Bond Review Committee (JBRC) and, for most projects, the State Fiscal
Accountability Authority (SFAA) a week or so later. Both publish PDFs whose
project entries share one layout:

    3. Project:   College of Charleston
       H15.9689:  Project 205 New Construction
    Request: Change Project Name and increase Phase I Pre-Design Budget ...
    Included in CPIP: Yes – 2024 CPIP Priority 4 of 8 in FY25 (estimated at $164,800,000)
    <funding table – mangled by text extraction, ignored>
    Summary of Work: ...
    Rationale: ...
    Facility Characteristics: ...
    Financial Impact: ...
    Full Project Estimate: $164,800,000 (internal). ...

(SFAA minutes write "(c) Project: JBRC Item 3: College of Charleston".)

This module discovers the documents, extracts those entries, keeps the
Charleston-area ones and returns them as PipelineEvent objects. It never
touches the database; the pipeline job stores and links the events.

Kept Python 3.9 compatible (local tooling); production runs 3.12.
"""

import asyncio
import io
import logging
import re
from datetime import date
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx

from app.services.pipeline.events import PipelineEvent, normalize_pip, pip_project_key

logger = logging.getLogger(__name__)

JBRC_INDEX_URL = (
    "https://www.scstatehouse.gov/CommitteeInfo/JointBondReviewCommittee/"
    "JointBondReviewCommittee.php"
)
SFAA_INDEX_URL = "https://sfaa.sc.gov/authority-meetings"

USER_AGENT = "Yabodle/1.0 (sitescan pipeline; contact@yabodle.com)"
REQUEST_DELAY_SECONDS = 2.0       # pause between document downloads
MAX_PDF_BYTES = 80 * 1024 * 1024  # JBRC packages run 6–38 MB
TEXT_LIMIT = 6000

# (url, meeting_date, title)
DocumentRef = Tuple[str, date, str]


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_NAME_RE = (
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|"
    r"Aug(?:ust)?|Sept?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
_DATE_WORDS_RE = re.compile(
    _MONTH_NAME_RE + r"\.?[\s_\-]*(\d{1,2})(?:st|nd|rd|th)?,?[\s_\-]*(\d{4})", re.I
)
_DATE_NUMERIC_RE = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")
_DATE_ISO_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")


def parse_meeting_date(text: str) -> Optional[date]:
    """First recognizable date in `text` ("June 3, 2025", "Feb 4 2026",
    "6/3/2025", "2025-06-03"); None if there is none."""
    if not text:
        return None
    candidates = []
    for m in _DATE_WORDS_RE.finditer(text):
        key = m.group(1).lower().rstrip(".")
        month = _MONTHS.get(key[:4] if key.startswith("sept") else key[:3])
        candidates.append((m.start(), month, int(m.group(2)), int(m.group(3))))
    for m in _DATE_NUMERIC_RE.finditer(text):
        candidates.append((m.start(), int(m.group(1)), int(m.group(2)), int(m.group(3))))
    for m in _DATE_ISO_RE.finditer(text):
        candidates.append((m.start(), int(m.group(2)), int(m.group(3)), int(m.group(1))))
    for _, month, day, year in sorted(candidates):
        try:
            return date(year, month, day)
        except (TypeError, ValueError):
            continue
    return None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _quote_url(url: str) -> str:
    """Percent-encode spaces etc. in the path (JBRC file names have spaces)."""
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=quote(parts.path, safe="/%:@-._~!$&'()*+,;=")))


def _soup(html: str):
    from bs4 import BeautifulSoup
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:  # lxml missing
        return BeautifulSoup(html, "html.parser")


def parse_jbrc_index(html: str, base_url: str = JBRC_INDEX_URL) -> List[DocumentRef]:
    """Agenda-package links from the JBRC committee page, newest first."""
    out: List[DocumentRef] = []
    seen = set()
    for a in _soup(html).find_all("a", href=True):
        href = a["href"].strip()
        if "/agendas/" not in href.lower() or not href.lower().endswith(".pdf"):
            continue
        title = " ".join(a.get_text(" ").split())
        filename = href.rsplit("/", 1)[-1]
        meeting = parse_meeting_date(title) or parse_meeting_date(filename.replace("_", " "))
        if not meeting:
            logger.info("jbrc: no date for agenda link %r (%s)", title, href)
            continue
        url = _quote_url(urljoin(base_url, href))
        if url in seen:
            continue
        seen.add(url)
        out.append((url, meeting, title or filename))
    out.sort(key=lambda d: d[1], reverse=True)
    return out


def parse_sfaa_index(html: str, base_url: str = SFAA_INDEX_URL) -> List[DocumentRef]:
    """Meeting-minutes links from an SFAA "Authority Meetings" page, newest first.

    The page lists each meeting as an expandable header ("click to open/close:
    June 16, 2026 meeting information") followed by a block with Agenda /
    Minutes buttons; file names are arbitrary, so the date comes from the
    nearest preceding meeting header.
    """
    soup = _soup(html)
    out: List[DocumentRef] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        label = " ".join((a.get_text(" ") + " " + (a.get("title") or "")).split())
        href = a["href"].strip()
        if "minutes" not in label.lower() or not href.lower().endswith(".pdf"):
            continue
        meeting = None
        header = a.find_previous("div", class_="ditem")
        if header is not None:
            meeting = parse_meeting_date(header.get_text(" "))
        if meeting is None:
            prev = a.find_previous("a", title=re.compile("meeting information", re.I))
            if prev is not None:
                meeting = parse_meeting_date(prev.get("title", ""))
        if meeting is None:
            meeting = parse_meeting_date(href.rsplit("/", 1)[-1].replace("_", " "))
        if meeting is None:
            logger.info("sfaa: no date for minutes link %s", href)
            continue
        url = _quote_url(urljoin(base_url, href))
        if url in seen:
            continue
        seen.add(url)
        out.append((url, meeting, f"SFAA Minutes {meeting.strftime('%B')} {meeting.day}, {meeting.year}"))
    out.sort(key=lambda d: d[1], reverse=True)
    return out


async def discover_jbrc_documents(client: httpx.AsyncClient) -> List[DocumentRef]:
    resp = await client.get(JBRC_INDEX_URL, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return parse_jbrc_index(resp.text, str(resp.url))


async def discover_sfaa_documents(client: httpx.AsyncClient, years: Iterable[int]) -> List[DocumentRef]:
    out: List[DocumentRef] = []
    seen = set()
    for year in sorted(set(years), reverse=True):
        try:
            resp = await client.post(
                SFAA_INDEX_URL, data={"mtgsel": str(year)}, headers={"User-Agent": USER_AGENT}
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("sfaa: meetings page for %s failed: %s", year, exc)
            continue
        for doc in parse_sfaa_index(resp.text, str(resp.url)):
            if doc[0] not in seen:
                seen.add(doc[0])
                out.append(doc)
        await asyncio.sleep(0.5)
    out.sort(key=lambda d: d[1], reverse=True)
    return out


# ---------------------------------------------------------------------------
# Text cleanup
# ---------------------------------------------------------------------------

_HEADER_LINE_RES = [
    re.compile(r"^\s*JOINT BOND REVIEW COMMITTEE SUMMARY\b.*$"),
    re.compile(r"^\s*PERMANENT IMPROVEMENTS PROPOSED BY AGENCIES\s*$"),
    re.compile(r"^\s*[A-Z][a-z]+\.? \d{1,2}, \d{4} through [A-Z][a-z]+\.? \d{1,2}, \d{4}\s*$"),
    re.compile(r"^\s*Minutes of State Fiscal Accountability Authority\s*$"),
    re.compile(r"^\s*[A-Z][a-z]+ \d{1,2}, \d{4}\s*-+\s*Page \d+\s*$"),
]


def clean_page(text: str) -> str:
    """Drop running headers and the trailing page number; trim blank edges."""
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if not any(r.match(ln) for r in _HEADER_LINE_RES)]
    while lines and not lines[-1].strip():
        lines.pop()
    if lines and re.fullmatch(r"\s*\d{1,3}\s*", lines[-1]):
        lines.pop()
    while lines and not lines[-1].strip():
        lines.pop()
    while lines and not lines[0].strip():
        lines.pop(0)
    return "\n".join(lines)


def _loose(phrase: str) -> str:
    """Regex for `phrase` tolerating the stray spaces PDF extraction inserts
    inside words ("Budge t", "Constructi on")."""
    parts = []
    for ch in phrase:
        if ch == " ":
            parts.append(r"\s+")
        elif ch == "-":
            parts.append(r"\s*-?\s*")
        else:
            parts.append(re.escape(ch) + r"\s?")
    return "".join(parts)


def _squash(text: str) -> str:
    return " ".join((text or "").split())


# ---------------------------------------------------------------------------
# Entry extraction
# ---------------------------------------------------------------------------

# "3. Project:", "(c) Project:", "(ii) Project:"; the agency follows (optionally
# after "JBRC Item 3:"), possibly wrapped over a line, then the PIP line.
_ENTRY_RE = re.compile(
    r"(?m)^[ \t]*(?:\(?[a-z0-9]{1,4}[.)][ \t]*)?Project:[ \t]*"
    r"(?P<head>(?:(?!Project:)[^\n]*\n?){1,3}?)"
    r"[ \t]*(?P<code>[A-Z]\d{2})[ \t]*\.[ \t]*(?P<num>\d{4})[ \t]*:[ \t]*(?P<title>[^\n]*)"
)

_FIELD_LABELS = {
    "request": "Request",
    "cpip": "Included in CPIP",
    "che": "CHE Approval",
    "phase1_approval": "Phase I Approval",
    "phase2_approval": "Phase II Approval",
    "supporting": "Supporting Details",
    "summary": "Summary of Work",
    "rationale": "Rationale",
    "facility_characteristics": "Facility Characteristics",
    "characteristics": "Characteristics",
    "financial_impact": "Financial Impact",
    "full_project_estimate": "Full Project Estimate",
}
# Labels whose value is one short paragraph; the funding table follows them.
_HEADER_FIELDS = ("request", "cpip", "che", "phase1_approval", "phase2_approval", "supporting")

_LABEL_RES = {
    key: re.compile(r"(?m)^[ \t]*" + _loose(label) + r"[ \t]*:")
    for key, label in _FIELD_LABELS.items()
}

_CMR_RE = re.compile(
    _loose("Construction Manager") + r"\s*(?:-\s*)?" + _loose("at") + r"\s*(?:-\s*)?" + _loose("Risk")
    + r"|\bCM\s?-\s?R\b|\bCM\s?@\s?R\b|\bCMR\b|\bCM\s+at\s+Risk\b",
    re.I,
)
_DB_RE = re.compile(r"\bdesign\s?[-/–]\s?build(?:er|ers)?\b|\bdesign\s+build\b", re.I)

_MONEY_RE = re.compile(
    r"\$\s*(\d{1,3}(?:\s?,\s?\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)\s*(million|billion)?", re.I
)


def parse_dollars(text: str) -> Optional[float]:
    """First dollar amount in `text`: "$164,800,000 (internal)" → 164800000.0,
    "$5.5 million" → 5500000.0."""
    m = _MONEY_RE.search(text or "")
    if not m:
        return None
    try:
        value = float(re.sub(r"[\s,]", "", m.group(1)))
    except ValueError:
        return None
    unit = (m.group(2) or "").lower()
    if unit == "million":
        value *= 1_000_000
    elif unit == "billion":
        value *= 1_000_000_000
    return value


def classify_request(request: str) -> str:
    """Request text → "phase2" / "land" / "phase1" / "other"."""
    spaced = " ".join((request or "").lower().split())
    r = re.sub(r"[\s\-]+", "", spaced)  # also joins words split by extraction
    if re.search(r"\bphase\s*(?:ii|2|two)\b", spaced) or "fullconstruction" in r:
        return "phase2"
    if "landacquisition" in r or "acquisitionofland" in r:
        return "land"
    if re.search(r"\bphase\s*(?:i|1|one)\b", spaced) or "predesign" in r:
        return "phase1"
    return "other"


def detect_delivery(text: str) -> Tuple[str, List[str]]:
    """(delivery_method, mentions). Set only when the text explicitly names
    CM at Risk or design-build (and not both)."""
    mentions = []
    if _CMR_RE.search(text or ""):
        mentions.append("cmr")
    if _DB_RE.search(text or ""):
        mentions.append("design-build")
    return (mentions[0] if len(mentions) == 1 else ""), mentions


def _fields(body: str) -> dict:
    """Locate labelled fields; value = text up to the next label."""
    hits = []
    for key, rx in _LABEL_RES.items():
        for m in rx.finditer(body):
            hits.append((m.start(), m.end(), key))
    hits.sort()
    # Labels are anchored at line start, so overlaps shouldn't occur; drop any defensively.
    dedup = []
    for h in hits:
        if dedup and h[0] < dedup[-1][1]:
            continue
        dedup.append(h)
    fields = {}
    for i, (start, end, key) in enumerate(dedup):
        stop = dedup[i + 1][0] if i + 1 < len(dedup) else len(body)
        value = body[end:stop]
        if key in _HEADER_FIELDS:
            value = re.split(r"\n[ \t]*\n", value.strip("\n"), maxsplit=1)[0]
        if key not in fields:
            fields[key] = (start, end, value)
    return fields


def _strip_funding_table(body: str, fields: dict) -> str:
    """Replace the mangled funding table with its "All Sources" line."""
    if "summary" not in fields:
        return body
    summary_start = fields["summary"][0]
    header_ends = []
    for key in _HEADER_FIELDS:
        if key in fields and fields[key][0] < summary_start:
            start, end, value = fields[key]
            header_ends.append(end + len(value) + (len(body[end:]) - len(body[end:].lstrip("\n"))))
    if not header_ends:
        return body
    table_start = max(header_ends)
    if table_start >= summary_start:
        return body
    table = body[table_start:summary_start]
    m = re.search(r"(?m)^[ \t]*All Sources\b[^\n]*", table)
    keep = "\n[Funding table omitted" + (f"; {m.group(0).strip()}" if m else "") + "]\n\n"
    return body[:table_start].rstrip() + "\n" + keep + body[summary_start:]


def _entry_end(body: str) -> int:
    """End the entry after the Full Project Estimate paragraph, so trailing
    section text (votes, the next section's heading) is not swept in."""
    m = _LABEL_RES["full_project_estimate"].search(body)
    if not m:
        return len(body)
    blank = re.search(r"\n[ \t]*\n", body[m.end():])
    return m.end() + blank.start() if blank else len(body)


def extract_entries(text_pages: Sequence[str]) -> List[dict]:
    """Split document text (one string per page) into project entries.

    Returns dicts with: agency, agency_code, pip, project_title, item_label,
    request, stage, estimate, cpip, summary, rationale,
    facility_characteristics, financial_impact, full_project_estimate,
    delivery_method, delivery_mentions, page (1-based), text.
    """
    cleaned = []
    offsets = []  # (char offset, page number)
    pos = 0
    for i, page in enumerate(text_pages):
        c = clean_page(page)
        if not c.strip():
            continue
        offsets.append((pos, i + 1))
        cleaned.append(c)
        pos += len(c) + 1
    doc = "\n".join(cleaned)

    def page_at(offset: int) -> int:
        page = offsets[0][1] if offsets else 1
        for start, num in offsets:
            if start > offset:
                break
            page = num
        return page

    matches = list(_ENTRY_RE.finditer(doc))
    entries = []
    for i, m in enumerate(matches):
        head = _squash(m.group("head"))
        item_label = ""
        im = re.match(r"(JBRC\s+Item\s+\d+)\s*:\s*", head, re.I)
        if im:
            item_label = _squash(im.group(1))
            head = head[im.end():]
        agency = head.strip(" :–-")
        if not agency or len(agency) > 120 or normalize_pip(agency):
            # empty, runaway, or a "Project: H27-6151" style reference line
            continue
        pip = f"{m.group('code')}.{m.group('num')}"
        next_start = matches[i + 1].start() if i + 1 < len(matches) else len(doc)
        body = doc[m.end():next_start]
        body = body[:_entry_end(body)]
        fields = _fields(body)

        def val(key: str) -> str:
            return _squash(fields[key][2]) if key in fields else ""

        request = val("request")
        fpe = val("full_project_estimate")
        cpip = val("cpip")
        estimate = parse_dollars(fpe)
        if estimate is None:
            em = re.search(r"estimated\s+at\s+(\$[^)]*)", cpip, re.I)
            estimate = parse_dollars(em.group(1)) if em else None
        if estimate is None:
            em = re.search(r"estimated\s+at\s+(\$[^)]*)", body, re.I)
            estimate = parse_dollars(em.group(1)) if em else None

        excerpt_body = _strip_funding_table(body, fields)
        header = doc[m.start():m.end()].strip()
        text = (header + "\n" + excerpt_body.strip("\n")).strip()
        text = re.sub(r"\n[ \t]*\n(?:[ \t]*\n)+", "\n\n", text)
        if len(text) > TEXT_LIMIT:
            text = text[:TEXT_LIMIT - 1].rstrip() + "…"
        delivery, mentions = detect_delivery(body)

        entries.append({
            "agency": agency,
            "agency_code": m.group("code"),
            "pip": normalize_pip(pip) or pip,
            "project_title": _squash(m.group("title")),
            "item_label": item_label,
            "request": request,
            "stage": classify_request(request),
            "estimate": estimate,
            "cpip": cpip,
            "summary": val("summary"),
            "rationale": val("rationale"),
            "facility_characteristics": val("facility_characteristics") or val("characteristics"),
            "financial_impact": val("financial_impact"),
            "full_project_estimate": fpe,
            "delivery_method": delivery,
            "delivery_mentions": mentions,
            "page": page_at(m.start()),
            "text": text,
        })
    return entries


# ---------------------------------------------------------------------------
# Charleston filter + events
# ---------------------------------------------------------------------------

CHARLESTON_AGENCY_CODES = {"H15", "H09", "H51"}  # CofC, The Citadel, MUSC
TECH_COLLEGE_CODE = "H59"


def charleston_reason(entry: dict) -> str:
    """Why an entry counts as Charleston-area ("" if it doesn't):
    "agency" (H15/H09/H51), "tech-college" (H59 naming Trident/Charleston)
    or "mention" (any other entry whose text says "Charleston" – broad: it also
    catches statewide lists naming North Charleston, a "Charleston Residence
    Hall" elsewhere, etc.)."""
    haystack = " ".join([entry.get("agency", ""), entry.get("project_title", ""), entry.get("text", "")])
    mentions_charleston = re.search(r"charleston", haystack, re.I) is not None
    code = entry.get("agency_code") or entry.get("pip", "")[:3]
    if code in CHARLESTON_AGENCY_CODES:
        return "agency"
    if code == TECH_COLLEGE_CODE:
        if mentions_charleston or re.search(r"trident", haystack, re.I):
            return "tech-college"
        return ""
    return "mention" if mentions_charleston else ""


def is_charleston_entry(entry: dict) -> bool:
    return bool(charleston_reason(entry))


def _short_request(request: str) -> str:
    r = re.split(r"\s+to\s+|\.\s", request, maxsplit=1)[0].strip()
    return r if len(r) <= 100 else r[:99].rstrip() + "…"


def entry_to_event(entry: dict, source: str, url: str, meeting_date: date) -> PipelineEvent:
    pip = entry["pip"]
    stage = entry["stage"]
    request = _short_request(entry.get("request", ""))
    title = f"{entry['project_title']} ({entry['agency']})"
    if request:
        title += f" – {request}"
    return PipelineEvent(
        source=source,
        external_id=f"{source}:{meeting_date.isoformat()}:{pip}:{stage}",
        project_key=pip_project_key(pip),
        title=title,
        source_url=url,
        event_date=meeting_date,
        stage=stage,
        owner=entry["agency"],
        pip_number=pip,
        delivery_method=entry.get("delivery_method", ""),
        estimate=entry.get("estimate"),
        text=entry.get("text", "")[:TEXT_LIMIT],
        extra={
            "agency_code": entry.get("agency_code", ""),
            "project_title": entry.get("project_title", ""),
            "item_label": entry.get("item_label", ""),
            "request": entry.get("request", ""),
            "cpip": entry.get("cpip", ""),
            "full_project_estimate": entry.get("full_project_estimate", ""),
            "summary": entry.get("summary", ""),
            "facility_characteristics": entry.get("facility_characteristics", ""),
            "delivery_mentions": entry.get("delivery_mentions", []),
            "page": entry.get("page"),
            "charleston_reason": charleston_reason(entry),
        },
    )


def events_from_pages(
    text_pages: Sequence[str], url: str, meeting_date: date, source: str
) -> List[PipelineEvent]:
    """Charleston-area events from already-extracted page text."""
    by_id = {}
    for entry in extract_entries(text_pages):
        if not is_charleston_entry(entry):
            continue
        ev = entry_to_event(entry, source, url, meeting_date)
        prev = by_id.get(ev.external_id)
        if prev is None or len(ev.text) > len(prev.text):
            by_id[ev.external_id] = ev
    return list(by_id.values())


def pdf_text_pages(pdf_bytes: bytes) -> List[str]:
    """Text per page via pypdf; scanned/unreadable pages come back empty."""
    from pypdf import PdfReader

    # pypdf logs a warning per font it can't fully decode (fontTools missing); noise only.
    logging.getLogger("pypdf").setLevel(logging.ERROR)
    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages = []
    for i, page in enumerate(reader.pages):
        try:
            pages.append(page.extract_text() or "")
        except Exception as exc:  # malformed page; keep going
            logger.debug("pdf page %d unreadable: %s", i + 1, exc)
            pages.append("")
    return pages


def parse_document(pdf_bytes: bytes, url: str, meeting_date: date, source: str) -> List[PipelineEvent]:
    """PDF → Charleston-area PipelineEvents. `source` is "jbrc" or "sfaa"."""
    if source not in ("jbrc", "sfaa"):
        raise ValueError(f"unknown source {source!r}")
    return events_from_pages(pdf_text_pages(pdf_bytes), url, meeting_date, source)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def _download(client: httpx.AsyncClient, url: str) -> bytes:
    async with client.stream("GET", url, headers={"User-Agent": USER_AGENT}) as resp:
        resp.raise_for_status()
        chunks = []
        size = 0
        async for chunk in resp.aiter_bytes():
            size += len(chunk)
            if size > MAX_PDF_BYTES:
                raise ValueError(f"{url} exceeds {MAX_PDF_BYTES} bytes")
            chunks.append(chunk)
    return b"".join(chunks)


async def fetch_state_approval_events(
    since: date, client: Optional[httpx.AsyncClient] = None
) -> List[PipelineEvent]:
    """Discover JBRC packages and SFAA minutes dated on/after `since`,
    download them one at a time and return Charleston-area events.

    A failure on one document (or one index page) is logged and skipped.
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, read=120.0), follow_redirects=True
        )
    events: List[PipelineEvent] = []
    try:
        docs: List[Tuple[str, DocumentRef]] = []
        try:
            docs += [("jbrc", d) for d in await discover_jbrc_documents(client)]
        except Exception as exc:
            logger.warning("jbrc discovery failed: %s", exc)
        try:
            years = range(since.year, date.today().year + 1)
            docs += [("sfaa", d) for d in await discover_sfaa_documents(client, years)]
        except Exception as exc:
            logger.warning("sfaa discovery failed: %s", exc)

        docs = [(s, d) for s, d in docs if d[1] >= since]
        docs.sort(key=lambda sd: sd[1][1])
        for n, (source, (url, meeting, title)) in enumerate(docs):
            if n:
                await asyncio.sleep(REQUEST_DELAY_SECONDS)
            try:
                pdf = await _download(client, url)
                parsed = await asyncio.to_thread(parse_document, pdf, url, meeting, source)
                logger.info("%s %s (%s): %d Charleston events", source, meeting, title, len(parsed))
                events.extend(parsed)
            except Exception as exc:
                logger.warning("%s document %s failed: %s", source, url, exc)
    finally:
        if own_client:
            await client.aclose()
    return events
