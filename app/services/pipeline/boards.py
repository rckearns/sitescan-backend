"""City of Charleston board agendas (BAR, Planning Commission, TRC, DRB) as a
pipeline source.

Reuses sitescan_boards' poller (AgendaCenter discovery + PDF download),
parser and classifier, but none of sitescan_boards.pipeline's database code.

  fetch_board_events(since)       async; runs the blocking work in a thread
  fetch_board_events_sync(since)  the same, for callers already in a thread
  agenda_items_to_events(...)     pure mapping, used by tests

Network use: one GET of /AgendaCenter (plus the RSS feed), one POST per board
category for each past year in range, then one PDF per agenda in range,
REQUEST_DELAY_SECONDS apart.
"""

import asyncio
import logging
import re
import time
from datetime import date
from typing import Callable, Iterable, List, Optional

from app.services.pipeline.events import PipelineEvent

logger = logging.getLogger("sitescan.pipeline.boards")

TEXT_LIMIT = 6000

CONCEPT_STAGES = {"conceptual", "preliminary", "rezoning", "concept_plan", "demolition"}
FINAL_STAGES = {"final"}

_PUD_RE = re.compile(r"\bPUD\b|planned\s+unit\s+development", re.IGNORECASE)
_SITE_PLAN_RE = re.compile(r"\bsite\s+plan\b", re.IGNORECASE)
_CONCEPT_RE = re.compile(r"\bconcept(?:ual)?\b|\bpreliminary\b|\brezon|\bzoning\b",
                         re.IGNORECASE)
# Items with no TMS/case number are kept only if the "address" looks like a
# place (ordinance amendments and staff updates are numbered items too).
_PLACE_RE = re.compile(r"^\d|\b(?:st|street|ave|avenue|rd|road|dr|drive|blvd|boulevard|"
                       r"hwy|highway|lane|ln|way|island|corner)\b", re.IGNORECASE)

_STREET_ABBR = [
    (r"\bstreet\b", "st"), (r"\bavenue\b", "ave"), (r"\broad\b", "rd"),
    (r"\bdrive\b", "dr"), (r"\bboulevard\b", "blvd"), (r"\blane\b", "ln"),
    (r"\bplace\b", "pl"), (r"\bcourt\b", "ct"), (r"\bhighway\b", "hwy"),
    (r"\bsaint\b", "st"), (r"\bnorth\b", "n"), (r"\bsouth\b", "s"),
    (r"\beast\b", "e"), (r"\bwest\b", "w"),
]


def normalize_address(address: str) -> str:
    """'106 Coming Street' -> '106 COMING ST' (for project keys)."""
    a = (address or "").lower()
    a = re.sub(r"[.,#()]", " ", a)
    for pat, rep in _STREET_ABBR:
        a = re.sub(pat, rep, a)
    return re.sub(r"\s+", " ", a).strip().upper()


def normalize_tms(tms: Optional[str]) -> str:
    """First parcel of a TMS field as digits only.

    '460-16-03-017' and '4601603017' are the same parcel (agendas switched
    formats in 2026); '4590503136,4590503139' -> '4590503136'.
    """
    if not tms:
        return ""
    first = re.split(r"[,&/]", tms)[0]
    digits = re.sub(r"\D", "", first)
    return digits if len(digits) >= 7 else ""


def board_project_key(item) -> str:
    tms = normalize_tms(item.tms)
    if tms:
        return f"BOARD:{tms}"
    if not item.case_number and not _PLACE_RE.search(item.address or ""):
        return ""
    addr = normalize_address(item.address)
    return f"BOARD:{addr}" if addr else ""


def pipeline_stage(stage_label: Optional[str], text: str) -> str:
    """Classifier stage label (+ request text) -> PipelineEvent stage."""
    if stage_label in FINAL_STAGES:
        return "board-final"
    if stage_label in CONCEPT_STAGES:
        return "board-concept"
    if _SITE_PLAN_RE.search(text or ""):
        return "board-final"
    if _PUD_RE.search(text or "") or _CONCEPT_RE.search(text or ""):
        return "board-concept"
    return "other"


def agenda_items_to_events(ref, items: Iterable, min_score: int = 0) -> List[PipelineEvent]:
    """Map parsed AgendaItems from one agenda (AgendaRef) to PipelineEvents."""
    from sitescan_boards import config
    from sitescan_boards.classifier import classify

    board_name = config.BOARDS.get(ref.board_code, {}).get("name", ref.board_code)
    events: List[PipelineEvent] = []
    for item in items:
        c = classify(item, ref.board_code)
        if c.score < min_score:
            continue
        key = board_project_key(item)
        if not key:
            continue
        request = item.request_text or ""
        title = f"{board_name}: {item.address}"
        if request:
            title += f" - {request[:140]}"
        local_id = item.case_number or f"{item.section or ''}-{item.item_number}"
        events.append(PipelineEvent(
            source="board",
            external_id=f"{ref.external_id}:{local_id}",
            project_key=key,
            title=title,
            source_url=ref.pdf_url,
            event_date=ref.meeting_date,
            stage=pipeline_stage(c.stage, request or item.raw_text),
            owner=item.owner or item.applicant or "",
            location="Charleston",
            address=item.address,
            text=f"{board_name} agenda, {ref.meeting_date:%B %d, %Y}\n{item.raw_text}"[:TEXT_LIMIT],
            extra={
                "board": ref.board_code,
                "board_name": board_name,
                "agenda_id": ref.external_id,
                "case_number": item.case_number or "",
                "tms": item.tms or "",
                "item_number": item.item_number,
                "section": item.section or "",
                "status": getattr(item, "status", None) or "",
                "applicant": item.applicant or "",
                "neighborhood": item.neighborhood or "",
                "acreage": item.acreage,
                "score": c.score,
                "stage_label": c.stage or "",
                "tags": list(c.tags),
            },
        ))
    return events


def discover_refs(since: date, until: Optional[date] = None, session=None) -> list:
    """Agenda refs with since <= meeting date <= until.

    ``until`` defaults to no upper bound, so agendas already posted for
    upcoming meetings are included.

    The current year comes from the AgendaCenter index (+RSS); earlier
    years in the range are listed via POST UpdateCategoryList.
    """
    from sitescan_boards import poller

    refs = {r.external_id: r for r in poller.discover()}
    current_year = date.today().year
    last_year = min((until or date.today()).year, current_year)
    for year in range(since.year, last_year + 1):
        if year == current_year:
            continue
        time.sleep(_delay())
        for r in poller.discover_year(year, session):
            refs.setdefault(r.external_id, r)
    out = [r for r in refs.values() if since <= r.meeting_date <= (until or date.max)]
    return sorted(out, key=lambda r: r.meeting_date, reverse=True)


def _delay() -> float:
    from sitescan_boards import config
    return config.REQUEST_DELAY_SECONDS


def fetch_board_events_sync(
    since: date,
    until: Optional[date] = None,
    *,
    boards: Optional[Iterable[str]] = None,
    min_score: int = 0,
    max_agendas: Optional[int] = None,
    fetch_pdf: Optional[Callable[[str], bytes]] = None,
    refs: Optional[list] = None,
    skip_urls: Optional[set] = None,
    on_document: Optional[Callable[[str, str, date, int], None]] = None,
) -> List[PipelineEvent]:
    """Blocking version: discover -> download -> parse -> classify -> events.

    ``boards`` limits to board codes (e.g. {"BAR-L", "PC"}). ``refs`` and
    ``fetch_pdf`` can be injected (tests, or an integrator with its own
    discovery). Agendas that fail to download or parse are logged and skipped.
    Agendas in ``skip_urls`` were read on an earlier run and are skipped once
    their meeting has passed (upcoming agendas can still be amended).
    ``on_document("board", url, meeting_date, n_events)`` is called per agenda read.
    """
    from sitescan_boards import poller
    from sitescan_boards.parser import parse_agenda_pdf

    if refs is None:
        refs = discover_refs(since, until)
    else:
        refs = [r for r in refs if since <= r.meeting_date <= (until or date.max)]
    if boards:
        wanted = set(boards)
        refs = [r for r in refs if r.board_code in wanted]
    if skip_urls:
        today = date.today()
        refs = [r for r in refs if not (r.pdf_url in skip_urls and r.meeting_date < today)]
    if max_agendas is not None:
        refs = refs[:max_agendas]

    session = poller._session() if fetch_pdf is None else None
    get_pdf = fetch_pdf or (lambda url: poller.fetch_pdf(url, session))

    events: List[PipelineEvent] = []
    for i, ref in enumerate(refs):
        if i and fetch_pdf is None:
            time.sleep(_delay())
        try:
            items = parse_agenda_pdf(get_pdf(ref.pdf_url))
        except Exception as exc:  # noqa: BLE001 - skip one bad agenda
            logger.warning("Board agenda %s failed: %s", ref.pdf_url, exc)
            continue
        new_events = agenda_items_to_events(ref, items, min_score=min_score)
        events.extend(new_events)
        if on_document:
            on_document("board", ref.pdf_url, ref.meeting_date, len(new_events))
    logger.info("Boards pipeline: %d events from %d agendas", len(events), len(refs))
    return events


async def fetch_board_events(since: date, until: Optional[date] = None,
                             **kwargs) -> List[PipelineEvent]:
    """Async wrapper: runs fetch_board_events_sync in a worker thread."""
    return await asyncio.to_thread(fetch_board_events_sync, since, until, **kwargs)
