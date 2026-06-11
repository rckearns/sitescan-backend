"""Discover new agenda PDFs from Charleston's CivicPlus AgendaCenter.

Two discovery strategies, both cheap:

  1. RSS:   GET /AgendaCenter/RSS  (CivicPlus publishes one item per posted
            agenda; category title identifies the board)
  2. HTML:  GET /AgendaCenter and scan for links matching
            /AgendaCenter/ViewFile/Agenda/_MMDDYYYY-NNNN, using the nearest
            preceding section header (h2) to identify the board.

The HTML path is the primary one because the RSS feed occasionally lags.
Returns a list of AgendaRef objects; the pipeline diffs them against the
DB to decide what's new.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from . import config

log = logging.getLogger(__name__)

AGENDA_LINK_RE = re.compile(
    r"/AgendaCenter/ViewFile/Agenda/_(\d{2})(\d{2})(\d{4})-(\d+)"
)


@dataclass(frozen=True)
class AgendaRef:
    board_code: str
    meeting_date: date
    pdf_url: str
    external_id: str        # e.g. "_01142026-10600" -- unique per agenda
    title: str = ""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = config.USER_AGENT
    return s


def _match_board(text: str) -> str | None:
    """Map a section header / category title to a canonical board code."""
    for code, board in config.BOARDS.items():
        for pat in board["patterns"]:
            if re.search(pat, text, re.IGNORECASE):
                return code
    return None


def _excluded(title: str) -> bool:
    return any(
        re.search(p, title, re.IGNORECASE)
        for p in config.EXCLUDE_TITLE_PATTERNS
    )


def _parse_link(href: str) -> tuple[date, str] | None:
    m = AGENDA_LINK_RE.search(href or "")
    if not m:
        return None
    mm, dd, yyyy, _id = m.groups()
    try:
        meeting = date(int(yyyy), int(mm), int(dd))
    except ValueError:
        return None
    external_id = f"_{mm}{dd}{yyyy}-{_id}"
    return meeting, external_id


def discover_html(session: requests.Session | None = None) -> list[AgendaRef]:
    """Scrape the AgendaCenter index page for agenda PDF links."""
    s = session or _session()
    resp = s.get(config.AGENDA_CENTER_URL, timeout=config.HTTP_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    refs: list[AgendaRef] = []
    # CivicPlus renders one table/section per category, preceded by a header.
    # Walk all agenda links and look upward for the nearest category header.
    for a in soup.find_all("a", href=AGENDA_LINK_RE):
        parsed = _parse_link(a["href"])
        if not parsed:
            continue
        meeting_date, external_id = parsed

        # Find the governing category: nearest prior h2/h3 or a parent
        # element CivicPlus tags with the category name.
        header_text = ""
        container = a.find_parent(["table", "div", "section"])
        node = container or a
        for prev in node.find_all_previous(["h2", "h3"]):
            header_text = prev.get_text(" ", strip=True)
            if header_text:
                break

        link_text = a.get_text(" ", strip=True)
        board = _match_board(header_text) or _match_board(link_text)
        if not board:
            continue
        if _excluded(link_text) or _excluded(header_text):
            continue

        refs.append(
            AgendaRef(
                board_code=board,
                meeting_date=meeting_date,
                pdf_url=config.BASE_URL + a["href"],
                external_id=external_id,
                title=link_text or header_text,
            )
        )
    log.info("HTML discovery found %d agenda refs", len(refs))
    return refs


def discover_rss(session: requests.Session | None = None) -> list[AgendaRef]:
    """Parse the AgendaCenter RSS feed (secondary strategy)."""
    s = session or _session()
    try:
        resp = s.get(
            f"{config.AGENDA_CENTER_URL}/RSS", timeout=config.HTTP_TIMEOUT
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("RSS discovery failed: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "xml")
    refs: list[AgendaRef] = []
    for item in soup.find_all("item"):
        title = item.title.get_text(strip=True) if item.title else ""
        link = item.link.get_text(strip=True) if item.link else ""
        parsed = _parse_link(link)
        board = _match_board(title)
        if not parsed or not board or _excluded(title):
            continue
        meeting_date, external_id = parsed
        url = link if link.startswith("http") else config.BASE_URL + link
        refs.append(
            AgendaRef(
                board_code=board,
                meeting_date=meeting_date,
                pdf_url=url,
                external_id=external_id,
                title=title,
            )
        )
    log.info("RSS discovery found %d agenda refs", len(refs))
    return refs


def discover() -> list[AgendaRef]:
    """Run both strategies, dedupe on external_id."""
    s = _session()
    seen: dict[str, AgendaRef] = {}
    for ref in discover_html(s):
        seen[ref.external_id] = ref
    time.sleep(config.REQUEST_DELAY_SECONDS)
    for ref in discover_rss(s):
        seen.setdefault(ref.external_id, ref)
    return sorted(seen.values(), key=lambda r: r.meeting_date, reverse=True)


def fetch_pdf(url: str, session: requests.Session | None = None) -> bytes:
    s = session or _session()
    resp = s.get(url, timeout=config.HTTP_TIMEOUT)
    resp.raise_for_status()
    if not resp.content[:5].startswith(b"%PDF"):
        raise ValueError(f"Response from {url} is not a PDF")
    return resp.content
