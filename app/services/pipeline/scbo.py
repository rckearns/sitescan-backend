"""SCBO (SC Business Opportunities) as a pipeline source.

Reads two SCBO categories from the daily online edition:

  c=2  Architectural-Engineering  -> stage "ae-selection", source "scbo-ae"
  c=3  Construction               -> "cmr-solicitation" / "design-build-solicitation"
                                     / "bid" / "other", source "scbo-construction"

How the listing pages behave (checked Oct 2026): a date page such as
``online-edition?c=3-2025-02-19`` lists every ad in that category that was
still open on that date, not only ads published that day (a single day's
Construction page has ~120-180 ads). So covering a date range does not need
one request per day: this module samples the range every ``step_days`` days
(always including both ends), de-duplicates by ad id, and by default keeps
only ads whose "Ad Publish Date" falls inside the range.

Fetching reuses ``app.services.scanners._fetch_scbo_html``. That helper uses
ZenRows when ``ZENROWS_API_KEY`` is set and falls back to curl_cffi (Chrome
impersonation) and then plain httpx otherwise. Without a ZenRows key the
direct request worked from a residential connection in testing; SCBO has
blocked cloud/datacenter IPs before, which is why ZenRows exists. Note that
the direct path has no block-page check, so this module treats a page with no
"Project Name:" markers as a failed fetch and logs it.

This module never writes to the database.
"""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from bs4 import BeautifulSoup

from app.services.pipeline.events import PipelineEvent, normalize_pip, pip_project_key

logger = logging.getLogger("sitescan.pipeline.scbo")

SCBO_BASE = "https://scbo.sc.gov"

# category code -> (event source, human label)
CATEGORIES: Dict[int, Tuple[str, str]] = {
    2: ("scbo-ae", "Architectural-Engineering"),
    3: ("scbo-construction", "Construction"),
}

DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_STEP_DAYS = 3
REQUEST_DELAY_SECONDS = 1.5
TEXT_LIMIT = 6000

# --- Charleston filter ----------------------------------------------------

CHARLESTON_PLACES_RE = re.compile(
    r"\b(?:north\s+charleston|charleston|mount\s+pleasant|mt\.?\s+pleasant|"
    r"summerville|goose\s+creek|hanahan|ladson|moncks\s+corner|"
    r"james\s+island|johns\s+island|daniel\s+island|isle\s+of\s+palms|"
    r"folly\s+beach|kiawah|berkeley\s+county|dorchester\s+county|"
    r"charleston\s+county)\b",
    re.IGNORECASE,
)
# State agency codes: H15 College of Charleston, H09 The Citadel, H51 MUSC.
CHARLESTON_AGENCY_CODES = ("H15", "H09", "H51")
CHARLESTON_AGENCIES_RE = re.compile(
    r"college\s+of\s+charleston|\bMUSC\b|medical\s+university|"
    r"\bthe\s+citadel\b|\bcitadel\b|trident\s+technical|"
    r"state\s+ports\s+authority|\bSCSPA\b|charleston\s+county\s+school",
    re.IGNORECASE,
)

# --- field mapping ----------------------------------------------------------

_IFB_RE = re.compile(
    r"\bIFB\b|\bITB\b|invitation\s+(?:for|to)\s+bids?|sealed\s+bids?|"
    r"bid\s+opening|lowest\s+responsive",
    re.IGNORECASE,
)

_MONEY_RE = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*(million|mil|mm|m|k)?\b", re.IGNORECASE
)

_DATE_RE = re.compile(r"([A-Z][a-z]+\.?\s+\d{1,2},\s*\d{4})")
_TIME_RE = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\.?", re.IGNORECASE)


def map_delivery_method(raw: str) -> str:
    """'CM-R' -> 'cmr', 'Design-Build' -> 'design-build', etc.; '' if unknown."""
    t = re.sub(r"[\s_]+", " ", (raw or "").strip().lower())
    if not t:
        return ""
    if re.search(r"\bcm\s*-?\s*(?:r|ar|at\s*risk)\b|construction\s+manager\s+at\s+risk", t):
        return "cmr"
    if re.search(r"design\s*-?\s*bid\s*-?\s*build|\bdbb\b", t):
        return "design-bid-build"
    if re.search(r"design\s*-?\s*build", t):
        return "design-build"
    return ""


def parse_cost_range(raw: str) -> Tuple[Optional[Tuple[float, float]], Optional[float]]:
    """'$60,000,000 to $100,000,000' -> ((60e6, 100e6), 100e6)."""
    amounts: List[float] = []
    for num, unit in _MONEY_RE.findall(raw or ""):
        try:
            v = float(num.replace(",", ""))
        except ValueError:
            continue
        u = (unit or "").lower()
        if u in ("million", "mil", "mm", "m"):
            v *= 1_000_000
        elif u == "k":
            v *= 1_000
        amounts.append(v)
    if not amounts:
        return None, None
    if len(amounts) == 1:
        high = amounts[0]
        low = 0.0 if re.search(r"under|less\s+than|up\s+to|below|<", raw, re.I) else high
        return (low, high), high
    low, high = min(amounts[:2]), max(amounts[:2])
    return (low, high), high


def parse_scbo_datetime(raw: str) -> Optional[datetime]:
    """'March 11, 2025 - 2:00pm' -> datetime(2025, 3, 11, 14, 0)."""
    m = _DATE_RE.search(raw or "")
    if not m:
        return None
    ds = m.group(1).replace(".", "")
    d = None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d,%Y"):
        try:
            d = datetime.strptime(ds, fmt)
            break
        except ValueError:
            continue
    if d is None:
        return None
    t = _TIME_RE.search(raw[m.end():])
    if t:
        hour = int(t.group(1)) % 12 + (12 if t.group(3).lower() == "p" else 0)
        d = d.replace(hour=hour, minute=int(t.group(2) or 0))
    return d


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


_PLACEHOLDER_RE = re.compile(r"^(?:n/?a|none|tbd|tba|-+|\.)?$", re.IGNORECASE)


def _project_number(raw: str) -> str:
    """Project number as written, or '' for placeholders like 'n/a'."""
    raw = _clean(raw)
    return "" if _PLACEHOLDER_RE.match(raw) else raw


# --- HTML parsing -----------------------------------------------------------

def parse_scbo_ads(html: str) -> List[dict]:
    """Split an SCBO online-edition page into ads.

    Works for listing pages (``?c=2-YYYY-MM-DD``) and single-ad pages
    (``?s=<id>``). Each ad is a run of ``div.adata`` rows starting with the
    row that holds "Project Name:". Returns dicts of label -> value text plus
    ``_ad_id`` and ``_links`` (label -> first href).
    """
    soup = BeautifulSoup(html or "", "html.parser")
    ads: List[dict] = []
    current: Optional[dict] = None
    for row in soup.find_all("div", class_="adata"):
        labels = row.find_all("b")
        if any(_clean(b.get_text()) == "Project Name:" for b in labels):
            current = {"_links": {}}
            ads.append(current)
        if current is None:
            continue
        for b in labels:
            label = _clean(b.get_text()).rstrip(":")
            if not label:
                continue
            cell = b.find_parent("div", class_="adata_itm")
            value_cell = cell.find_next_sibling("div", class_="adata_itm") if cell else None
            if value_cell is None:
                continue
            current[label] = _clean(value_cell.get_text(" "))
            a = value_cell.find("a", href=True)
            if a and not a["href"].startswith("mailto:"):
                current["_links"][label] = a["href"]
        pr = row.find("a", href=re.compile(r"printad\?a=\d+"))
        if pr:
            current["_ad_id"] = re.search(r"a=(\d+)", pr["href"]).group(1)
    return ads


def is_charleston_area(ad: dict) -> bool:
    proj_num = (ad.get("Project Number") or "").upper()
    if proj_num[:3] in CHARLESTON_AGENCY_CODES:
        return True
    agency = ad.get("Agency/Owner", "")
    if CHARLESTON_AGENCIES_RE.search(agency):
        return True
    location = _strip_street_names(ad.get("Project Location", ""))
    if location.strip():
        # A stated location decides it: an Aiken job on "Charleston Hwy" isn't ours.
        return bool(CHARLESTON_PLACES_RE.search(location))
    hay = " ".join([agency, ad.get("Project Name", ""), ad.get("Description", "")])
    return bool(CHARLESTON_PLACES_RE.search(_strip_street_names(hay)))


STREET_NAME_RE = re.compile(
    r"\bcharleston\s+(?:st|street|ave|avenue|rd|road|hwy|highway|blvd|boulevard|dr|drive|ln|lane|way|pike)\b\.?",
    re.IGNORECASE,
)


def _strip_street_names(text: str) -> str:
    return STREET_NAME_RE.sub(" ", text or "")


def _construction_stage(delivery: str, ad: dict) -> str:
    if delivery == "cmr":
        return "cmr-solicitation"
    if delivery == "design-build":
        return "design-build-solicitation"
    if delivery == "design-bid-build":
        return "bid"
    hay = " ".join([ad.get("Project Number", ""), ad.get("Project Name", ""),
                    ad.get("Description", "")])
    return "bid" if _IFB_RE.search(hay) else "other"


def ad_to_event(ad: dict, category: int, listing_url: str = "") -> PipelineEvent:
    source, cat_label = CATEGORIES[category]
    name = ad.get("Project Name", "")
    proj_num = _project_number(ad.get("Project Number", ""))
    agency = ad.get("Agency/Owner", "")
    location = ad.get("Project Location", "")
    ad_id = ad.get("_ad_id", "")

    delivery_raw = ad.get("Project Delivery Method") or ad.get("Anticipated Project Delivery Method") or ""
    delivery = map_delivery_method(delivery_raw)
    stage = "ae-selection" if category == 2 else _construction_stage(delivery, ad)

    cost_raw = ad.get("Construction Cost Range", "")
    cost_range, estimate = parse_cost_range(cost_raw)

    due_raw = ad.get("Bid/Submittal Date & Time") or ad.get("Resume Deadline") or ""
    deadline = parse_scbo_datetime(due_raw)
    published = parse_scbo_datetime(ad.get("Ad Publish Date", ""))

    pip = normalize_pip(proj_num)
    project_key = pip_project_key(pip) or f"SCBO:{proj_num or ad_id}"
    ad_url = f"{SCBO_BASE}/online-edition?s={ad_id}" if ad_id else listing_url
    form_url = ad.get("_links", {}).get("Project Details", "")

    text_lines = [
        f"SCBO {cat_label} ad",
        f"Project Name: {name}",
        f"Project Number: {proj_num}",
        f"Agency/Owner: {agency}",
        f"Project Location: {location}",
    ]
    if delivery_raw:
        label = "Project Delivery Method" if category == 3 else "Anticipated Project Delivery Method"
        text_lines.append(f"{label}: {delivery_raw}")
    if cost_raw:
        text_lines.append(f"Construction Cost Range: {cost_raw}")
    if due_raw:
        text_lines.append(f"Due: {due_raw}")
    if ad.get("Prime Contractor License (Special Standard of Responsibility)"):
        text_lines.append("Prime Contractor License: "
                          + ad["Prime Contractor License (Special Standard of Responsibility)"])
    text_lines.append(f"Description: {ad.get('Description', '')}")
    text = "\n".join(text_lines)[:TEXT_LIMIT]

    return PipelineEvent(
        source=source,
        external_id=ad_id or f"{proj_num}:{ad.get('Ad Publish Date', '')}",
        project_key=project_key,
        title=name,
        source_url=ad_url,
        event_date=published.date() if published else None,
        stage=stage,
        owner=agency,
        pip_number=pip,
        delivery_method=delivery,
        estimate=estimate,
        cost_range=cost_range,
        location=location,
        deadline=deadline,
        text=text,
        extra={
            "category": cat_label,
            "ad_id": ad_id,
            "project_number": proj_num,
            "delivery_method_raw": delivery_raw,
            "cost_range_raw": cost_raw,
            "form_url": form_url,
            "listing_url": listing_url,
        },
    )


def parse_scbo_page(html: str, category: int, listing_url: str = "",
                    charleston_only: bool = True) -> List[PipelineEvent]:
    """Parse one SCBO page into events (no network)."""
    events = []
    for ad in parse_scbo_ads(html):
        if not ad.get("Project Name"):
            continue
        if charleston_only and not is_charleston_area(ad):
            continue
        events.append(ad_to_event(ad, category, listing_url))
    return events


# --- fetching ---------------------------------------------------------------

def listing_url(category: int, day: date) -> str:
    return f"{SCBO_BASE}/online-edition?c={category}-{day.year}-{day.month:02d}-{day.day:02d}"


def sample_days(since: date, until: date, step_days: int) -> List[date]:
    """until, until-step, ... down to since (both ends always included)."""
    step = max(1, int(step_days))
    days, d = [], until
    while d > since:
        days.append(d)
        d -= timedelta(days=step)
    days.append(since)
    return days


async def _fetch(url: str, client=None) -> str:
    if client is None:
        from app.services.scanners import _fetch_scbo_html
        return await _fetch_scbo_html(url)
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.text


async def fetch_scbo_pipeline_events(
    since: Optional[date] = None,
    until: Optional[date] = None,
    client=None,
    *,
    categories: Iterable[int] = (2, 3),
    step_days: int = DEFAULT_STEP_DAYS,
    charleston_only: bool = True,
    published_in_range: bool = True,
    delay_seconds: float = REQUEST_DELAY_SECONDS,
) -> List[PipelineEvent]:
    """SCBO A/E and Construction ads for Charleston-area projects.

    ``since`` defaults to 14 days before ``until`` (default today). ``client``
    is optional: anything with ``async get(url)`` returning a response with
    ``raise_for_status()`` and ``.text`` (e.g. ``httpx.AsyncClient``). Without
    it, pages go through ``scanners._fetch_scbo_html`` (ZenRows when keyed).
    A failed page is logged and skipped; it never raises for one bad page.
    """
    until = until or date.today()
    since = since or (until - timedelta(days=DEFAULT_LOOKBACK_DAYS))
    if since > until:
        since, until = until, since

    seen: Dict[Tuple[str, str], PipelineEvent] = {}
    first = True
    for cat in categories:
        if cat not in CATEGORIES:
            raise ValueError(f"unsupported SCBO category {cat}")
        for day in sample_days(since, until, step_days):
            url = listing_url(cat, day)
            if not first and delay_seconds:
                await asyncio.sleep(delay_seconds)
            first = False
            try:
                html = await _fetch(url, client)
            except Exception as exc:  # noqa: BLE001 - one bad page shouldn't stop the run
                logger.warning("SCBO fetch failed for %s: %s", url, exc)
                continue
            if "Project Name:" not in html:
                logger.warning("SCBO %s: no ads found (%d bytes) - blocked or empty page",
                               url, len(html))
                continue
            for ev in parse_scbo_page(html, cat, url, charleston_only):
                if published_in_range and ev.event_date and not (since <= ev.event_date <= until):
                    continue
                seen.setdefault((ev.source, ev.external_id), ev)
    events = sorted(seen.values(), key=lambda e: (e.event_date or date.min), reverse=True)
    logger.info("SCBO pipeline: %d events (%s to %s)", len(events), since, until)
    return events
