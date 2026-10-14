"""Recent new construction on a parcel, from City of Charleston building permits.

The county assessor adds a building's value only at a later reassessment, so a lot
with a new building under way (or just finished) still looks vacant in county data.
The City's public "New Construction since 2010" permit layer carries the parcel
number, so Home can flag those parcels. City of Charleston parcels only.
"""

import logging
import re
from datetime import date, datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger("sitescan.construction")

NEW_CONSTRUCTION_URL = (
    "https://gis.charleston-sc.gov/arcgis2/rest/services/External/Applications/MapServer/21/query"
)
LOOKBACK_YEARS = 5          # covers the assessor's reassessment lag
MIN_VALUATION = 100_000     # skip trivial items
STALE_YEARS = 3             # an "Issued" permit this old with no final is treated as abandoned
# "New" permits that aren't a new building: facade mock-ups, interior fit-outs, walls, signs.
NOT_A_BUILDING_RE = re.compile(
    r"mock[\s-]*up|sample\s+panel|up[\s-]*fit|tenant\s+improvement|interior\s+(?:build|finish|renovation)|"
    r"\bsign(?:age)?\b|retaining\s+wall|trip\s+wall|sea\s*wall|\bfence\b|\bdock\b|\bpool\b|\btank\b",
    re.IGNORECASE,
)
PAGE_SIZE = 5000
FIELDS = "MAIN_PARCEL_NUMBER,PERMIT_NUMBER,WORK_CLASS,PERMIT_STATUS,ISSUE_DATE,FINALED_DATE,VALUATION,DESCRIPTION"


def _year(v) -> Optional[int]:
    """Year from an epoch-ms number or an "MM/DD/YYYY" string (the layer uses both)."""
    if not v:
        return None
    if isinstance(v, (int, float)):
        try:
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc).year
        except (ValueError, OverflowError, OSError):
            return None
    m = re.search(r"(\d{4})", str(v))
    return int(m.group(1)) if m else None


def _counts(a: dict, this_year: int) -> bool:
    """Whether a permit means a new building on the parcel (not a mock-up, fit-out or stale permit)."""
    if NOT_A_BUILDING_RE.search(str(a.get("DESCRIPTION") or "")):
        return False
    status = str(a.get("PERMIT_STATUS") or "").strip().lower()
    issued = _year(a.get("ISSUE_DATE"))
    return not (status != "completed" and issued and issued < this_year - STALE_YEARS)


def _summary(a: dict) -> dict:
    return {
        "status": "completed" if str(a.get("PERMIT_STATUS") or "").strip().lower() == "completed" else "underway",
        "issued": _year(a.get("ISSUE_DATE")),
        "finaled": _year(a.get("FINALED_DATE")),
        "valuation": a.get("VALUATION") or 0,
        "permit": a.get("PERMIT_NUMBER") or "",
        "description": " ".join(str(a.get("DESCRIPTION") or "").split())[:200],
    }


async def fetch_recent_construction(client: Optional[httpx.AsyncClient] = None,
                                    since_year: Optional[int] = None) -> dict:
    """{TMS: summary of the largest new-construction permit since `since_year`}."""
    this_year = date.today().year
    since_year = since_year or this_year - LOOKBACK_YEARS
    own = client is None
    client = client or httpx.AsyncClient(timeout=60.0)
    best: dict = {}
    try:
        offset = 0
        while True:
            resp = await client.post(NEW_CONSTRUCTION_URL, data={
                "where": f"ISSUE_YEAR >= {int(since_year)} AND VALUATION >= {MIN_VALUATION}",
                "outFields": FIELDS, "returnGeometry": "false", "orderByFields": "OBJECTID",
                "resultOffset": str(offset), "resultRecordCount": str(PAGE_SIZE), "f": "json",
            })
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise httpx.HTTPError(f"Permit layer error: {data['error']}")
            rows = [f.get("attributes") or {} for f in data.get("features") or []]
            for a in rows:
                if not _counts(a, this_year):
                    continue
                pid = str(a.get("MAIN_PARCEL_NUMBER") or "").strip().upper()
                tms = pid[1:] if pid.startswith("C") else pid
                if tms and (tms not in best or (a.get("VALUATION") or 0) > best[tms].get("VALUATION", 0)):
                    best[tms] = a
            if len(rows) < PAGE_SIZE and not data.get("exceededTransferLimit"):
                break
            offset += PAGE_SIZE
    finally:
        if own:
            await client.aclose()
    return {tms: _summary(a) for tms, a in best.items()}


def annotate_construction(features: list, permits: dict) -> int:
    """Attach CONSTRUCTION to parcels with recent new-construction permits; returns the count."""
    n = 0
    for f in features:
        props = f.get("properties") or {}
        info = permits.get(str(props.get("TMS") or ""))
        if info:
            props["CONSTRUCTION"] = info
            n += 1
    return n
