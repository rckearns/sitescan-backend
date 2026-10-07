"""Charleston parcel data — shared ArcGIS query plus opportunity scoring/ranking.

Used by the map proxy endpoint (`GET /projects/map/parcels`) and the nightly
parcel-estimates job so both see exactly the same parcel list.
"""

import json
import math
import re
from typing import Any, Optional

import httpx

ARCGIS_PARCELS_URL = (
    "https://gis.charleston-sc.gov/arcgis2/rest/services/"
    "External/Zoning/MapServer/26/query"
)
PARCEL_OUT_FIELDS = "TMS,PARCELID,OWNER,STREET,HOUSE,GENUSE,YRBUILT,APPRVAL,IMP_APPR,LAND_APPR,GISACRES"

# The Home page's parcel query (sitescan-frontend loadParcelOpportunities).
HOME_BBOX = {"west": -80.2, "south": 32.55, "east": -79.7, "north": 33.05}
HOME_LIMIT = 1000
HOME_GENUSE = "commercial"
MIN_OPPORTUNITY_SCORE = 55


def build_parcel_query_params(
    west: float, south: float, east: float, north: float,
    limit: int, genuse: Optional[str] = None,
) -> dict[str, str]:
    """ArcGIS query params for parcels in a bbox, optionally filtered by GENUSE substring."""
    if genuse:
        safe = genuse.replace("'", "").replace(";", "")[:50]
        where_clause = f"UPPER(GENUSE) LIKE UPPER('%{safe}%')"
    else:
        where_clause = "1=1"
    return {
        "geometry": f"{west},{south},{east},{north}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "outSR": "4326",
        "outFields": PARCEL_OUT_FIELDS,
        "where": where_clause,
        "f": "geojson",
        "resultRecordCount": str(limit),
    }


async def fetch_parcels_geojson(
    west: float, south: float, east: float, north: float,
    limit: int, genuse: Optional[str] = None, timeout: float = 15.0,
) -> bytes:
    """Fetch raw parcel GeoJSON from Charleston ArcGIS. Raises httpx.HTTPError on failure."""
    params = build_parcel_query_params(west, south, east, north, limit, genuse)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(ARCGIS_PARCELS_URL, params=params)
        resp.raise_for_status()
        return resp.content


async def fetch_home_parcel_features() -> list[dict]:
    """The same parcel features the Home page loads (Charleston metro, commercial, 1000 max)."""
    raw = await fetch_parcels_geojson(limit=HOME_LIMIT, genuse=HOME_GENUSE, timeout=30.0, **HOME_BBOX)
    return json.loads(raw).get("features") or []


# ─── SCORING (mirror of sitescan-frontend parcelOppScore) ────────────────────

_FLOAT_PREFIX = re.compile(r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")


def _js_parse_float(v: Any) -> float:
    """`parseFloat(v) || 0` semantics: leading numeric prefix, else 0."""
    if v is None or isinstance(v, bool):
        return 0.0
    if isinstance(v, (int, float)):
        f = float(v)
    else:
        m = _FLOAT_PREFIX.match(str(v))
        if not m:
            return 0.0
        f = float(m.group(0))
    return 0.0 if math.isnan(f) else f


def _js_round(x: float) -> int:
    """JavaScript Math.round (half rounds up, unlike Python's banker's rounding)."""
    return math.floor(x + 0.5)


def opportunity_score(props: dict) -> int:
    """0–100: how under-improved a parcel is relative to its land value."""
    genuse = str(props.get("GENUSE") or "").lower()
    if "undevelopable" in genuse:
        return 3
    land = _js_parse_float(props.get("LAND_APPR"))
    imp = _js_parse_float(props.get("IMP_APPR"))
    if land == 0 and imp == 0:
        return 50
    if imp == 0:
        return 95
    if land == 0:
        return 15
    imp_ratio = imp / (land + imp)
    return max(3, _js_round((1 - imp_ratio) * 100))


def rank_parcels(features: list[dict], min_score: int = MIN_OPPORTUNITY_SCORE) -> list[dict]:
    """Filter to score >= min_score and sort by score desc, then land value desc.

    Matches the frontend: value is LAND_APPR || APPRVAL; ties keep ArcGIS order
    (both sorts are stable). Returns the feature `properties` dicts.
    """
    scored = []
    for feat in features:
        props = (feat or {}).get("properties") or {}
        score = opportunity_score(props)
        if score >= min_score:
            value = _js_parse_float(props.get("LAND_APPR")) or _js_parse_float(props.get("APPRVAL"))
            scored.append((score, value, props))
    scored.sort(key=lambda t: (-t[0], -t[1]))
    return [props for _, _, props in scored]
