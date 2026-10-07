"""Charleston parcel data — ArcGIS queries plus opportunity scoring/ranking.

Two sources:
- City of Charleston "Property lines" layer: drawn on the map (`GET /projects/map/parcels`).
- Charleston County ProVal parcels: the Home opportunity list (`GET /projects/home/parcels`)
  and the nightly parcel-estimates job. The city layer is unsuitable there: it caps
  at 1000 arbitrary rows, has no site addresses, misses vacant commercial lots
  (class 952) and only covers part of the county.
"""

import asyncio
import json
import math
import re
import time
from typing import Any, Optional

import httpx

ARCGIS_PARCELS_URL = (
    "https://gis.charleston-sc.gov/arcgis2/rest/services/"
    "External/Zoning/MapServer/26/query"
)
PARCEL_OUT_FIELDS = "TMS,PARCELID,OWNER,STREET,HOUSE,GENUSE,YRBUILT,APPRVAL,IMP_APPR,LAND_APPR,GISACRES"

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


# ─── CHARLESTON COUNTY OPPORTUNITY PARCELS ───────────────────────────────────

COUNTY_PARCELS_URL = (
    "https://gisccapps.charlestoncounty.org/arcgis/rest/services/"
    "ProVal/ParcelMap/MapServer/0/query"
)
# County class codes → the use label the frontend turns into a title.
COUNTY_CLASS_LABELS = {
    "500": "Commercial",
    "952": "Vacant Commercial",
    "910": "Commercial Development Acreage",
}
_P = "SDE.P_POLY_PARCEL."
_C = "SDE.CAMA."
COUNTY_OUT_FIELDS = ",".join([
    _P + "PID", _C + "OWNER1", _C + "PROP_ST_NO", _C + "PROP_ST_NAME", _C + "PROP_CITY",
    _C + "CLASS_CODE", _C + "LAND_APPR", _C + "IMP_APPR", _C + "APPRAISAL", _P + "ACRES_CAL",
])
COUNTY_PAGE_SIZE = 1000   # the service's maxRecordCount
COUNTY_MAX_PAGES = 30     # safety stop (~4.4k rows today)
COUNTY_CACHE_SECONDS = 6 * 3600

_county_cache: dict[str, Any] = {"at": 0.0, "features": None}
_county_lock = asyncio.Lock()


def county_where_clause() -> str:
    """Commercial / vacant-commercial parcels whose buildings are worth less than the land."""
    codes = " OR ".join(f"{_C}CLASS_CODE LIKE '{c}%'" for c in COUNTY_CLASS_LABELS)
    return f"({codes}) AND {_C}LAND_APPR > 0 AND {_C}IMP_APPR < {_C}LAND_APPR"


def _clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v)).strip() if v is not None else ""


# County PROP_CITY is free text with typos and variants; map them to one spelling.
_CITY_ALIASES = {
    "cityofcharleston": "Charleston",
    "ncharleston": "North Charleston", "norhtcharleston": "North Charleston",
    "northcharlesotn": "North Charleston", "northchas": "North Charleston",
    "mtpleasant": "Mount Pleasant", "mountpl": "Mount Pleasant", "mtpl": "Mount Pleasant",
    "mcclellanville": "McClellanville", "holllywood": "Hollywood",
}


def _normalize_city(v: Any) -> str:
    city = re.sub(r"\s+(sc|s\.c\.)?\s*\d{5}(-\d{4})?$|\s+sc$", "", _clean(v), flags=re.I)
    city = city.title()
    key = re.sub(r"[^a-z]", "", city.lower())
    return _CITY_ALIASES.get(key, re.sub(r"^Mt\.? ", "Mount ", city))


def _centroid(geometry: Optional[dict]) -> Optional[list[float]]:
    """Vertex average of the first outer ring (same approximation the frontend used)."""
    if not geometry:
        return None
    coords = geometry.get("coordinates") or []
    if geometry.get("type") == "MultiPolygon":
        coords = coords[0] if coords else []
    ring = coords[0] if coords else []
    if not ring:
        return None
    return [sum(c[0] for c in ring) / len(ring), sum(c[1] for c in ring) / len(ring)]


def county_feature_to_parcel(feat: dict) -> Optional[dict]:
    """County ProVal feature → GeoJSON Point feature with the property names the app uses."""
    a = (feat or {}).get("properties") or {}
    center = _centroid((feat or {}).get("geometry"))
    tms = _clean(a.get(_P + "PID"))
    if not tms or not center:
        return None
    code = _clean(a.get(_C + "CLASS_CODE"))[:3]
    house = _clean(a.get(_C + "PROP_ST_NO"))
    land = a.get(_C + "LAND_APPR") or 0
    imp = a.get(_C + "IMP_APPR") or 0
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [round(center[0], 6), round(center[1], 6)]},
        "properties": {
            "TMS": tms,
            "PARCELID": tms,
            "OWNER": _clean(a.get(_C + "OWNER1")),
            "HOUSE": "" if house in ("", "0") else house,
            "STREET": _clean(a.get(_C + "PROP_ST_NAME")),
            "CITY": _normalize_city(a.get(_C + "PROP_CITY")),
            "CLASS_CODE": code,
            "GENUSE": COUNTY_CLASS_LABELS.get(code, "Commercial"),
            "LAND_APPR": land,
            "IMP_APPR": imp,
            "APPRVAL": a.get(_C + "APPRAISAL") or (land + imp),
            "GISACRES": a.get(_P + "ACRES_CAL"),
            "SOURCE": "charleston-county",
        },
    }


async def fetch_county_parcel_features(client: Optional[httpx.AsyncClient] = None) -> list[dict]:
    """Every matching county parcel, paged by OBJECTID. Raises httpx.HTTPError on failure."""
    own = client is None
    client = client or httpx.AsyncClient(timeout=60.0)
    features: list[dict] = []
    try:
        for page in range(COUNTY_MAX_PAGES):
            params = {
                "where": county_where_clause(),
                "outFields": COUNTY_OUT_FIELDS,
                "returnGeometry": "true",
                "outSR": "4326",
                "maxAllowableOffset": "0.0001",
                "geometryPrecision": "6",
                "orderByFields": _P + "OBJECTID",
                "resultOffset": str(page * COUNTY_PAGE_SIZE),
                "resultRecordCount": str(COUNTY_PAGE_SIZE),
                "f": "geojson",
            }
            resp = await client.get(COUNTY_PARCELS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise httpx.HTTPError(f"County parcel service error: {data['error']}")
            batch = data.get("features") or []
            features.extend(f for f in (county_feature_to_parcel(x) for x in batch) if f)
            if len(batch) < COUNTY_PAGE_SIZE and not data.get("exceededTransferLimit"):
                break
    finally:
        if own:
            await client.aclose()
    return features


async def fetch_home_parcel_features() -> list[dict]:
    """County opportunity parcels, cached in memory for a few hours."""
    async with _county_lock:
        fresh = time.monotonic() - _county_cache["at"] < COUNTY_CACHE_SECONDS
        if _county_cache["features"] is None or not fresh:
            _county_cache["features"] = await fetch_county_parcel_features()
            _county_cache["at"] = time.monotonic()
        return _county_cache["features"]


def ranked_home_parcels(features: list[dict], min_score: int = MIN_OPPORTUNITY_SCORE) -> list[dict]:
    """Features scoring >= min_score, best first, with the score attached."""
    by_tms = {}
    for feat in features:
        props = feat["properties"]
        by_tms[props["TMS"]] = feat
    ranked = rank_parcels(list(by_tms.values()), min_score)
    out = []
    for props in ranked:
        feat = by_tms[props["TMS"]]
        out.append({**feat, "properties": {**props, "SCORE": opportunity_score(props)}})
    return out


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
