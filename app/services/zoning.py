"""City of Charleston zoning at a parcel: base zoning, height district and overlays.

Feeds the AI highest-and-best-use prompt so scenarios respect what the site
allows. Source: the City's public "External/Zoning" MapServer. Parcels outside
the City's zoning data (unincorporated county, other municipalities) return
None, and the prompt says zoning is unknown.

Every lookup is best-effort: a network failure returns None rather than
blocking an analysis.
"""

import asyncio
import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger("sitescan.zoning")

ZONING_SERVICE = "https://gis.charleston-sc.gov/arcgis2/rest/services/External/Zoning/MapServer"
CITY_PARCELS_LAYER = 26
LAYERS = {
    "base": 66,            # Base Zoning (ZONE_BASE, e.g. "MU-2/WH")
    "height": 0,           # Old City Height Districts (ZONE_HD + HD_TYPE)
    "accommodations": 2,   # Accommodations Overlay (ACCOM, e.g. "A-1")
    "historic": 4,         # Old and Historic District
    "area": 70,            # City Districts (e.g. "Peninsula")
}
TIMEOUT = 10.0


async def _query(client: httpx.AsyncClient, layer: int, params: dict) -> list[dict]:
    resp = await client.get(f"{ZONING_SERVICE}/{layer}/query", params={**params, "f": "json"})
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise httpx.HTTPError(f"Zoning layer {layer} error: {data['error']}")
    return [f.get("attributes") or {} for f in data.get("features") or []]


def _clean(v: Any) -> str:
    return " ".join(str(v).split()) if v not in (None, "") else ""


async def zoning_at(lon: float, lat: float, client: Optional[httpx.AsyncClient] = None) -> Optional[dict]:
    """Zoning facts at a WGS84 point, or None if the point is outside City data or the service fails."""
    own = client is None
    client = client or httpx.AsyncClient(timeout=TIMEOUT)
    params = {
        "geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint", "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects", "outFields": "*", "returnGeometry": "false",
    }
    try:
        keys = list(LAYERS)
        results = await asyncio.gather(*(_query(client, LAYERS[k], params) for k in keys))
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(f"Zoning lookup failed at {lon},{lat}: {exc}")
        return None
    finally:
        if own:
            await client.aclose()
    hits = dict(zip(keys, results))
    if not hits["base"] and not hits["area"]:
        return None   # outside the City's zoning data

    base = _clean((hits["base"][0] if hits["base"] else {}).get("ZONE_BASE"))
    height = hits["height"][0] if hits["height"] else {}
    accom = hits["accommodations"][0] if hits["accommodations"] else {}
    return {
        "base_zoning": base or None,
        "height_district": _clean(height.get("ZONE_HD")) or None,
        "height_type": _clean(height.get("HD_TYPE")) or None,
        "accommodations_overlay": _clean(accom.get("ACCOM") or accom.get("ZONE_A")) or None,
        "old_and_historic": bool(hits["historic"]),
        "area": _clean((hits["area"][0] if hits["area"] else {}).get("citychs.DBO.Districts.AREA")) or None,
    }


async def city_parcel_point(tms: str, client: Optional[httpx.AsyncClient] = None) -> Optional[tuple[float, float]]:
    """(lon, lat) vertex-average of the City's parcel outline for a TMS, or None."""
    safe = "".join(ch for ch in str(tms) if ch.isalnum())
    if not safe:
        return None
    own = client is None
    client = client or httpx.AsyncClient(timeout=TIMEOUT)
    try:
        resp = await client.get(f"{ZONING_SERVICE}/{CITY_PARCELS_LAYER}/query", params={
            "where": f"TMS='{safe}'", "outFields": "TMS", "returnGeometry": "true", "outSR": "4326", "f": "json",
        })
        resp.raise_for_status()
        feats = resp.json().get("features") or []
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(f"City parcel lookup failed for {safe}: {exc}")
        return None
    finally:
        if own:
            await client.aclose()
    ring = ((feats[0].get("geometry") or {}).get("rings") or [[]])[0] if feats else []
    if not ring:
        return None
    return sum(c[0] for c in ring) / len(ring), sum(c[1] for c in ring) / len(ring)


async def lookup_parcel_zoning(parcel: dict, tms: str) -> Optional[dict]:
    """Zoning for a parcel: use its LON/LAT if present, else locate it by TMS. Never raises."""
    try:
        lon, lat = parcel.get("LON"), parcel.get("LAT")
        point = (float(lon), float(lat)) if lon not in (None, "") and lat not in (None, "") else None
    except (TypeError, ValueError):
        point = None
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            point = point or await city_parcel_point(tms, client)
            return await zoning_at(point[0], point[1], client) if point else None
    except Exception as exc:  # zoning must never break an analysis
        logger.warning(f"Zoning lookup failed for {tms}: {exc}")
        return None


def describe_zoning(z: Optional[dict]) -> str:
    """Plain-English zoning block for the prompt."""
    if not z:
        return ("Zoning: not available (parcel is outside City of Charleston zoning data). "
                "State your zoning assumptions in each scenario.")
    lines = [f"Base zoning: {z.get('base_zoning') or 'unknown'}"]
    if z.get("area"):
        lines.append(f"City area: {z['area']}")
    hd, ht = z.get("height_district"), (z.get("height_type") or "").lower()
    if hd:
        lines.append(f"Height district: {hd} stories maximum" if ht.startswith("stor")
                     else f"Height district: {hd} ({z.get('height_type')})")
    if z.get("accommodations_overlay"):
        lines.append(f"Accommodations Overlay: {z['accommodations_overlay']} (hotel/accommodations use can be considered)")
    else:
        lines.append("Accommodations Overlay: none (hotel/accommodations use is generally not permitted here; "
                     "any hotel scenario would need a rezoning and is not_allowed by right)")
    if z.get("old_and_historic"):
        lines.append("Old and Historic District: yes (Board of Architectural Review approval required)")
    return "\n".join(lines)
