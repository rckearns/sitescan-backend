"""Geocoder with fast static SC lookup + Nominatim fallback.

Fast path: a hardcoded table of SC cities/counties returns instantly with
no network call and no rate-limit sleep.  Only truly unknown locations
(outside the table) fall through to the Nominatim API.
"""

import asyncio
import logging
from typing import Optional, Tuple

import httpx

logger = logging.getLogger("sitescan.geocode")

# ── Static SC location table ─────────────────────────────────────────────────
# Covers the cities/counties that appear in SCBO, SAM.gov, and CHS Bids data.
_SC_LOCATIONS: dict[str, Tuple[float, float]] = {
    # State-level fallbacks
    "south carolina":                  (33.8361, -81.1637),
    "sc":                              (33.8361, -81.1637),
    "sc, usa":                         (33.8361, -81.1637),
    "south carolina, usa":             (33.8361, -81.1637),
    # Lowcountry
    "charleston":                      (32.7765, -79.9311),
    "charleston, sc":                  (32.7765, -79.9311),
    "charleston, south carolina":      (32.7765, -79.9311),
    "charleston county":               (32.7765, -79.9311),
    "north charleston":                (32.8546, -79.9748),
    "mount pleasant":                  (32.8323, -79.8284),
    "goose creek":                     (32.9810, -80.0326),
    "summerville":                     (33.0185, -80.1756),
    "bluffton":                        (32.2371, -80.8604),
    "hilton head island":              (32.2163, -80.7526),
    "beaufort":                        (32.4316, -80.6698),
    "beaufort county":                 (32.4316, -80.6698),
    "berkeley county":                 (33.1899, -80.0095),
    "dorchester county":               (33.0877, -80.4213),
    # Midlands
    "columbia":                        (34.0007, -81.0348),
    "columbia, sc":                    (34.0007, -81.0348),
    "columbia, south carolina":        (34.0007, -81.0348),
    "columbia, south carolina, usa":   (34.0007, -81.0348),
    "richland county":                 (34.0007, -81.0348),
    "lexington":                       (33.9776, -81.2373),
    "lexington county":                (33.9776, -81.2373),
    "orangeburg":                      (33.4918, -80.8651),
    "orangeburg county":               (33.4918, -80.8651),
    "sumter":                          (33.9204, -80.3412),
    "sumter county":                   (33.9204, -80.3412),
    "aiken":                           (33.5604, -81.7198),
    "aiken county":                    (33.5604, -81.7198),
    "newberry county":                 (34.2776, -81.6135),
    "kershaw county":                  (34.2891, -80.5851),
    "calhoun county":                  (33.6754, -80.7840),
    # Pee Dee
    "florence":                        (34.1954, -79.7626),
    "florence county":                 (34.1954, -79.7626),
    "myrtle beach":                    (33.6891, -78.8867),
    "horry county":                    (33.6891, -78.8867),
    "conway":                          (33.8360, -79.0481),
    "darlington county":               (34.3196, -79.8761),
    "marion county":                   (34.1798, -79.3997),
    "dillon county":                   (34.4151, -79.3722),
    "williamsburg county":             (33.6193, -79.7289),
    "georgetown county":               (33.3762, -79.2945),
    # Upstate
    "greenville":                      (34.8526, -82.3940),
    "greenville county":               (34.8526, -82.3940),
    "spartanburg":                     (34.9496, -81.9321),
    "spartanburg county":              (34.9496, -81.9321),
    "anderson":                        (34.5034, -82.6501),
    "anderson county":                 (34.5034, -82.6501),
    "rock hill":                       (34.9249, -81.0251),
    "york county":                     (34.9960, -81.2417),
    "cherokee county":                 (35.0457, -81.6237),
    "union county":                    (34.7154, -81.6237),
    "chester county":                  (34.7043, -81.1565),
    "laurens county":                  (34.4990, -81.9835),
    "pickens county":                  (34.8845, -82.7071),
    "oconee county":                   (34.7654, -83.0604),
    "abbeville county":                (34.2243, -82.3871),
    "mccormick county":                (33.9119, -82.2954),
    "edgefield county":                (33.7929, -81.9546),
    "saluda county":                   (34.0076, -81.7726),
}


def _static_lookup(location: str) -> Optional[Tuple[float, float]]:
    """Return coordinates from the static table, or None."""
    key = location.strip().lower()
    if key in _SC_LOCATIONS:
        return _SC_LOCATIONS[key]
    # Strip "city of " / "town of " prefix
    for prefix in ("city of ", "town of ", "county of "):
        if key.startswith(prefix):
            stripped = key[len(prefix):]
            if stripped in _SC_LOCATIONS:
                return _SC_LOCATIONS[stripped]
            # e.g. "city of columbia, south carolina"
            city_part = stripped.split(",")[0].strip()
            if city_part in _SC_LOCATIONS:
                return _SC_LOCATIONS[city_part]
    # Substring match — e.g. "richland county, sc"
    for known, coords in _SC_LOCATIONS.items():
        if known in key:
            return coords
    return None


# ── Nominatim fallback ────────────────────────────────────────────────────────
_cache: dict[str, Optional[Tuple[float, float]]] = {}
_lock = asyncio.Lock()


async def geocode(location: str, country: str = "us") -> Optional[Tuple[float, float]]:
    """Return (latitude, longitude) for a location string.

    Checks the static SC lookup table first (instant, no network call).
    Falls back to Nominatim for truly unknown locations with a 1.1-second
    rate-limit sleep between unique API calls.
    """
    if not location:
        return None

    # Fast path — no lock, no sleep, no network
    fast = _static_lookup(location)
    if fast:
        return fast

    # Slow path — Nominatim
    key = f"{location.strip().lower()}|{country}"
    if key in _cache:
        return _cache[key]

    async with _lock:
        if key in _cache:
            return _cache[key]

        result: Optional[Tuple[float, float]] = None
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "q": location,
                        "format": "json",
                        "limit": 1,
                        "countrycodes": country,
                    },
                    headers={"User-Agent": "Yabodle/1.0 (sitescan geocoder; contact@yabodle.com)"},
                )
                data = resp.json()
                if data:
                    result = (float(data[0]["lat"]), float(data[0]["lon"]))
                    logger.info(f"Nominatim geocoded '{location}' → {result}")
                else:
                    logger.debug(f"No Nominatim result for '{location}'")
        except Exception as exc:
            logger.warning(f"Nominatim geocode failed for '{location}': {exc}")

        _cache[key] = result
        await asyncio.sleep(1.1)  # Only sleeps when a real network call was made
        return result
