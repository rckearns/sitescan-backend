"""Nominatim geocoder with in-process cache and 1 req/sec rate limit."""

import asyncio
import logging
from typing import Optional, Tuple

import httpx

logger = logging.getLogger("sitescan.geocode")

# In-process result cache: key → (lat, lng) or None
_cache: dict[str, Optional[Tuple[float, float]]] = {}
_lock = asyncio.Lock()


async def geocode(location: str, country: str = "us") -> Optional[Tuple[float, float]]:
    """Return (latitude, longitude) for a location string, or None if not found.

    Uses Nominatim (OpenStreetMap). Results are cached in-process so the same
    location string is only fetched once per process lifetime. A 1.1-second
    sleep is inserted after each *uncached* network call to respect Nominatim's
    usage policy.
    """
    if not location:
        return None

    key = f"{location.strip().lower()}|{country}"
    if key in _cache:
        return _cache[key]

    async with _lock:
        # Re-check after acquiring lock (another coroutine may have populated it)
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
                    logger.info(f"Geocoded '{location}' → {result}")
                else:
                    logger.debug(f"No geocode result for '{location}'")
        except Exception as exc:
            logger.warning(f"Geocode failed for '{location}': {exc}")

        _cache[key] = result
        # Rate-limit: only sleep after a real network call
        await asyncio.sleep(1.1)
        return result
