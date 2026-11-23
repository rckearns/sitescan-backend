"""Phone, website and address for subs, from Google Places, looked up once per contractor.

Permits only carry a company name. A daily job looks up the subs shown in the subs lists
that have no listing yet (Text Search, biased toward the job's market) and stores the result
in contractor_profiles, including a miss, so nobody is looked up twice. The listing's county
says whether the sub is local to the market (permit_sources.Market).
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
from sqlalchemy import select

from app.config import get_settings
from app.models.database import ContractorProfile, get_session_factory
from app.services.permit_sources import MARKETS, Market

log = logging.getLogger("sitescan.contacts")

PLACES_URL = "https://places.googleapis.com/v1/places:searchText"
FIELD_MASK = ",".join([
    "places.id", "places.displayName", "places.formattedAddress", "places.nationalPhoneNumber",
    "places.websiteUri", "places.addressComponents", "places.location",
])
# Retry a miss after this long (a new firm may have listed itself); errors after a day.
RETRY_NOT_FOUND = timedelta(days=90)
RETRY_ERROR = timedelta(days=1)
SEARCH_RADIUS_M = 50_000.0


def _component(place: dict, kind: str, short: bool = False) -> str:
    for c in place.get("addressComponents") or []:
        if kind in (c.get("types") or []):
            return (c.get("shortText") if short else c.get("longText")) or ""
    return ""


US_STATES = set("""AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV
NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY""".split())
# Listings farther than this from the market need an exact name; licensed-local subs must be nearer.
NEAR_KM = 250
LICENSED_NEAR_KM = 80


def _km(a: tuple[float, float], b: tuple[float, float]) -> float:
    (lat1, lng1), (lat2, lng2) = a, b
    p = math.pi / 180
    h = math.sin((lat2 - lat1) * p / 2) ** 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lng2 - lng1) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def licensed_in(market: Market | None, licenses: list[dict]) -> bool:
    """The sub holds a state license registered in one of the market's cities, so its business is local."""
    if not market or not market.license_cities:
        return False
    cities = {c.lower() for c in market.license_cities}
    return any((lic.get("city") or "").strip().lower() in cities for lic in licenses)


def listing_ok(name: str, listing: str, lat: float | None, lng: float | None, state: str,
               market: Market | None, licensed_local: bool = False) -> bool:
    """Is this Google listing the sub? Places returns the nearest plausible business, which for a
    small sub is often someone else: BE Electrical of SC (licensed in Charleston) came back as
    BE Electric Co in St. Augustine, FL; Jana Electric as a shop in India."""
    from app.services.contractor_match import _MATCH_THRESHOLD, match_score, normalize_name
    if not listing or match_score(listing, name) < _MATCH_THRESHOLD:
        return False
    if not market or lat is None or lng is None:
        return True
    km = _km(market.center, (lat, lng))
    if licensed_local:
        return km <= LICENSED_NEAR_KM
    if km <= NEAR_KM:
        return True
    return state in US_STATES and normalize_name(listing) == normalize_name(name)


def best_listing(name: str, places: list[dict], market: Market | None = None,
                 licensed_local: bool = False) -> dict | None:
    """The first result that passes listing_ok."""
    for place in places:
        listing = ((place.get("displayName") or {}).get("text") or "").strip()
        loc = place.get("location") or {}
        state = profile_fields(place).get("state", "")
        if listing_ok(name, listing, loc.get("latitude"), loc.get("longitude"), state, market, licensed_local):
            return place
    return None


def profile_fields(place: dict | None) -> dict[str, Any]:
    if not place:
        return {"status": "not_found"}
    loc = place.get("location") or {}
    return {
        "status": "found",
        "listing_name": ((place.get("displayName") or {}).get("text") or "")[:255],
        "phone": place.get("nationalPhoneNumber") or "",
        "website": (place.get("websiteUri") or "")[:500],
        "address": (place.get("formattedAddress") or "")[:500],
        "city": _component(place, "locality"),
        "county": _component(place, "administrative_area_level_2"),
        "state": _component(place, "administrative_area_level_1", short=True),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "place_id": place.get("id") or "",
    }


async def search_places(client: httpx.AsyncClient, api_key: str, name: str, market: Market) -> list[dict]:
    lat, lng = market.center
    resp = await client.post(PLACES_URL, headers={
        "X-Goog-Api-Key": api_key, "X-Goog-FieldMask": FIELD_MASK, "Content-Type": "application/json",
    }, json={
        "textQuery": name,
        "maxResultCount": 5,
        "locationBias": {"circle": {"center": {"latitude": lat, "longitude": lng}, "radius": SEARCH_RADIUS_M}},
    }, timeout=20)
    resp.raise_for_status()
    return resp.json().get("places") or []


def _row_ok(row: ContractorProfile, name: str, market: Market | None, licensed_local: bool) -> bool:
    return listing_ok(name, row.listing_name, row.latitude, row.longitude, row.state, market, licensed_local)


def _due(row: ContractorProfile | None, now: datetime, name: str = "", market: Market | None = None,
         licensed_local: bool = False) -> bool:
    if row is None:
        return True
    if row.status == "found" and name and not _row_ok(row, name, market, licensed_local):
        return True          # matched under looser rules; look again
    age = now - (row.looked_up_at or datetime.min)
    if row.status == "not_found":
        return age > RETRY_NOT_FOUND
    if row.status == "error":
        return age > RETRY_ERROR
    return False


def _unpack(v: tuple) -> tuple[str, str, bool]:
    name, market_id, *rest = v
    return name, market_id, bool(rest and rest[0])


async def lookup_missing(names: dict[str, tuple], limit: int | None = None) -> dict[str, int]:
    """Look up contractors with no listing yet. `names` maps key -> (name, market id[, licensed
    locally]). Returns counts; does nothing without GOOGLE_MAPS_API_KEY."""
    settings = get_settings()
    if not settings.google_maps_api_key:
        log.info("Contact lookup skipped: GOOGLE_MAPS_API_KEY not set")
        return {"skipped": len(names)}
    limit = settings.contact_lookups_per_run if limit is None else limit
    now = datetime.utcnow()
    counts = {"found": 0, "not_found": 0, "error": 0}
    async with get_session_factory()() as db:
        rows = {r.key: r for r in (await db.execute(
            select(ContractorProfile).where(ContractorProfile.key.in_(list(names))))).scalars().all()}
        def market_of(mid: str) -> Market:
            return MARKETS.get(mid) or next(iter(MARKETS.values()))
        todo = [k for k in names if _due(rows.get(k), now, _unpack(names[k])[0],
                                         market_of(_unpack(names[k])[1]), _unpack(names[k])[2])][:limit]
        async with httpx.AsyncClient() as client:
            for key in todo:
                name, market_id, licensed_local = _unpack(names[key])
                market = market_of(market_id)
                try:
                    fields = profile_fields(best_listing(name, await search_places(
                        client, settings.google_maps_api_key, name, market), market, licensed_local))
                except Exception as exc:     # one bad lookup never stops the run
                    log.warning("Places lookup failed for %r: %s", name, exc)
                    fields = {"status": "error"}
                row = rows.get(key) or ContractorProfile(key=key)
                for col in ("listing_name", "phone", "website", "address", "city", "county", "state", "place_id"):
                    setattr(row, col, fields.get(col, ""))
                row.latitude, row.longitude = fields.get("latitude"), fields.get("longitude")
                row.name, row.status, row.looked_up_at = name[:255], fields["status"], now
                db.add(row)
                counts[fields["status"]] += 1
                if sum(counts.values()) % 25 == 0:
                    await db.commit()
        await db.commit()
    log.info("Contact lookup: %s (%d still waiting)", counts, max(0, len(names) - len(todo)))
    return counts


async def load_profiles(keys: list[str]) -> dict[str, ContractorProfile]:
    if not keys:
        return {}
    async with get_session_factory()() as db:
        rows = (await db.execute(select(ContractorProfile).where(ContractorProfile.key.in_(keys)))).scalars().all()
    return {r.key: r for r in rows}


def contact_out(row: ContractorProfile | None, market: Market | None, name: str = "",
                licensed_local: bool = False) -> dict[str, Any]:
    """What the subs lists show: contact details and whether the sub is local. A sub licensed
    in the market's cities is local even when no listing passes."""
    if row is not None and row.status == "found" and name and not _row_ok(row, name, market, licensed_local):
        row = SimpleNamespace(status="not_found")      # an old, looser match: hide until re-checked
    if row is None or row.status != "found":
        return {"contact": None, "local": True if licensed_local else None,
                "contact_status": "pending" if row is None else row.status}
    return {
        "contact": {"phone": row.phone, "website": row.website, "address": row.address,
                    "city": row.city, "state": row.state, "listing_name": row.listing_name},
        "local": True if licensed_local else (market.is_local(row.state, row.county) if market else None),
        "contact_status": "found",
    }


async def run_contacts_job() -> None:
    """Daily: look up every sub in the subs lists that has no listing yet (never raises)."""
    try:
        from app.services.contractor_match import sub_names_for_lookup
        names = await sub_names_for_lookup()
        await lookup_missing(names)
    except Exception as exc:
        log.error("Contact lookup job failed: %s", exc)
