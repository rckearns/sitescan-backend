"""Caching: Bid Assist narratives, geocode lookups, the saved county parcel list, ETags.

No network, no real database.
"""
import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services import bid_assist, geocode, parcels  # noqa: E402


def run(coro):
    return asyncio.run(coro)


# ─── Bid Assist ──────────────────────────────────────────────────────────────

def _org(**kw):
    base = dict(id=1, legal_name="Acme Builders", entity_type="LLC", address_city="Charleston",
                address_state="SC", contractor_license_number="G123", license_classifications=["BD"],
                bonding_capacity="$10M", emr="0.8", safety_meeting_frequency="weekly",
                principals=[], project_refs=[], personnel=[])
    base.update(kw)
    return SimpleNamespace(**base)


def test_narrative_key_is_stable_and_changes_with_profile_or_rfq():
    p1 = bid_assist.build_user_prompt(_org(), "New fire station, CM at Risk")
    p2 = bid_assist.build_user_prompt(_org(), "New fire station, CM at Risk")
    assert bid_assist.narrative_cache_key(p1) == bid_assist.narrative_cache_key(p2)
    other_rfq = bid_assist.build_user_prompt(_org(), "Parking deck, design-build")
    other_profile = bid_assist.build_user_prompt(_org(bonding_capacity="$25M"), "New fire station, CM at Risk")
    assert len({bid_assist.narrative_cache_key(x) for x in (p1, other_rfq, other_profile)}) == 3


def test_generate_bid_narrative_uses_async_client():
    class Fake:
        def __init__(self):
            self.messages = self
            self.kwargs = None

        async def create(self, **kw):
            self.kwargs = kw
            return SimpleNamespace(content=[SimpleNamespace(text="Narrative")])

    fake = Fake()
    assert run(bid_assist.generate_bid_narrative("prompt", client=fake)) == "Narrative"
    assert fake.kwargs["model"] == bid_assist.BID_MODEL and fake.kwargs["system"] == bid_assist.SYSTEM_PROMPT


# ─── Geocode ─────────────────────────────────────────────────────────────────

class _Resp:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def _patch_geocode(monkeypatch, db, http_result):
    calls = []

    async def db_get(key):
        return db[key] if key in db else geocode._MISS

    async def db_put(key, result):
        db[key] = result

    class Client:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            calls.append(k["params"]["q"])
            if isinstance(http_result, Exception):
                raise http_result
            return _Resp(http_result)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(geocode, "_db_get", db_get)
    monkeypatch.setattr(geocode, "_db_put", db_put)
    monkeypatch.setattr(geocode.httpx, "AsyncClient", Client)
    monkeypatch.setattr(geocode.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(geocode, "_cache", {})
    return calls


def test_geocode_saved_lookup_skips_network(monkeypatch):
    db = {"zzz unusual place|us": (33.1, -80.1)}
    calls = _patch_geocode(monkeypatch, db, [{"lat": "1", "lon": "2"}])
    assert run(geocode.geocode("ZZZ Unusual Place")) == (33.1, -80.1)
    assert calls == []


def test_geocode_saves_answers_but_not_errors(monkeypatch):
    db = {}
    _patch_geocode(monkeypatch, db, [{"lat": "32.5", "lon": "-80.5"}])
    assert run(geocode.geocode("Qqq Hamlet")) == (32.5, -80.5)
    assert db == {"qqq hamlet|us": (32.5, -80.5)}

    db2 = {}
    calls = _patch_geocode(monkeypatch, db2, httpx.ConnectError("down"))
    assert run(geocode.geocode("Www Nowhere")) is None
    assert db2 == {} and geocode._cache == {}          # error not remembered -> retried later
    run(geocode.geocode("Www Nowhere"))
    assert len(calls) == 2


# ─── Saved county parcel list ────────────────────────────────────────────────

def _patch_parcels(monkeypatch, saved, age, county):
    state = {"saved": saved, "age": age, "county_calls": 0}

    async def load():
        return state["saved"], state["age"]

    async def save(features):
        state["saved"], state["age"] = features, 0

    async def fetch():
        state["county_calls"] += 1
        if isinstance(county, Exception):
            raise county
        return county

    monkeypatch.setattr(parcels, "_load_saved_features", load)
    monkeypatch.setattr(parcels, "_save_features", save)
    monkeypatch.setattr(parcels, "fetch_county_parcel_features", fetch)
    monkeypatch.setattr(parcels, "_county_cache", {"at": 0.0, "features": None})
    return state


def test_restart_uses_fresh_saved_copy(monkeypatch):
    state = _patch_parcels(monkeypatch, ["saved"], 60, ["county"])
    assert run(parcels.fetch_home_parcel_features()) == ["saved"]
    assert state["county_calls"] == 0


def test_stale_copy_is_refreshed_and_saved(monkeypatch):
    state = _patch_parcels(monkeypatch, ["old"], parcels.COUNTY_CACHE_SECONDS + 1, ["county"])
    assert run(parcels.fetch_home_parcel_features()) == ["county"]
    assert state["saved"] == ["county"] and state["county_calls"] == 1


def test_county_outage_serves_saved_copy(monkeypatch):
    _patch_parcels(monkeypatch, ["old"], parcels.COUNTY_CACHE_SECONDS + 1, httpx.ConnectError("down"))
    assert run(parcels.fetch_home_parcel_features()) == ["old"]


def test_county_outage_without_copy_raises(monkeypatch):
    _patch_parcels(monkeypatch, None, None, httpx.ConnectError("down"))
    with pytest.raises(httpx.HTTPError):
        run(parcels.fetch_home_parcel_features())


# ─── ETag on /projects/home/parcels ──────────────────────────────────────────

def _load_projects_router():
    spec = importlib.util.spec_from_file_location("projects_router_under_test", ROOT / "app/routers/projects.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_home_parcels_etag_and_304(monkeypatch):
    try:
        router = _load_projects_router()
    except (TypeError, SyntaxError) as e:   # module needs Python 3.10+ syntax
        pytest.skip(f"projects router not importable on this Python: {e}")
    feats = [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [-79.9, 32.8]},
              "properties": {"TMS": "1", "LAND_APPR": 100000, "IMP_APPR": 0, "GENUSE": "Commercial"}}]

    async def fetch():
        return feats

    monkeypatch.setattr(router, "fetch_home_parcel_features", fetch)
    req = lambda h: SimpleNamespace(headers=h)  # noqa: E731
    first = run(router.home_parcels(request=req({}), user=None))
    assert first.status_code == 200 and first.headers["etag"]
    again = run(router.home_parcels(request=req({"if-none-match": first.headers["etag"]}), user=None))
    assert again.status_code == 304 and again.body == b""
