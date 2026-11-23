import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import contractor_contacts as cc  # noqa: E402
from app.services import contractor_match as cm  # noqa: E402
from app.services import permit_sources as ps  # noqa: E402
from tests.test_contractors import _p  # noqa: E402


def test_residential_permits_from_category_and_permit_type():
    res = lambda **raw: ps.is_residential_permit(SimpleNamespace(category="electrical", source_id="charleston-permits", raw_data=raw))
    assert res(PERMIT_TYPE="Electrical - Single Family/Duplex Dwelling", WORK_CLASS="Generator")
    assert res(PERMIT_TYPE="Mechanical", WORK_CLASS="Residential")
    assert not res(PERMIT_TYPE="Mechanical", WORK_CLASS="Commercial")
    assert not res(PERMIT_TYPE="Building Multi-Family", WORK_CLASS="New")
    assert ps.is_residential_permit(SimpleNamespace(category="residential", source_id="mt-pleasant-permits", raw_data={}))


def test_primarily_residential_subs_are_hidden_unless_asked():
    job = _p("Choate Construction | Hyman Plumbing Co. | House Plumbing LLC", 20e6, parcel="R1")
    links = cm.link_subs(cm.group_jobs([job]), [])
    residential = {"hyman plumbing": (2, 10), "house plumbing": (9, 10)}
    out = cm.aggregate_subs(links, residential=residential, include_residential=False)
    assert [r["name"] for r in out["plumbing"]["subs"]] == ["Hyman Plumbing Co."]
    assert out["plumbing"]["hidden_residential"] == 1
    assert out["plumbing"]["subs"][0]["residential_share"] == 0.2
    both = cm.aggregate_subs(links, residential=residential, include_residential=True)
    assert {r["name"]: r["primarily_residential"] for r in both["plumbing"]["subs"]} == {
        "Hyman Plumbing Co.": False, "House Plumbing LLC": True}


def test_local_means_a_county_in_the_market():
    chs = ps.MARKETS["charleston-sc"]
    assert chs.is_local("SC", "Berkeley County") is True
    assert chs.is_local("SC", "Richland County") is False
    assert chs.is_local("GA", "Charleston County") is False
    assert chs.is_local("", "") is None
    assert ps.MARKETS["charlotte-nc"].is_local("SC", "York County") is True
    assert ps.market_for("charleston-permits") is chs


PLACE = {
    "id": "abc", "displayName": {"text": "Hyman Plumbing Company"}, "formattedAddress": "123 Main St, Summerville, SC 29483, USA",
    "nationalPhoneNumber": "(843) 555-0100", "websiteUri": "https://hymanplumbing.example",
    "location": {"latitude": 33.0, "longitude": -80.1},
    "addressComponents": [
        {"longText": "Summerville", "shortText": "Summerville", "types": ["locality", "political"]},
        {"longText": "Dorchester County", "shortText": "Dorchester County", "types": ["administrative_area_level_2", "political"]},
        {"longText": "South Carolina", "shortText": "SC", "types": ["administrative_area_level_1", "political"]},
    ],
}


def test_places_listing_must_match_the_permit_name():
    other = dict(PLACE, displayName={"text": "Lowcountry Drain Pros"})
    assert cc.best_listing("Hyman Plumbing Co.", [other, PLACE]) is PLACE
    assert cc.best_listing("Hyman Plumbing Co.", [other]) is None
    f = cc.profile_fields(PLACE)
    assert (f["phone"], f["city"], f["county"], f["state"]) == ("(843) 555-0100", "Summerville", "Dorchester County", "SC")
    assert cc.profile_fields(None) == {"status": "not_found"}


def test_contact_out_says_local_for_the_jobs_market():
    row = SimpleNamespace(status="found", **{k: v for k, v in cc.profile_fields(PLACE).items() if k != "status"})
    out = cc.contact_out(row, ps.MARKETS["charleston-sc"])
    assert out["local"] is True and out["contact"]["phone"] == "(843) 555-0100"
    assert cc.contact_out(row, ps.MARKETS["charlotte-nc"])["local"] is False
    assert cc.contact_out(None, ps.MARKETS["charleston-sc"]) == {"contact": None, "local": None, "contact_status": "pending"}


def test_lookup_stores_hits_and_misses_once(monkeypatch):
    import asyncio
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from sqlalchemy.pool import StaticPool
    from app.models.database import Base, ContractorProfile

    async def run():
        engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[ContractorProfile.__table__]))
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        calls = []

        async def fake_search(client, key, name, market):
            calls.append(name)
            return [PLACE] if "Hyman" in name else []

        monkeypatch.setattr(cc, "get_session_factory", lambda: factory)
        monkeypatch.setattr(cc, "search_places", fake_search)
        monkeypatch.setattr(cc, "get_settings", lambda: SimpleNamespace(google_maps_api_key="k", contact_lookups_per_run=10))
        names = {"hyman plumbing": ("Hyman Plumbing Co.", "charleston-sc"), "ghost electric": ("Ghost Electric", "charleston-sc")}
        assert await cc.lookup_missing(names) == {"found": 1, "not_found": 1, "error": 0}
        assert await cc.lookup_missing(names) == {"found": 0, "not_found": 0, "error": 0}   # cached
        assert len(calls) == 2
        profiles = await cc.load_profiles(list(names))
        assert profiles["hyman plumbing"].phone == "(843) 555-0100"
        assert profiles["ghost electric"].status == "not_found"

    asyncio.run(run())


def test_lookup_needs_an_api_key(monkeypatch):
    import asyncio
    monkeypatch.setattr(cc, "get_settings", lambda: SimpleNamespace(google_maps_api_key="", contact_lookups_per_run=10))
    assert asyncio.run(cc.lookup_missing({"a": ("A", "charleston-sc")})) == {"skipped": 1}


def test_far_listings_need_an_exact_name_and_licensed_subs_stay_near():
    chs = ps.MARKETS["charleston-sc"]
    ok = lambda name, listing, lat, lng, st, lic=False: cc.listing_ok(name, listing, lat, lng, st, chs, lic)
    assert ok("Hyman Plumbing Co.", "Hyman Plumbing Company", 33.0, -80.1, "SC")                  # near
    assert not ok("BE Electrical of SC LLC", "BE Electric Co", 29.9, -81.3, "FL")                  # far, loose
    assert ok("POWER DESIGN, Inc.", "Power Design, Inc.", 27.8, -82.6, "FL")                      # far, exact
    assert not ok("Jana Electric Inc", "The Jana Electric", 22.6, 88.4, "WB")                     # not the US
    assert not ok("Osborne Electrical, LLC", "Osborne Electrical LLC", 35.5, -97.5, "OK", lic=True)  # licensed here
    assert cc.licensed_in(chs, [{"city": "Summerville"}]) and not cc.licensed_in(chs, [{"city": "Columbia"}])


def test_licensed_local_sub_is_local_without_a_listing():
    out = cc.contact_out(None, ps.MARKETS["charleston-sc"], "Ghost Electric", licensed_local=True)
    assert out["local"] is True and out["contact"] is None
    far = SimpleNamespace(status="found", listing_name="BE Electric Co", latitude=29.9, longitude=-81.3, state="FL",
                          county="St. Johns County", phone="1", website="", address="", city="St. Augustine")
    hidden = cc.contact_out(far, ps.MARKETS["charleston-sc"], "BE Electrical of SC LLC", licensed_local=True)
    assert hidden["contact"] is None and hidden["local"] is True
