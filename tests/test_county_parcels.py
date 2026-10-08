"""Charleston County opportunity parcels: mapping, paging, ranking, caching (no network)."""

import asyncio

import httpx
import pytest

from app.services import parcels


def county_feat(pid, code="500 - General Commercial", land=100000, imp=0, st_no="12", street="KING", city="MT PLEASANT"):
    return {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [[[-79.9, 32.8], [-79.8, 32.8], [-79.8, 32.9], [-79.9, 32.9], [-79.9, 32.8]]]},
        "properties": {
            "SDE.P_POLY_PARCEL.PID": pid,
            "SDE.CAMA.OWNER1": "OWNER LLC   ",
            "SDE.CAMA.PROP_ST_NO": st_no,
            "SDE.CAMA.PROP_ST_NAME": street + "      ",
            "SDE.CAMA.PROP_CITY": city + "    ",
            "SDE.CAMA.CLASS_CODE": code + "      ",
            "SDE.CAMA.LAND_APPR": land,
            "SDE.CAMA.IMP_APPR": imp,
            "SDE.CAMA.APPRAISAL": land + imp,
            "SDE.P_POLY_PARCEL.ACRES_CAL": 1.5,
        },
    }


def test_where_clause_covers_commercial_and_vacant_codes():
    w = parcels.county_where_clause()
    for code in ("500", "952", "910"):
        assert f"CLASS_CODE LIKE '{code}%'" in w
    assert "IMP_APPR < SDE.CAMA.LAND_APPR" in w


def test_feature_mapping():
    f = parcels.county_feature_to_parcel(county_feat("123", code="952 - VAC-COMM-LOT", st_no="0"))
    p = f["properties"]
    assert f["geometry"]["type"] == "Point"
    assert f["geometry"]["coordinates"] == pytest.approx([-79.86, 32.84])
    assert p["TMS"] == "123" and p["OWNER"] == "OWNER LLC"
    assert p["HOUSE"] == "" and p["STREET"] == "KING"   # "0" house number dropped
    assert p["CITY"] == "Mount Pleasant"
    assert p["CLASS_CODE"] == "952" and p["GENUSE"] == "Vacant Commercial"
    assert p["GISACRES"] == 1.5 and p["APPRVAL"] == 100000


def test_feature_without_pid_or_geometry_is_dropped():
    assert parcels.county_feature_to_parcel(county_feat("")) is None
    bad = county_feat("1"); bad["geometry"] = None
    assert parcels.county_feature_to_parcel(bad) is None


def _paged_client(pages):
    calls = []

    def handler(request):
        offset = int(request.url.params["resultOffset"])
        calls.append(offset)
        page = pages[offset // parcels.COUNTY_PAGE_SIZE]
        return httpx.Response(200, json={"type": "FeatureCollection", "features": page,
                                         "exceededTransferLimit": len(page) == parcels.COUNTY_PAGE_SIZE})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), calls


def test_fetch_pages_until_short_page(monkeypatch):
    monkeypatch.setattr(parcels, "COUNTY_PAGE_SIZE", 2)
    pages = [[county_feat("1"), county_feat("2")], [county_feat("3"), county_feat("4")], [county_feat("5")]]
    client, calls = _paged_client(pages)
    feats = asyncio.run(parcels.fetch_county_parcel_features(client))
    assert [f["properties"]["TMS"] for f in feats] == ["1", "2", "3", "4", "5"]
    assert calls == [0, 2, 4]


def test_fetch_raises_on_service_error():
    client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json={"error": {"code": 400, "message": "bad"}})))
    with pytest.raises(httpx.HTTPError):
        asyncio.run(parcels.fetch_county_parcel_features(client))


def test_ranked_home_parcels_scores_dedupes_and_sorts():
    feats = [parcels.county_feature_to_parcel(x) for x in (
        county_feat("a", land=100000, imp=90000),   # score 53 → dropped
        county_feat("b", land=200000, imp=0),       # 100
        county_feat("c", land=500000, imp=0),       # 100, more land → first
        county_feat("c", land=500000, imp=0),       # duplicate TMS
        county_feat("d", land=100000, imp=20000),   # 83
    )]
    ranked = parcels.ranked_home_parcels(feats)
    assert [f["properties"]["TMS"] for f in ranked] == ["c", "b", "d"]
    assert ranked[0]["properties"]["SCORE"] == 100


def test_home_features_are_cached(monkeypatch):
    calls = []

    async def fake_fetch(client=None):
        calls.append(1)
        return [parcels.county_feature_to_parcel(county_feat("1"))]

    monkeypatch.setattr(parcels, "fetch_county_parcel_features", fake_fetch)
    monkeypatch.setattr(parcels, "_county_cache", {"at": 0.0, "features": None})

    async def run():
        await parcels.fetch_home_parcel_features()
        await parcels.fetch_home_parcel_features()

    asyncio.run(run())
    assert len(calls) == 1


@pytest.mark.parametrize("raw,expected", [
    ("CITY OF CHARLESTON", "Charleston"), ("FOLLY BEACH SC 29439", "Folly Beach"),
    ("MC CLELLANVILLE", "McClellanville"), ("MT PL", "Mount Pleasant"), ("MT. PLEASANT", "Mount Pleasant"),
    ("N CHARLESTON", "North Charleston"), ("NORHT CHARLESTON", "North Charleston"),
    ("HOLLLYWOOD", "Hollywood"), ("JOHNS ISLAND", "Johns Island"), ("", ""),
])
def test_city_names_are_normalized(raw, expected):
    assert parcels._normalize_city(raw) == expected
