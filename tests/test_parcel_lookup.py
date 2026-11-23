import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import parcels as P  # noqa: E402


def test_lookup_query_forms():
    assert P.lookup_where_clause("131 Coming St") == "SDE.CAMA.PROP_ST_NO = '131' AND SDE.CAMA.PROP_ST_NAME LIKE 'COMING%'"
    assert P.lookup_where_clause("460-160-10-81") == "SDE.P_POLY_PARCEL.PID = '4601601081'"
    assert "OWNER1 LIKE '%O''BRIEN%'" in P.lookup_where_clause("O'Brien")
    assert P.lookup_where_clause("ab") is None
    assert ";" not in P.lookup_where_clause("1 x'; DROP TABLE y")


def _props(**kw):
    base = {"LAND_APPR": 500_000, "IMP_APPR": 50_000, "GISACRES": 0.3, "OWNER": "X LLC",
            "LON": -79.94, "LAT": 32.78, "GENUSE": "Commercial"}
    return {**base, **kw}


def test_reasons_a_parcel_is_not_listed():
    raw = lambda cls: {P._C + "CLASS_CODE": cls}
    assert "Religious (691)" in P.why_not_listed(raw("691 - RELIGIOUS"), _props(LAND_APPR=0, IMP_APPR=0, GISACRES=0.23))
    assert "0.20 acres" in P.why_not_listed(raw("460 - PARKING"), _props(LAND_APPR=0, IMP_APPR=0, GISACRES=0.2))
    assert "Resid" in P.why_not_listed(raw("130 - RESID-DUP/TRI"), _props())
    assert "worth more than the land" in P.why_not_listed(raw("500 - COMMERCIAL"), _props(IMP_APPR=900_000))
    assert "starts at 55%" in P.why_not_listed(raw("500 - COMMERCIAL"), _props(IMP_APPR=450_000))
    assert P.why_not_listed(raw("500 - COMMERCIAL"), _props()) is None
    assert P.why_not_listed(raw("460 - PARKING"), _props(LAND_APPR=0, IMP_APPR=0, GISACRES=0.5)) is None


def test_unassessed_commercial_lots_are_fetched_like_exempt_ones():
    w = P.exempt_where_clause()
    assert "LAND_APPR IS NULL" in w and "ACRES_CAL >= 0.25" in w
    raw = {"properties": {P._P + "PID": "4600802148", P._C + "CLASS_CODE": "952 - VAC-COMM-LOT",
                          P._C + "LAND_APPR": None, P._C + "IMP_APPR": None, P._C + "OWNER1": "CITY OF CHARLESTON",
                          P._C + "PROP_ST_NO": "129", P._C + "PROP_ST_NAME": "COLUMBUS", P._P + "ACRES_CAL": 0.33},
           "geometry": {"type": "Polygon", "coordinates": [[[-79.9405, 32.7938], [-79.9404, 32.7938], [-79.9404, 32.7939]]]}}
    props = P.county_feature_to_parcel(raw)["properties"]
    assert props["EXEMPT"] and props["UNASSESSED"]
    assert P.why_not_listed(raw["properties"], props) is None
    res = dict(raw["properties"], **{P._C + "CLASS_CODE": "905 - VAC-RES-LOT"})
    assert "Not yet assessed" in P.why_not_listed(res, P.county_feature_to_parcel({**raw, "properties": res})["properties"])
