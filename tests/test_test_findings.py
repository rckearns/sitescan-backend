"""Fixes from the October 2026 site test: parcel pieces, city names, SCBO and TRC parsing."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services import parcels  # noqa: E402
from app.services.pipeline import scbo  # noqa: E402
from sitescan_boards.parser import ITEM_START_RE  # noqa: E402


def _feat(tms, acres, lon):
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, 32.8]},
            "properties": {"TMS": tms, "GISACRES": acres, "LAND_APPR": 1_000_000, "LON": lon}}


def test_multi_piece_parcels_are_merged():
    out = parcels.merge_parcel_pieces([_feat("A", 0.1, -79.1), _feat("B", 1.0, -79.5), _feat("A", 0.5, -79.2)])
    by = {f["properties"]["TMS"]: f["properties"] for f in out}
    assert len(out) == 2
    assert abs(by["A"]["GISACRES"] - 0.6) < 1e-9 and by["A"]["LON"] == -79.2   # largest piece's location
    assert by["A"]["LAND_APPR"] == 1_000_000                                    # value counted once


def test_city_names():
    assert parcels._normalize_city("ISLE OF PALMS") == "Isle of Palms"
    assert parcels._normalize_city("N CHAS") == "North Charleston"
    assert parcels._normalize_city("chas") == "Charleston"


def test_scbo_location_decides_and_street_names_dont_count():
    f = scbo.is_charleston_area
    assert not f({"Project Location": "Aiken, SC", "Description": "Colleton St and Charleston Hwy"})
    assert not f({"Project Location": "Charleston Hwy, Aiken"})
    assert f({"Project Location": "North Charleston, SC"})
    assert not f({"Description": "Resurfacing of Charleston Street in Columbia"})


def test_trc_items_numbered_with_hash():
    m = ITEM_START_RE.match("#1. INPATIENT COMPREHENSIVE CANCER HOSPITAL eReview")
    assert m and m.group(1) == "1" and m.group(2).startswith("INPATIENT")
    assert ITEM_START_RE.match("12. 106 Coming Street")


TRC_TEXT = """#1. POINT HOPE - CAPSTONE COTTAGES eReview
09:00 Project Classification: TRC - Site Plan
Address: 1730 CLEMENTS FERRY RD City Project ID#: TRC-SP2026-000912
Location: CAINHOY Submittal Review #: 3
Primary TMS: B2620000028 Board Approvals Required:
Acres: 20.92 Council District #: 1
# Lots: Owner: Capstone Collegiate Communities
# Units: 250 Applicant: Thomas & Hutton
Description: Proposed construction of new multi-family development with associated infrastructure.
REVIEW HISTORY SUBMIT DATE MEETING DATE SUBMIT to MEETING
1 2026-03-02 2026-04-16 45
"""


def test_trc_agenda_item_fields():
    from sitescan_boards.parser import parse_agenda_text
    items = parse_agenda_text(TRC_TEXT)
    assert len(items) == 1
    it = items[0]
    assert it.address == "1730 CLEMENTS FERRY RD" and it.tms == "2620000028" and it.acreage == 20.92
    assert it.neighborhood == "CAINHOY" and it.owner == "Capstone Collegiate Communities"
    assert it.request_text.startswith("POINT HOPE - CAPSTONE COTTAGES: Proposed construction of new multi-family")


def test_exempt_lots_score_as_vacant_and_get_a_land_estimate():
    def feat(tms, land, acres, lon=-79.94, lat=32.79, exempt=False):
        props = {"TMS": tms, "LAND_APPR": land, "IMP_APPR": 0, "GISACRES": acres, "LON": lon, "LAT": lat,
                 "GENUSE": "Parking Lot"}
        if exempt:
            props["EXEMPT"] = True
        return {"type": "Feature", "geometry": None, "properties": props}
    small = [feat(f"S{i}", v, 0.2) for i, v in enumerate([1e6, 2e6, 3e6, 4e6, 5e6])]   # $5M–25M/acre
    mid = [feat(f"M{i}", v, 1.0) for i, v in enumerate([4e6, 5e6, 6e6, 7e6, 8e6])]      # $4M–8M/acre
    feats = small + mid + [feat("FAR", 100_000, 1.0, lon=-80.2), feat("CH", 0, 0.5, exempt=True)]
    parcels.estimate_exempt_land(feats)
    church = feats[-1]["properties"]
    assert church["LAND_EST"] == 3_000_000            # 0.5 ac is in the 0.5–1.5 band: median $6M/acre
    assert parcels.opportunity_score(church) == 100
    assert parcels.opportunity_score({"LAND_APPR": 0, "IMP_APPR": 0}) == 50   # not exempt: unknown

    from app.services.parcel_analysis import build_prompt, land_basis
    assert land_basis(church) == 3_000_000
    assert "not assessed (tax-exempt owner)" in build_prompt(church)


def test_exempt_query_targets_zero_value_lots_of_a_quarter_acre():
    w = parcels.exempt_where_clause()
    assert "LAND_APPR = 0" in w and "ACRES_CAL >= 0.25" in w and "'905%'" in w and "'460%'" in w
    assert "'460%'" in parcels.county_where_clause()


def test_not_for_sale_owners_and_size_cap():
    assert parcels.NOT_FOR_SALE_OWNER_RE.search("UNITED STATES OF AMERICA")
    assert parcels.NOT_FOR_SALE_OWNER_RE.search("SOUTH CAROLINA STATE PORTS AUTHORITY")
    assert not parcels.NOT_FOR_SALE_OWNER_RE.search("CITADEL SQUARE BAPTIST CHURCH")
    assert not parcels.NOT_FOR_SALE_OWNER_RE.search("CITY OF CHARLESTON")
    assert "ACRES_CAL <= 3.0" in parcels.exempt_where_clause()
