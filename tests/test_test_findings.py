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
