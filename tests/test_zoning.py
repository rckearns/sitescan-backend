"""City zoning lookup, against recorded responses for 483 Meeting St (TMS 4590503130)."""
import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services import zoning as z  # noqa: E402

MEETING_483 = {
    66: [{"ZONE_BASE": "MU-2/WH", "ORDSTAT": "ACTIVE"}],
    0: [{"HD_TYPE": "Story", "ZONE_HD": "8"}],
    2: [{"ACCOM": "A-1", "ZONE_A": "A"}],
    4: [{"District": "Old and Historic District"}],
    70: [{"citychs.DBO.Districts.AREA": "Peninsula"}],
    26: [{"attributes": {"TMS": "4590503130"},
          "geometry": {"rings": [[[-79.9406, 32.7955], [-79.9404, 32.7957], [-79.9405, 32.7958]]]}}],
}


def _client(layers, fail=False):
    def handler(request: httpx.Request):
        if fail:
            return httpx.Response(503)
        layer = int(request.url.path.rstrip("/").split("/")[-2])
        rows = layers.get(layer, [])
        feats = rows if layer == 26 else [{"attributes": a} for a in rows]
        return httpx.Response(200, json={"features": feats})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_zoning_at_483_meeting():
    out = asyncio.run(z.zoning_at(-79.9405, 32.7956, _client(MEETING_483)))
    assert out == {"base_zoning": "MU-2/WH", "height_district": "8", "height_type": "Story",
                   "accommodations_overlay": "A-1", "old_and_historic": True, "area": "Peninsula"}
    text = z.describe_zoning(out)
    assert "8 stories maximum" in text and "A-1" in text and "Board of Architectural Review" in text


def test_outside_city_and_service_failure_return_none():
    assert asyncio.run(z.zoning_at(-80.2, 32.9, _client({}))) is None
    assert asyncio.run(z.zoning_at(-79.94, 32.79, _client(MEETING_483, fail=True))) is None


def test_city_parcel_point_by_tms():
    lon, lat = asyncio.run(z.city_parcel_point("4590503130", _client(MEETING_483)))
    assert round(lon, 4) == -79.9405 and round(lat, 4) == 32.7957
    assert asyncio.run(z.city_parcel_point("'; drop", _client({}))) is None
