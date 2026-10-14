"""Recent new-construction permits flag parcels the assessor still shows as vacant."""
import asyncio
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.services import construction  # noqa: E402

MS_2025 = 1735689600000   # 2025-01-01
MS_2026 = 1767225600000   # 2026-01-01


def test_fetch_keeps_largest_permit_per_parcel_and_strips_prefix():
    pages = [[
        {"MAIN_PARCEL_NUMBER": "C4590601012", "PERMIT_NUMBER": "A", "VALUATION": 95_018_887,
         "ISSUE_DATE": MS_2025, "FINALED_DATE": None, "PERMIT_STATUS": "Issued", "DESCRIPTION": "Core & Shell: new 7-story building"},
        {"MAIN_PARCEL_NUMBER": "C4590601012", "PERMIT_NUMBER": "B", "VALUATION": 400_000,
         "ISSUE_DATE": MS_2025, "FINALED_DATE": None, "DESCRIPTION": "Mock up panel"},
        {"MAIN_PARCEL_NUMBER": "C4600000001", "PERMIT_NUMBER": "C", "VALUATION": 18_500_000,
         "ISSUE_DATE": MS_2025, "FINALED_DATE": "09/23/2026          ", "PERMIT_STATUS": "Completed", "DESCRIPTION": "50 key hotel"},
    ]]
    seen = []

    def handler(request):
        seen.append(dict(httpx.QueryParams(request.content.decode())))
        return httpx.Response(200, json={"features": [{"attributes": a} for a in pages[0]]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    out = asyncio.run(construction.fetch_recent_construction(client, since_year=2021))
    assert set(out) == {"4590601012", "4600000001"}
    assert out["4590601012"]["permit"] == "A" and out["4590601012"]["status"] == "underway"
    assert out["4600000001"]["status"] == "completed" and out["4600000001"]["finaled"] == 2026
    assert "ISSUE_YEAR >= 2021" in seen[0]["where"]


def test_annotate():
    feats = [{"properties": {"TMS": "4590601012"}}, {"properties": {"TMS": "1"}}]
    n = construction.annotate_construction(feats, {"4590601012": {"status": "underway"}})
    assert n == 1 and feats[0]["properties"]["CONSTRUCTION"]["status"] == "underway"
    assert "CONSTRUCTION" not in feats[1]["properties"]


def test_mockups_fitouts_and_stale_permits_dont_count():
    from app.services.construction import _counts
    year = 2026
    assert not _counts({"DESCRIPTION": "MOCK UP PANEL for facade", "PERMIT_STATUS": "Issued", "ISSUE_DATE": "01/05/2026"}, year)
    assert not _counts({"DESCRIPTION": "First generation tenant upfit", "PERMIT_STATUS": "Completed"}, year)
    assert not _counts({"DESCRIPTION": "New 7-story building", "PERMIT_STATUS": "Issued", "ISSUE_DATE": "03/01/2021"}, year)
    assert _counts({"DESCRIPTION": "New 7-story building", "PERMIT_STATUS": "Issued", "ISSUE_DATE": "03/01/2024"}, year)
    assert _counts({"DESCRIPTION": "50 key hotel", "PERMIT_STATUS": "Completed", "ISSUE_DATE": "03/01/2021"}, year)
