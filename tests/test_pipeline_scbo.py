"""SCBO pipeline source: parsing, mapping, filtering, fetching (no network).

Fixtures are real SCBO listing pages trimmed to a few ad blocks:
  scbo_c2_2024-11-19.html  A/E: Project 205 A/E ad (s=53461) + 2 others
  scbo_c3_2025-02-19.html  Construction: Project 205 CM-R ad (s=55536) + 4 others
"""
import asyncio
from datetime import date, datetime
from pathlib import Path

import pytest

from app.services.pipeline import scbo
from app.services.pipeline.events import STAGES, PipelineEvent

FIX = Path(__file__).parent / "fixtures" / "scbo"
C2 = (FIX / "scbo_c2_2024-11-19.html").read_text()
C3 = (FIX / "scbo_c3_2025-02-19.html").read_text()


def _by_id(events, ad_id):
    return next(e for e in events if e.external_id == ad_id)


# --- Project 205 --------------------------------------------------------------

def test_project_205_ae_ad():
    ev = _by_id(scbo.parse_scbo_page(C2, 2), "53461")
    assert isinstance(ev, PipelineEvent)
    assert ev.source == "scbo-ae"
    assert ev.stage == "ae-selection"
    assert ev.title == "St. Philip Housing Innovation District Site Development"
    assert ev.owner == "College of Charleston"
    assert ev.pip_number == "H15.9689"
    assert ev.project_key == "PIP:H15.9689"
    assert ev.delivery_method == "cmr"          # "Anticipated Project Delivery Method: CM-R"
    assert ev.location == "Charleston, SC"
    assert ev.event_date == date(2024, 11, 19)
    assert ev.deadline == datetime(2024, 12, 18, 14, 0)   # Resume Deadline
    assert ev.cost_range is None and ev.estimate is None
    assert ev.source_url == "https://scbo.sc.gov/online-edition?s=53461"
    assert ev.extra["form_url"] == "https://scbo.sc.gov/files/scbo/H15-9689-ML%20SE-210.pdf"
    assert ev.extra["project_number"] == "H15-9689-ML"
    assert "1,100-1,300 bed student housing" in ev.text
    assert "Anticipated Project Delivery Method: CM-R" in ev.text


def test_project_205_cmr_ad():
    ev = _by_id(scbo.parse_scbo_page(C3, 3), "55536")
    assert ev.source == "scbo-construction"
    assert ev.stage == "cmr-solicitation"
    assert ev.delivery_method == "cmr"
    assert ev.pip_number == "H15.9689"
    assert ev.project_key == "PIP:H15.9689"
    assert ev.cost_range == (60_000_000, 100_000_000)
    assert ev.estimate == 100_000_000
    assert ev.deadline == datetime(2025, 3, 11, 14, 0)
    assert ev.event_date == date(2025, 2, 19)
    assert ev.owner == "College of Charleston"
    assert ev.source_url == "https://scbo.sc.gov/online-edition?s=55536"
    assert ev.extra["form_url"] == "https://scbo.sc.gov/files/scbo/H15-9689-ML_SE-410_1.pdf"
    assert "Full preconstruction and construction management services" in ev.text


def test_both_project_205_ads_link_to_same_project():
    ae = _by_id(scbo.parse_scbo_page(C2, 2), "53461")
    cm = _by_id(scbo.parse_scbo_page(C3, 3), "55536")
    assert ae.project_key == cm.project_key == "PIP:H15.9689"


# --- other ads / filter -------------------------------------------------------

def test_charleston_filter_drops_out_of_area_ads():
    ids2 = {e.external_id for e in scbo.parse_scbo_page(C2, 2)}
    assert ids2 == {"53461", "52764"}            # Lexington SD One dropped
    ids3 = {e.external_id for e in scbo.parse_scbo_page(C3, 3)}
    assert ids3 == {"55536", "55521", "55436", "55406"}   # Town of Lexington dropped
    all3 = scbo.parse_scbo_page(C3, 3, charleston_only=False)
    assert len(all3) == 5


def test_construction_stages_and_keys():
    evs = {e.external_id: e for e in scbo.parse_scbo_page(C3, 3)}
    # Blank delivery method, but the project number says IFB -> bid
    assert evs["55521"].stage == "bid"
    assert evs["55521"].delivery_method == ""
    assert evs["55521"].project_key == "SCBO:IFB No. 6055-25C"
    # Delivery "Other", no IFB wording -> other; "n/a" number -> ad id key
    assert evs["55436"].stage == "other"
    assert evs["55436"].project_key == "SCBO:55436"
    # Design-Bid-Build -> bid; MUSC's H51-N368 isn't a PIP number
    assert evs["55406"].stage == "bid"
    assert evs["55406"].delivery_method == "design-bid-build"
    assert evs["55406"].pip_number == ""
    assert evs["55406"].project_key == "SCBO:H51-N368-ML"
    assert evs["55406"].cost_range == (750_000, 1_000_000)
    for e in evs.values():
        assert e.stage in STAGES


@pytest.mark.parametrize("raw,expected", [
    ("CM-R", "cmr"), ("CM at Risk", "cmr"), ("Construction Manager at Risk", "cmr"),
    ("Design-Build", "design-build"), ("Design Build", "design-build"),
    ("Design-Bid-Build", "design-bid-build"), ("Other", ""), ("N/A", ""), ("", ""),
])
def test_map_delivery_method(raw, expected):
    assert scbo.map_delivery_method(raw) == expected


@pytest.mark.parametrize("raw,rng,est", [
    ("$60,000,000 to $100,000,000", (60e6, 100e6), 100e6),
    ("$1.5M - $2M", (1.5e6, 2e6), 2e6),
    ("Under $50,000", (0, 50_000), 50_000),
    ("https://aptg.co/tbF-Q3", None, None),
    ("", None, None),
])
def test_parse_cost_range(raw, rng, est):
    assert scbo.parse_cost_range(raw) == (rng, est)


def test_parse_datetime():
    assert scbo.parse_scbo_datetime("March 11, 2025 - 2:00pm") == datetime(2025, 3, 11, 14)
    assert scbo.parse_scbo_datetime("December 4, 2024 - 11:30am") == datetime(2024, 12, 4, 11, 30)
    assert scbo.parse_scbo_datetime("October 7, 2026") == datetime(2026, 10, 7)
    assert scbo.parse_scbo_datetime("TBD") is None


def test_charleston_area_rules():
    f = scbo.is_charleston_area
    assert f({"Project Number": "H09-9626", "Project Location": "Columbia"})        # agency code
    assert f({"Agency/Owner": "Trident Technical College"})
    assert f({"Agency/Owner": "SC State Ports Authority"})
    assert f({"Project Location": "Mt. Pleasant, SC"})
    assert f({"Project Location": "Kiawah Island"})
    assert f({"Description": "Work at the Goose Creek campus"})
    assert not f({"Agency/Owner": "Clemson University", "Project Location": "Clemson, SC"})


def test_sample_days():
    assert scbo.sample_days(date(2026, 10, 1), date(2026, 10, 7), 3) == [
        date(2026, 10, 7), date(2026, 10, 4), date(2026, 10, 1)]
    assert scbo.sample_days(date(2026, 10, 7), date(2026, 10, 7), 3) == [date(2026, 10, 7)]


# --- fetch orchestration with a fake client ----------------------------------

class _Resp:
    def __init__(self, text, status=200):
        self.text, self.status = text, status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")


class _FakeClient:
    def __init__(self, pages):
        self.pages, self.urls = pages, []

    async def get(self, url):
        self.urls.append(url)
        for key, resp in self.pages.items():
            if key in url:
                return resp
        return _Resp("<html>blocked</html>")


def test_fetch_events_dedupes_and_filters_by_publish_date():
    client = _FakeClient({"c=2-": _Resp(C2), "c=3-": _Resp(C3)})
    events = asyncio.run(scbo.fetch_scbo_pipeline_events(
        date(2024, 11, 1), date(2025, 2, 28), client, step_days=30, delay_seconds=0))
    # Every sampled page returns the same ads; each ad is kept once.
    assert len(client.urls) == 2 * len(scbo.sample_days(date(2024, 11, 1), date(2025, 2, 28), 30))
    ids = [e.external_id for e in events]
    assert len(ids) == len(set(ids))
    assert {"53461", "55536"} <= set(ids)
    assert all(date(2024, 11, 1) <= e.event_date <= date(2025, 2, 28) for e in events)
    assert any(u.startswith("https://scbo.sc.gov/online-edition?c=2-2025-02-28") for u in client.urls)


def test_fetch_window_excludes_older_ads():
    client = _FakeClient({"c=2-": _Resp(C2), "c=3-": _Resp(C3)})
    events = asyncio.run(scbo.fetch_scbo_pipeline_events(
        date(2025, 2, 10), date(2025, 2, 20), client, delay_seconds=0))
    ids = {e.external_id for e in events}
    assert "55536" in ids and "53461" not in ids


def test_fetch_survives_errors_and_block_pages():
    client = _FakeClient({"c=2-": _Resp("", 503)})    # c=3 gets a tiny "blocked" page
    events = asyncio.run(scbo.fetch_scbo_pipeline_events(
        date(2025, 2, 10), date(2025, 2, 20), client, delay_seconds=0))
    assert events == []
