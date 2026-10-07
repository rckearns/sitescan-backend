"""JBRC / SFAA state-approval source: entry parsing, filter, discovery (no network).

Fixtures in tests/fixtures/state_approvals/ are cut verbatim from pypdf text of
the real documents (JBRC agenda package June 3, 2025; SFAA minutes June 18,
2024) and from the real index pages, trimmed to a few pages / links.
"""

import asyncio
import re
from datetime import date
from pathlib import Path

import httpx

from app.services.pipeline import state_approvals as sa
from app.services.pipeline.events import STAGES, PipelineEvent

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "state_approvals"


def load_pages(name):
    """Fixture text uses '=====PAGE N' separators, one per PDF page."""
    text = (FIXTURES / name).read_text()
    return re.split(r"(?m)^=====PAGE \d+\n", text)[1:]


JBRC_PAGES = load_pages("jbrc_2025-06-03_excerpt.txt")
SFAA_PAGES = load_pages("sfaa_2024-06-18_minutes_excerpt.txt")
JBRC_URL = "https://www.scstatehouse.gov/CommitteeInfo/JointBondReviewCommittee/Agendas/x.pdf"
SFAA_URL = "https://sfaa.sc.gov/files/mtgs/minutes.pdf"


def by_pip(entries, pip):
    return [e for e in entries if e["pip"] == pip]


# --- entry splitter / field parser -------------------------------------------

def test_jbrc_entries_split_including_wrapped_agency_and_blank_page():
    entries = sa.extract_entries(JBRC_PAGES)
    assert [e["pip"] for e in entries] == ["H12.9972", "H15.9689"]
    clemson = entries[0]
    assert clemson["agency"] == "Clemson University"  # agency wrapped over two lines
    assert clemson["stage"] == "phase2"
    assert clemson["estimate"] == 57_500_000  # no Full Project Estimate on page 21 → CPIP estimate


def test_jbrc_project_205_june_2025():
    (e,) = by_pip(sa.extract_entries(JBRC_PAGES), "H15.9689")
    assert e["agency"] == "College of Charleston"
    assert e["agency_code"] == "H15"
    assert e["project_title"] == "Project 205 New Construction"
    assert e["request"].startswith("Change Project Name and increase Phase I Pre-Design Budget")
    assert e["stage"] == "phase1"
    assert e["estimate"] == 164_800_000
    assert e["delivery_method"] == "cmr"
    assert "Construction Manager at Risk" in e["full_project_estimate"]  # crosses a page break
    assert e["summary"].startswith("The site currently contains a 250-bed apartment building")
    assert "YMCA" in e["facility_characteristics"]
    assert e["page"] == 4  # 1-based index within the pages passed in
    # running headers and page numbers are gone, the funding table is collapsed
    assert "JOINT BOND REVIEW COMMITTEE SUMMARY" not in e["text"]
    assert "PERMANENT IMPROVEMENTS PROPOSED" not in e["text"]
    assert "Funding table omitted; All Sources" in e["text"]
    assert "Proviso 118.19" not in e["text"]
    assert len(e["text"]) <= sa.TEXT_LIMIT


def test_sfaa_entries_june_2024():
    entries = sa.extract_entries(SFAA_PAGES)
    assert [e["pip"] for e in entries] == ["H15.9689", "H15.9681", "H17.9630"]
    p205 = entries[0]
    assert p205["agency"] == "College of Charleston"
    assert p205["item_label"] == "JBRC Item 2"
    assert p205["project_title"] == "St. Philip Housing Innovation District"
    assert p205["stage"] == "phase1"
    # Full Project Estimate wins over the CPIP line's "$11,000,000"
    assert p205["estimate"] == 164_800_000
    assert p205["delivery_method"] == ""  # 2024 entry never names a delivery method
    assert "Minutes of State Fiscal Accountability Authority" not in p205["text"]
    coastal = entries[2]
    assert coastal["agency"] == "Coastal Carolina University"
    assert coastal["stage"] == "land"


def test_classify_request_variants():
    cases = {
        "Establish Phase II Full Constructi on Budget": "phase2",
        "Revise Scope and Increase Phase II Full Construction Budget": "phase2",
        "Establish Phase I Pre- Design Budget": "phase1",
        "Establish Phase I Pre-Design B udget to renovate": "phase1",
        "Establish Preliminary Land Acquisiti on for the purpose of investigating": "land",
        "Establish Final Land Acquisition to purchase +/- 13 acres": "land",
        "Change Source of Funds in this project": "other",
        "Revise Scope": "other",
    }
    for request, stage in cases.items():
        assert sa.classify_request(request) == stage, request


def test_parse_dollars_and_delivery():
    assert sa.parse_dollars("$164,800,000 (internal). Phase II") == 164_800_000
    assert sa.parse_dollars("$5.5 million from gifts") == 5_500_000
    assert sa.parse_dollars("no money here") is None
    assert sa.detect_delivery("hire a Construction Manager at Risk")[0] == "cmr"
    assert sa.detect_delivery("cover the Constructi on Manager at Risk procurement method")[0] == "cmr"
    assert sa.detect_delivery("delivered as design-build")[0] == "design-build"
    assert sa.detect_delivery("CM-R or design-build") == ("", ["cmr", "design-build"])
    assert sa.detect_delivery("design-bid-build") == ("", [])


# --- Charleston filter + events ----------------------------------------------

def test_charleston_filter():
    jbrc = sa.extract_entries(JBRC_PAGES)
    assert [sa.is_charleston_entry(e) for e in jbrc] == [False, True]  # Clemson out
    sfaa = sa.extract_entries(SFAA_PAGES)
    assert {e["pip"]: sa.is_charleston_entry(e) for e in sfaa} == {
        "H15.9689": True, "H15.9681": True, "H17.9630": False,
    }
    tech = {"agency_code": "H59", "agency": "Greenville Technical College", "project_title": "x", "text": "Greenville"}
    assert not sa.is_charleston_entry(tech)
    assert sa.is_charleston_entry(dict(tech, agency="Trident Technical College"))
    other = {"agency_code": "E24", "agency": "Adjutant General", "project_title": "Readiness Center", "text": "Joint Base Charleston"}
    assert sa.charleston_reason(other) == "mention"


def test_events_from_jbrc_pages():
    events = sa.events_from_pages(JBRC_PAGES, JBRC_URL, date(2025, 6, 3), "jbrc")
    assert len(events) == 1
    ev = events[0]
    assert isinstance(ev, PipelineEvent)
    assert ev.source == "jbrc"
    assert ev.external_id == "jbrc:2025-06-03:H15.9689:phase1"
    assert ev.project_key == "PIP:H15.9689"
    assert ev.pip_number == "H15.9689"
    assert ev.owner == "College of Charleston"
    assert ev.stage == "phase1" and ev.stage in STAGES
    assert ev.delivery_method == "cmr"
    assert ev.estimate == 164_800_000
    assert ev.event_date == date(2025, 6, 3)
    assert ev.source_url == JBRC_URL
    assert ev.title.startswith("Project 205 New Construction (College of Charleston) – Change Project Name")
    assert ev.text.startswith("3. Project:")
    assert ev.extra["request"].startswith("Change Project Name")
    assert ev.extra["charleston_reason"] == "agency"


def test_parse_document_uses_pdf_text(monkeypatch):
    monkeypatch.setattr(sa, "pdf_text_pages", lambda pdf: SFAA_PAGES)
    events = sa.parse_document(b"%PDF-fake", SFAA_URL, date(2024, 6, 18), "sfaa")
    ids = sorted(e.external_id for e in events)
    assert ids == ["sfaa:2024-06-18:H15.9681:phase2", "sfaa:2024-06-18:H15.9689:phase1"]
    p205 = next(e for e in events if e.pip_number == "H15.9689")
    assert p205.project_key == "PIP:H15.9689"
    assert p205.delivery_method == ""


# --- discovery -----------------------------------------------------------------

def test_parse_meeting_date():
    assert sa.parse_meeting_date("June 3, 2025 Meeting Agenda") == date(2025, 6, 3)
    assert sa.parse_meeting_date("Joint Bond Review Committee - Feb 4 2026.pdf") == date(2026, 2, 4)
    assert sa.parse_meeting_date("Sept. 9, 2025") == date(2025, 9, 9)
    assert sa.parse_meeting_date("6/16/2026") == date(2026, 6, 16)
    assert sa.parse_meeting_date("June 10 FINAL") is None


def test_parse_jbrc_index():
    docs = sa.parse_jbrc_index((FIXTURES / "jbrc_index.html").read_text())
    assert len(docs) == 12
    url, meeting, title = docs[0]
    assert meeting == date(2026, 8, 18)
    assert url == (
        "https://www.scstatehouse.gov/CommitteeInfo/JointBondReviewCommittee/Agendas/"
        "Joint%20Bond%20Review%20Committee%20Agenda%20August%2018%202026.pdf"
    )
    assert title == "August 18, 2026 Meeting Agenda"
    dates = [d[1] for d in docs]
    assert dates == sorted(dates, reverse=True)
    assert date(2026, 6, 10) in dates  # file name has no year; link text does
    assert date(2025, 6, 3) in dates
    assert all("/Minutes/" not in d[0] for d in docs)


def test_parse_jbrc_index_falls_back_to_filename():
    html = '<a href="/CommitteeInfo/JointBondReviewCommittee/Agendas/JBRC Agenda March 26, 2025.pdf">Agenda</a>'
    assert sa.parse_jbrc_index(html)[0][1] == date(2025, 3, 26)


def test_parse_sfaa_index():
    docs = sa.parse_sfaa_index((FIXTURES / "sfaa_meetings_2026.html").read_text())
    assert [(d[0], d[1]) for d in docs] == [
        ("https://sfaa.sc.gov/files/mtgs/Final_Minutes_June_16_2026_Meeting.pdf", date(2026, 6, 16)),
        ("https://sfaa.sc.gov/files/mtgs/Meeting_Minutes_March_31_2026.pdf", date(2026, 3, 31)),
        ("https://sfaa.sc.gov/files/mtgs/SFAA_min_1.pdf", date(2026, 2, 10)),  # date from meeting header
    ]
    assert docs[0][2] == "SFAA Minutes June 16, 2026"


# --- orchestration ---------------------------------------------------------------

def test_fetch_state_approval_events_with_mock_transport(monkeypatch):
    jbrc_html = (FIXTURES / "jbrc_index.html").read_text()
    sfaa_html = (FIXTURES / "sfaa_meetings_2026.html").read_text()
    requested = []

    def handler(request):
        requested.append((request.method, str(request.url)))
        if request.url.host == "www.scstatehouse.gov" and request.url.path.endswith(".php"):
            return httpx.Response(200, text=jbrc_html)
        if request.url.path == "/authority-meetings":
            assert request.method == "POST"
            year = request.content.decode()
            return httpx.Response(200, text=sfaa_html if year == "mtgsel=2026" else "<html></html>")
        if "August%2018%202026" in str(request.url):
            return httpx.Response(500)  # one document fails; the rest continue
        if request.url.path.endswith(".pdf"):
            return httpx.Response(200, content=b"%PDF-" + request.url.path.encode())
        return httpx.Response(404)

    def fake_pages(pdf_bytes):
        return SFAA_PAGES if b"mtgs" in pdf_bytes else JBRC_PAGES

    monkeypatch.setattr(sa, "pdf_text_pages", fake_pages)
    monkeypatch.setattr(sa, "REQUEST_DELAY_SECONDS", 0)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await sa.fetch_state_approval_events(date(2026, 3, 1), client=client)

    events = asyncio.run(run())
    pdfs = [u for m, u in requested if u.endswith(".pdf")]
    # JBRC: Aug 18 2026 (fails), Jun 10 2026, Mar 25 2026; SFAA: Jun 16 2026, Mar 31 2026
    assert len(pdfs) == 5
    ids = {e.external_id for e in events}
    assert "jbrc:2026-06-10:H15.9689:phase1" in ids
    assert "jbrc:2026-03-25:H15.9689:phase1" in ids
    assert "sfaa:2026-06-16:H15.9689:phase1" in ids
    assert not any(i.startswith("jbrc:2026-08-18") for i in ids)
    assert len(events) == 2 * 1 + 2 * 2
