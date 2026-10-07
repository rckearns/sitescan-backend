"""Board agendas: BAR-L item-splitter regression, institutional scoring,
pipeline adapter, past-year discovery. No network.

Fixtures in tests/fixtures/boards/ are pypdf text extracts of real BAR-L
agendas ("=====PAGE n" lines come from the extractor and are stripped here):
  _06112025-10088  June 11, 2025  (106 Coming St full demolition, Project 205)
  _07082026-11007  July 8, 2026   (35 Bee St MUSC; 106 Coming St conceptual)
  _08312026-11163  Aug 31, 2026   (106 Coming St conceptual, special meeting)
  _09092026-11173  Sept 9, 2026   (two minutes items before the applications)
"""
import asyncio
import re
from datetime import date
from pathlib import Path

from app.services.pipeline import boards
from app.services.pipeline.events import STAGES
from sitescan_boards import poller
from sitescan_boards.classifier import classify
from sitescan_boards.parser import AgendaItem, parse_agenda_text

FIX = Path(__file__).parent / "fixtures" / "boards"


def agenda(ext_id):
    text = (FIX / f"bar_l{ext_id}.txt").read_text()
    return re.sub(r"(?m)^=====PAGE \d+\s*$", "", text)


def by_address(items, address):
    return next(i for i in items if i.address == address)


# --- 1. parser: numbering restarts under "A. MINUTES" / "B. APPLICATIONS" ----

def test_first_application_not_merged_into_minutes_july_2026():
    items = parse_agenda_text(agenda("_07082026-11007"))
    assert [i.address for i in items][:3] == [
        "35 Bee Street", "106 Coming Street", "989 Morrison Drive"]
    assert not any("Minutes" in i.address for i in items)
    bee = items[0]
    assert bee.item_number == 1
    assert bee.section == "APPLICATIONS"
    assert bee.case_number == "BAR2026-002699"
    assert bee.tms == "4601501043"
    assert bee.owner.startswith("Medical University of South Carolina")
    assert bee.applicant == "Liollio Architecture"
    assert len(items) == 7


def test_first_application_not_merged_into_minutes_june_2025():
    items = parse_agenda_text(agenda("_06112025-10088"))
    first = items[0]
    assert first.address == "71 George Street"
    assert first.case_number == "BAR2025-001929"
    assert first.owner == "College of Charleston Board of Trustees"
    coming = by_address(items, "106 Coming Street")
    assert coming.case_number == "BAR2025-001918"
    assert "Full Demolition" in coming.request_text
    assert len(items) == 7


def test_two_minutes_items_sept_2026():
    items = parse_agenda_text(agenda("_09092026-11173"))
    assert items[0].address == "989 Morrison Drive"
    assert items[0].case_number == "BAR2026-002845"
    assert items[0].item_number == 1
    assert not any("Minutes" in i.address for i in items)
    assert by_address(items, "16 Sabin Street").case_number == "BAR2026-002852"


def test_agenda_without_minutes_section():
    items = parse_agenda_text(agenda("_08312026-11163"))
    assert len(items) == 1
    assert items[0].address == "106 Coming Street"
    assert items[0].section == "APPLICATIONS"


# --- 2. institutional scoring -------------------------------------------------

def test_project_205_items_score_at_least_50():
    june = by_address(parse_agenda_text(agenda("_06112025-10088")), "106 Coming Street")
    july = by_address(parse_agenda_text(agenda("_07082026-11007")), "106 Coming Street")
    aug = by_address(parse_agenda_text(agenda("_08312026-11163")), "106 Coming Street")
    scores = {k: classify(v, "BAR-L") for k, v in
              {"june_demo": june, "july_concept": july, "aug_concept": aug}.items()}
    # Before: 27 / 40 / 22
    for name, c in scores.items():
        assert c.score >= 50, f"{name} scored {c.score}"
        assert "inst_owner:College of Charleston" in c.tags
    assert scores["june_demo"].stage == "demolition"
    assert scores["july_concept"].stage == "conceptual"
    assert "inst_use:student housing" in scores["aug_concept"].tags


def test_musc_hospital_scores_high():
    sabin = by_address(parse_agenda_text(agenda("_09092026-11173")), "16 Sabin Street")
    c = classify(sabin, "BAR-L")
    assert c.score >= 75
    assert "inst_owner:MUSC" in c.tags      # named in the request, owner is a developer


def test_institutional_minor_scope_gets_owner_bump_only():
    george = by_address(parse_agenda_text(agenda("_06112025-10088")), "66 George Street")
    c = classify(george, "BAR-L")          # signage package for CofC
    assert "inst_owner:College of Charleston" in c.tags
    assert "inst_redevelopment" not in c.tags
    assert c.score < 50


def test_developer_items_unchanged():
    # No institutional signal -> same score as before this change.
    items = parse_agenda_text(agenda("_07082026-11007"))
    assert classify(by_address(items, "989 Morrison Drive"), "BAR-L").score == 45
    assert classify(by_address(items, "251 King Street"), "BAR-L").score == 0
    sept = parse_agenda_text(agenda("_09092026-11173"))
    assert classify(by_address(sept, "295 Calhoun Street"), "BAR-L").score == 81


def test_institutional_keywords_on_plain_items():
    def item(req, owner="Somebody LLC"):
        return AgendaItem(item_number=1, address="1 Test St", request_text=req,
                          raw_text=f"1 Test St\n{req}\nOwner: {owner}", owner=owner)
    plain = classify(item("Requesting Conceptual Approval of a building."), "BAR-L").score
    for req in ["a new residence hall", "a dormitory", "an academic building",
                "a research laboratory", "a medical office building",
                "a parking garage", "a new parking deck"]:
        s = classify(item(f"Requesting Conceptual Approval of {req}."), "BAR-L").score
        assert s > plain, req
    for owner in ["The Citadel", "Trident Technical College",
                  "Charleston County School District", "MUSC"]:
        s = classify(item("Requesting Conceptual Approval of a building.", owner), "BAR-L").score
        assert s > plain, owner


# --- 3. pipeline adapter ------------------------------------------------------

def _ref(ext_id, board="BAR-L"):
    m = re.match(r"_(\d{2})(\d{2})(\d{4})", ext_id)
    mm, dd, yyyy = map(int, m.groups())
    return poller.AgendaRef(board_code=board, meeting_date=date(yyyy, mm, dd),
                            pdf_url=f"https://www.charleston-sc.gov/AgendaCenter/ViewFile/Agenda/{ext_id}",
                            external_id=ext_id, title="BAR-L Agenda")


def test_items_to_events_project_205():
    evs = []
    for ext in ("_06112025-10088", "_07082026-11007", "_08312026-11163"):
        items = parse_agenda_text(agenda(ext))
        evs += [e for e in boards.agenda_items_to_events(_ref(ext), items)
                if e.address == "106 Coming Street"]
    assert len(evs) == 3
    # TMS written "460-16-03-017" in 2025 and "4601603017" in 2026 -> one key
    assert {e.project_key for e in evs} == {"BOARD:4601603017"}
    demo, july, aug = evs
    assert demo.stage == "board-concept" and demo.extra["stage_label"] == "demolition"
    assert july.stage == "board-concept" and aug.stage == "board-concept"
    for e in evs:
        assert e.source == "board"
        assert e.owner == "College of Charleston"
        assert e.location == "Charleston"
        assert e.extra["board"] == "BAR-L"
        assert e.extra["score"] >= 50
        assert e.extra["case_number"].startswith("BAR20")
        assert e.stage in STAGES
        assert "106 Coming Street" in e.text
    assert aug.event_date == date(2026, 8, 31)
    assert aug.external_id == "_08312026-11163:BAR2026-002703"
    assert aug.source_url.endswith("/_08312026-11163")


def test_final_stage_and_min_score():
    items = parse_agenda_text(agenda("_09092026-11173"))
    evs = boards.agenda_items_to_events(_ref("_09092026-11173"), items)
    calhoun = next(e for e in evs if e.address == "295 Calhoun Street")
    assert calhoun.stage == "board-final"
    assert len({e.external_id for e in evs}) == len(evs)
    high = boards.agenda_items_to_events(_ref("_09092026-11173"), items, min_score=50)
    assert 0 < len(high) < len(evs)


def test_pipeline_stage_mapping():
    assert boards.pipeline_stage("rezoning", "") == "board-concept"
    assert boards.pipeline_stage("preliminary", "") == "board-concept"
    assert boards.pipeline_stage("final", "") == "board-final"
    assert boards.pipeline_stage(None, "Request approval of a site plan") == "board-final"
    assert boards.pipeline_stage(None, "Request for a PUD master plan") == "board-concept"
    assert boards.pipeline_stage(None, "Request approval of a mockup panel") == "other"


def test_project_key_fallback_to_address():
    it = AgendaItem(item_number=1, address="5 McClennan Banks Drive")
    assert boards.board_project_key(it) == "BOARD:5 MCCLENNAN BANKS DR"
    it.tms = "4590503136, 9"
    assert boards.board_project_key(it) == "BOARD:4590503136"


def test_fetch_board_events_with_injected_refs():
    from sitescan_boards import parser
    texts = {f"/{e}": agenda(e) for e in ("_07082026-11007", "_08312026-11163")}
    refs = [_ref(e) for e in ("_07082026-11007", "_08312026-11163", "_06112025-10088")]

    orig = parser.extract_pdf_text
    parser.extract_pdf_text = lambda b: b.decode()
    try:
        fetch = lambda url: next(t for k, t in texts.items() if url.endswith(k)).encode()
        evs = asyncio.run(boards.fetch_board_events(
            date(2026, 7, 1), refs=refs, fetch_pdf=fetch))
    finally:
        parser.extract_pdf_text = orig
    agendas = {e.extra["agenda_id"] for e in evs}
    assert agendas == {"_07082026-11007", "_08312026-11163"}   # 2025 agenda out of range
    assert sum(e.address == "106 Coming Street" for e in evs) == 2


# --- discovery parsing ---------------------------------------------------------

YEAR_FRAGMENT = """
<span id="section1"><table>
<tr><td><h3><strong>Jun 11, 2025</strong></h3>
<p><a href="/AgendaCenter/ViewFile/Agenda/_06112025-10088">BAR-L Agenda</a></p></td>
<td><a href="/AgendaCenter/ViewFile/Agenda/_06112025-10088">Agenda</a></td></tr>
<tr><td><p><a href="/AgendaCenter/ViewFile/Agenda/_06112025-10089">BAR-L Agenda (Image Overview)</a></p></td></tr>
<tr><td><p><a href="/AgendaCenter/ViewFile/Agenda/_06122025-10129">BAR-S Public Comment</a></p></td></tr>
<tr><td><p><a href="/AgendaCenter/ViewFile/Agenda/_06122025-10100">BAR-S Agenda</a></p></td></tr>
</table></span>
"""


def test_parse_index_html_skips_image_overview_and_comments():
    refs = poller.parse_index_html(YEAR_FRAGMENT)
    assert [(r.board_code, r.external_id) for r in refs] == [
        ("BAR-L", "_06112025-10088"), ("BAR-S", "_06122025-10100")]


class _FakeSession:
    def __init__(self):
        self.posts = []

    def post(self, url, data=None, timeout=None):
        self.posts.append((url, data))

        class R:
            text = YEAR_FRAGMENT if data["catID"] == "1" else (
                '<a href="/AgendaCenter/ViewFile/Agenda/_06172025-10111">Agenda Packet</a>')

            def raise_for_status(self):
                pass
        return R()


def test_discover_year_posts_per_category(monkeypatch):
    monkeypatch.setattr(poller.time, "sleep", lambda s: None)
    s = _FakeSession()
    refs = poller.discover_year(2025, s, {"Board of Architectural Review": 1,
                                          "Planning Commission": 5})
    assert s.posts[0] == ("https://www.charleston-sc.gov/AgendaCenter/UpdateCategoryList",
                          {"year": "2025", "catID": "1"})
    codes = {(r.board_code, r.external_id) for r in refs}
    assert ("BAR-L", "_06112025-10088") in codes
    assert ("PC", "_06172025-10111") in codes      # board from the category name


# --- Planning Commission agenda (live text, Sept 16 2026, trimmed) ------------

PC_SEPT_2026 = """\
The following applications will be considered:
A. MINUTES
1. Request Approval of Minutes from the July & August Meetings
B. ORDINANCE AMENDMENTS
1. DEFERRED | Request approval to amend Sections 54-208 and 54-227 to incorporate
wording and definition changes to the Short-Term Rental Ordinance intended to
clarify and quantify certain terms, limits, submittal and review criteria, and penalties
C. PLANNED UNIT DEVELOPMENT & CONCEPT PLAN
1. DEFERRED | 3294 & 3280 Maybank Highway, 1730 & 1738 Fern Hill Drive
Johns Island | TMS# 2790000288, -009, -008, 3130000047, 3130000237, &
2790000006 | Council District 3 | Approx. 13.47 ac.
Request to rezone from Commercial Transitional & Rural Residential (CT & RR-1) to
Planned Unit Development (PUD). Request Zoning Planned Unit Development (PUD). Zoned
Residential (R-4) & Johns Island Maybank Highway Corridor Overlay (JO-MHC-O) in
Charleston County.
Owner: The Charleston Real Estate Company LP, Sherry B. Bailey & Dennis
R. Bailey, Rosemary Lynn Knox, & Bight Oak Properties LLC.
Applicant: Kimley-Horn
Planning Commission
Agenda | September 16, 2026 Page 2
D. REZONINGS
1. A Portion of 1176 & 1180 Sam Rittenberg Boulevard
West Ashley | TMS# 3520800016 & 3520800012 | Council District 9 | Approx.
0.68 ac.
Request to rezone from Single-Family Residential (SR-1) to General Business (GB).
Owner: 1180 Sam Rittenberg Blvd. LLC.
Applicant: Brian A. Hellman
E. ZONINGS
1. 1657 Ashley Hall Road
West Ashley (Village Square) | TMS# 3510800027| Council District 9 | Approx. 1.61
ac.
Request Zoning General Business (GB). Zoned Community Commercial (CC) in Charleston
County.
Owner: C Level Investments LLC
"""


def test_pc_minutes_status_prefix_and_ordinances():
    items = parse_agenda_text(PC_SEPT_2026)
    addrs = [i.address for i in items]
    assert not any("Minutes" in a for a in addrs)
    # The ordinance amendment has no parcel data and is dropped by the parser.
    assert len(items) == 3
    assert addrs[0] == "3294 & 3280 Maybank Highway, 1730 & 1738 Fern Hill Drive"
    assert items[0].status == "DEFERRED"
    assert items[0].section == "PLANNED UNIT DEVELOPMENT & CONCEPT PLAN"
    assert addrs[1] == "A Portion of 1176 & 1180 Sam Rittenberg Boulevard"

    evs = boards.agenda_items_to_events(
        poller.AgendaRef("PC", date(2026, 9, 16), "https://x/_09162026-11192",
                         "_09162026-11192"), items)
    assert [e.address for e in evs] == addrs
    pud, rezone, zoning = evs
    assert pud.project_key == "BOARD:2790000288"
    assert pud.extra["status"] == "DEFERRED"
    assert pud.stage == rezone.stage == zoning.stage == "board-concept"
    assert rezone.extra["section"] == "REZONINGS"


def test_items_without_parcel_or_place_get_no_event():
    it = AgendaItem(item_number=1, address="Request approval to amend Sections 54-208",
                    request_text="Request approval to amend Sections 54-208")
    assert boards.board_project_key(it) == ""
    ref = poller.AgendaRef("PC", date(2026, 9, 16), "https://x/_1", "_1")
    assert boards.agenda_items_to_events(ref, [it]) == []


def test_discover_refs_range_and_past_years(monkeypatch):
    this_year = date.today().year
    cur = [_ref(f"_0101{this_year}-1"), _ref(f"_1231{this_year}-2")]   # Dec 31 may be "future"
    past = [_ref(f"_0611{this_year - 1}-3"), _ref(f"_0101{this_year - 1}-4")]
    years = []
    monkeypatch.setattr(poller, "discover", lambda: cur)
    monkeypatch.setattr(poller, "discover_year", lambda y, s=None: years.append(y) or past)
    monkeypatch.setattr(boards.time, "sleep", lambda s: None)
    refs = boards.discover_refs(date(this_year - 1, 3, 1))
    assert years == [this_year - 1]
    assert [r.external_id for r in refs] == [
        f"_1231{this_year}-2", f"_0101{this_year}-1", f"_0611{this_year - 1}-3"]
    years.clear()
    assert [r.external_id for r in boards.discover_refs(date(this_year, 1, 1),
                                                        date(this_year, 6, 30))] == [
        f"_0101{this_year}-1"]
    assert years == []


def test_fetch_pdf_refuses_huge_packets(monkeypatch):
    from sitescan_boards import poller

    class Resp:
        def __init__(self, headers, chunks):
            self.headers, self._chunks = headers, chunks
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def raise_for_status(self): pass
        def iter_content(self, chunk_size): return iter(self._chunks)

    class Session:
        def __init__(self, resp): self.resp = resp
        def get(self, *a, **k): return self.resp

    import pytest
    big = Resp({"Content-Length": str(poller.MAX_AGENDA_BYTES + 1)}, [])
    with pytest.raises(ValueError, match="skipping"):
        poller.fetch_pdf("https://x/a.pdf", Session(big))
    undeclared = Resp({}, [b"%PDF" + b"x" * (poller.MAX_AGENDA_BYTES // 2)] * 3)
    with pytest.raises(ValueError, match="exceeds"):
        poller.fetch_pdf("https://x/b.pdf", Session(undeclared))
    ok = Resp({"Content-Length": "9"}, [b"%PDF-1.7\n"])
    assert poller.fetch_pdf("https://x/c.pdf", Session(ok)) == b"%PDF-1.7\n"
