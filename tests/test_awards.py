"""State award notices: link discovery, flattening the AI's output, pipeline events, saving, rankings."""
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.models.database import (Base, ContractAward, PipelineDocument, PipelineEventRow,  # noqa: E402
                                 PipelineProject)
from app.services import awards  # noqa: E402

URL = "https://charleston.edu/facilities-management/p205_h15-9689-pd-a_se370_ipw.pdf"
P205 = {"notices": [
    {"form": "SE-370", "notice_type": "award", "delivery": "design-bid-build", "agency": "College of Charleston",
     "project_name": "Project 205 New Construction - Geophysics Enabling Stage", "project_number": "H15-9689-PD-A",
     "posting_date": "2026-07-15", "firms": [{"name": "IPW Construction Group LLC", "role": "contractor", "amount": 305416}]},
    {"form": "SE-310", "notice_type": "invitation", "firms": []},
]}
SHORTLIST = {"notices": [
    {"form": "SE-412", "notice_type": "shortlist", "delivery": "cmr", "agency": "College of Charleston",
     "project_name": "Project 205 New Construction", "project_number": "H15-9689-ML", "posting_date": "2026-09-01",
     "firms": [{"name": "Choate Construction Company", "role": "cm"}, {"name": "Hill Construction", "role": "cm"},
               {"name": " ", "role": "cm"}]},
]}


def test_notice_links_keep_notices_only():
    html = ('<a href="/fm/internal_files/contract-awards/se-370-noi-signed.pdf">x</a>'
            '<a href="https://x.edu/SE-212_H09.pdf">y</a><a href="/campus-map.pdf">map</a>'
            '<a href="/fm/internal_files/contract-awards/se-370-noi-signed.pdf">dup</a>')
    out = awards.notice_links(html, "https://charleston.edu/facilities-management/contract-awards.php")
    assert out == ["https://charleston.edu/fm/internal_files/contract-awards/se-370-noi-signed.pdf",
                   "https://x.edu/SE-212_H09.pdf"]


def test_rows_skip_invitations_and_blank_firms():
    rows = awards.rows_from_extraction(P205, URL, "College of Charleston")
    assert len(rows) == 1
    r = rows[0]
    assert r["firm"] == "IPW Construction Group LLC" and r["amount"] == 305416 and r["pip_number"] == "H15.9689"
    assert r["notice_type"] == "award" and r["posted_date"] == datetime(2026, 7, 15)
    assert len(awards.rows_from_extraction(SHORTLIST, URL, "College of Charleston")) == 2


def test_award_events_one_per_notice_on_the_state_project():
    rows = awards.rows_from_extraction(SHORTLIST, URL, "College of Charleston")
    events = awards.award_events(rows)
    assert len(events) == 1
    e = events[0]
    assert e.project_key == "PIP:H15.9689" and e.stage == "award"
    assert e.title.startswith("Shortlisted: Choate Construction Company, Hill Construction")


class FakeClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.messages = self

    async def create(self, **kw):
        import json
        assert kw["messages"][0]["content"][0]["type"] == "document"
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=json.dumps(self.payloads.pop(0)))])


async def _factory():
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[
            ContractAward.__table__, PipelineDocument.__table__, PipelineProject.__table__, PipelineEventRow.__table__]))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def test_job_reads_each_pdf_once_and_links_to_pipeline(monkeypatch):
    monkeypatch.setattr(awards, "AGENCY_PAGES", [("College of Charleston", "https://charleston.edu/fm/contract-awards.php")])
    monkeypatch.setattr(awards, "_fetch_text", lambda url: '<a href="/a/se-370-p205.pdf">a</a><a href="/a/se-412-p205.pdf">b</a>')
    monkeypatch.setattr(awards, "_fetch_bytes", lambda url: b"%PDF-1.4 fake")
    monkeypatch.setattr(awards, "get_settings", lambda: SimpleNamespace(anthropic_api_key="test"))

    async def go():
        factory = await _factory()
        first = await awards.run_awards_job(factory, FakeClient([P205, SHORTLIST]))
        again = await awards.run_awards_job(factory, FakeClient([]))     # nothing new -> no AI calls
        async with factory() as db:
            rows = (await db.execute(select(ContractAward))).scalars().all()
            events = (await db.execute(select(PipelineEventRow))).scalars().all()
            projects = (await db.execute(select(PipelineProject))).scalars().all()
        return first, again, rows, events, projects
    first, again, rows, events, projects = asyncio.run(go())
    assert first["new_documents"] == 2 and first["rows"] == 3 and again["new_documents"] == 0
    assert {r.firm for r in rows} == {"IPW Construction Group LLC", "Choate Construction Company", "Hill Construction"}
    assert len(events) == 2 and len(projects) == 1 and projects[0].pip_number == "H15.9689"

    ranked = awards.firm_rankings(rows)
    assert ranked[0]["name"] == "IPW Construction Group LLC" and ranked[0]["awards"] == 1
    assert {f["name"]: f["shortlists"] for f in ranked}["Choate Construction Company"] == 1


def test_branch_suffixes_and_agency_spellings_merge():
    assert awards.firm_key("McMillan Pazdan Smith Architecture - Charleston") == awards.firm_key("McMillan, Pazdan, Smith Architecture")
    assert awards.firm_key("Hill Construction Services of Charleston, Inc.") == awards.firm_key("Hill Construction Services")
    assert awards.firm_key("J MUSSELMAN") == awards.firm_key("J. Musselman Construction, Inc.")
    assert awards.firm_key("Coastal Charleston Builders") != awards.firm_key("Coastal Builders")
    assert awards.agency_name("COLLEGE OF CHARLESTON, Facility Management") == "College of Charleston"
    assert awards.agency_name("Citadel") == "The Citadel"


def test_a_second_run_waits_its_turn(monkeypatch):
    from contextlib import asynccontextmanager
    import app.services.parcel_analysis as pa

    @asynccontextmanager
    async def busy(session_factory, key=0):
        yield False
    monkeypatch.setattr(pa, "_single_runner_lock", busy)
    monkeypatch.setattr(awards, "get_settings", lambda: SimpleNamespace(anthropic_api_key="test"))
    out = asyncio.run(awards.run_awards_job(object(), FakeClient([])))
    assert out["skipped"] == "another run is in progress"
