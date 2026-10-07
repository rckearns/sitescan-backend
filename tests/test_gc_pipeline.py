"""GC pipeline core: storing/linking events, rollups, classification, matching, job, API.

In-memory SQLite and a mocked Anthropic client — no network, no real database.
"""
import asyncio
import importlib.util
import json
import sys
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.models.database import Base, PipelineEventRow, PipelineProject  # noqa: E402
from app.services.pipeline import job as pjob  # noqa: E402
from app.services.pipeline.classify import CLASSIFY_VERSION, apply_classification, build_input  # noqa: E402
from app.services.pipeline.events import PipelineEvent, normalize_pip, pip_project_key  # noqa: E402
from app.services.pipeline.match import match_project  # noqa: E402
from app.services.pipeline.store import store_events, street_pattern  # noqa: E402


def run(coro):
    return asyncio.run(coro)


async def _factory():
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool,
                                 connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(
            c, tables=[PipelineProject.__table__, PipelineEventRow.__table__]))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class FakeClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []
        self.messages = self

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(self.payload))])


def p205_events():
    pip = normalize_pip("H15.9689")
    key = pip_project_key(pip)
    return [
        PipelineEvent(source="sfaa", external_id="sfaa:2024-06-18:H15.9689:phase1", project_key=key,
                      title="St. Philip Housing Innovation District", event_date=date(2024, 6, 18),
                      stage="phase1", owner="College of Charleston", pip_number=pip, estimate=164_800_000,
                      text="Phase I pre-design budget. Site: 106 Coming Street and 99 St. Philip Street."),
        PipelineEvent(source="scbo-ae", external_id="scbo:53461", project_key=key,
                      title="A/E services H15-9689-ML", event_date=date(2024, 11, 19), stage="ae-selection",
                      owner="College of Charleston", pip_number=pip, delivery_method="cmr"),
        PipelineEvent(source="scbo-construction", external_id="scbo:55536", project_key=key,
                      title="CM-R services Project 205", event_date=date(2025, 2, 19),
                      stage="cmr-solicitation", owner="College of Charleston", pip_number=pip,
                      delivery_method="cmr", cost_range=(60_000_000, 100_000_000),
                      deadline=datetime(2025, 3, 11)),
        PipelineEvent(source="jbrc", external_id="jbrc:2025-06-03:H15.9689:phase1", project_key=key,
                      title="Project 205 New Construction", event_date=date(2025, 6, 3), stage="phase1",
                      owner="College of Charleston", pip_number=pip, delivery_method="cmr",
                      estimate=164_800_000),
    ]


def board_event():
    return PipelineEvent(source="board", external_id="board:BAR2026-002703", project_key="BOARD:106 COMING ST",
                         title="New construction student housing", event_date=date(2026, 7, 8),
                         stage="board-concept", owner="College of Charleston",
                         address="106 Coming Street", location="Charleston")


# ─── store / link / rollup ───────────────────────────────────────────────────

def test_street_pattern():
    assert street_pattern("106 Coming Street, Charleston") == "106 Coming"
    assert street_pattern("35 Bee St") == "35 Bee"
    assert street_pattern("Coming Street") is None


def test_events_link_into_one_project_with_board_item_by_address():
    async def go():
        f = await _factory()
        async with f() as db:
            changed = await store_events(db, p205_events() + [board_event()])
            await db.commit()
            projects = (await db.execute(select(PipelineProject))).scalars().all()
            events = (await db.execute(select(PipelineEventRow))).scalars().all()
            return changed, projects, events
    changed, projects, events = run(go())
    assert len(projects) == 1 and len(events) == 5
    p = projects[0]
    assert p.project_key == "PIP:H15.9689" and p.pip_number == "H15.9689"
    assert p.title == "Project 205 New Construction"
    assert p.owner == "College of Charleston"
    assert p.delivery_method == "cmr" and p.delivery_basis == "stated"
    assert p.estimate == 164_800_000 and p.estimate_basis == "stated"
    assert p.current_stage == "board-concept"           # latest event (July 2026 BAR)
    assert p.first_event_date == datetime(2024, 6, 18)
    assert p.needs_classification is True


def test_duplicate_events_are_skipped():
    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, p205_events())
            await db.commit()
            again = await store_events(db, p205_events())
            n = len((await db.execute(select(PipelineEventRow))).scalars().all())
            return again, n
    again, n = run(go())
    assert again == set() and n == 4


def test_cmr_wins_over_enabling_hard_bid():
    evs = p205_events()[:1] + [PipelineEvent(
        source="scbo-construction", external_id="scbo:9", project_key="PIP:H15.9689",
        title="Geophysics enabling stage", event_date=date(2026, 7, 1), stage="bid",
        pip_number="H15.9689", delivery_method="design-bid-build")] + p205_events()[2:3]

    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, evs)
            await db.commit()
            return (await db.execute(select(PipelineProject))).scalars().one()
    assert run(go()).delivery_method == "cmr"


# ─── classification ──────────────────────────────────────────────────────────

AI = {"title": "CofC student housing", "owner": "College of Charleston", "building_type": "higher-ed",
      "construction_type": "unknown", "construction_reason": "Mid-rise housing; structure not stated.",
      "delivery_method": "design-bid-build", "delivery_basis": "inferred",
      "estimated_construction_value": 90_000_000, "city": "Charleston",
      "in_charleston_area": True, "summary": "1,200-bed student housing on Coming St."}


def test_ai_does_not_override_stated_facts():
    p = PipelineProject(project_key="PIP:H15.9689", pip_number="H15.9689", title="Project 205 New Construction",
                        delivery_method="cmr", delivery_basis="stated",
                        estimate=164_800_000, estimate_basis="stated")
    apply_classification(p, AI)
    assert p.delivery_method == "cmr" and p.estimate == 164_800_000
    assert p.title == "Project 205 New Construction"     # official state name kept
    assert p.building_type == "higher-ed" and p.construction_type == "unknown"
    assert p.ai_version == CLASSIFY_VERSION and p.needs_classification is False


def test_ai_fills_unknowns_and_never_claims_stated():
    p = PipelineProject(project_key="BOARD:1 X ST", delivery_basis="unknown", estimate_basis="unknown")
    apply_classification(p, {**AI, "delivery_method": "cmr", "delivery_basis": "stated"})
    assert p.delivery_method == "cmr" and p.delivery_basis == "inferred"
    assert p.estimate == 90_000_000 and p.estimate_basis == "inferred"
    assert p.title == "CofC student housing"


def test_ai_garbage_values_fall_back():
    p = PipelineProject(project_key="BOARD:2 Y ST", delivery_basis="unknown", estimate_basis="unknown")
    apply_classification(p, {"building_type": "spaceport", "construction_type": "cardboard",
                             "delivery_method": "vibes", "estimated_construction_value": "lots"})
    assert (p.building_type, p.construction_type, p.delivery_method, p.estimate) == ("other", "unknown", "", None)


def test_build_input_is_bounded_and_dated():
    p = PipelineProject(project_key="PIP:H15.9689", owner="College of Charleston")
    evs = [PipelineEventRow(source="jbrc", stage="phase1", title="t", event_date=datetime(2025, 6, 3),
                            text="x" * 10000, delivery_method="cmr", estimate=1e8) for _ in range(5)]
    s = build_input(p, evs)
    assert "2025-06-03" in s and "delivery method stated: cmr" in s
    assert len(s) < 20000


# ─── matching ────────────────────────────────────────────────────────────────

def M(**kw):
    base = dict(delivery_method="cmr", construction_type="non-wood", estimate=5e6,
                building_type="higher-ed", in_charleston_area=True)
    base.update(kw)
    return match_project(**base)


def test_match_rules():
    assert M() == ("match", [])
    assert M(delivery_method="design-bid-build")[0] == "excluded"
    assert M(construction_type="wood")[0] == "excluded"
    assert M(estimate=500_000)[0] == "excluded"
    assert M(in_charleston_area=False)[0] == "excluded"
    status, reasons = M(delivery_method="", construction_type="unknown", estimate=None)
    assert status == "unconfirmed" and len(reasons) == 3
    assert M(project_types=["healthcare"])[0] == "excluded"
    assert M(building_type="other", project_types=["healthcare"])[0] == "unconfirmed"
    assert M(construction_type="wood", exclude_wood_frame=False) == ("match", [])
    assert M(estimate=None, min_value=0) == ("match", [])


def test_project_205_matches_the_example_profile():
    # CM-R stated, $164.8M, construction type unknown (mid-rise housing) -> unconfirmed, not excluded
    status, reasons = M(construction_type="unknown", estimate=164_800_000)
    assert status == "unconfirmed" and reasons == ["Construction type not confirmed"]


# ─── job ─────────────────────────────────────────────────────────────────────

def test_job_stores_links_and_classifies(monkeypatch):
    async def state_src(since):
        assert since == date(2026, 10, 7) - pjob.timedelta(days=830)
        return p205_events()

    async def board_src(since):
        return [board_event()]

    async def broken_src(since):
        raise RuntimeError("site down")

    client = FakeClient(AI)

    async def go():
        f = await _factory()
        summary = await pjob.run_pipeline_job(
            sources=[("jbrc", state_src, None, 830, 60), ("board", board_src, None, 365, 30),
                     ("scbo-ae", broken_src, None, 30, 4)],
            client=client, session_factory=f, today=date(2026, 10, 7))
        async with f() as db:
            p = (await db.execute(select(PipelineProject))).scalars().one()
        return summary, p
    summary, p = run(go())
    assert summary["events"] == 5 and summary["classified"] == 1
    assert summary["sources"]["scbo-ae"].startswith("failed")
    assert p.ai_version == CLASSIFY_VERSION and p.building_type == "higher-ed"
    assert p.delivery_method == "cmr"   # stated, not the AI's design-bid-build
    assert len(client.calls) == 1


def test_job_tolerates_missing_source_module():
    async def go():
        f = await _factory()
        return await pjob.run_pipeline_job(
            sources=[("jbrc", "app.services.pipeline.not_built_yet", "fetch", 1, 1)],
            client=FakeClient(AI), session_factory=f, today=date(2026, 10, 7))
    assert run(go())["sources"]["jbrc"] == "unavailable"


# ─── API ─────────────────────────────────────────────────────────────────────

def _load_router():
    spec = importlib.util.spec_from_file_location("pipeline_router_under_test", ROOT / "app/routers/pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_api_filters_by_user_profile():
    router = _load_router()

    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, p205_events())
            await store_events(db, [PipelineEvent(
                source="scbo-construction", external_id="scbo:77", project_key="SCBO:77", title="Parking lot paving",
                event_date=date(2026, 9, 1), stage="bid", delivery_method="design-bid-build", estimate=300_000)])
            for proj in (await db.execute(select(PipelineProject))).scalars().all():
                apply_classification(proj, {**AI, "construction_type": "non-wood"})
            await db.commit()
        user = SimpleNamespace(gc_delivery_methods=["cmr", "design-build", "qualifications"],
                               gc_exclude_wood_frame=True, gc_min_value=1_000_000,
                               gc_project_types=[], gc_show_unconfirmed=True)
        async with f() as db:
            default = await router.list_pipeline_projects(include_excluded=False, db=db, user=user)
        async with f() as db:
            everything = await router.list_pipeline_projects(include_excluded=True, db=db, user=user)
        return default, everything
    default, everything = run(go())
    assert [p["pip_number"] for p in default["projects"]] == ["H15.9689"]
    assert default["projects"][0]["match"] == "match"
    assert len(default["projects"][0]["events"]) == 4
    assert default["counts"] == {"match": 1, "unconfirmed": 0, "excluded": 1}
    assert len(everything["projects"]) == 2
