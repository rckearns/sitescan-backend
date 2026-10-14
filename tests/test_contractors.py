"""Contractors tab: jobs (phase permits grouped), GC detection, leaderboard, subs."""
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.models.database import Base, Project  # noqa: E402
from app.services import contractor_match as cm  # noqa: E402

NOW = datetime(2026, 10, 1)
_ids = iter(range(1, 10_000))


def _p(contractor, value, category="commercial", days_ago=30, parcel=None, address=None, status="Issued", **kw):
    pid = next(_ids)
    return SimpleNamespace(
        id=pid, title=kw.get("title", f"Job {pid}"), address=address or f"{pid} Main St", location="Charleston",
        value=value, category=category, posted_date=NOW - timedelta(days=days_ago), permit_number=f"BP{pid}",
        latitude=None, longitude=None, source_id="charleston-permits", source_url="", contractor=contractor,
        status=status, raw_data={"MAIN_PARCEL_NUMBER": parcel} if parcel else {})


LLR = cm.LlrIndex({   # keys normalized the same way load_llr_index does
    cm.normalize_name("B L Harbert International LLC"): [{"classification": "BD"}],
    cm.normalize_name("Reks Contractors Inc"): [{"classification": "EL"}],
})


def test_phase_permits_are_one_job_valued_once():
    phases = [_p("Carter & Carter Construction", 78e6, "multi-family", parcel="C1") for _ in range(10)]
    phases.append(_p("Carter & Carter Construction", 0, "multi-family", parcel="C1"))
    jobs = cm.group_jobs(phases)
    assert len(jobs) == 1 and jobs[0].value == 78e6 and len(jobs[0].permits) == 11
    later = _p("Carter & Carter Construction", 5e6, parcel="C1", days_ago=-500)   # a new job on the site later
    assert len(cm.group_jobs(phases + [later])) == 2


def test_gc_detection():
    assert cm.is_gc("Choate Construction Company", [])
    assert cm.is_gc("B L Harbert International LLC", LLR.match(cm.normalize_name("B L Harbert International LLC")))
    assert not cm.is_gc("Mt. Pleasant Radio Co, Inc.", [])
    assert not cm.is_gc("VSC Fire & Security", [])
    assert not cm.is_gc("Reks Contractors Inc", LLR.match(cm.normalize_name("Reks Contractors Inc")))   # licensed electrician
    assert not cm.is_gc("Lowcountry Roofing LLC", [])


def test_leaderboard_counts_jobs_not_phase_permits_and_skips_non_gcs():
    jobs = [_p("Carter & Carter Construction | B & C Utilities, Inc.", 78e6, "multi-family", parcel="C1")
            for _ in range(5)]
    jobs += [
        _p("Choate Construction Company | Mt. Pleasant Radio Co | A+ Hoodsystems", 30e6, "healthcare", parcel="C2"),
        _p("Choate Construction Co.", 10e6, "education", parcel="C3"),
        _p("Big Homes Construction", 9e6, "residential", parcel="C4"),
        _p("Hill Construction", 500_000, parcel="C5"),
        _p("Hill Construction", 50e6, parcel="C6", days_ago=1000),
    ]
    out = cm.gc_leaderboard_from(jobs, min_value=1e6, since=NOW - timedelta(days=730), llr=LLR)
    assert [c["name"] for c in out] == ["Carter & Carter Construction", "Choate Construction Company"]
    assert out[0]["job_count"] == 1 and out[0]["total_value"] == 78e6
    assert out[1]["job_count"] == 2 and out[1]["median_value"] == 20e6
    only_health = cm.gc_leaderboard_from(jobs, min_value=1e6, since=None, categories={"healthcare"}, llr=LLR)
    assert [c["name"] for c in only_health] == ["Choate Construction Company"]


def test_subs_from_trade_permits_and_co_listed_names():
    job = _p("Choate Construction Company | VSC Fire & Security", 30e6, "healthcare", parcel="C9", days_ago=400)
    trades = [
        _p("Reks Contractors Inc", 1, "electrical", parcel="C9", days_ago=300),
        _p("Reks Contractors Inc", 1, "electrical", parcel="C9", days_ago=200),
        _p("Cool Air Mechanical", 1, "mechanical", parcel="C9", days_ago=350),
        _p("Old Plumbing Co", 1, "plumbing", parcel="C9", days_ago=600),          # before the window
        _p("Choate Construction Company", 1, "electrical", parcel="C9", days_ago=300),  # the GC itself
    ]
    out = cm.aggregate_subs(cm.link_subs(cm.group_jobs([job]), trades, LLR), LLR)
    assert set(out) == {"electrical", "mechanical", "fire-sprinkler"}
    reks = out["electrical"]["subs"][0]
    assert reks["name"] == "Reks Contractors Inc" and reks["job_count"] == 1
    assert reks["gcs"] == ["Choate Construction Company"]
    assert out["fire-sprinkler"]["subs"][0]["name"] == "VSC Fire & Security"


def test_address_fallback_groups_floors_of_one_building():
    a = _p("Choate Construction Company", 30e6, address="1010 CONSERVATION PL Stage /Conser Flr1")
    b = _p("Choate Construction Company", 30e6, address="1010 CONSERVATION PL Garage")
    assert len(cm.group_jobs([a, b])) == 1


def test_roster_values_jobs_and_counts_open_work():
    projects = [_p("Reks Contractors Inc", 2e6, "electrical", parcel="C1") for _ in range(3)]
    projects.append(_p("Reks Contractors Inc", 1e6, "electrical", parcel="C2", status="Finaled"))
    out = cm.roster_from(projects, LLR, now=NOW)
    reks = out["trades"]["electrical"]["contractors"][0]
    assert reks["job_count"] == 2 and reks["permit_count"] == 4
    assert reks["total_permit_value"] == 3e6 and reks["open_jobs"] == 1


async def _session_with(projects):
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[Project.__table__]))
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        for i, (contractor, value, days, address) in enumerate(projects):
            s.add(Project(source_id="charleston-permits", external_id=str(i), title=f"Job {i}", address=address,
                          contractor=contractor, value=value, posted_date=NOW - timedelta(days=days), status="Issued"))
        await s.commit()
    return factory


def test_contractor_permits_groups_jobs_newest_first():
    async def go():
        factory = await _session_with([
            ("Choate Construction Company", 30e6, 100, "171 ASHLEY AVE"),
            ("Choate Construction Co.", 30e6, 95, "171 ASHLEY AVE MASTER"),
            ("Electric Co | Choate Construction Co.", 5e6, 10, "66 GEORGE ST"),
            ("Choateville Builders", 1e6, 5, "1 KING ST"),
        ])
        key = cm.normalize_name("Choate Construction Company")
        async with factory() as s:
            return await cm.contractor_permits(key, session=s)
    out = asyncio.run(go())
    assert out["job_count"] == 2 and out["permit_count"] == 3 and out["total_value"] == 35e6
    assert [p["value"] for p in out["permits"]] == [5e6, 30e6]


def test_out_of_town_gc_without_license_is_the_gc_when_no_one_else_is():
    job = _p("B L Harbert International | VSC Fire & Security | Hallway Plumbing LLC", 60e6, "hotel", parcel="H1")
    out = cm.gc_leaderboard_from([job], min_value=1e6, since=None, llr=cm.LlrIndex({}))
    assert [c["name"] for c in out] == ["B L Harbert International"]
    subs = cm.aggregate_subs(cm.link_subs(cm.group_jobs([job]), [], cm.LlrIndex({})), cm.LlrIndex({}))
    assert set(subs) == {"fire-sprinkler", "plumbing"}
    assert subs["plumbing"]["subs"][0]["gcs"] == ["B L Harbert International"]
    with_gc = _p("Choate Construction Company | Scout Services", 10e6, parcel="H2")
    assert [c["name"] for c in cm.gc_leaderboard_from([with_gc], min_value=1e6, since=None)] == ["Choate Construction Company"]


def test_roster_puts_fire_and_controls_companies_in_their_trades():
    projects = [_p("VSC Fire & Security", 1e6, "commercial"), _p("Johnson Controls, Inc.", 1e6, "commercial")]
    out = cm.roster_from(projects, cm.LlrIndex({}), now=NOW)
    assert "general" not in out["trades"]
    assert out["trades"]["fire-protection"]["contractors"][0]["name"] == "VSC Fire & Security"
