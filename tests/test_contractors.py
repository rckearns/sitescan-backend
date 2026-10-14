"""Contractors tab: one contractor's permits, and the GC leaderboard."""
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


def _p(contractor, value, category="commercial", days_ago=30, **kw):
    return SimpleNamespace(id=kw.get("id", 1), title=kw.get("title", "Job"), address="1 Main", location="Charleston",
                           value=value, category=category, posted_date=NOW - timedelta(days=days_ago),
                           permit_number="BP1", latitude=None, longitude=None, source_id="charleston-permits",
                           source_url="", contractor=contractor)


def test_leaderboard_ranks_gcs_and_skips_trades_residential_small_and_old():
    since = NOW - timedelta(days=730)
    jobs = [
        _p("Choate Construction Company", 30e6, "healthcare"),
        _p("Choate Construction Co.", 10e6, "education"),
        _p("Hill Construction", 25e6, "office"),
        _p("Lowcountry Roofing LLC", 5e6),               # specialty trade
        _p("Big Homes Inc", 9e6, "residential"),         # residential
        _p("Hill Construction", 500_000),                # under the floor
        _p("Hill Construction", 50e6, days_ago=1000),    # outside the window
    ]
    out = cm.gc_leaderboard_from(jobs, min_value=1e6, since=since)
    assert [c["name"] for c in out] == ["Choate Construction Company", "Hill Construction"]
    choate = out[0]
    assert choate["job_count"] == 2 and choate["total_value"] == 40e6 and choate["median_value"] == 20e6
    assert choate["categories"] == {"healthcare": 1, "education": 1}
    assert choate["top_jobs"][0]["value"] == 30e6


async def _session_with(projects):
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[Project.__table__]))
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        for i, (contractor, value, days) in enumerate(projects):
            s.add(Project(source_id="charleston-permits", external_id=str(i), title=f"Job {i}",
                          contractor=contractor, value=value, posted_date=NOW - timedelta(days=days)))
        await s.commit()
    return factory


def test_contractor_permits_match_normalized_names_newest_first():
    async def go():
        factory = await _session_with([
            ("Choate Construction Company", 30e6, 100),
            ("Electric Co | Choate Construction Co.", 5e6, 10),
            ("Choateville Builders", 1e6, 5),
        ])
        key = cm.normalize_name("Choate Construction Company")
        async with factory() as s:
            return await cm.contractor_permits(key, session=s)
    out = asyncio.run(go())
    assert out["permit_count"] == 2 and out["total_value"] == 35e6
    assert [p["value"] for p in out["permits"]] == [5e6, 30e6]


def _job(id, contractor, value, category, days_ago, parcel=None, address="10 King St"):
    return SimpleNamespace(id=id, contractor=contractor, value=value, category=category, address=address,
                           posted_date=NOW - timedelta(days=days_ago),
                           raw_data={"MAIN_PARCEL_NUMBER": parcel} if parcel else {})


def test_subs_link_by_parcel_and_window():
    gc = _job(1, "Choate Construction Company", 30e6, "healthcare", 400, parcel="C4570000001")
    other_site = _job(2, "Hill Construction", 20e6, "office", 400, parcel="C4570000002")
    trades = [
        _job(10, "Sparky Electric LLC", 1, "electrical", 300, parcel="C4570000001"),          # in window
        _job(11, "Sparky Electric LLC", 1, "electrical", 200, parcel="C4570000001"),          # same job again
        _job(12, "Cool Air Mechanical", 1, "mechanical", 350, parcel="C4570000001"),
        _job(13, "Old Plumbing Co", 1, "plumbing", 600, parcel="C4570000001"),                # before window
        _job(14, "Sparky Electric LLC", 1, "electrical", 300, parcel="C4570000002"),          # Hill's job
        _job(15, "Choate Construction Company", 1, "electrical", 300, parcel="C4570000001"),  # the GC itself
    ]
    out = cm.aggregate_subs(cm.link_subs([gc, other_site], trades))
    assert list(out) == ["electrical", "mechanical"]
    sparky = out["electrical"]["subs"][0]
    assert sparky["name"] == "Sparky Electric LLC" and sparky["job_count"] == 2
    assert sparky["gcs"] == ["Choate Construction Company", "Hill Construction"]
    assert out["mechanical"]["subs"][0]["job_count"] == 1


def test_subs_fall_back_to_address_without_parcel():
    gc = _job(1, "Choate Construction Company", 30e6, "healthcare", 100, address="573 MEETING ST")
    t = _job(10, "Sparky Electric LLC", 1, "electrical", 50, address="573 Meeting St MASTER")
    assert len(cm.link_subs([gc], [t])) == 1


def test_leaderboard_category_filter_and_trade_permits_excluded():
    jobs = [_p("Choate Construction Company", 30e6, "healthcare"), _p("Hill Construction", 25e6, "office"),
            _p("Sparky Electric", 5e6, "electrical")]
    out = cm.gc_leaderboard_from(jobs, min_value=1e6, since=None, categories={"healthcare"})
    assert [c["name"] for c in out] == ["Choate Construction Company"]
    assert [c["name"] for c in cm.gc_leaderboard_from(jobs, min_value=1e6, since=None)] == [
        "Choate Construction Company", "Hill Construction"]
