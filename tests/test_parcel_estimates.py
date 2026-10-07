"""Tests for parcel scoring/ranking, analysis versioning, and the nightly job.

Uses an in-memory SQLite DB and a mocked Anthropic client — never calls the
real API or a real database. Run: python3 -m pytest tests/
"""
import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.models.database import Base, ParcelAnalysis  # noqa: E402
from app.services import parcel_analysis as pa  # noqa: E402
from app.services.parcels import opportunity_score, rank_parcels  # noqa: E402


def _load_analyze_router():
    # Load the module file directly: importing the app.routers package pulls in
    # every router, some of which need Python 3.10+ syntax.
    spec = importlib.util.spec_from_file_location("analyze_under_test", ROOT / "app/routers/analyze.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(coro):
    return asyncio.run(coro)


async def _make_factory():
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool,
                                 connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[ParcelAnalysis.__table__]))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _ai_json(land=500_000, hard=2_000_000, soft=300_000, total=None, value=4_000_000, with_land=True):
    pf = {"estimated_hard_cost": hard, "soft_costs": soft,
          "total_development_cost": total if total is not None else land + hard + soft,
          "stabilized_noi": 300_000, "cap_rate": 7.0, "projected_value": value, "profit_margin": "20%"}
    if with_land:
        pf["land_cost"] = land
    return json.dumps({"summary": "s", "location_context": "l",
                       "scenarios": [{"name": "A", "use_type": "Retail", "description": "d", "proforma": pf}],
                       "recommended_scenario": "A", "next_steps": ["x"]})


class FakeClient:
    """Mimics AsyncAnthropic().messages.create; tracks calls and peak concurrency."""

    def __init__(self, text=None, delay=0.01):
        self.text = text or _ai_json()
        self.delay = delay
        self.calls = []
        self.active = 0
        self.peak = 0
        self.messages = self

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(self.delay)
        self.active -= 1
        return SimpleNamespace(content=[SimpleNamespace(text=self.text)])


# ─── scoring / ranking ───────────────────────────────────────────────────────

@pytest.mark.parametrize("props,expected", [
    ({"GENUSE": "Commercial - UNDEVELOPABLE", "LAND_APPR": 100, "IMP_APPR": 0}, 3),
    ({"GENUSE": "Commercial"}, 50),
    ({"LAND_APPR": 0, "IMP_APPR": 0}, 50),
    ({"LAND_APPR": 100000, "IMP_APPR": 0}, 95),
    ({"LAND_APPR": None, "IMP_APPR": 50000}, 15),
    ({"LAND_APPR": 750000, "IMP_APPR": 250000}, 75),
    ({"LAND_APPR": "300000", "IMP_APPR": "100000"}, 75),   # strings parse like parseFloat
    ({"LAND_APPR": 1, "IMP_APPR": 999}, 3),                 # floor of 3
    ({"LAND_APPR": 1, "IMP_APPR": 7}, 13),                  # 12.5 -> JS Math.round -> 13 (Python round -> 12)
    ({"LAND_APPR": "abc", "IMP_APPR": 10}, 15),
])
def test_opportunity_score(props, expected):
    assert opportunity_score(props) == expected


def test_rank_parcels_filters_and_orders():
    feats = [
        {"properties": {"TMS": "low", "LAND_APPR": 100, "IMP_APPR": 900}},           # 10 -> dropped
        {"properties": {"TMS": "mid", "LAND_APPR": 600, "IMP_APPR": 400}},           # 60
        {"properties": {"TMS": "vac_small", "LAND_APPR": 1000, "IMP_APPR": 0}},      # 95
        {"properties": {"TMS": "vac_big", "LAND_APPR": 9000, "IMP_APPR": 0}},        # 95
        {"properties": {"TMS": "unknown", "APPRVAL": 5000}},                         # 50 -> dropped
        {"properties": {"TMS": "edge", "LAND_APPR": 55, "IMP_APPR": 45}},            # 55 -> kept
        {"properties": {"TMS": "undev", "GENUSE": "undevelopable", "LAND_APPR": 1}}, # 3
    ]
    assert [p["TMS"] for p in rank_parcels(feats)] == ["vac_big", "vac_small", "mid", "edge"]


# ─── versioning / normalization ──────────────────────────────────────────────

def test_is_current():
    assert pa.is_current({"version": pa.ANALYSIS_VERSION})
    assert not pa.is_current({"summary": "v1 row, no version"})
    assert not pa.is_current({"version": 1})
    assert not pa.is_current(None)


def test_normalize_adds_land_and_fixes_total():
    a = json.loads(_ai_json(hard=2_000_000, soft=300_000, total=2_300_000, value=4_000_000, with_land=False))
    out = pa.normalize_analysis(a, 700_000)
    pf = out["scenarios"][0]["proforma"]
    assert out["version"] == pa.ANALYSIS_VERSION and out["land_basis"] == 700_000
    assert pf["land_cost"] == 700_000
    assert pf["total_development_cost"] == 3_000_000
    assert pf["profit_margin"] == "33%"


def test_normalize_keeps_consistent_total():
    a = json.loads(_ai_json(land=500_000))
    pf = pa.normalize_analysis(a, 500_000)["scenarios"][0]["proforma"]
    assert pf["total_development_cost"] == 2_800_000 and pf["profit_margin"] == "20%"


def test_land_basis_and_prompt():
    assert pa.land_basis({"LAND_APPR": 250000, "APPRVAL": 900000}) == 250000
    assert pa.land_basis({"APPRVAL": 900000}) == 900000
    assert "Land Acquisition Basis: $250,000" in pa.build_prompt({"TMS": "1", "LAND_APPR": 250000})
    assert "land_cost" in pa.SYSTEM_PROMPT and "MUST include land" in pa.SYSTEM_PROMPT


def test_generate_analysis_with_fenced_json():
    client = FakeClient(text="```json\n" + _ai_json() + "\n```")
    out = run(pa.generate_analysis({"TMS": "t", "LAND_APPR": 500000}, "t", client=client))
    assert out["version"] == pa.ANALYSIS_VERSION
    assert client.calls[0]["model"] == pa.MODEL


def test_generate_analysis_invalid_json():
    with pytest.raises(pa.AnalysisInvalidJSON):
        run(pa.generate_analysis({"TMS": "t"}, "t", client=FakeClient(text="not json")))


# ─── save / staleness ────────────────────────────────────────────────────────

def test_save_overwrites_stale_row_in_place():
    async def go():
        sf = await _make_factory()
        async with sf() as s:
            s.add(ParcelAnalysis(tms="T1", parcel_data={}, analysis={"summary": "old v1"}))
            await s.commit()
        new = {"summary": "new", "version": pa.ANALYSIS_VERSION}
        async with sf() as s:
            stored = await pa.save_analysis(s, "T1", {"TMS": "T1"}, new)
            await s.commit()
        async with sf() as s:
            rows = (await s.execute(select(ParcelAnalysis))).scalars().all()
        return stored, rows
    stored, rows = run(go())
    assert len(rows) == 1 and rows[0].id == 1
    assert rows[0].analysis["summary"] == "new" and stored["summary"] == "new"


def test_save_tolerates_unique_race(monkeypatch):
    """Another writer inserted the TMS between our read and our insert."""
    async def go(existing_analysis):
        sf = await _make_factory()
        async with sf() as s:
            s.add(ParcelAnalysis(tms="T1", parcel_data={}, analysis=existing_analysis))
            await s.commit()
        real = pa.get_analysis_row
        calls = {"n": 0}

        async def first_miss(db, tms):
            calls["n"] += 1
            return None if calls["n"] == 1 else await real(db, tms)
        monkeypatch.setattr(pa, "get_analysis_row", first_miss)
        mine = {"summary": "mine", "version": pa.ANALYSIS_VERSION}
        async with sf() as s:
            stored = await pa.save_analysis(s, "T1", {}, mine)
            await s.commit()
        monkeypatch.setattr(pa, "get_analysis_row", real)
        async with sf() as s:
            rows = (await s.execute(select(ParcelAnalysis))).scalars().all()
        return stored, rows

    winner = {"summary": "winner", "version": pa.ANALYSIS_VERSION}
    stored, rows = run(go(winner))
    assert len(rows) == 1 and stored["summary"] == "winner" and rows[0].analysis["summary"] == "winner"

    stored, rows = run(go({"summary": "stale winner"}))
    assert len(rows) == 1 and stored["summary"] == "mine" and rows[0].analysis["summary"] == "mine"


# ─── endpoints (called directly, no HTTP) ────────────────────────────────────

def test_endpoints_regenerate_stale_and_hide_stale_from_cache(monkeypatch):
    analyze = _load_analyze_router()
    monkeypatch.setattr(analyze, "get_settings", lambda: SimpleNamespace(anthropic_api_key="test"))
    gen_calls = []

    async def fake_generate(parcel, tms, api_key=None, client=None):
        gen_calls.append(tms)
        return pa.normalize_analysis(json.loads(_ai_json()), pa.land_basis(parcel))
    monkeypatch.setattr(analyze, "generate_analysis", fake_generate)

    async def go():
        sf = await _make_factory()
        async with sf() as s:
            s.add(ParcelAnalysis(tms="STALE", parcel_data={}, analysis={"summary": "v1"}))
            s.add(ParcelAnalysis(tms="FRESH", parcel_data={}, analysis={"summary": "v2", "version": pa.ANALYSIS_VERSION}))
            await s.commit()
        async with sf() as s:
            cached = await analyze.cached_parcel_analyses(tms="STALE,FRESH,NONE", user=None, db=s)
        async with sf() as s:
            fresh = await analyze.analyze_parcel("FRESH", analyze.ParcelPayload(parcel={"TMS": "FRESH"}), user=None, db=s)
        async with sf() as s:
            regen = await analyze.analyze_parcel("STALE", analyze.ParcelPayload(parcel={"TMS": "STALE", "LAND_APPR": 500000}), user=None, db=s)
            await s.commit()
        async with sf() as s:
            n = (await s.execute(select(func.count()).select_from(ParcelAnalysis))).scalar()
            cached_after = await analyze.cached_parcel_analyses(tms="STALE,FRESH", user=None, db=s)
        return cached, fresh, regen, n, cached_after

    cached, fresh, regen, n, cached_after = run(go())
    assert set(cached["analyses"]) == {"FRESH"}
    assert fresh["cached"] is True and gen_calls == ["STALE"]
    assert regen["cached"] is False and regen["analysis"]["version"] == pa.ANALYSIS_VERSION
    assert regen["analysis"]["scenarios"][0]["proforma"]["land_cost"] == 500000
    assert n == 2
    assert set(cached_after["analyses"]) == {"STALE", "FRESH"}


# ─── nightly job ─────────────────────────────────────────────────────────────

def _features(n):
    # Distinct land values so ranking is deterministic: P0 has the highest.
    return [{"properties": {"TMS": f"P{i}", "LAND_APPR": 1_000_000 - i, "IMP_APPR": 0}} for i in range(n)]


def test_job_generates_top_n_missing_with_bounded_concurrency(monkeypatch):
    monkeypatch.setattr(pa, "get_settings", lambda: SimpleNamespace(anthropic_api_key="test"))
    client = FakeClient()

    async def go():
        sf = await _make_factory()
        async with sf() as s:
            s.add(ParcelAnalysis(tms="P0", parcel_data={}, analysis={"version": pa.ANALYSIS_VERSION}))  # current
            s.add(ParcelAnalysis(tms="P1", parcel_data={}, analysis={"summary": "v1"}))                # stale
            await s.commit()

        async def fetch():
            return _features(60) + [{"properties": {"TMS": "LOW", "LAND_APPR": 1, "IMP_APPR": 99}}]

        summary = await pa.run_parcel_estimates_job(top_n=10, concurrency=3, fetch_features=fetch,
                                                    client=client, session_factory=sf)
        async with sf() as s:
            rows = {r.tms: r.analysis for r in (await s.execute(select(ParcelAnalysis))).scalars().all()}
        return summary, rows

    summary, rows = run(go())
    assert summary["fetched"] == 61 and summary["ranked"] == 60 and summary["top"] == 10
    assert summary["already_current"] == 1 and summary["generated"] == 9 and summary["failed"] == 0
    assert len(client.calls) == 9 and client.peak <= 3
    assert set(rows) == {f"P{i}" for i in range(10)}
    assert all(pa.is_current(a) for a in rows.values())


def test_job_skips_without_api_key(monkeypatch):
    monkeypatch.setattr(pa, "get_settings", lambda: SimpleNamespace(anthropic_api_key=""))

    async def fetch():
        raise AssertionError("should not fetch parcels without an API key")
    summary = run(pa.run_parcel_estimates_job(fetch_features=fetch, session_factory=object()))
    assert summary["skipped"] == "no_api_key"


def test_job_counts_failures_and_never_raises(monkeypatch):
    monkeypatch.setattr(pa, "get_settings", lambda: SimpleNamespace(anthropic_api_key="test"))

    async def go():
        sf = await _make_factory()

        async def fetch():
            return _features(4)
        return await pa.run_parcel_estimates_job(top_n=4, fetch_features=fetch,
                                                 client=FakeClient(text="garbage"), session_factory=sf)
    summary = run(go())
    assert summary["failed"] == 4 and summary["generated"] == 0
