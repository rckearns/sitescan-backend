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


def _scenario(name="A", gsf=40_000, psf=300, soft=0.25, noi=1_500_000, cap=7.0, fit="by_right"):
    return {"name": name, "use_type": "Retail", "description": "d", "zoning_fit": fit, "zoning_notes": "z",
            "assumptions": {"gross_sf": gsf, "units": None, "unit_label": None, "hard_cost_psf": psf,
                            "soft_cost_pct": soft, "stabilized_noi": noi, "noi_basis": "b", "cap_rate": cap}}


def _ai_json(*scenarios):
    return json.dumps({"summary": "s", "location_context": "l",
                       "scenarios": list(scenarios) or [_scenario()], "next_steps": ["x"]})


async def _no_zoning(parcel, tms):
    return None


@pytest.fixture(autouse=True)
def _stub_zoning(monkeypatch):
    """Never call the City zoning service from tests."""
    monkeypatch.setattr(pa, "lookup_parcel_zoning", _no_zoning)


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
    ({"LAND_APPR": 100000, "IMP_APPR": 0}, 100),            # vacant ranks with the best, not below them
    ({"LAND_APPR": 3631600, "IMP_APPR": 65400}, 98),        # 483 Meeting St
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
        {"properties": {"TMS": "vac_small", "LAND_APPR": 1000, "IMP_APPR": 0}},      # 100
        {"properties": {"TMS": "vac_big", "LAND_APPR": 9000, "IMP_APPR": 0}},        # 100
        {"properties": {"TMS": "shed", "LAND_APPR": 99999, "IMP_APPR": 1}},          # 100, most land
        {"properties": {"TMS": "unknown", "APPRVAL": 5000}},                         # 50 -> dropped
        {"properties": {"TMS": "edge", "LAND_APPR": 55, "IMP_APPR": 45}},            # 55 -> kept
        {"properties": {"TMS": "undev", "GENUSE": "undevelopable", "LAND_APPR": 1}}, # 3
    ]
    assert [p["TMS"] for p in rank_parcels(feats)] == ["shed", "vac_big", "vac_small", "mid", "edge"]


# ─── versioning / normalization ──────────────────────────────────────────────

def test_is_current():
    assert pa.is_current({"version": pa.ANALYSIS_VERSION})
    assert not pa.is_current({"summary": "v1 row, no version"})
    assert not pa.is_current({"version": 1})
    assert not pa.is_current(None)


def test_proforma_is_computed_not_trusted():
    # 483 Meeting St as the AI first described it: 70 keys, $12.0M hard, $1.8M soft, $1.35M NOI at 7.8%.
    pf = pa.build_proforma({"gross_sf": 48_000, "hard_cost_psf": 250, "soft_cost_pct": 0.15,
                            "stabilized_noi": 1_350_000, "cap_rate": 7.8}, 3_631_600)
    assert pf["estimated_hard_cost"] == 12_000_000 and pf["soft_costs"] == 1_800_000
    assert pf["total_development_cost"] == 17_431_600
    assert pf["projected_value"] == 17_307_692
    assert pf["profit_margin"] == "-1%" and pf["pencils"] is False
    assert pf["yield_on_cost"] == 7.74


def test_proforma_unit_slips_and_bad_input():
    pf = pa.build_proforma({"gross_sf": 10_000, "hard_cost_psf": 200, "soft_cost_pct": 25,
                            "stabilized_noi": 280_000, "cap_rate": 0.07}, 500_000)
    assert pf["soft_costs"] == 500_000 and pf["cap_rate"] == 7.0      # 25 -> 0.25, 0.07 -> 7.0
    assert pf["total_development_cost"] == 3_000_000 and pf["projected_value"] == 4_000_000
    assert pf["profit_margin"] == "33%" and pf["pencils"] is True
    assert pa.build_proforma({"gross_sf": 0, "hard_cost_psf": 200, "stabilized_noi": 1, "cap_rate": 7}, 1) is None
    assert pa.build_proforma({"gross_sf": "abc"}, 1) is None


def test_finalize_recommends_best_allowed_scenario():
    a = json.loads(_ai_json(
        _scenario("Hotel", noi=3_000_000, fit="not_allowed"),     # best numbers but zoning says no
        _scenario("Retail", noi=1_500_000),                        # 40k*300*1.25=15M + 1M land = 16M; 21.4M value
        _scenario("Office", noi=1_200_000, fit="needs_approval"),
        {"name": "Broken", "assumptions": {}},                     # dropped: unusable assumptions
    ))
    out = pa.finalize_analysis(a, 1_000_000, {"base_zoning": "MU-2/WH"})
    assert [s["name"] for s in out["scenarios"]] == ["Hotel", "Retail", "Office"]
    assert out["recommended_scenario"] == "Retail"
    assert out["pencils"] is True and out["zoning"] == {"base_zoning": "MU-2/WH"}
    assert out["version"] == pa.ANALYSIS_VERSION and out["land_basis"] == 1_000_000
    assert out["scenarios"][1]["proforma"]["land_cost"] == 1_000_000


def test_finalize_flags_when_nothing_pencils():
    out = pa.finalize_analysis(json.loads(_ai_json(_scenario("Thin", noi=1_000_000))), 1_000_000)
    assert out["recommended_scenario"] == "Thin" and out["pencils"] is False


def test_finalize_unknown_zoning_fit_defaults_to_needs_approval():
    out = pa.finalize_analysis(json.loads(_ai_json(_scenario(fit="maybe?"))), 1)
    assert out["scenarios"][0]["zoning_fit"] == "needs_approval"


def test_land_basis_and_prompt():
    assert pa.land_basis({"LAND_APPR": 250000, "APPRVAL": 900000}) == 250000
    assert pa.land_basis({"APPRVAL": 900000}) == 900000
    prompt = pa.build_prompt({"TMS": "1", "LAND_APPR": 250000, "GISACRES": 0.43}, {
        "base_zoning": "MU-2/WH", "height_district": "8", "height_type": "Story",
        "accommodations_overlay": "A-1", "old_and_historic": True, "area": "Peninsula"})
    assert "$250,000 as the land cost" in prompt and "18,731 SF" in prompt
    assert "Base zoning: MU-2/WH" in prompt and "8 stories maximum" in prompt
    assert "Accommodations Overlay: A-1" in prompt and "Board of Architectural Review" in prompt
    assert "not available" in pa.build_prompt({"TMS": "1"}, None)
    assert "Do not compute totals" in pa.SYSTEM_PROMPT and "zoning_fit" in pa.SYSTEM_PROMPT


def test_no_accommodations_overlay_rules_out_hotels():
    from app.services.zoning import describe_zoning
    assert "not_allowed by right" in describe_zoning({"base_zoning": "GB"})


def test_generate_analysis_with_fenced_json():
    client = FakeClient(text="```json\n" + _ai_json() + "\n```")
    seen = []

    async def zoning(parcel, tms):
        seen.append(tms)
        return {"base_zoning": "GB"}
    out = run(pa.generate_analysis({"TMS": "t", "LAND_APPR": 500000}, "t", client=client, zoning_lookup=zoning))
    assert out["version"] == pa.ANALYSIS_VERSION and seen == ["t"]
    assert out["zoning"] == {"base_zoning": "GB"} and out["recommended_scenario"] == "A"
    assert client.calls[0]["model"] == pa.model_name() == out["model"]
    assert "Base zoning: GB" in client.calls[0]["messages"][0]["content"]


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
        return pa.finalize_analysis(json.loads(_ai_json()), pa.land_basis(parcel))
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
