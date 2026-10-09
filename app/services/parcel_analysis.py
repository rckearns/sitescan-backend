"""AI highest-and-best-use analyses for parcels — generation, caching, nightly job.

Shared by `POST /analyze/parcel/{tms}` and the scheduled parcel-estimates job.

Analyses carry a `version`. Rows in `parcel_analyses` whose analysis is not the
current ANALYSIS_VERSION are stale: they are regenerated (and overwritten in
place) on the next request or nightly run, and are not served from the cache
endpoint.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Callable, Awaitable, Optional

import anthropic
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models.database import ParcelAnalysis, get_session_factory
from app.services.parcels import fetch_home_parcel_features, rank_parcels, _js_parse_float
from app.services.zoning import describe_zoning, lookup_parcel_zoning
from app.services.ai_text import response_text

logger = logging.getLogger("sitescan.parcel_analysis")

# v1: original schema (total_development_cost excluded land).
# v2: total_development_cost includes land acquisition; proforma.land_cost added.
# v3: the model supplies assumptions only (GSF, $/SF, soft %, NOI, cap rate) and
#     zoning fit; cost, value, profit on cost and the recommendation are computed
#     here. Prompt includes City zoning, height district and overlays.
ANALYSIS_VERSION = 3
DEFAULT_MODEL = "claude-sonnet-5-5"
# Thinking (on by default for Sonnet 5.5) counts toward max_tokens; leave room for it
# plus the JSON answer. 16k is the recommended ceiling for non-streaming requests.
MAX_TOKENS = 16000
ATTEMPTS = 2
# A scenario "pencils" when profit on cost clears this (15%).
PENCIL_THRESHOLD = 0.15
ZONING_FITS = ("by_right", "needs_approval", "not_allowed")

JOB_TOP_N = 50
JOB_CONCURRENCY = 3
# Arbitrary constant key for pg_try_advisory_lock so only one replica runs the job.
_JOB_LOCK_KEY = 7_324_115_002


class AnalysisError(Exception):
    """AI analysis could not be produced."""


class AnalysisInvalidJSON(AnalysisError):
    """The model's response was not valid JSON."""


SYSTEM_PROMPT = """You are a commercial real estate development analyst for Charleston, SC.
Given a parcel's data and its zoning, propose 2-3 realistic development scenarios and give the
underwriting ASSUMPTIONS for each. Do not compute totals, value or profit: the application
calculates the pro forma from your assumptions, so every number must be defensible on its own.
Respond with valid JSON matching exactly this structure:
{
  "summary": "1-2 sentence overview of the opportunity",
  "location_context": "Brief description of the neighborhood/submarket",
  "scenarios": [
    {
      "name": "Scenario name",
      "use_type": "e.g. Boutique Hotel, Mixed-Use Retail/Office, Multifamily",
      "description": "2-3 sentences on why this use fits and market demand",
      "zoning_fit": "by_right | needs_approval | not_allowed",
      "zoning_notes": "One sentence: what the zoning allows for this scenario and what approvals it needs",
      "assumptions": {
        "gross_sf": 0,
        "units": 0,
        "unit_label": "keys | units | null",
        "hard_cost_psf": 0,
        "soft_cost_pct": 0.0,
        "stabilized_noi": 0,
        "noi_basis": "One line showing how NOI was derived, e.g. 70 keys x $285 ADR x 78% occ x 365 = $5.7M rooms rev; 32% NOI margin",
        "cap_rate": 0.0
      }
    }
  ],
  "next_steps": ["step 1", "step 2", "step 3"]
}
Rules:
- Respect the zoning given in the prompt: base zoning, height district (stories) and overlays.
  Size gross_sf to what the lot and height limit can realistically hold. Mark zoning_fit honestly.
- hard_cost_psf: current construction cost per gross SF for this building type in Charleston,
  including structured parking if the scenario needs it.
- soft_cost_pct: soft costs as a decimal fraction of hard cost (e.g. 0.25), covering design, permits,
  fees, financing, FF&E and pre-opening where applicable (hotels typically need more than other uses).
- stabilized_noi: annual NOI at stabilization, consistent with noi_basis.
- cap_rate: market exit cap rate as a percent, e.g. 7.0.
- Use whole dollars (integers) for gross_sf, units, hard_cost_psf and stabilized_noi.
- Be realistic, not optimistic: it is fine for a scenario not to pencil.
Respond with the JSON object only."""


def land_basis(parcel: dict) -> int:
    """Land acquisition basis: appraised land value (LAND_APPR), else total appraisal (APPRVAL)."""
    return int(round(_js_parse_float(parcel.get("LAND_APPR")) or _js_parse_float(parcel.get("APPRVAL"))))


def build_prompt(parcel: dict, zoning: Optional[dict] = None) -> str:
    addr = " ".join(filter(None, [str(parcel.get("HOUSE") or ""), str(parcel.get("STREET") or "")])).strip()
    if not addr:
        addr = "No street address (parcel only)"
    land = float(land_basis(parcel))
    imp = _js_parse_float(parcel.get("IMP_APPR"))
    total = _js_parse_float(parcel.get("APPRVAL")) or (land + imp)
    yr = parcel.get("YRBUILT", "unknown")
    genuse = parcel.get("GENUSE", "General Commercial")
    owner = parcel.get("OWNER", "Unknown")
    tms = parcel.get("TMS") or parcel.get("PARCELID", "")
    acres = _js_parse_float(parcel.get("GISACRES") or parcel.get("LGLACRES"))
    acres_line = f"\nLot Size: {acres:.2f} acres ({acres * 43560:,.0f} SF)" if acres else ""
    city = parcel.get("CITY")
    city_line = f"\nMunicipality: {city}" if city else ""

    return f"""Analyze this Charleston County, SC commercial parcel:

Address: {addr}
TMS: {tms}{city_line}
Current Use: {genuse}
Owner: {owner}
Land Value (assessor): ${land:,.0f}
Improvements Value: ${imp:,.0f}
Total Appraised Value: ${total:,.0f}
Year Built: {yr}{acres_line}
Improvement Ratio: {round(imp / total * 100) if total else 0}% (lower = more opportunity)

{describe_zoning(zoning)}

The pro forma will use ${land:,.0f} as the land cost. Give 2-3 scenarios that fit this zoning."""


def is_current(analysis: Any) -> bool:
    """True if a stored analysis was generated with the current schema/prompt version."""
    return isinstance(analysis, dict) and analysis.get("version") == ANALYSIS_VERSION


def _num(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", "").replace("$", "").replace("%", "").strip())
        except ValueError:
            return None
    return None


def build_proforma(assumptions: dict, land: float) -> Optional[dict]:
    """Deterministic pro forma from the model's assumptions; None if they are unusable."""
    a = assumptions or {}
    gsf, psf = _num(a.get("gross_sf")), _num(a.get("hard_cost_psf"))
    soft_pct, noi, cap = _num(a.get("soft_cost_pct")), _num(a.get("stabilized_noi")), _num(a.get("cap_rate"))
    if not gsf or not psf or noi is None or not cap or gsf <= 0 or psf <= 0 or cap <= 0:
        return None
    if soft_pct is None or soft_pct < 0:
        soft_pct = 0.0
    if soft_pct > 1:           # model gave a percent (25) instead of a fraction (0.25)
        soft_pct /= 100
    if cap < 1:                # model gave a fraction (0.07) instead of a percent (7.0)
        cap *= 100
    hard = gsf * psf
    soft = hard * soft_pct
    total = land + hard + soft
    value = noi / (cap / 100)
    poc = (value - total) / total if total > 0 else None
    yoc = noi / total if total > 0 else None
    return {
        "land_cost": int(round(land)),
        "estimated_hard_cost": int(round(hard)),
        "soft_costs": int(round(soft)),
        "total_development_cost": int(round(total)),
        "stabilized_noi": int(round(noi)),
        "cap_rate": round(cap, 2),
        "projected_value": int(round(value)),
        "profit_on_cost": round(poc, 4) if poc is not None else None,
        "profit_margin": f"{round(poc * 100)}%" if poc is not None else None,
        "yield_on_cost": round(yoc * 100, 2) if yoc is not None else None,
        "pencils": poc is not None and poc >= PENCIL_THRESHOLD,
    }


def finalize_analysis(analysis: dict, basis: int, zoning: Optional[dict] = None) -> dict:
    """Compute every scenario's pro forma, pick the recommendation and stamp the version.

    The recommended scenario is the highest profit on cost among scenarios the
    zoning allows (by right or with approval); if none is allowed, the best overall.
    `pencils` says whether that recommendation clears PENCIL_THRESHOLD.
    """
    scenarios = []
    for s in analysis.get("scenarios") or []:
        if not isinstance(s, dict):
            continue
        fit = str(s.get("zoning_fit") or "").strip().lower().replace(" ", "_")
        s["zoning_fit"] = fit if fit in ZONING_FITS else "needs_approval"
        assumptions = s.get("assumptions")
        pf = build_proforma(assumptions if isinstance(assumptions, dict) else {}, float(basis))
        if pf is None:
            continue
        s["proforma"] = pf
        scenarios.append(s)
    analysis["scenarios"] = scenarios

    def poc(s):
        return s["proforma"]["profit_on_cost"] if s["proforma"]["profit_on_cost"] is not None else float("-inf")
    allowed = [s for s in scenarios if s["zoning_fit"] != "not_allowed"]
    best = max(allowed or scenarios, key=poc, default=None)
    analysis["recommended_scenario"] = best["name"] if best else None
    analysis["pencils"] = bool(best and best["proforma"]["pencils"])
    analysis["pencil_threshold"] = PENCIL_THRESHOLD
    analysis["zoning"] = zoning
    analysis["land_basis"] = int(basis)
    analysis["model"] = analysis.get("model") or model_name()
    analysis["version"] = ANALYSIS_VERSION
    return analysis


def model_name() -> str:
    try:
        return get_settings().parcel_analysis_model or DEFAULT_MODEL
    except Exception:
        return DEFAULT_MODEL


def _parse_json(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}")
        if start == -1 or end <= start:
            raise
        data = json.loads(raw[start:end + 1])
    if not isinstance(data, dict):
        raise json.JSONDecodeError("expected a JSON object", raw, 0)
    return data


async def generate_analysis(
    parcel: dict,
    tms: str,
    api_key: Optional[str] = None,
    client: Optional[Any] = None,
    zoning_lookup: Optional[Callable[[dict, str], Awaitable[Optional[dict]]]] = None,
) -> dict:
    """Look up zoning, call Claude for one parcel and return a finalized current-version analysis.

    Raises AnalysisInvalidJSON / AnalysisError. Pass `client` to reuse an
    AsyncAnthropic instance (the nightly job) or to inject a mock in tests, and
    `zoning_lookup` to stub the City zoning service.
    """
    zoning = await (zoning_lookup or lookup_parcel_zoning)(parcel, tms)
    model = model_name()
    if client is None:
        client = anthropic.AsyncAnthropic(api_key=api_key or get_settings().anthropic_api_key)
    prompt = build_prompt(parcel, zoning)
    # One retry: overloads and the odd malformed or cut-off answer usually clear on a second try.
    for attempt in range(1, ATTEMPTS + 1):
        try:
            message = await client.messages.create(
                model=model,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response_text(message)
        except Exception as e:
            logger.error(f"Claude API error for TMS {tms} (attempt {attempt}): {type(e).__name__}: {e}")
            if attempt < ATTEMPTS:
                continue
            raise AnalysisError(f"AI analysis failed: {str(e)[:200]}") from e
        try:
            analysis = _parse_json(raw)
            break
        except json.JSONDecodeError as e:
            stop = getattr(message, "stop_reason", None)
            logger.error(f"Claude returned invalid JSON for TMS {tms} (attempt {attempt}, stop_reason={stop}): {e}")
            if attempt < ATTEMPTS:
                continue
            raise AnalysisInvalidJSON("AI returned invalid response") from e
    analysis["model"] = model
    return finalize_analysis(analysis, land_basis(parcel), zoning)


async def get_analysis_row(db: AsyncSession, tms: str) -> Optional[ParcelAnalysis]:
    return (await db.execute(select(ParcelAnalysis).where(ParcelAnalysis.tms == tms))).scalar_one_or_none()


async def save_analysis(db: AsyncSession, tms: str, parcel: dict, analysis: dict) -> dict:
    """Insert or overwrite the row for `tms`; returns the analysis now stored.

    Tolerates the unique-constraint race (another request/replica inserting the
    same TMS concurrently): on IntegrityError it re-reads the winner's row,
    keeps it if it is current, otherwise overwrites it. Caller commits.
    """
    row = await get_analysis_row(db, tms)
    if row is not None:
        row.parcel_data = parcel
        row.analysis = analysis
        row.updated_at = datetime.utcnow()
        await db.flush()
        return analysis

    db.add(ParcelAnalysis(tms=tms, parcel_data=parcel, analysis=analysis))
    try:
        await db.flush()
        return analysis
    except IntegrityError:
        await db.rollback()
        row = await get_analysis_row(db, tms)
        if row is None:  # winner vanished; nothing sensible to do but retry once
            db.add(ParcelAnalysis(tms=tms, parcel_data=parcel, analysis=analysis))
            await db.flush()
            return analysis
        if is_current(row.analysis):
            return row.analysis
        row.parcel_data = parcel
        row.analysis = analysis
        row.updated_at = datetime.utcnow()
        await db.flush()
        return analysis


# ─── NIGHTLY JOB ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def _single_runner_lock(session_factory, key: int = _JOB_LOCK_KEY):
    """Postgres session advisory lock so only one replica runs the job at a time.

    Yields True if this process should run. Non-Postgres DBs (local sqlite)
    always run. Lock is released explicitly (pooled connections aren't closed).
    """
    async with session_factory() as probe:
        dialect = (await probe.connection()).dialect.name
    if dialect != "postgresql":
        yield True
        return
    async with session_factory() as session:
        conn = await session.connection()
        got = bool(await conn.scalar(text("SELECT pg_try_advisory_lock(:k)"), {"k": key}))
        try:
            yield got
        finally:
            if got:
                try:
                    await conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
                    await session.commit()
                except Exception as e:  # connection dropped -> lock already released
                    logger.warning(f"Parcel estimates: advisory unlock failed: {e}")


async def run_parcel_estimates_job(
    top_n: int = JOB_TOP_N,
    concurrency: int = JOB_CONCURRENCY,
    *,
    fetch_features: Callable[[], Awaitable[list]] = fetch_home_parcel_features,
    client: Optional[Any] = None,
    session_factory=None,
) -> dict:
    """Generate current-version analyses for the top-ranked Home page parcels that lack one.

    Returns a summary dict (also logged). Never raises.
    """
    summary = {"fetched": 0, "ranked": 0, "top": 0, "already_current": 0,
               "to_generate": 0, "generated": 0, "failed": 0, "skipped": None}
    settings = get_settings()
    if not settings.anthropic_api_key and client is None:
        summary["skipped"] = "no_api_key"
        logger.info("Parcel estimates job skipped: ANTHROPIC_API_KEY not configured")
        return summary

    session_factory = session_factory or get_session_factory()
    try:
        async with _single_runner_lock(session_factory) as should_run:
            if not should_run:
                summary["skipped"] = "locked"
                logger.info("Parcel estimates job skipped: another replica holds the lock")
                return summary
            await _run_job_body(summary, top_n, concurrency, fetch_features, client, session_factory, settings)
    except Exception as e:
        summary["skipped"] = f"error: {type(e).__name__}"
        logger.error(f"Parcel estimates job failed: {e}", exc_info=True)
    return summary


async def _run_job_body(summary, top_n, concurrency, fetch_features, client, session_factory, settings):
    logger.info("=== Parcel estimates job starting ===")
    features = await fetch_features()
    summary["fetched"] = len(features)
    # Parcels with a recent new-construction permit aren't opportunities (the
    # assessor just hasn't caught up); don't spend AI calls on them.
    ranked = [p for p in rank_parcels(features) if not p.get("CONSTRUCTION")]
    summary["ranked"] = len(ranked)
    # Same as the frontend: take the top N, then drop rows without a TMS.
    top = [p for p in ranked[:top_n] if p.get("TMS")]
    summary["top"] = len(top)

    tms_list = [str(p["TMS"]) for p in top]
    async with session_factory() as session:
        rows = (await session.execute(
            select(ParcelAnalysis.tms, ParcelAnalysis.analysis).where(ParcelAnalysis.tms.in_(tms_list))
        )).all() if tms_list else []
    current = {tms for tms, analysis in rows if is_current(analysis)}
    todo = [p for p in top if str(p["TMS"]) not in current]
    summary["already_current"] = len(top) - len(todo)
    summary["to_generate"] = len(todo)

    if client is None:
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    sem = asyncio.Semaphore(concurrency)

    async def work(parcel: dict):
        tms = str(parcel["TMS"])
        async with sem:
            try:
                # Re-check: a user request or another replica may have filled it meanwhile.
                async with session_factory() as session:
                    row = await get_analysis_row(session, tms)
                    if row is not None and is_current(row.analysis):
                        summary["already_current"] += 1
                        summary["to_generate"] -= 1
                        return
                analysis = await generate_analysis(parcel, tms, client=client)
                async with session_factory() as session:
                    await save_analysis(session, tms, parcel, analysis)
                    await session.commit()
                summary["generated"] += 1
            except Exception as e:
                summary["failed"] += 1
                logger.warning(f"Parcel estimates: TMS {tms} failed: {e}")

    await asyncio.gather(*(work(p) for p in todo))
    logger.info(
        "=== Parcel estimates job complete: %d parcels fetched, %d scored >= 55, top %d, "
        "%d already current, %d generated, %d failed ===",
        summary["fetched"], summary["ranked"], summary["top"],
        summary["already_current"], summary["generated"], summary["failed"],
    )
