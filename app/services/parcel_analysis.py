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

logger = logging.getLogger("sitescan.parcel_analysis")

# v1: original schema (total_development_cost excluded land).
# v2: total_development_cost includes land acquisition; proforma.land_cost added.
ANALYSIS_VERSION = 2
MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 2000

JOB_TOP_N = 50
JOB_CONCURRENCY = 3
# Arbitrary constant key for pg_try_advisory_lock so only one replica runs the job.
_JOB_LOCK_KEY = 7_324_115_002


class AnalysisError(Exception):
    """AI analysis could not be produced."""


class AnalysisInvalidJSON(AnalysisError):
    """The model's response was not valid JSON."""


SYSTEM_PROMPT = """You are a commercial real estate analyst specializing in Charleston, SC development opportunities.
Given a parcel's data, generate a concise highest-and-best-use analysis with 2-3 development scenarios.
Always respond with valid JSON matching exactly this structure:
{
  "summary": "1-2 sentence overview of the opportunity",
  "location_context": "Brief description of the neighborhood/submarket",
  "scenarios": [
    {
      "name": "Scenario name",
      "use_type": "e.g. Boutique Hotel, Mixed-Use Retail/Office, Multifamily Commercial",
      "description": "2-3 sentences on why this use fits and market demand",
      "proforma": {
        "land_cost": 0,
        "estimated_hard_cost": 0,
        "soft_costs": 0,
        "total_development_cost": 0,
        "stabilized_noi": 0,
        "cap_rate": 0.0,
        "projected_value": 0,
        "profit_margin": "0%"
      }
    }
  ],
  "recommended_scenario": "Name of the best scenario",
  "next_steps": ["step 1", "step 2", "step 3"]
}
Rules:
- land_cost is the cost to acquire the site. Use the Land Acquisition Basis given in the prompt (the parcel's appraised land value). Only if that basis is $0 or missing, estimate land cost from comparable Charleston land values.
- total_development_cost MUST include land: total_development_cost = land_cost + estimated_hard_cost + soft_costs.
- profit_margin = (projected_value - total_development_cost) / total_development_cost, as a percent string like "18%".
- All dollar values are integers (USD). cap_rate is a float like 7.5. Be realistic for the Charleston market.
Respond with the JSON object only."""


def land_basis(parcel: dict) -> int:
    """Land acquisition basis: appraised land value (LAND_APPR), else total appraisal (APPRVAL)."""
    return int(round(_js_parse_float(parcel.get("LAND_APPR")) or _js_parse_float(parcel.get("APPRVAL"))))


def build_prompt(parcel: dict) -> str:
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
    acres_line = f"\nLot Size: {acres:.2f} acres" if acres else ""
    basis_line = (
        f"${land:,.0f} (appraised land value — use this as land_cost)"
        if land else "unknown — estimate land_cost from comparable Charleston land values"
    )

    return f"""Analyze this Charleston, SC commercial parcel:

Address: {addr}
TMS: {tms}
Current Use: {genuse}
Owner: {owner}
Land Value: ${land:,.0f}
Improvements Value: ${imp:,.0f}
Total Appraised Value: ${total:,.0f}
Year Built: {yr}{acres_line}
Improvement Ratio: {round(imp / total * 100) if total else 0}% (lower = more opportunity)
Land Acquisition Basis: {basis_line}

Generate a highest-and-best-use analysis with 2-3 realistic development scenarios appropriate for this Charleston location. Every scenario's total_development_cost must include land_cost."""


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
            return float(v.replace(",", "").replace("$", "").strip())
        except ValueError:
            return None
    return None


def normalize_analysis(analysis: dict, basis: int) -> dict:
    """Stamp version and make sure every proforma carries land in its total cost.

    - Missing/non-numeric land_cost → the parcel's land basis.
    - If total_development_cost < land + hard + soft (model left land out),
      raise it to that sum and recompute profit_margin.
    """
    for scenario in analysis.get("scenarios") or []:
        pf = scenario.get("proforma") if isinstance(scenario, dict) else None
        if not isinstance(pf, dict):
            continue
        land = _num(pf.get("land_cost"))
        if land is None:
            land = float(basis)
            pf["land_cost"] = int(basis)
        hard, soft, total = _num(pf.get("estimated_hard_cost")), _num(pf.get("soft_costs")), _num(pf.get("total_development_cost"))
        if hard is not None and soft is not None:
            floor_total = land + hard + soft
            if total is None or total < floor_total * 0.99:
                pf["total_development_cost"] = int(round(floor_total))
                value = _num(pf.get("projected_value"))
                if value is not None and floor_total > 0:
                    pf["profit_margin"] = f"{round((value - floor_total) / floor_total * 100)}%"
    analysis["land_basis"] = int(basis)
    analysis["version"] = ANALYSIS_VERSION
    return analysis


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
) -> dict:
    """Call Claude for one parcel and return a normalized current-version analysis.

    Raises AnalysisInvalidJSON / AnalysisError. Pass `client` to reuse an
    AsyncAnthropic instance (the nightly job) or to inject a mock in tests.
    """
    if client is None:
        client = anthropic.AsyncAnthropic(api_key=api_key or get_settings().anthropic_api_key)
    try:
        message = await client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": build_prompt(parcel)}],
        )
        raw = message.content[0].text
    except Exception as e:
        logger.error(f"Claude API error for TMS {tms}: {e}")
        raise AnalysisError(f"AI analysis failed: {str(e)[:200]}") from e
    try:
        analysis = _parse_json(raw)
    except json.JSONDecodeError as e:
        logger.error(f"Claude returned invalid JSON for TMS {tms}: {e}")
        raise AnalysisInvalidJSON("AI returned invalid response") from e
    return normalize_analysis(analysis, land_basis(parcel))


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
    ranked = rank_parcels(features)
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
