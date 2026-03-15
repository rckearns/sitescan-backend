"""Parcel analysis endpoints — AI-generated use cases and proformas."""

import json
import logging
from typing import Any

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.config import get_settings
from app.models.database import ParcelAnalysis, User, get_db

logger = logging.getLogger("sitescan.analyze")
router = APIRouter(prefix="/analyze", tags=["analyze"])


class ParcelPayload(BaseModel):
    parcel: dict[str, Any]


_SYSTEM = """You are a commercial real estate analyst specializing in Charleston, SC development opportunities.
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
All dollar values are integers (USD). cap_rate is a float like 7.5. Be realistic for the Charleston market."""


def _build_prompt(parcel: dict) -> str:
    addr = " ".join(filter(None, [parcel.get("HOUSE", ""), parcel.get("STREET", "")]))
    if not addr:
        addr = "No street address (parcel only)"
    land = parcel.get("LAND_APPR") or parcel.get("APPRVAL", 0)
    imp = parcel.get("IMP_APPR", 0)
    total = parcel.get("APPRVAL") or (land + imp)
    yr = parcel.get("YRBUILT", "unknown")
    genuse = parcel.get("GENUSE", "General Commercial")
    owner = parcel.get("OWNER", "Unknown")
    tms = parcel.get("TMS") or parcel.get("PARCELID", "")

    return f"""Analyze this Charleston, SC commercial parcel:

Address: {addr}
TMS: {tms}
Current Use: {genuse}
Owner: {owner}
Land Value: ${land:,.0f}
Improvements Value: ${imp:,.0f}
Total Appraised Value: ${total:,.0f}
Year Built: {yr}
Improvement Ratio: {round(imp / total * 100) if total else 0}% (lower = more opportunity)

Generate a highest-and-best-use analysis with 2-3 realistic development scenarios appropriate for this Charleston location."""


@router.post("/parcel/{tms}")
async def analyze_parcel(
    tms: str,
    payload: ParcelPayload,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return cached AI analysis for a parcel, generating it if not yet stored."""
    # Check cache
    result = await db.execute(select(ParcelAnalysis).where(ParcelAnalysis.tms == tms))
    cached = result.scalar_one_or_none()
    if cached:
        return {"tms": tms, "cached": True, "analysis": cached.analysis}

    # Generate
    settings = get_settings()
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="AI analysis not configured")

    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            system=_SYSTEM,
            messages=[{"role": "user", "content": _build_prompt(payload.parcel)}],
        )
        raw = message.content[0].text.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        analysis = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"Claude returned invalid JSON for TMS {tms}: {e}")
        raise HTTPException(status_code=502, detail="AI returned invalid response")
    except Exception as e:
        logger.error(f"Claude API error for TMS {tms}: {e}")
        raise HTTPException(status_code=502, detail=f"AI analysis failed: {str(e)[:200]}")

    # Cache it
    record = ParcelAnalysis(tms=tms, parcel_data=payload.parcel, analysis=analysis)
    db.add(record)
    await db.flush()

    return {"tms": tms, "cached": False, "analysis": analysis}
