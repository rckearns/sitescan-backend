"""Parcel analysis endpoints — AI-generated use cases and proformas.

Generation/caching logic lives in app.services.parcel_analysis (shared with the
nightly parcel-estimates job).
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.config import get_settings
from app.models.database import ParcelAnalysis, User, get_db
from app.services.parcel_analysis import (
    AnalysisError, AnalysisInvalidJSON, add_custom_scenario, generate_analysis, get_analysis_row,
    is_servable, refinalize, save_analysis,
)

logger = logging.getLogger("sitescan.analyze")
router = APIRouter(prefix="/analyze", tags=["analyze"])


class ParcelPayload(BaseModel):
    parcel: dict[str, Any]


@router.get("/parcels/cached")
async def cached_parcel_analyses(
    tms: str = Query(..., description="Comma-separated TMS numbers"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return already-generated current-version analyses for the given parcels; never calls the AI.

    Stale (older-version) analyses are omitted so the client treats them as missing.
    """
    tms_list = [t.strip() for t in tms.split(",") if t.strip()][:500]
    if not tms_list:
        return {"analyses": {}}
    result = await db.execute(select(ParcelAnalysis).where(ParcelAnalysis.tms.in_(tms_list)))
    return {"analyses": {row.tms: refinalize(row.analysis) for row in result.scalars().all() if is_servable(row.analysis)}}


@router.post("/parcel/{tms}")
async def analyze_parcel(
    tms: str,
    payload: ParcelPayload,
    refresh: bool = Query(False, description="Regenerate even if a stored analysis exists"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return cached AI analysis for a parcel, generating it if missing or stale."""
    cached = await get_analysis_row(db, tms)
    if cached is not None and is_servable(cached.analysis) and refresh is not True:
        return {"tms": tms, "cached": True, "analysis": refinalize(cached.analysis)}

    settings = get_settings()
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="AI analysis not configured")

    try:
        analysis = await generate_analysis(payload.parcel, tms, api_key=settings.anthropic_api_key)
    except AnalysisInvalidJSON:
        raise HTTPException(status_code=502, detail="AI returned invalid response")
    except AnalysisError as e:
        raise HTTPException(status_code=502, detail=str(e))

    stored = await save_analysis(db, tms, payload.parcel, analysis)
    return {"tms": tms, "cached": stored is not analysis, "analysis": refinalize(stored)}


class ScenarioPayload(BaseModel):
    parcel: dict[str, Any]
    use: str


@router.post("/parcel/{tms}/scenario")
async def add_parcel_scenario(
    tms: str,
    payload: ScenarioPayload,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a scenario for a use the user names (e.g. "boutique hotel") to the stored analysis."""
    cached = await get_analysis_row(db, tms)
    if cached is None or not is_servable(cached.analysis):
        raise HTTPException(status_code=404, detail="Run the estimate first")
    settings = get_settings()
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="AI analysis not configured")
    try:
        analysis = await add_custom_scenario(payload.parcel, tms, payload.use, refinalize(cached.analysis),
                                             api_key=settings.anthropic_api_key)
    except AnalysisInvalidJSON as e:
        raise HTTPException(status_code=502, detail=str(e))
    except AnalysisError as e:
        raise HTTPException(status_code=502 if "failed" in str(e) else 400, detail=str(e))
    stored = await save_analysis(db, tms, payload.parcel, analysis)
    return {"tms": tms, "analysis": refinalize(stored)}
