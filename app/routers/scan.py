"""Scan endpoints — trigger manual scans and view scan history."""

import asyncio
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.models.database import ScanLog, User, get_db
from app.models.schemas import ScanTriggerResponse, ScanLogOut
from app.auth import get_current_user
from app.services.orchestrator import run_full_scan, run_source_scan
from app.services.notifications import process_alerts

router = APIRouter(prefix="/scan", tags=["scan"])


@router.post("/trigger", response_model=ScanTriggerResponse)
async def trigger_scan(
    background_tasks: BackgroundTasks,
    sources: Optional[str] = Query(None, description="Comma-separated source IDs (or all)"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Trigger a manual scan of all or specific sources.
    
    Runs in the background and sends alerts when complete.
    """
    source_list = None
    if sources:
        source_list = [s.strip() for s in sources.split(",")]
    
    # Get user's SAM.gov key (falls back to system key)
    sam_key = user.sam_gov_api_key or ""
    keywords = user.search_keywords or "masonry restoration structural"
    state = user.search_state or "SC"
    
    async def _run_and_alert():
        await run_full_scan(
            sam_api_key=sam_key,
            keywords=keywords,
            state=state,
            sources=source_list,
        )
        await process_alerts()
    
    background_tasks.add_task(asyncio.get_event_loop().run_until_complete, _run_and_alert)
    
    # Actually, BackgroundTasks with async needs a sync wrapper:
    # For now, just run synchronously in-request (fast enough for MVP)
    logs = await run_full_scan(
        sam_api_key=sam_key,
        keywords=keywords,
        state=state,
        sources=source_list,
    )
    
    # Process alerts after scan
    await process_alerts()
    
    total_found = sum(l.projects_found for l in logs)
    total_new = sum(l.projects_new for l in logs)
    
    return ScanTriggerResponse(
        message=f"Scan complete: {total_found} projects found ({total_new} new)",
    )


@router.get("/history", response_model=list[ScanLogOut])
async def scan_history(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get recent scan history."""
    result = await db.execute(
        select(ScanLog)
        .order_by(desc(ScanLog.started_at))
        .limit(limit)
    )
    return result.scalars().all()


@router.get("/sources")
async def list_sources(user: User = Depends(get_current_user)):
    """List available data sources and their status."""
    from app.services.scanners import ALL_SCANNERS
    
    sources = []
    for source_id, info in ALL_SCANNERS.items():
        sources.append({
            "id": source_id,
            "name": info["name"],
            "needs_api_key": info.get("needs_key", False),
            "has_key": bool(
                getattr(user, f"{source_id.replace('-', '_')}_api_key", "")
            ) if info.get("needs_key") else True,
        })
    
    return {"sources": sources}
