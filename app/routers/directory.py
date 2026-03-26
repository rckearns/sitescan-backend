"""
Directory router — licensed contractor directory from SC LLR + permit-derived
trade roster.

GET  /directory/trades                — permit-derived trade roster (market share)
GET  /directory/contractors           — list directory entries (filterable)
GET  /directory/contractors/enriched  — list with permit activity (market share)
POST /directory/refresh               — trigger a background LLR scrape (admin)
POST /directory/enrich                — trigger permit cross-reference (admin)
GET  /directory/classifications       — list available SC LLR trade codes + labels
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import DirectoryEntry, get_db
from app.routers.auth import get_current_user
from app.services.llr_scraper import (
    CLASSIFICATION_MAP,
    DEFAULT_CLASSIFICATIONS,
    DEFAULT_CITIES,
    LLRCaptchaRequired,
    scrape_llr_full,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/directory", tags=["directory"])


# ──────────────────────────────────────────────────────────────────────────────
# Background task
# ──────────────────────────────────────────────────────────────────────────────

async def _run_llr_refresh(
    classifications: list[str],
    cities: list[str],
    api_key: str,
) -> None:
    """Scrape LLR and upsert into directory_entries."""
    from app.models.database import get_session_factory

    log.info("LLR directory refresh started: %d classes × %d cities", len(classifications), len(cities))
    try:
        results = await scrape_llr_full(classifications, cities, api_key=api_key)
    except LLRCaptchaRequired as e:
        log.warning("LLR refresh skipped: %s", e)
        return
    except Exception as e:
        log.error("LLR refresh error: %s", e, exc_info=True)
        return

    session_factory = get_session_factory()
    async with session_factory() as session:
        upserted = 0
        for r in results:
            if not r.get("company_name") or not r.get("license_number"):
                continue
            # Try to find existing record
            existing = await session.scalar(
                select(DirectoryEntry).where(
                    DirectoryEntry.source == "sc-llr",
                    DirectoryEntry.external_id == r["license_number"],
                    DirectoryEntry.classification == r.get("classification", ""),
                )
            )
            if existing:
                existing.license_status = r.get("license_status", "")
                existing.license_expires = r.get("license_expires", "")
                existing.last_scraped = datetime.utcnow()
            else:
                session.add(DirectoryEntry(
                    source="sc-llr",
                    external_id=r["license_number"],
                    company_name=r["company_name"],
                    city=r.get("city", ""),
                    state=r.get("state", "SC"),
                    classification=r.get("classification", ""),
                    trade_label=r.get("trade_label", ""),
                    license_status=r.get("license_status", ""),
                    license_expires=r.get("license_expires", ""),
                ))
                upserted += 1
        await session.commit()
        log.info("LLR directory refresh: %d new records upserted (total %d)", upserted, len(results))


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/trades")
async def list_trades(
    trade: str | None = Query(None, description="Filter to a single trade, e.g. framing, masonry, concrete, roofing"),
    source: str | None = Query(None, description="Filter by permit source, e.g. charleston-permits"),
    min_value: float = Query(0, description="Minimum total permit value to include a contractor"),
    _user=Depends(get_current_user),
):
    """
    Trade roster — contractors ranked by permit activity (market share).

    Determines each contractor's trade from their COMPANY NAME and LLR license
    classification (not from permit descriptions).  A company called
    "ABC Framing LLC" is classified as framing.  A company with an SC LLR
    "SF" (Structural Framing) license is classified as framing.

    Available trades: framing, masonry, concrete, roofing, electrical, plumbing,
    hvac, drywall-finishes, siding-exterior, windows-doors, grading-sitework,
    demolition, fire-protection, insulation, general.

    Example: GET /directory/trades?trade=framing&min_value=500000
    → every framing company with over $500K in permit activity.
    """
    from app.services.contractor_match import build_permit_trade_roster

    return await build_permit_trade_roster(trade=trade, source=source, min_value=min_value)


@router.get("/trades/list")
async def available_trades(
    _user=Depends(get_current_user),
):
    """Return all available trade IDs with human labels."""
    from app.services.contractor_match import TRADE_LABELS

    return [
        {"id": trade_id, "label": label}
        for trade_id, label in TRADE_LABELS.items()
    ]


@router.get("/classifications")
async def list_classifications(
    _user=Depends(get_current_user),
):
    """Return all SC LLR classification codes with human labels."""
    return [
        {"code": code, "label": label}
        for code, label in CLASSIFICATION_MAP.items()
    ]


@router.get("/contractors")
async def list_directory_contractors(
    classification: str | None = Query(None, description="SC LLR code, e.g. CT"),
    city: str | None = Query(None, description="City name filter (case-insensitive)"),
    active_only: bool = Query(True, description="Only return ACTIVE licenses"),
    limit: int = Query(500, le=2000),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """
    List directory contractors from SC LLR.

    Optionally filter by classification (trade code) and/or city.
    """
    q = select(DirectoryEntry).where(DirectoryEntry.source == "sc-llr")

    if classification:
        q = q.where(DirectoryEntry.classification == classification.upper())
    if city:
        q = q.where(DirectoryEntry.city.ilike(f"%{city}%"))
    if active_only:
        q = q.where(DirectoryEntry.license_status == "ACTIVE")

    q = q.order_by(DirectoryEntry.company_name).limit(limit)
    rows = (await db.scalars(q)).all()

    return [
        {
            "id": r.id,
            "company_name": r.company_name,
            "city": r.city,
            "state": r.state,
            "phone": r.phone,
            "classification": r.classification,
            "trade_label": r.trade_label,
            "license_status": r.license_status,
            "license_expires": r.license_expires,
            "last_scraped": r.last_scraped.isoformat() if r.last_scraped else None,
        }
        for r in rows
    ]


@router.get("/contractors/enriched")
async def list_enriched_contractors(
    classification: str | None = Query(None, description="SC LLR code, e.g. SF, WF, CT"),
    city: str | None = Query(None, description="City name filter (case-insensitive)"),
    active_only: bool = Query(True, description="Only return ACTIVE licenses"),
    sort_by: str = Query("permit_value", description="Sort: permit_value, permit_count, or name"),
    limit: int = Query(500, le=2000),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """
    List directory contractors enriched with permit activity data.

    Returns contractors sorted by total permit value (market share proxy).
    Each entry includes permit_count, total_permit_value, last_permit_date,
    and matched_names showing which permit contractor names were linked.

    Run POST /directory/enrich first to populate the cross-reference data.
    """
    q = select(DirectoryEntry).where(DirectoryEntry.source == "sc-llr")

    if classification:
        q = q.where(DirectoryEntry.classification == classification.upper())
    if city:
        q = q.where(DirectoryEntry.city.ilike(f"%{city}%"))
    if active_only:
        q = q.where(DirectoryEntry.license_status == "ACTIVE")

    if sort_by == "permit_count":
        q = q.order_by(DirectoryEntry.permit_count.desc(), DirectoryEntry.total_permit_value.desc())
    elif sort_by == "name":
        q = q.order_by(DirectoryEntry.company_name)
    else:
        q = q.order_by(DirectoryEntry.total_permit_value.desc(), DirectoryEntry.permit_count.desc())

    q = q.limit(limit)
    rows = (await db.scalars(q)).all()

    # Compute market share percentages within this result set
    total_value_all = sum(r.total_permit_value or 0 for r in rows)

    return {
        "total_contractors": len(rows),
        "total_permit_value_in_set": total_value_all,
        "enriched_at": rows[0].enriched_at.isoformat() if rows and rows[0].enriched_at else None,
        "contractors": [
            {
                "id": r.id,
                "company_name": r.company_name,
                "city": r.city,
                "state": r.state,
                "phone": r.phone,
                "classification": r.classification,
                "trade_label": r.trade_label,
                "license_status": r.license_status,
                "license_expires": r.license_expires,
                "permit_count": r.permit_count or 0,
                "total_permit_value": r.total_permit_value or 0.0,
                "market_share_pct": round(
                    ((r.total_permit_value or 0) / total_value_all * 100), 1
                ) if total_value_all > 0 else 0.0,
                "last_permit_date": r.last_permit_date.isoformat() if r.last_permit_date else None,
                "matched_names": r.matched_names or [],
                "enriched_at": r.enriched_at.isoformat() if r.enriched_at else None,
            }
            for r in rows
        ],
    }


@router.post("/enrich")
async def trigger_enrich(
    background_tasks: BackgroundTasks,
    _user=Depends(get_current_user),
):
    """
    Trigger background cross-reference of directory entries against permit data.

    Matches LLR company names to permit contractor names using normalized
    fuzzy matching, then writes permit_count, total_permit_value, etc.
    back to each directory entry.  Typically takes a few seconds.
    """
    from app.services.contractor_match import enrich_directory_with_permits

    background_tasks.add_task(enrich_directory_with_permits)
    return {
        "status": "started",
        "message": "Cross-referencing directory entries against permit contractor data in background",
    }


@router.post("/refresh")
async def trigger_llr_refresh(
    background_tasks: BackgroundTasks,
    classifications: list[str] | None = None,
    cities: list[str] | None = None,
    _user=Depends(get_current_user),
):
    """
    Trigger a background scrape of SC LLR contractor licenses.

    Requires TWOCAPTCHA_API_KEY env var to be set on the server.
    Returns immediately; scrape runs in background (may take 10-30 min).
    """
    api_key = os.environ.get("TWOCAPTCHA_API_KEY", "")
    if not api_key:
        return {
            "status": "skipped",
            "message": "TWOCAPTCHA_API_KEY not configured — set this env var to enable LLR scraping",
        }

    classes = classifications or DEFAULT_CLASSIFICATIONS
    city_list = cities or DEFAULT_CITIES

    background_tasks.add_task(_run_llr_refresh, classes, city_list, api_key)
    return {
        "status": "started",
        "classifications": classes,
        "cities": city_list,
        "message": f"Scraping {len(classes)} trade types × {len(city_list)} cities in background",
    }


@router.get("/status")
async def directory_status(
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """Return summary counts of directory entries by classification."""
    rows = await db.execute(
        text("""
            SELECT classification, trade_label, COUNT(*) AS cnt,
                   MAX(last_scraped) AS last_scraped
            FROM directory_entries
            WHERE source = 'sc-llr'
            GROUP BY classification, trade_label
            ORDER BY classification
        """)
    )
    results = rows.fetchall()
    has_api_key = bool(os.environ.get("TWOCAPTCHA_API_KEY"))
    return {
        "has_api_key": has_api_key,
        "total_entries": sum(r.cnt for r in results),
        "by_classification": [
            {
                "classification": r.classification,
                "trade_label": r.trade_label,
                "count": r.cnt,
                "last_scraped": r.last_scraped.isoformat() if r.last_scraped else None,
            }
            for r in results
        ],
    }
