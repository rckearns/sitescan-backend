"""Scan orchestrator — runs scanners, deduplicates, upserts into database."""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import Project, ScanLog, User, get_session_factory
from app.services.scanners import (
    scan_sam_gov, scan_charleston_permits, scan_scbo,
    scan_charleston_bids, ALL_SCANNERS,
)
from app.config import get_settings

logger = logging.getLogger("sitescan.orchestrator")


async def upsert_projects(session: AsyncSession, projects: list[dict]) -> tuple[int, int]:
    """Insert new projects or update existing ones. Returns (total, new_count)."""
    new_count = 0

    with session.no_autoflush:
        for proj in projects:
            # Check if exists
            result = await session.execute(
                select(Project).where(
                    Project.source_id == proj["source_id"],
                    Project.external_id == proj["external_id"],
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                # Update last_seen and any changed fields
                existing.last_seen = datetime.utcnow()
                existing.title = proj.get("title", existing.title)
                existing.description = proj.get("description", existing.description)
                existing.status = proj.get("status", existing.status)
                existing.match_score = proj.get("match_score", existing.match_score)
                existing.value = proj.get("value", existing.value)
                existing.is_active = True
                if proj.get("deadline"):
                    existing.deadline = proj["deadline"]
            else:
                # Insert new
                new_proj = Project(
                    source_id=proj.get("source_id", ""),
                    external_id=proj.get("external_id", ""),
                    title=proj.get("title", ""),
                    description=proj.get("description", ""),
                    location=proj.get("location", ""),
                    address=proj.get("address", ""),
                    latitude=proj.get("latitude"),
                    longitude=proj.get("longitude"),
                    value=proj.get("value"),
                    category=proj.get("category", "residential"),
                    match_score=proj.get("match_score", 50),
                    status=proj.get("status", "Open"),
                    posted_date=proj.get("posted_date"),
                    deadline=proj.get("deadline"),
                    agency=proj.get("agency", ""),
                    solicitation_number=proj.get("solicitation_number", ""),
                    naics_code=proj.get("naics_code", ""),
                    permit_number=proj.get("permit_number", ""),
                    contractor=proj.get("contractor", ""),
                    source_url=proj.get("source_url", ""),
                    raw_data=proj.get("raw_data"),
                )
                session.add(new_proj)
                new_count += 1

    await session.flush()
    return len(projects), new_count


async def run_source_scan(
    session: AsyncSession,
    source_id: str,
    sam_api_key: Optional[str] = None,
    keywords: Optional[str] = None,
    state: str = "SC",
) -> ScanLog:
    """Run a single source scanner and log results."""
    settings = get_settings()
    
    scan_log = ScanLog(
        source_id=source_id,
        started_at=datetime.utcnow(),
        status="running",
    )
    session.add(scan_log)
    await session.flush()  # persist scan_log in outer transaction

    try:
        # Use a savepoint so a DB error in one source doesn't poison the session
        async with session.begin_nested():
            projects = []

            if source_id == "sam-gov":
                key = sam_api_key or settings.sam_gov_api_key
                # Check cache — if we already scanned SAM today, skip the API call
                today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                cached = await session.execute(
                    select(Project).where(
                        Project.source_id == "sam-gov",
                        Project.last_seen >= today_start,
                    ).limit(1)
                )
                if cached.scalar_one_or_none():
                    logger.info("SAM.gov: using cached results from today")
                    all_cached = await session.execute(
                        select(Project).where(Project.source_id == "sam-gov", Project.is_active == True)
                    )
                    projects = [{"source_id": "sam-gov", "external_id": p.external_id, "title": p.title,
                                 "description": p.description, "location": p.location, "value": p.value,
                                 "category": p.category, "match_score": p.match_score, "status": p.status,
                                 "posted_date": p.posted_date, "deadline": p.deadline, "agency": p.agency,
                                 "solicitation_number": p.solicitation_number, "naics_code": p.naics_code,
                                 "source_url": p.source_url, "raw_data": p.raw_data}
                                for p in all_cached.scalars().all()]
                else:
                    projects = await scan_sam_gov(api_key=key, state=state, keywords=keywords)

            elif source_id == "charleston-permits":
                projects = await scan_charleston_permits(arcgis_url=settings.charleston_arcgis_url)

            elif source_id == "scbo":
                projects = await scan_scbo()

            elif source_id == "charleston-city-bids":
                projects = await scan_charleston_bids()

            total, new_count = await upsert_projects(session, projects)

            scan_log.finished_at = datetime.utcnow()
            scan_log.status = "success"
            scan_log.projects_found = total
            scan_log.projects_new = new_count
            logger.info(f"Scan {source_id}: {total} found, {new_count} new")

    except Exception as e:
        # Savepoint rolled back — outer transaction (scan_log) is still valid
        scan_log.finished_at = datetime.utcnow()
        scan_log.status = "error"
        scan_log.error_message = str(e)[:500]
        logger.error(f"Scan {source_id} failed: {e}")

    return scan_log


async def run_full_scan(
    sam_api_key: Optional[str] = None,
    keywords: Optional[str] = None,
    state: str = "SC",
    sources: Optional[list[str]] = None,
) -> list[ScanLog]:
    """Run all enabled scanners and return logs.
    
    This is the main entry point called by both the scheduler
    and the manual scan API endpoint.
    """
    session_factory = get_session_factory()
    source_ids = sources or list(ALL_SCANNERS.keys())
    logs = []
    
    async with session_factory() as session:
        try:
            for source_id in source_ids:
                if source_id not in ALL_SCANNERS:
                    continue
                    
                logger.info(f"Starting scan: {source_id}")
                scan_log = await run_source_scan(
                    session=session,
                    source_id=source_id,
                    sam_api_key=sam_api_key,
                    keywords=keywords,
                    state=state,
                )
                logs.append(scan_log)
            
            # Mark projects not seen recently as inactive
            await session.execute(
                update(Project)
                .where(Project.is_active == True)
                .where(Project.last_seen < datetime.utcnow().replace(hour=0, minute=0, second=0))
                .values(is_active=False)
            )
            
            await session.commit()
            
        except Exception as e:
            await session.rollback()
            logger.error(f"Full scan failed: {e}")
            raise
    
    return logs


async def scheduled_scan_job():
    """Called by APScheduler on a cron interval."""
    logger.info("=== Scheduled scan starting ===")
    settings = get_settings()
    
    try:
        logs = await run_full_scan(
            sam_api_key=settings.sam_gov_api_key,
            keywords="masonry restoration structural construction",
            state="SC",
        )
        
        total_found = sum(l.projects_found for l in logs)
        total_new = sum(l.projects_new for l in logs)
        errors = [l for l in logs if l.status == "error"]
        
        logger.info(
            f"=== Scheduled scan complete: {total_found} found, "
            f"{total_new} new, {len(errors)} errors ==="
        )
        
    except Exception as e:
        logger.error(f"=== Scheduled scan failed: {e} ===")
