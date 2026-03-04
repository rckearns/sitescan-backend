"""SiteScan Backend — FastAPI application with scheduled scanning.

Starts the API server and background scan scheduler.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.models.database import init_db, get_session_factory
from app.routers import auth_router, projects_router, scan_router, contractors_router, profile_router
from app.services.orchestrator import scheduled_scan_job
from app.services.notifications import process_alerts

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# SQLAlchemy emits one log line per query at INFO level — with 500+ EnerGov
# calls per scan this floods Railway's 500 log/sec limit and drops app logs.
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
logger = logging.getLogger("sitescan")

# ─── SCHEDULER ───────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()


async def scan_and_alert():
    """Combined scan + alert job for the scheduler."""
    await scheduled_scan_job()
    await process_alerts()


# ─── APP LIFECYCLE ───────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    settings = get_settings()
    
    # Initialize database
    import os
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PRIVATE_URL") or os.environ.get("POSTGRES_URL") or ""
    if db_url.startswith("postgres"):
        scheme = db_url.split("@")[0].split("://")[0] if "@" in db_url else db_url[:30]
        logger.info(f"Database: postgresql (host hidden) [{scheme}]")
    else:
        logger.info("Database: sqlite (no DATABASE_URL found — ephemeral!)")
    await init_db()
    logger.info("Database ready")

    # Restore any CHS permits that were marked inactive by a failed scan.
    # On each restart we re-activate them so they remain visible while the
    # next scan runs and upserts them with fresh last_seen timestamps.
    try:
        from datetime import datetime
        from sqlalchemy import update
        from app.models.database import Project
        session_factory = get_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                update(Project)
                .where(Project.source_id == "charleston-permits")
                .where(Project.is_active == False)
                .values(is_active=True, last_seen=datetime.utcnow())
                .returning(Project.id)
            )
            restored = len(result.fetchall())
            await session.commit()
            if restored:
                logger.info(f"Startup: restored {restored} inactive CHS permits to active")
    except Exception as e:
        logger.warning(f"Startup permit restore failed (non-fatal): {e}")
    
    # Kick off a CHS Permits background scan so contractor data stays fresh.
    # Uses the EnerGov-skip optimization — only enriches permits without
    # contractor data, so this completes in ~2 min instead of 25+ min.
    async def _startup_permits_scan():
        try:
            from app.services.orchestrator import run_full_scan
            logger.info("Startup: triggering CHS Permits background scan...")
            await run_full_scan(sources=["charleston-permits"])
            logger.info("Startup: CHS Permits scan complete")
        except Exception as e:
            logger.warning(f"Startup CHS Permits scan failed (non-fatal): {e}")

    asyncio.create_task(_startup_permits_scan())

    # Start scheduler
    scheduler.add_job(
        scan_and_alert,
        trigger=IntervalTrigger(hours=settings.scan_cron_hours),
        id="scheduled_scan",
        name="Scheduled opportunity scan",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started — scanning every {settings.scan_cron_hours} hours")
    
    yield
    
    # Shutdown
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


# ─── APP ─────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SiteScan API",
    description=(
        "Construction project opportunity intelligence API. "
        "Scans SAM.gov, Charleston permits, SCBO, and local bid portals "
        "for masonry, restoration, and structural opportunities."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler — ensures unhandled exceptions return JSON with
# CORS headers instead of Starlette's bare 500 (which strips headers).
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception on {request.method} {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {type(exc).__name__}: {str(exc)[:300]}"},
        headers={"Access-Control-Allow-Origin": "*"},
    )


# Mount routers
app.include_router(auth_router, prefix="/api/v1")
app.include_router(projects_router, prefix="/api/v1")
app.include_router(scan_router, prefix="/api/v1")
app.include_router(contractors_router, prefix="/api/v1")
app.include_router(profile_router, prefix="/api/v1")


# ─── HEALTH CHECK ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "sitescan-api", "version": "1.0.0"}


@app.get("/debug/test-profile")
async def debug_test_profile():
    """Diagnostic: test org creation without auth to isolate the error."""
    from sqlalchemy import text as _t
    from app.models.database import get_session_factory, Organization
    from app.routers.profile import _load_org
    sf = get_session_factory()
    steps = []
    async with sf() as s:
        try:
            steps.append("creating org object")
            org = Organization()
            s.add(org)
            steps.append("flushing insert")
            await s.flush()
            steps.append(f"org.id = {org.id}")
            steps.append("reloading with selectinload")
            org2 = await _load_org(org.id, s)
            steps.append(f"reload ok, principals={org2.principals}, project_refs={org2.project_refs}")
            steps.append("building dict")
            result = {
                "id": org2.id, "legal_name": org2.legal_name or "",
                "license_classifications": org2.license_classifications or [],
                "compliance_flags": org2.compliance_flags or {},
                "principals": [], "project_refs": [], "personnel": [],
            }
            steps.append("rolling back (debug only, no actual create)")
            await s.rollback()
            return {"status": "ok", "steps": steps, "sample": result}
        except Exception as e:
            await s.rollback()
            return {"status": "error", "steps": steps, "error": str(e), "type": type(e).__name__}


@app.get("/health/db")
async def health_db():
    """Diagnostic endpoint — checks whether key DB tables/columns exist."""
    from sqlalchemy import text as _t
    from app.models.database import get_session_factory
    results = {}
    sf = get_session_factory()
    checks = [
        ("SELECT COUNT(*) FROM organizations", "organizations_table"),
        ("SELECT org_id FROM users LIMIT 0", "users_org_id_column"),
        ("SELECT COUNT(*) FROM users", "users_table"),
    ]
    async with sf() as s:
        for sql, label in checks:
            try:
                r = await s.execute(_t(sql))
                val = r.scalar()
                results[label] = f"ok ({val})" if val is not None else "ok"
            except Exception as e:
                results[label] = f"ERROR: {e}"
    return results


@app.get("/")
async def root():
    return {
        "name": "SiteScan API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
# force redeploy Wed Mar  4 2026
