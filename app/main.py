"""SiteScan Backend — FastAPI application with scheduled scanning.

Starts the API server and background scan scheduler.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from app.config import Settings, get_settings
from app.models.database import init_db, get_session_factory
from app.routers import auth_router, projects_router, scan_router, contractors_router, profile_router, directory_router, analyze_router, boards_router, pipeline_router
from app.services.orchestrator import scheduled_scan_job
from app.services.notifications import process_alerts
from app.services.parcel_analysis import run_parcel_estimates_job
from app.services.pipeline.job import run_pipeline_job
from sitescan_boards.pipeline import run_boards_scrape

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# SQLAlchemy emits one log line per query at INFO level — with 500+ EnerGov
# calls per scan this floods Railway's 500 log/sec limit and drops app logs.
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
# httpx/httpcore log every request URL at INFO, and ZenRows (and SAM.gov) take
# their API key as a query parameter, so request logging would leak keys.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
logger = logging.getLogger("sitescan")

# ─── SCHEDULER ───────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()


async def scan_and_alert():
    """Combined scan + alert job for the scheduler."""
    await scheduled_scan_job()
    await process_alerts()


async def boards_scrape_job():
    """Daily Charleston board agenda scrape job."""
    logger.info("=== Board agendas scrape starting ===")
    try:
        summary = await run_boards_scrape()
        logger.info(
            "=== Board agendas scrape complete: %d discovered, %d new, "
            "%d items, %d alerts ===",
            summary["discovered"], summary["new_agendas"],
            summary["items"], summary["alerts"],
        )
    except Exception as e:
        logger.error("=== Board agendas scrape failed: %s ===", e)


async def gc_pipeline_job():
    """Pull GC pipeline sources and classify changed projects (never raises)."""
    await run_pipeline_job()


async def parcel_estimates_job():
    """Pre-generate AI estimates for the top Home page parcels (never raises)."""
    await run_parcel_estimates_job()


# ─── APP LIFECYCLE ───────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    if get_settings().secret_key == Settings.model_fields["secret_key"].default:
        logger.error("SECRET_KEY is the public default from the repo; anyone can forge login tokens. Set a random SECRET_KEY.")
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
    scheduler.add_job(
        boards_scrape_job,
        trigger=IntervalTrigger(hours=24),
        id="boards_scrape",
        name="Charleston board agendas scrape",
        replace_existing=True,
    )
    # Parcel estimates: daily at 08:30 UTC (~3:30/4:30am Charleston) plus once
    # shortly after startup so a deploy fills in missing/stale estimates.
    scheduler.add_job(
        parcel_estimates_job,
        trigger=CronTrigger(hour=8, minute=30, timezone="UTC"),
        id="parcel_estimates_daily",
        name="Parcel AI estimates (daily)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        parcel_estimates_job,
        trigger=DateTrigger(run_date=datetime.now(timezone.utc) + timedelta(minutes=3)),
        id="parcel_estimates_startup",
        name="Parcel AI estimates (startup)",
        replace_existing=True,
    )
    # GC pipeline: daily at 09:30 UTC (after the parcel job) plus ~6 min after startup.
    scheduler.add_job(
        gc_pipeline_job,
        trigger=CronTrigger(hour=9, minute=30, timezone="UTC"),
        id="gc_pipeline_daily",
        name="GC pipeline (daily)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        gc_pipeline_job,
        trigger=DateTrigger(run_date=datetime.now(timezone.utc) + timedelta(minutes=6)),
        id="gc_pipeline_startup",
        name="GC pipeline (startup)",
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

# CORS — restrict to known frontend origins
_ALLOWED_ORIGINS = [
    "https://www.yabodle.com",
    "https://yabodle.com",
    "https://yabodle.pages.dev",
    "https://rckearns.github.io",
    "http://localhost:5173",   # Vite dev server
    "http://localhost:3000",
]
# Home parcel lists are ~1.7 MB of JSON; gzip cuts that to a fraction.
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_origin_regex=r"https://[a-z0-9]+\.yabodle\.pages\.dev",
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
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
app.include_router(directory_router, prefix="/api/v1")
app.include_router(analyze_router, prefix="/api/v1")
app.include_router(boards_router, prefix="/api/v1")
app.include_router(pipeline_router, prefix="/api/v1")


# ─── HEALTH CHECK ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "sitescan-api", "version": "1.0.0"}





@app.get("/")
async def root():
    return {
        "name": "SiteScan API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
# force redeploy Thu Mar  5 2026
