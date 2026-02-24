"""SiteScan Backend — FastAPI application with scheduled scanning.

Starts the API server and background scan scheduler.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.models.database import init_db
from app.routers import auth_router, projects_router, scan_router
from app.services.orchestrator import scheduled_scan_job
from app.services.notifications import process_alerts

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
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
    logger.info("Initializing database...")
    await init_db()
    logger.info("Database ready")
    
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

# Mount routers
app.include_router(auth_router, prefix="/api/v1")
app.include_router(projects_router, prefix="/api/v1")
app.include_router(scan_router, prefix="/api/v1")


# ─── HEALTH CHECK ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    from app.config import get_settings
    s = get_settings()
    db = s.database_url
    return {
        "status": "ok",
        "service": "sitescan-api",
        "version": "1.0.0",
        "db_type": "postgres" if "postgres" in db else "sqlite",
        "db_host": db.split("@")[-1].split("/")[0] if "@" in db else "local",
    }


@app.get("/")
async def root():
    return {
        "name": "SiteScan API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
