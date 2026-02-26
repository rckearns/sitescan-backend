"""SQLAlchemy models and async database engine."""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Text, ForeignKey,
    Index, UniqueConstraint, JSON, text,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import get_settings

Base = declarative_base()

# ─── MODELS ──────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), default="")
    company = Column(String(255), default="")
    phone = Column(String(50), default="")
    
    # Notification preferences
    email_alerts = Column(Boolean, default=True)
    sms_alerts = Column(Boolean, default=False)
    min_match_score = Column(Integer, default=75)  # only alert if match >= this
    
    # Scan preferences
    search_keywords = Column(Text, default="masonry restoration structural")
    search_location = Column(String(255), default="Charleston, SC")
    search_state = Column(String(10), default="SC")
    search_radius_miles = Column(Integer, default=25)
    enabled_categories = Column(JSON, default=list)  # list of category IDs
    enabled_sources = Column(JSON, default=list)      # list of source IDs

    # Match scoring criteria — projects meeting ALL set criteria score 100%
    criteria_min_value = Column(Float, nullable=True)      # e.g. 1000000
    criteria_categories = Column(JSON, default=list)       # e.g. ["commercial", "government"]
    criteria_statuses = Column(JSON, default=list)         # e.g. ["Open", "Accepting Bids"]
    criteria_sources = Column(JSON, default=list)          # e.g. ["sam-gov", "scbo"]
    
    # API keys (encrypted in prod — stored plain for MVP)
    sam_gov_api_key = Column(String(255), default="")
    constructconnect_api_key = Column(String(255), default="")
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    saved_projects = relationship("SavedProject", back_populates="user", cascade="all, delete-orphan")
    alert_history = relationship("AlertHistory", back_populates="user", cascade="all, delete-orphan")


class Project(Base):
    """A construction project opportunity from any source."""
    __tablename__ = "projects"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String(50), nullable=False, index=True)    # sam-gov, charleston-permits, etc.
    external_id = Column(String(255), nullable=False)              # ID from the source system
    
    title = Column(String(500), nullable=False)
    description = Column(Text, default="")
    location = Column(String(255), default="")
    address = Column(String(500), default="")
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    value = Column(Float, nullable=True)                           # estimated project value in USD
    category = Column(String(50), default="residential")           # auto-classified
    match_score = Column(Integer, default=50)                      # 0-99 relevance score
    
    status = Column(String(50), default="Open")
    posted_date = Column(DateTime, nullable=True)
    deadline = Column(DateTime, nullable=True)
    
    # Source-specific metadata
    agency = Column(String(255), default="")
    solicitation_number = Column(String(100), default="")
    naics_code = Column(Text, default="")             # can be comma-separated list
    permit_number = Column(String(100), default="")
    contractor = Column(String(255), default="")
    source_url = Column(String(500), default="")
    
    # Raw JSON from source for future use
    raw_data = Column(JSON, nullable=True)
    
    # Tracking
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_source_external"),
        Index("ix_projects_category", "category"),
        Index("ix_projects_match", "match_score"),
        Index("ix_projects_posted", "posted_date"),
        Index("ix_projects_active", "is_active"),
    )


class SavedProject(Base):
    """User's bookmarked/saved projects."""
    __tablename__ = "saved_projects"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    notes = Column(Text, default="")
    status = Column(String(50), default="interested")  # interested, contacted, bidding, won, lost
    saved_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="saved_projects")
    project = relationship("Project")
    
    __table_args__ = (
        UniqueConstraint("user_id", "project_id", name="uq_user_project"),
    )


class AlertHistory(Base):
    """Track which alerts have been sent to avoid duplicates."""
    __tablename__ = "alert_history"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    alert_type = Column(String(20), default="email")  # email, sms
    sent_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="alert_history")
    project = relationship("Project")


class ScanLog(Base):
    """Log of automated scan runs."""
    __tablename__ = "scan_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String(50), nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="running")  # running, success, error
    projects_found = Column(Integer, default=0)
    projects_new = Column(Integer, default=0)
    error_message = Column(Text, default="")


# ─── DATABASE ENGINE ─────────────────────────────────────────────────────────

def get_engine():
    import os
    db_url = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("DATABASE_PRIVATE_URL")
        or os.environ.get("POSTGRES_URL")
        or "sqlite+aiosqlite:///./sitescan.db"
    )
    # Railway (and Heroku) may provide postgres:// or postgresql:// — both need
    # the asyncpg driver prefix for SQLAlchemy async.
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    settings = get_settings()
    return create_async_engine(
        db_url,
        echo=settings.app_env == "development",
    )


_engine = None
_session_factory = None


def get_session_factory():
    global _engine, _session_factory
    if _session_factory is None:
        _engine = get_engine()
        _session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    return _session_factory


async def init_db():
    """Create all tables and run lightweight column migrations."""
    get_session_factory()  # ensures _engine is initialized
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Widen naics_code from VARCHAR(20) → TEXT (safe no-op if already TEXT)
        await conn.execute(text(
            "ALTER TABLE projects ALTER COLUMN naics_code TYPE TEXT"
        ))
        # Add scoring criteria columns (safe no-op if already exist)
        for col, typedef in [
            ("criteria_min_value", "FLOAT"),
            ("criteria_categories", "JSON"),
            ("criteria_statuses", "JSON"),
            ("criteria_sources", "JSON"),
        ]:
            await conn.execute(text(
                f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col} {typedef}"
            ))


async def get_db():
    """FastAPI dependency for database sessions."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
