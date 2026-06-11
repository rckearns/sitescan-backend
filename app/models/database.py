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

class Organization(Base):
    """Shared company profile — multiple users can belong to the same org."""
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    legal_name = Column(String(255), default="")
    entity_type = Column(String(50), default="")        # Corporation, LLC, Partnership, etc.
    # Address
    address_street = Column(String(255), default="")
    address_city = Column(String(100), default="")
    address_state = Column(String(10), default="SC")
    address_zip = Column(String(20), default="")
    # Contact
    phone = Column(String(50), default="")
    fax = Column(String(50), default="")
    email = Column(String(255), default="")
    website = Column(String(255), default="")
    # License
    contractor_license_number = Column(String(100), default="")
    license_classifications = Column(JSON, default=list)    # ["General", "Masonry"]
    # Insurance
    insurance_company = Column(String(255), default="")
    insurance_agent_name = Column(String(255), default="")
    insurance_agent_phone = Column(String(50), default="")
    # Bonding
    bonding_company = Column(String(255), default="")
    bonding_agent_name = Column(String(255), default="")
    bonding_agent_phone = Column(String(50), default="")
    bonding_capacity = Column(String(100), default="")      # e.g. "$5,000,000"
    # Safety
    emr = Column(String(20), default="")                    # Experience Modification Rate
    safety_meeting_frequency = Column(String(100), default="")
    # Compliance Y/N flags — {key: {value: "Yes"/"No", explanation: ""}}
    compliance_flags = Column(JSON, default=dict)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    principals = relationship(
        "OrgPrincipal", back_populates="org",
        cascade="all, delete-orphan", order_by="OrgPrincipal.order",
    )
    project_refs = relationship("ProjectReference", back_populates="org", cascade="all, delete-orphan")
    personnel = relationship("KeyPersonnel", back_populates="org", cascade="all, delete-orphan")
    users = relationship("User", back_populates="org")


class OrgPrincipal(Base):
    """A principal/owner of the organization (Part II of SC OSE SOQ)."""
    __tablename__ = "org_principals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    name = Column(String(255), default="")
    title = Column(String(100), default="")
    other_businesses = Column(Text, default="")
    order = Column(Integer, default=0)

    org = relationship("Organization", back_populates="principals")


class ProjectReference(Base):
    """A past project reference in the org portfolio (Part III-A of SC OSE SOQ)."""
    __tablename__ = "project_references"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    ref_type = Column(String(20), default="general")    # "general" or "state"
    project_name = Column(String(255), default="")
    owner_name = Column(String(255), default="")
    owner_contact = Column(String(255), default="")     # contact person name
    owner_phone = Column(String(50), default="")
    contract_value = Column(Float, nullable=True)
    completion_date = Column(String(50), default="")    # free text: "March 2024"
    description = Column(Text, default="")
    scope_of_work = Column(Text, default="")
    your_role = Column(String(50), default="")          # GC, Prime, Sub

    org = relationship("Organization", back_populates="project_refs")


class KeyPersonnel(Base):
    """A key person (PM or Superintendent) in the org (Part III-B of SC OSE SOQ)."""
    __tablename__ = "key_personnel"

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    name = Column(String(255), default="")
    role = Column(String(20), default="pm")             # "pm" or "super"
    resume_summary = Column(Text, default="")
    projects = Column(JSON, default=list)               # [{name, owner, value, role, completed}]

    org = relationship("Organization", back_populates="personnel")


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

    # Company profile link
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)

    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    org = relationship("Organization", back_populates="users")
    saved_projects = relationship("SavedProject", back_populates="user", cascade="all, delete-orphan")
    alert_history = relationship("AlertHistory", back_populates="user", cascade="all, delete-orphan")
    contractors = relationship("Contractor", back_populates="user", cascade="all, delete-orphan")


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
    contractor = Column(Text, default="")
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


class Contractor(Base):
    """A general contractor or subcontractor tracked by a user."""
    __tablename__ = "contractors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    type = Column(String(10), default="gc")        # "gc" or "sub"
    specialty = Column(String(255), default="")    # e.g. "masonry", "structural", "MEP"
    phone = Column(String(50), default="")
    email = Column(String(255), default="")
    website = Column(String(255), default="")
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="contractors")


class DirectoryEntry(Base):
    """A licensed contractor from an external directory (SC LLR, ABC Carolinas, etc.)."""
    __tablename__ = "directory_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)          # "sc-llr"
    external_id = Column(String(100), nullable=False)    # license number
    company_name = Column(String(255), nullable=False)
    city = Column(String(100), default="")
    state = Column(String(10), default="SC")
    phone = Column(String(50), default="")
    classification = Column(String(50), default="")      # SC LLR code: "CT", "MS", "SF", etc.
    trade_label = Column(String(100), default="")        # human label: "Concrete", "Masonry"
    license_status = Column(String(50), default="")      # "ACTIVE", "INACTIVE", "LAPSED"
    license_expires = Column(String(50), default="")
    last_scraped = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Permit cross-reference (populated by background enrichment task)
    permit_count = Column(Integer, default=0)
    total_permit_value = Column(Float, default=0.0)
    last_permit_date = Column(DateTime, nullable=True)
    matched_names = Column(JSON, default=list)           # permit contractor names that matched
    enriched_at = Column(DateTime, nullable=True)        # when cross-ref last ran

    __table_args__ = (
        UniqueConstraint("source", "external_id", "classification", name="uq_dir_source_ext_class"),
    )


class ParcelAnalysis(Base):
    """Cached AI analysis for a parcel (keyed by TMS)."""
    __tablename__ = "parcel_analyses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tms = Column(String(50), nullable=False, unique=True, index=True)
    parcel_data = Column(JSON, nullable=False)   # raw parcel properties sent for analysis
    analysis = Column(JSON, nullable=False)      # generated analysis object
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


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


# ─── BOARD AGENDA MODELS ─────────────────────────────────────────────────────

class BoardAgenda(Base):
    """A single Charleston board meeting agenda (PDF)."""
    __tablename__ = "board_agendas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String(100), unique=True, nullable=False)
    board_code = Column(String(20), nullable=False)
    meeting_date = Column(DateTime, nullable=False)
    pdf_url = Column(Text, nullable=False)
    title = Column(Text, default="")
    fetched_at = Column(DateTime, default=datetime.utcnow)
    parse_ok = Column(Boolean, default=True)
    item_count = Column(Integer, default=0)


class BoardProject(Base):
    """Rolled-up project record keyed by case number (or TMS fallback)."""
    __tablename__ = "board_projects"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_key = Column(String(255), unique=True, nullable=False)
    case_number = Column(String(100), nullable=True)
    tms = Column(String(100), nullable=True)
    address = Column(Text, nullable=True)
    neighborhood = Column(String(255), nullable=True)
    council_district = Column(Integer, nullable=True)
    acreage = Column(Float, nullable=True)
    owner = Column(Text, nullable=True)
    applicant = Column(Text, nullable=True)
    current_stage = Column(String(50), nullable=True)
    max_score = Column(Integer, default=0)
    first_seen_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_board_projects_tms", "tms"),
        Index("ix_board_projects_stage", "current_stage"),
        Index("ix_board_projects_score", "max_score"),
    )


class BoardAgendaItem(Base):
    """A single line-item parsed from a board agenda."""
    __tablename__ = "board_agenda_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    agenda_id = Column(Integer, ForeignKey("board_agendas.id", ondelete="CASCADE"), nullable=False)
    project_id = Column(Integer, ForeignKey("board_projects.id"), nullable=True)
    item_number = Column(Integer, nullable=True)
    section = Column(Text, nullable=True)
    address = Column(Text, nullable=True)
    case_number = Column(String(100), nullable=True)
    tms = Column(String(100), nullable=True)
    neighborhood = Column(String(255), nullable=True)
    council_district = Column(Integer, nullable=True)
    acreage = Column(Float, nullable=True)
    request_text = Column(Text, nullable=True)
    owner = Column(Text, nullable=True)
    applicant = Column(Text, nullable=True)
    stage = Column(String(50), nullable=True)
    score = Column(Integer, default=0)
    tags = Column(JSON, default=list)
    raw_text = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("agenda_id", "item_number", name="uq_agenda_item"),
        Index("ix_board_items_case", "case_number"),
        Index("ix_board_items_score", "score"),
    )


class BoardProjectEvent(Base):
    """Stage-transition event for a board project — the alert trigger."""
    __tablename__ = "board_project_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, ForeignKey("board_projects.id", ondelete="CASCADE"), nullable=False)
    event_type = Column(String(50), nullable=False)  # new_project / stage_change
    from_stage = Column(String(50), nullable=True)
    to_stage = Column(String(50), nullable=True)
    agenda_id = Column(Integer, ForeignKey("board_agendas.id"), nullable=True)
    score = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    notified_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_board_events_unnotified", "created_at"),
    )


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
    is_postgres = "postgresql" in str(_engine.url)

    # create_all in its own transaction — migrations run separately so a
    # failed ALTER TABLE can't roll back the table creation.
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    if is_postgres:
        # Each migration in its own connection so a failure (e.g. column
        # already exists with a constraint) doesn't abort the others.
        migrations = [
            "ALTER TABLE projects ALTER COLUMN naics_code TYPE TEXT",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS address VARCHAR(500) DEFAULT ''",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS latitude FLOAT",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS longitude FLOAT",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS permit_number VARCHAR(100) DEFAULT ''",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS contractor TEXT DEFAULT ''",
            "ALTER TABLE projects ALTER COLUMN contractor TYPE TEXT",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS source_url VARCHAR(500) DEFAULT ''",
            "ALTER TABLE projects ADD COLUMN IF NOT EXISTS raw_data JSON",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS criteria_min_value FLOAT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS criteria_categories JSON",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS criteria_statuses JSON",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS criteria_sources JSON",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS org_id INTEGER REFERENCES organizations(id)",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
            # parcel_analyses — created via create_all; migration only needed for existing DBs
            """CREATE TABLE IF NOT EXISTS parcel_analyses (
                id SERIAL PRIMARY KEY,
                tms VARCHAR(50) NOT NULL UNIQUE,
                parcel_data JSON NOT NULL,
                analysis JSON NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )""",
            # directory_entries permit cross-reference columns
            "ALTER TABLE directory_entries ADD COLUMN IF NOT EXISTS permit_count INTEGER DEFAULT 0",
            "ALTER TABLE directory_entries ADD COLUMN IF NOT EXISTS total_permit_value FLOAT DEFAULT 0.0",
            "ALTER TABLE directory_entries ADD COLUMN IF NOT EXISTS last_permit_date TIMESTAMP",
            "ALTER TABLE directory_entries ADD COLUMN IF NOT EXISTS matched_names JSON",
            "ALTER TABLE directory_entries ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMP",
            # directory_entries — created via create_all; migration only needed for existing DBs
            """CREATE TABLE IF NOT EXISTS directory_entries (
                id SERIAL PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                external_id VARCHAR(100) NOT NULL,
                company_name VARCHAR(255) NOT NULL,
                city VARCHAR(100) DEFAULT '',
                state VARCHAR(10) DEFAULT 'SC',
                phone VARCHAR(50) DEFAULT '',
                classification VARCHAR(50) DEFAULT '',
                trade_label VARCHAR(100) DEFAULT '',
                license_status VARCHAR(50) DEFAULT '',
                license_expires VARCHAR(50) DEFAULT '',
                last_scraped TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(source, external_id, classification)
            )""",
            # board agenda monitoring tables
            """CREATE TABLE IF NOT EXISTS board_agendas (
                id BIGSERIAL PRIMARY KEY,
                external_id TEXT UNIQUE NOT NULL,
                board_code TEXT NOT NULL,
                meeting_date DATE NOT NULL,
                pdf_url TEXT NOT NULL,
                title TEXT,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                parse_ok BOOLEAN NOT NULL DEFAULT true,
                item_count INT NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS board_projects (
                id BIGSERIAL PRIMARY KEY,
                project_key TEXT UNIQUE NOT NULL,
                case_number TEXT,
                tms TEXT,
                address TEXT,
                neighborhood TEXT,
                council_district INT,
                acreage NUMERIC,
                owner TEXT,
                applicant TEXT,
                current_stage TEXT,
                max_score INT NOT NULL DEFAULT 0,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )""",
            "CREATE INDEX IF NOT EXISTS idx_board_projects_tms ON board_projects (tms)",
            "CREATE INDEX IF NOT EXISTS idx_board_projects_stage ON board_projects (current_stage)",
            "CREATE INDEX IF NOT EXISTS idx_board_projects_score ON board_projects (max_score DESC)",
            """CREATE TABLE IF NOT EXISTS board_agenda_items (
                id BIGSERIAL PRIMARY KEY,
                agenda_id BIGINT NOT NULL REFERENCES board_agendas(id) ON DELETE CASCADE,
                project_id BIGINT REFERENCES board_projects(id),
                item_number INT,
                section TEXT,
                address TEXT,
                case_number TEXT,
                tms TEXT,
                neighborhood TEXT,
                council_district INT,
                acreage NUMERIC,
                request_text TEXT,
                owner TEXT,
                applicant TEXT,
                stage TEXT,
                score INT NOT NULL DEFAULT 0,
                tags TEXT[] NOT NULL DEFAULT '{}',
                raw_text TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (agenda_id, item_number)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_board_items_case ON board_agenda_items (case_number)",
            "CREATE INDEX IF NOT EXISTS idx_board_items_score ON board_agenda_items (score DESC)",
            """CREATE TABLE IF NOT EXISTS board_project_events (
                id BIGSERIAL PRIMARY KEY,
                project_id BIGINT NOT NULL REFERENCES board_projects(id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                from_stage TEXT,
                to_stage TEXT,
                agenda_id BIGINT REFERENCES board_agendas(id),
                score INT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                notified_at TIMESTAMPTZ
            )""",
            """CREATE INDEX IF NOT EXISTS idx_board_events_unnotified
                ON board_project_events (created_at) WHERE notified_at IS NULL""",
        ]
        for sql in migrations:
            try:
                async with _engine.begin() as conn:
                    await conn.execute(text(sql))
            except Exception as e:
                # Column/type already correct — safe to ignore
                import logging as _lg
                _lg.getLogger("sitescan.db").warning(f"Migration skipped ({e.__class__.__name__}): {sql[:60]}")

    # Startup diagnostics — log whether critical tables/columns exist
    import logging as _lg
    _dblog = _lg.getLogger("sitescan.db")
    if is_postgres:
        for check_sql, label in [
            ("SELECT COUNT(*) FROM organizations", "organizations table"),
            ("SELECT org_id FROM users LIMIT 0", "users.org_id column"),
        ]:
            try:
                async with _engine.begin() as conn:
                    await conn.execute(text(check_sql))
                _dblog.info(f"DB check OK: {label}")
            except Exception as e:
                _dblog.warning(f"DB check FAILED: {label}: {e}")


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
