"""Pydantic schemas for API requests and responses."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr


# ─── AUTH ────────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: str
    password: str
    full_name: str = ""
    company: str = ""

class UserLogin(BaseModel):
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UserProfile(BaseModel):
    id: int
    email: str
    full_name: str
    company: str
    phone: str
    email_alerts: bool
    sms_alerts: bool
    min_match_score: int
    search_keywords: str
    search_location: str
    search_state: str
    search_radius_miles: int
    enabled_categories: list
    enabled_sources: list
    # Match scoring criteria
    criteria_min_value: Optional[float] = None
    criteria_categories: list = []
    criteria_statuses: list = []
    criteria_sources: list = []
    created_at: datetime

    class Config:
        from_attributes = True

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    company: Optional[str] = None
    phone: Optional[str] = None
    email_alerts: Optional[bool] = None
    sms_alerts: Optional[bool] = None
    min_match_score: Optional[int] = None
    search_keywords: Optional[str] = None
    search_location: Optional[str] = None
    search_state: Optional[str] = None
    search_radius_miles: Optional[int] = None
    enabled_categories: Optional[list] = None
    enabled_sources: Optional[list] = None
    sam_gov_api_key: Optional[str] = None
    constructconnect_api_key: Optional[str] = None
    # Match scoring criteria
    criteria_min_value: Optional[float] = None
    criteria_categories: Optional[list] = None
    criteria_statuses: Optional[list] = None
    criteria_sources: Optional[list] = None


# ─── PROJECTS ────────────────────────────────────────────────────────────────

class ProjectOut(BaseModel):
    id: int
    source_id: str
    external_id: str
    title: str
    description: str
    location: str
    address: str
    latitude: Optional[float]
    longitude: Optional[float]
    value: Optional[float]
    category: str
    match_score: int
    status: str
    posted_date: Optional[datetime]
    deadline: Optional[datetime]
    agency: str
    solicitation_number: str
    naics_code: str
    permit_number: str
    contractor: str
    source_url: str
    first_seen: datetime
    last_seen: datetime
    is_active: bool

    class Config:
        from_attributes = True

class ProjectListResponse(BaseModel):
    total: int
    projects: list[ProjectOut]
    scan_status: Optional[str] = None

class ProjectFilters(BaseModel):
    categories: Optional[list[str]] = None
    sources: Optional[list[str]] = None
    min_match: Optional[int] = None
    min_value: Optional[float] = None
    status: Optional[str] = None
    search: Optional[str] = None
    sort_by: str = "match_score"
    sort_dir: str = "desc"
    limit: int = 50
    offset: int = 0


# ─── SAVED PROJECTS ─────────────────────────────────────────────────────────

class SaveProjectRequest(BaseModel):
    project_id: int
    notes: str = ""
    status: str = "interested"

class SavedProjectOut(BaseModel):
    id: int
    project_id: int
    notes: str
    status: str
    saved_at: datetime
    project: ProjectOut

    class Config:
        from_attributes = True


# ─── SCAN ────────────────────────────────────────────────────────────────────

class ScanTriggerResponse(BaseModel):
    message: str
    scan_id: Optional[int] = None

class ScanLogOut(BaseModel):
    id: int
    source_id: str
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    projects_found: int
    projects_new: int
    error_message: str

    class Config:
        from_attributes = True
