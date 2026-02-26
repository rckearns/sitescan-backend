"""Projects endpoints — list, filter, search, save, and manage opportunities."""

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from typing import Optional

from app.models.database import Project, SavedProject, ScanLog, get_db, User
from app.models.schemas import (
    ProjectOut, ProjectListResponse, SaveProjectRequest,
    SavedProjectOut,
)
from app.auth import get_current_user
from app.services.scoring import score_against_profile

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=ProjectListResponse)
async def list_projects(
    categories: Optional[str] = Query(None, description="Comma-separated category IDs"),
    sources: Optional[str] = Query(None, description="Comma-separated source IDs"),
    min_match: int = Query(0, ge=0, le=100),
    min_value: Optional[float] = Query(None),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None, description="Keyword search in title and description"),
    sort_by: str = Query("match_score", enum=["match_score", "value", "posted_date", "first_seen"]),
    sort_dir: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    active_only: bool = Query(True),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List projects with filtering, sorting, and pagination.

    Match scores are computed dynamically against the requesting user's
    saved criteria profile — not from a stored keyword score.
    """
    query = select(Project)
    conditions = []

    if active_only:
        conditions.append(Project.is_active == True)

    if categories:
        cat_list = [c.strip() for c in categories.split(",")]
        conditions.append(Project.category.in_(cat_list))

    if sources:
        src_list = [s.strip() for s in sources.split(",")]
        conditions.append(Project.source_id.in_(src_list))

    if min_value is not None:
        conditions.append(Project.value >= min_value)

    if status:
        conditions.append(Project.status == status)

    if search:
        search_term = f"%{search}%"
        conditions.append(
            or_(
                Project.title.ilike(search_term),
                Project.description.ilike(search_term),
                Project.agency.ilike(search_term),
                Project.location.ilike(search_term),
            )
        )

    if conditions:
        query = query.where(and_(*conditions))

    # Fetch all matching rows — scoring and min_match happen in Python
    result = await db.execute(query)
    all_projects = result.scalars().all()

    # Compute dynamic scores against user's profile criteria
    scored = [(p, score_against_profile(p, user)) for p in all_projects]

    # Apply min_match filter
    if min_match > 0:
        scored = [(p, s) for p, s in scored if s >= min_match]

    # Sort
    reverse = sort_dir == "desc"
    if sort_by == "match_score":
        scored.sort(key=lambda x: x[1], reverse=reverse)
    elif sort_by == "value":
        scored.sort(
            key=lambda x: (x[0].value is None, x[0].value or 0),
            reverse=reverse,
        )
    elif sort_by == "posted_date":
        scored.sort(
            key=lambda x: (x[0].posted_date is None, x[0].posted_date or datetime.min),
            reverse=reverse,
        )
    elif sort_by == "first_seen":
        scored.sort(key=lambda x: x[0].first_seen, reverse=reverse)

    total = len(scored)
    page = scored[offset: offset + limit]

    projects_out = []
    for p, score in page:
        p_out = ProjectOut.model_validate(p)
        p_out.match_score = score
        projects_out.append(p_out)

    return ProjectListResponse(total=total, projects=projects_out)


@router.get("/{project_id}", response_model=ProjectOut)
async def get_project(
    project_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get a single project by ID."""
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    p_out = ProjectOut.model_validate(project)
    p_out.match_score = score_against_profile(project, user)
    return p_out


@router.get("/stats/summary")
async def project_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get summary statistics computed against the user's scoring criteria."""
    # Fetch all active projects and score dynamically
    result = await db.execute(select(Project).where(Project.is_active == True))
    projects = result.scalars().all()

    scores = [score_against_profile(p, user) for p in projects]
    total = len(projects)
    total_value = sum(p.value for p in projects if p.value)
    avg_match = round(sum(scores) / total, 1) if total else 0
    high_match = sum(1 for s in scores if s >= 80)

    by_source = {}
    for p in projects:
        by_source[p.source_id] = by_source.get(p.source_id, 0) + 1

    by_category = {}
    for p in projects:
        by_category[p.category] = by_category.get(p.category, 0) + 1

    # Last successful scan timestamp
    last_scan_result = await db.execute(
        select(ScanLog)
        .where(ScanLog.status == "success")
        .order_by(desc(ScanLog.started_at))
        .limit(1)
    )
    last_scan = last_scan_result.scalar_one_or_none()

    return {
        "total_projects": total,
        "total_pipeline_value": total_value,
        "avg_match_score": avg_match,
        "high_match_count": high_match,
        "by_source": by_source,
        "by_category": by_category,
        "last_scan_at": last_scan.started_at.isoformat() if last_scan else None,
    }


# ─── SAVED PROJECTS ─────────────────────────────────────────────────────────

@router.post("/save", response_model=SavedProjectOut)
async def save_project(
    data: SaveProjectRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Save/bookmark a project."""
    result = await db.execute(select(Project).where(Project.id == data.project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    existing = await db.execute(
        select(SavedProject).where(
            SavedProject.user_id == user.id,
            SavedProject.project_id == data.project_id,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Project already saved")

    saved = SavedProject(
        user_id=user.id,
        project_id=data.project_id,
        notes=data.notes,
        status=data.status,
    )
    db.add(saved)
    await db.flush()

    result = await db.execute(
        select(SavedProject)
        .options(joinedload(SavedProject.project))
        .where(SavedProject.id == saved.id)
    )
    return result.scalar_one()


@router.get("/saved/list", response_model=list[SavedProjectOut])
async def list_saved_projects(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all saved projects for the current user."""
    result = await db.execute(
        select(SavedProject)
        .options(joinedload(SavedProject.project))
        .where(SavedProject.user_id == user.id)
        .order_by(SavedProject.saved_at.desc())
    )
    return result.scalars().all()


@router.delete("/saved/{saved_id}")
async def unsave_project(
    saved_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Remove a saved project."""
    result = await db.execute(
        select(SavedProject).where(
            SavedProject.id == saved_id,
            SavedProject.user_id == user.id,
        )
    )
    saved = result.scalar_one_or_none()
    if not saved:
        raise HTTPException(status_code=404, detail="Saved project not found")

    await db.delete(saved)
    return {"message": "Project removed from saved list"}
