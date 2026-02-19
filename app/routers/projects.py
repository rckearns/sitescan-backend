"""Projects endpoints — list, filter, search, save, and manage opportunities."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_, or_, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from typing import Optional

from app.models.database import Project, SavedProject, get_db, User
from app.models.schemas import (
    ProjectOut, ProjectListResponse, SaveProjectRequest,
    SavedProjectOut,
)
from app.auth import get_current_user

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=ProjectListResponse)
async def list_projects(
    categories: Optional[str] = Query(None, description="Comma-separated category IDs"),
    sources: Optional[str] = Query(None, description="Comma-separated source IDs"),
    min_match: int = Query(0, ge=0, le=99),
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
    """List projects with filtering, sorting, and pagination."""
    query = select(Project)
    count_query = select(func.count(Project.id))
    
    # Filters
    conditions = []
    
    if active_only:
        conditions.append(Project.is_active == True)
    
    if categories:
        cat_list = [c.strip() for c in categories.split(",")]
        conditions.append(Project.category.in_(cat_list))
    
    if sources:
        src_list = [s.strip() for s in sources.split(",")]
        conditions.append(Project.source_id.in_(src_list))
    
    if min_match > 0:
        conditions.append(Project.match_score >= min_match)
    
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
        count_query = count_query.where(and_(*conditions))
    
    # Sorting
    sort_col = getattr(Project, sort_by, Project.match_score)
    if sort_by == "value":
        # Put nulls last
        query = query.order_by(
            Project.value.is_(None).asc() if sort_dir == "desc" else Project.value.is_(None).desc(),
            desc(sort_col) if sort_dir == "desc" else asc(sort_col),
        )
    else:
        query = query.order_by(desc(sort_col) if sort_dir == "desc" else asc(sort_col))
    
    # Count
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Paginate
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    projects = result.scalars().all()
    
    return ProjectListResponse(
        total=total,
        projects=[ProjectOut.model_validate(p) for p in projects],
    )


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
    return project


@router.get("/stats/summary")
async def project_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get summary statistics for the project pipeline."""
    result = await db.execute(
        select(
            func.count(Project.id).label("total"),
            func.sum(Project.value).label("total_value"),
            func.avg(Project.match_score).label("avg_match"),
            func.count(Project.id).filter(Project.match_score >= 80).label("high_match"),
        ).where(Project.is_active == True)
    )
    row = result.one()
    
    # Count by source
    source_result = await db.execute(
        select(
            Project.source_id,
            func.count(Project.id).label("count"),
        ).where(Project.is_active == True).group_by(Project.source_id)
    )
    by_source = {r[0]: r[1] for r in source_result.all()}
    
    # Count by category
    cat_result = await db.execute(
        select(
            Project.category,
            func.count(Project.id).label("count"),
        ).where(Project.is_active == True).group_by(Project.category)
    )
    by_category = {r[0]: r[1] for r in cat_result.all()}
    
    return {
        "total_projects": row.total or 0,
        "total_pipeline_value": row.total_value or 0,
        "avg_match_score": round(row.avg_match or 0, 1),
        "high_match_count": row.high_match or 0,
        "by_source": by_source,
        "by_category": by_category,
    }


# ─── SAVED PROJECTS ─────────────────────────────────────────────────────────

@router.post("/save", response_model=SavedProjectOut)
async def save_project(
    data: SaveProjectRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Save/bookmark a project."""
    # Check project exists
    result = await db.execute(select(Project).where(Project.id == data.project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Check if already saved
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
    
    # Reload with project relationship
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
