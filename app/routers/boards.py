"""Board agenda endpoints — projects pipeline and stage-change events."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.models.database import BoardProject, BoardProjectEvent, get_db, User
from app.models.schemas import (
    BoardProjectOut, BoardProjectListResponse,
    BoardEventOut, BoardEventListResponse,
)
from app.auth import get_current_user

router = APIRouter(prefix="/boards", tags=["boards"])


@router.get("/projects", response_model=BoardProjectListResponse)
async def list_board_projects(
    stage: Optional[str] = Query(None, description="Filter by current_stage"),
    min_score: int = Query(0, ge=0, le=100, description="Minimum max_score"),
    neighborhood: Optional[str] = Query(None, description="Keyword match on neighborhood"),
    sort_by: str = Query("score", enum=["score", "last_seen", "first_seen"]),
    sort_dir: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List board projects with filtering by stage, score, and neighborhood.

    Sorted by score descending by default — highest-signal projects first.
    """
    query = select(BoardProject)
    conditions = []

    if stage:
        conditions.append(BoardProject.current_stage == stage)
    if min_score > 0:
        conditions.append(BoardProject.max_score >= min_score)
    if neighborhood:
        conditions.append(BoardProject.neighborhood.ilike(f"%{neighborhood}%"))

    if conditions:
        query = query.where(and_(*conditions))

    order_col = {
        "score":      BoardProject.max_score,
        "last_seen":  BoardProject.last_seen_at,
        "first_seen": BoardProject.first_seen_at,
    }.get(sort_by, BoardProject.max_score)

    query = query.order_by(
        desc(order_col) if sort_dir == "desc" else order_col
    )

    result = await db.execute(query)
    all_projects = result.scalars().all()
    total = len(all_projects)
    page = all_projects[offset: offset + limit]

    return BoardProjectListResponse(total=total, projects=page)


@router.get("/events", response_model=BoardEventListResponse)
async def list_board_events(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    unnotified_only: bool = Query(False, description="Only return events where notified_at IS NULL"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Recent board project stage-change events, newest first.

    Includes denormalized project fields (address, applicant, neighborhood)
    so the frontend can render event cards without a second request.
    """
    stmt = (
        select(BoardProjectEvent, BoardProject)
        .join(BoardProject, BoardProjectEvent.project_id == BoardProject.id)
        .order_by(desc(BoardProjectEvent.created_at))
    )
    if unnotified_only:
        stmt = stmt.where(BoardProjectEvent.notified_at.is_(None))

    result = await db.execute(stmt)
    rows = result.all()
    total = len(rows)
    page = rows[offset: offset + limit]

    events = [
        BoardEventOut(
            id=evt.id,
            project_id=evt.project_id,
            event_type=evt.event_type,
            from_stage=evt.from_stage,
            to_stage=evt.to_stage,
            score=evt.score,
            created_at=evt.created_at,
            project_address=proj.address,
            project_applicant=proj.applicant,
            project_neighborhood=proj.neighborhood,
        )
        for evt, proj in page
    ]

    return BoardEventListResponse(total=total, events=events)
