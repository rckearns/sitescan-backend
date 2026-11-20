"""GC pipeline API: projects from early approvals through solicitation, matched to the user's profile.

Also:
- research notes (admin only): hand-entered public facts added to a project's timeline
- saves (per user, private): the caller's status and notes on a project
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status as http_status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_admin
from app.models.database import PIPELINE_SAVE_STATUSES, PipelineProject, PipelineSave, User, get_db
from app.services.pipeline.match import match_for_user
from app.services.pipeline.notes import ResearchNote, add_notes

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_STATUS_ORDER = {"match": 0, "unconfirmed": 1, "excluded": 2}
NOTES_MAX_CHARS = 10_000


def _iso(d):
    return d.isoformat() if d else None


def _save_out(s: Optional[PipelineSave]) -> Optional[dict]:
    if s is None:
        return None
    return {"status": s.status, "notes": s.notes or "", "updated_at": _iso(s.updated_at)}


def _project_out(p: PipelineProject, status: str, reasons: list, saved: Optional[PipelineSave] = None) -> dict:
    return {
        "id": p.id,
        "project_key": p.project_key,
        "title": p.title,
        "owner": p.owner,
        "pip_number": p.pip_number,
        "address": p.address,
        "city": p.city,
        "current_stage": p.current_stage,
        "first_event_date": _iso(p.first_event_date),
        "last_event_date": _iso(p.last_event_date),
        "next_deadline": _iso(p.next_deadline),
        "delivery_method": p.delivery_method,
        "delivery_basis": p.delivery_basis,
        "construction_type": p.construction_type,
        "construction_reason": p.construction_reason,
        "building_type": p.building_type,
        "estimate": p.estimate,
        "estimate_basis": p.estimate_basis,
        "in_charleston_area": p.in_charleston_area,
        "is_building_project": p.is_building_project,
        "summary": p.summary,
        "match": status,
        "match_reasons": reasons,
        "saved": _save_out(saved),
        "events": [
            {
                "source": e.source, "stage": e.stage, "date": _iso(e.event_date), "title": e.title,
                "url": e.source_url, "delivery_method": e.delivery_method, "estimate": e.estimate,
                "cost_low": e.cost_low, "cost_high": e.cost_high, "deadline": _iso(e.deadline),
            }
            for e in sorted(p.events, key=lambda e: (e.event_date is None, e.event_date))
        ],
    }


async def _saves_by_project(db: AsyncSession, user) -> dict:
    user_id = getattr(user, "id", None)
    if user_id is None:
        return {}
    rows = (await db.execute(select(PipelineSave).where(PipelineSave.user_id == user_id))).scalars().all()
    return {s.project_id: s for s in rows}


@router.get("/projects")
async def list_pipeline_projects(
    include_excluded: bool = Query(False, description="Also return projects that conflict with the profile"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    saved_only: bool = False,
):
    """Pipeline projects matched against the user's GC preferences, best matches and newest first.

    Projects the user saved are always returned (with their match status), and
    each project carries the caller's own `saved` status and notes, or null.
    `saved_only=true` returns just those.
    """
    saves = await _saves_by_project(db, user)
    rows = (await db.execute(
        select(PipelineProject).options(selectinload(PipelineProject.events))
        .where(PipelineProject.ai_version > 0)
    )).scalars().all()
    out, counts = [], {"match": 0, "unconfirmed": 0, "excluded": 0}
    for p in rows:
        status, reasons = match_for_user(p, user)
        counts[status] += 1
        saved = saves.get(p.id)
        if saved_only and saved is None:
            continue
        if status == "excluded" and not include_excluded and saved is None:
            continue
        out.append(_project_out(p, status, reasons, saved))
    out.sort(key=lambda x: (_STATUS_ORDER[x["match"]], -(_ts(x["last_event_date"]))))
    return {"projects": out, "counts": counts}


def _ts(iso):
    return datetime.fromisoformat(iso).timestamp() if iso else 0


# ─── Research notes (admin) ──────────────────────────────────────────────────

class ResearchNotesIn(BaseModel):
    notes: list[ResearchNote] = Field(..., min_length=1, max_length=50)
    allow_new_project: bool = Field(False, description="Create the project if no project has this key")


@router.post("/notes")
async def create_research_notes(
    body: ResearchNotesIn,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    """Add research notes to pipeline projects. Every user sees them.

    Use for public facts only (hearings, design packets, press). A note dated
    after a project's last hearing doesn't change its stage. Re-sending the
    same note is a no-op ("duplicate").
    """
    results = await add_notes(db, body.notes, allow_new_project=body.allow_new_project)
    await db.commit()
    return {"results": results}


# ─── Saves (per user, private) ───────────────────────────────────────────────

class PipelineSaveIn(BaseModel):
    status: str = Field("interested", description=f"One of: {', '.join(PIPELINE_SAVE_STATUSES)}")
    notes: str = Field("", max_length=NOTES_MAX_CHARS)

    @field_validator("status")
    @classmethod
    def _status(cls, v: str) -> str:
        if v not in PIPELINE_SAVE_STATUSES:
            raise ValueError(f"status must be one of {', '.join(PIPELINE_SAVE_STATUSES)}")
        return v


@router.put("/projects/{project_id}/save")
async def save_pipeline_project(
    project_id: int,
    body: PipelineSaveIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create or update the caller's private status and notes for a project."""
    if await db.get(PipelineProject, project_id) is None:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Pipeline project not found")
    save = (await db.execute(select(PipelineSave).where(and_(
        PipelineSave.user_id == user.id, PipelineSave.project_id == project_id,
    )))).scalars().first()
    if save is None:
        save = PipelineSave(user_id=user.id, project_id=project_id)
        db.add(save)
    save.status = body.status
    save.notes = body.notes
    save.updated_at = datetime.utcnow()
    await db.commit()
    return {"project_id": project_id, "saved": _save_out(save)}


@router.delete("/projects/{project_id}/save")
async def unsave_pipeline_project(
    project_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Remove the caller's status and notes for a project (no-op if none)."""
    save = (await db.execute(select(PipelineSave).where(and_(
        PipelineSave.user_id == user.id, PipelineSave.project_id == project_id,
    )))).scalars().first()
    if save is not None:
        await db.delete(save)
        await db.commit()
    return {"project_id": project_id, "saved": None}
