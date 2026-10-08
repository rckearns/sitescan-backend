"""GC pipeline API: projects from early approvals through solicitation, matched to the user's profile."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user
from app.models.database import PipelineProject, User, get_db
from app.services.pipeline.match import match_for_user

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_STATUS_ORDER = {"match": 0, "unconfirmed": 1, "excluded": 2}


def _iso(d):
    return d.isoformat() if d else None


def _project_out(p: PipelineProject, status: str, reasons: list) -> dict:
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
        "events": [
            {
                "source": e.source, "stage": e.stage, "date": _iso(e.event_date), "title": e.title,
                "url": e.source_url, "delivery_method": e.delivery_method, "estimate": e.estimate,
                "cost_low": e.cost_low, "cost_high": e.cost_high, "deadline": _iso(e.deadline),
            }
            for e in sorted(p.events, key=lambda e: (e.event_date is None, e.event_date))
        ],
    }


@router.get("/projects")
async def list_pipeline_projects(
    include_excluded: bool = Query(False, description="Also return projects that conflict with the profile"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Pipeline projects matched against the user's GC preferences, best matches and newest first."""
    rows = (await db.execute(
        select(PipelineProject).options(selectinload(PipelineProject.events))
        .where(PipelineProject.ai_version > 0)
    )).scalars().all()
    out, counts = [], {"match": 0, "unconfirmed": 0, "excluded": 0}
    for p in rows:
        status, reasons = match_for_user(p, user)
        counts[status] += 1
        if status == "excluded" and not include_excluded:
            continue
        out.append(_project_out(p, status, reasons))
    out.sort(key=lambda x: (_STATUS_ORDER[x["match"]], -(_ts(x["last_event_date"]))))
    return {"projects": out, "counts": counts}


def _ts(iso):
    from datetime import datetime
    return datetime.fromisoformat(iso).timestamp() if iso else 0
