"""State award notices (shortlists, awards, prequalified bidders) from Charleston-area agencies."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.models.database import ContractAward, get_db
from app.services.awards import AGENCY_PAGES, award_out, firm_rankings

router = APIRouter(prefix="/awards", tags=["awards"])

CONSTRUCTION_ROLES = ("contractor", "cm", "design-builder")


@router.get("")
async def list_awards(
    role: str = Query("construction", description="construction (GCs, CMs, design-builders), design (A/E) or all"),
    days: int = Query(0, ge=0, le=3650, description="Look-back in days by posting date (0 = everything kept)"),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """Firms ranked by state awards and shortlists, plus the latest notices."""
    q = select(ContractAward)
    if role == "construction":
        q = q.where(ContractAward.role.in_(CONSTRUCTION_ROLES))
    elif role == "design":
        q = q.where(ContractAward.role == "architect-engineer")
    if days:
        q = q.where(ContractAward.posted_date >= datetime.utcnow() - timedelta(days=days))
    rows = (await db.execute(q)).scalars().all()
    recent = sorted(rows, key=lambda r: r.posted_date or datetime.min, reverse=True)[:60]
    return {
        "agencies": [a for a, _ in AGENCY_PAGES],
        "total_notices": len({r.source_url for r in rows}),
        "firms": firm_rankings(rows),
        "recent": [award_out(r) for r in recent],
    }


@router.get("/project")
async def awards_for_project(
    pip: str = Query(..., description="State project number, e.g. H15.9689"),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    rows = (await db.execute(select(ContractAward).where(ContractAward.pip_number == pip))).scalars().all()
    return {"pip": pip, "awards": [award_out(r) for r in sorted(rows, key=lambda r: r.posted_date or datetime.min)]}
