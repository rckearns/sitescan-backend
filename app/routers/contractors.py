"""Contractors endpoints — manage GC and subcontractor lists per user."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import Contractor, get_db, User
from app.models.schemas import ContractorCreate, ContractorUpdate, ContractorOut
from app.auth import get_current_user

router = APIRouter(prefix="/contractors", tags=["contractors"])


@router.get("", response_model=list[ContractorOut])
async def list_contractors(
    type: str | None = None,          # filter by "gc" or "sub"
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all contractors for the current user, optionally filtered by type."""
    q = select(Contractor).where(Contractor.user_id == user.id)
    if type in ("gc", "sub"):
        q = q.where(Contractor.type == type)
    q = q.order_by(Contractor.name)
    result = await db.execute(q)
    return result.scalars().all()


@router.post("", response_model=ContractorOut, status_code=201)
async def create_contractor(
    data: ContractorCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Add a contractor to the user's list."""
    contractor = Contractor(
        user_id=user.id,
        name=data.name.strip(),
        type=data.type if data.type in ("gc", "sub") else "gc",
        specialty=data.specialty,
        phone=data.phone,
        email=data.email,
        website=data.website,
        notes=data.notes,
    )
    db.add(contractor)
    await db.flush()
    await db.refresh(contractor)
    return contractor


@router.patch("/{contractor_id}", response_model=ContractorOut)
async def update_contractor(
    contractor_id: int,
    data: ContractorUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a contractor's details."""
    result = await db.execute(
        select(Contractor).where(
            Contractor.id == contractor_id,
            Contractor.user_id == user.id,
        )
    )
    contractor = result.scalar_one_or_none()
    if not contractor:
        raise HTTPException(status_code=404, detail="Contractor not found")

    for field, value in data.model_dump(exclude_none=True).items():
        setattr(contractor, field, value)

    await db.flush()
    await db.refresh(contractor)
    return contractor


@router.delete("/{contractor_id}", status_code=204)
async def delete_contractor(
    contractor_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Remove a contractor from the user's list."""
    result = await db.execute(
        select(Contractor).where(
            Contractor.id == contractor_id,
            Contractor.user_id == user.id,
        )
    )
    contractor = result.scalar_one_or_none()
    if not contractor:
        raise HTTPException(status_code=404, detail="Contractor not found")
    await db.delete(contractor)
