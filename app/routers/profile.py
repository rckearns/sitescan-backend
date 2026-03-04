"""Company profile endpoints — org info, project portfolio, key personnel, SOQ generation."""

from io import BytesIO

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import (
    Organization, OrgPrincipal, ProjectReference, KeyPersonnel,
    User, get_db,
)
from app.models.schemas import (
    OrgProfileOut, OrgProfileUpdate,
    OrgPrincipalIn, OrgPrincipalOut,
    ProjectRefIn, ProjectRefOut,
    KeyPersonnelIn, KeyPersonnelOut,
    SOQGenerateRequest,
)
from app.auth import get_current_user

router = APIRouter(prefix="/profile", tags=["profile"])


# ─── HELPERS ─────────────────────────────────────────────────────────────────

async def _load_org(org_id: int, db: AsyncSession) -> Organization:
    """Load org with all relationships eager-loaded."""
    result = await db.execute(
        select(Organization)
        .where(Organization.id == org_id)
        .options(
            selectinload(Organization.principals),
            selectinload(Organization.project_refs),
            selectinload(Organization.personnel),
        )
    )
    return result.scalar_one_or_none()


async def _get_or_create_org(user: User, db: AsyncSession) -> Organization:
    """Return the user's org, creating a blank one if they don't have one yet."""
    if user.org_id:
        org = await _load_org(user.org_id, db)
        if org:
            return org

    # Create a new org and link it to the user
    org = Organization()
    db.add(org)
    await db.flush()
    await db.refresh(org)

    user.org_id = org.id
    await db.flush()

    # Return with empty relationship lists
    org.principals = []
    org.project_refs = []
    org.personnel = []
    return org


def _assert_org(user: User) -> int:
    """Raise 404 if user has no org yet."""
    if not user.org_id:
        raise HTTPException(status_code=404, detail="No company profile found — GET /profile/org first")
    return user.org_id


# ─── ORG PROFILE ─────────────────────────────────────────────────────────────

@router.get("/org", response_model=OrgProfileOut)
async def get_org(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get the current user's org profile (auto-creates a blank one if needed)."""
    org = await _get_or_create_org(user, db)
    return org


@router.put("/org", response_model=OrgProfileOut)
async def update_org(
    data: OrgProfileUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update org profile fields (partial — omitted fields unchanged)."""
    org = await _get_or_create_org(user, db)
    for field, value in data.model_dump(exclude_none=True).items():
        setattr(org, field, value)
    await db.flush()
    org = await _load_org(org.id, db)
    return org


# ─── PRINCIPALS ──────────────────────────────────────────────────────────────

@router.post("/org/principals", response_model=OrgPrincipalOut, status_code=201)
async def add_principal(
    data: OrgPrincipalIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    principal = OrgPrincipal(org_id=org_id, **data.model_dump())
    db.add(principal)
    await db.flush()
    await db.refresh(principal)
    return principal


@router.patch("/org/principals/{principal_id}", response_model=OrgPrincipalOut)
async def update_principal(
    principal_id: int,
    data: OrgPrincipalIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    result = await db.execute(
        select(OrgPrincipal).where(
            OrgPrincipal.id == principal_id,
            OrgPrincipal.org_id == org_id,
        )
    )
    principal = result.scalar_one_or_none()
    if not principal:
        raise HTTPException(status_code=404, detail="Principal not found")
    for field, value in data.model_dump(exclude_none=True).items():
        setattr(principal, field, value)
    await db.flush()
    await db.refresh(principal)
    return principal


@router.delete("/org/principals/{principal_id}", status_code=204)
async def delete_principal(
    principal_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    result = await db.execute(
        select(OrgPrincipal).where(
            OrgPrincipal.id == principal_id,
            OrgPrincipal.org_id == org_id,
        )
    )
    principal = result.scalar_one_or_none()
    if not principal:
        raise HTTPException(status_code=404, detail="Principal not found")
    await db.delete(principal)


# ─── PROJECT REFERENCES ───────────────────────────────────────────────────────

@router.post("/org/projects", response_model=ProjectRefOut, status_code=201)
async def add_project_ref(
    data: ProjectRefIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    ref = ProjectReference(org_id=org_id, **data.model_dump())
    db.add(ref)
    await db.flush()
    await db.refresh(ref)
    return ref


@router.patch("/org/projects/{ref_id}", response_model=ProjectRefOut)
async def update_project_ref(
    ref_id: int,
    data: ProjectRefIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    result = await db.execute(
        select(ProjectReference).where(
            ProjectReference.id == ref_id,
            ProjectReference.org_id == org_id,
        )
    )
    ref = result.scalar_one_or_none()
    if not ref:
        raise HTTPException(status_code=404, detail="Project reference not found")
    for field, value in data.model_dump(exclude_none=True).items():
        setattr(ref, field, value)
    await db.flush()
    await db.refresh(ref)
    return ref


@router.delete("/org/projects/{ref_id}", status_code=204)
async def delete_project_ref(
    ref_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    result = await db.execute(
        select(ProjectReference).where(
            ProjectReference.id == ref_id,
            ProjectReference.org_id == org_id,
        )
    )
    ref = result.scalar_one_or_none()
    if not ref:
        raise HTTPException(status_code=404, detail="Project reference not found")
    await db.delete(ref)


# ─── KEY PERSONNEL ────────────────────────────────────────────────────────────

@router.post("/org/personnel", response_model=KeyPersonnelOut, status_code=201)
async def add_personnel(
    data: KeyPersonnelIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    person = KeyPersonnel(org_id=org_id, **data.model_dump())
    db.add(person)
    await db.flush()
    await db.refresh(person)
    return person


@router.patch("/org/personnel/{person_id}", response_model=KeyPersonnelOut)
async def update_personnel(
    person_id: int,
    data: KeyPersonnelIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    result = await db.execute(
        select(KeyPersonnel).where(
            KeyPersonnel.id == person_id,
            KeyPersonnel.org_id == org_id,
        )
    )
    person = result.scalar_one_or_none()
    if not person:
        raise HTTPException(status_code=404, detail="Personnel not found")
    for field, value in data.model_dump(exclude_none=True).items():
        setattr(person, field, value)
    await db.flush()
    await db.refresh(person)
    return person


@router.delete("/org/personnel/{person_id}", status_code=204)
async def delete_personnel(
    person_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    org_id = _assert_org(user)
    result = await db.execute(
        select(KeyPersonnel).where(
            KeyPersonnel.id == person_id,
            KeyPersonnel.org_id == org_id,
        )
    )
    person = result.scalar_one_or_none()
    if not person:
        raise HTTPException(status_code=404, detail="Personnel not found")
    await db.delete(person)


# ─── SOQ GENERATION ───────────────────────────────────────────────────────────

@router.post("/soq/generate")
async def generate_soq_endpoint(
    req: SOQGenerateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Generate a filled SOQ .docx from the org's profile data.

    Returns the document as a downloadable file. The template must have
    {{ placeholder }} tags — see app/templates/soq_template.docx.
    """
    org_id = _assert_org(user)
    org = await _load_org(org_id, db)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch PM
    pm = None
    if req.pm_id:
        result = await db.execute(
            select(KeyPersonnel).where(
                KeyPersonnel.id == req.pm_id,
                KeyPersonnel.org_id == org_id,
            )
        )
        pm = result.scalar_one_or_none()
        if not pm:
            raise HTTPException(status_code=404, detail=f"PM (id={req.pm_id}) not found")

    # Fetch superintendent
    superintendent = None
    if req.super_id:
        result = await db.execute(
            select(KeyPersonnel).where(
                KeyPersonnel.id == req.super_id,
                KeyPersonnel.org_id == org_id,
            )
        )
        superintendent = result.scalar_one_or_none()
        if not superintendent:
            raise HTTPException(status_code=404, detail=f"Superintendent (id={req.super_id}) not found")

    # Fetch project references (validate all belong to org)
    async def _fetch_refs(ids: list[int]) -> list:
        if not ids:
            return []
        result = await db.execute(
            select(ProjectReference).where(
                ProjectReference.id.in_(ids),
                ProjectReference.org_id == org_id,
            )
        )
        refs = {r.id: r for r in result.scalars().all()}
        missing = [i for i in ids if i not in refs]
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f"Project reference(s) not found: {missing}",
            )
        return [refs[i] for i in ids]  # preserve requested order

    general_projects = await _fetch_refs(req.general_project_ids)
    state_projects = await _fetch_refs(req.state_project_ids)

    from app.services.soq_generator import generate_soq
    try:
        docx_bytes = generate_soq(org, pm, superintendent, general_projects, state_projects)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SOQ generation failed: {e}")

    safe_name = (org.legal_name or "Company").replace(" ", "_").replace("/", "-")
    filename = f"SOQ_{safe_name}.docx"

    return StreamingResponse(
        BytesIO(docx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
