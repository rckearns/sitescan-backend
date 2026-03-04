"""Company profile endpoints — org info, project portfolio, key personnel, SOQ generation."""

from io import BytesIO

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
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

    user.org_id = org.id
    await db.flush()

    # Re-load with selectinload so relationships are eagerly loaded —
    # avoids MissingGreenlet errors when Pydantic serializes them synchronously.
    return await _load_org(org.id, db)


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
    try:
        org = await _get_or_create_org(user, db)
        # Build a plain dict to avoid any SQLAlchemy lazy-load / MissingGreenlet
        # issues when Pydantic synchronously serializes the response model.
        def _principal(p):
            return {"id": p.id, "name": p.name or "", "title": p.title or "",
                    "other_businesses": p.other_businesses or "", "order": p.order or 0}
        def _ref(r):
            return {"id": r.id, "ref_type": r.ref_type or "general",
                    "project_name": r.project_name or "", "owner_name": r.owner_name or "",
                    "owner_contact": r.owner_contact or "", "owner_phone": r.owner_phone or "",
                    "contract_value": r.contract_value, "completion_date": r.completion_date or "",
                    "description": r.description or "", "scope_of_work": r.scope_of_work or "",
                    "your_role": r.your_role or ""}
        def _person(k):
            return {"id": k.id, "name": k.name or "", "role": k.role or "pm",
                    "resume_summary": k.resume_summary or "", "projects": k.projects or []}
        return {
            "id": org.id,
            "legal_name": org.legal_name or "",
            "entity_type": org.entity_type or "",
            "address_street": org.address_street or "",
            "address_city": org.address_city or "",
            "address_state": org.address_state or "",
            "address_zip": org.address_zip or "",
            "phone": org.phone or "",
            "fax": org.fax or "",
            "email": org.email or "",
            "website": org.website or "",
            "contractor_license_number": org.contractor_license_number or "",
            "license_classifications": org.license_classifications or [],
            "insurance_company": org.insurance_company or "",
            "insurance_agent_name": org.insurance_agent_name or "",
            "insurance_agent_phone": org.insurance_agent_phone or "",
            "bonding_company": org.bonding_company or "",
            "bonding_agent_name": org.bonding_agent_name or "",
            "bonding_agent_phone": org.bonding_agent_phone or "",
            "bonding_capacity": org.bonding_capacity or "",
            "emr": org.emr or "",
            "safety_meeting_frequency": org.safety_meeting_frequency or "",
            "compliance_flags": org.compliance_flags or {},
            "principals": [_principal(p) for p in (org.principals or [])],
            "project_refs": [_ref(r) for r in (org.project_refs or [])],
            "personnel": [_person(k) for k in (org.personnel or [])],
        }
    except Exception as e:
        import logging as _lg
        _lg.getLogger("sitescan.profile").error(f"GET /org failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Profile load failed: {type(e).__name__}: {e}")


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


# ─── BID ASSIST ───────────────────────────────────────────────────────────────

class BidAssistRequest(BaseModel):
    rfq_text: str


@router.post("/bid-assist")
async def bid_assist(
    req: BidAssistRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Generate a bid narrative from the org profile + RFQ text using Claude."""
    org_id = _assert_org(user)
    org = await _load_org(org_id, db)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    from app.services.bid_assist import generate_bid_narrative
    try:
        narrative = generate_bid_narrative(org, req.rfq_text)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bid assist failed: {e}")

    return {"narrative": narrative}


@router.post("/bid-assist/parse-pdf")
async def parse_pdf(
    files: list[UploadFile] = File(...),
    user: User = Depends(get_current_user),
):
    """Extract text from one or more uploaded PDFs and return combined text."""
    try:
        from pypdf import PdfReader
    except ImportError:
        raise HTTPException(status_code=503, detail="pypdf not installed — add it to requirements.txt")

    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    all_texts = []
    for file in files:
        if not file.filename or not file.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"{file.filename}: only PDF files are supported")

        contents = await file.read()
        if len(contents) > 50 * 1024 * 1024:
            raise HTTPException(status_code=413, detail=f"{file.filename}: file too large (max 50 MB)")

        try:
            reader = PdfReader(BytesIO(contents))
            pages = [page.extract_text() or "" for page in reader.pages]
            text = "\n\n".join(p.strip() for p in pages if p.strip())
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"{file.filename}: could not parse PDF: {e}")

        if text.strip():
            header = f"=== {file.filename} ===\n" if len(files) > 1 else ""
            all_texts.append(header + text)

    combined = "\n\n".join(all_texts)
    if not combined.strip():
        raise HTTPException(status_code=422, detail="No text could be extracted (PDFs may be image-only)")

    return {"text": combined[:12000]}
