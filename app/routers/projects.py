"""Projects endpoints — list, filter, search, save, and manage opportunities."""

import math
import statistics
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from typing import Optional
import httpx


def _safe_float(v):
    """Return None if v is None or NaN (JSON-incompatible float)."""
    if v is None:
        return None
    try:
        return None if math.isnan(float(v)) else float(v)
    except (TypeError, ValueError):
        return None

from app.models.database import Project, SavedProject, ScanLog, get_db, User
from app.models.schemas import (
    ProjectOut, ProjectListResponse, SaveProjectRequest,
    SavedProjectOut, SubcontractorProjectOut, SubcontractorOut,
    SubcontractorListResponse,
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
    limit: int = Query(50, ge=1, le=2000),
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
        # Exclude finaled/complete permits from the project list — they still exist
        # in the DB for contractor discovery but aren't actionable opportunities
        conditions.append(Project.status.not_in(["Completed", "Void", "Cancelled"]))

    # Residential and trade-permit categories are not shown in the project list.
    # Trade permits are stored for contractor discovery only (see /subcontractors endpoint).
    _HIDDEN_CATS = ["residential", "trade-permit", "electrical", "fire-sprinkler", "plumbing", "mechanical", "roofing"]
    conditions.append(Project.category.not_in(_HIDDEN_CATS))

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
        # Treat None/0 as 0 — sorts to bottom on high-to-low, top on low-to-high
        scored.sort(key=lambda x: x[0].value or 0, reverse=reverse)
    elif sort_by == "posted_date":
        # Records without a posted_date sort to the bottom regardless of direction.
        # Secondary key is first_seen so undated records still have a meaningful order.
        scored.sort(
            key=lambda x: (
                x[0].posted_date is not None,
                x[0].posted_date or x[0].first_seen,
            ),
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
        p_out.latitude = _safe_float(p_out.latitude)
        p_out.longitude = _safe_float(p_out.longitude)
        projects_out.append(p_out)

    return ProjectListResponse(total=total, projects=projects_out)


@router.get("/map/points")
async def map_points(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return all active projects that have coordinates, for map display.

    Returns a lightweight payload — no 100-project limit — so the map
    always shows everything available regardless of scanner filters.
    """
    result = await db.execute(
        select(Project).where(
            Project.is_active == True,
            Project.category.not_in(["residential", "trade-permit", "electrical", "fire-sprinkler", "plumbing", "mechanical", "roofing"]),
            Project.status.not_in(["Completed", "Void", "Cancelled"]),
            Project.latitude.is_not(None),
            Project.longitude.is_not(None),
        )
    )
    projects = result.scalars().all()

    result_list = []
    for p in projects:
        lat = _safe_float(p.latitude)
        lng = _safe_float(p.longitude)
        if lat is None or lng is None:
            continue  # skip records with NaN coordinates stored in DB
        result_list.append({
            "id": p.id,
            "title": p.title,
            "location": p.location,
            "latitude": lat,
            "longitude": lng,
            "match_score": score_against_profile(p, user),
            "value": _safe_float(p.value),
            "status": p.status,
            "category": p.category,
            "source_id": p.source_id,
            "source_url": p.source_url,
        })
    return result_list


@router.get("/map/parcels")
async def map_parcels(
    west: float = Query(...),
    south: float = Query(...),
    east: float = Query(...),
    north: float = Query(...),
    limit: int = Query(800, ge=1, le=1000),
    user: User = Depends(get_current_user),
):
    """Proxy parcel GeoJSON from Charleston ArcGIS to avoid browser CORS restrictions.

    The GIS server does not send Access-Control-Allow-Origin headers, so the
    browser cannot call it directly. This endpoint fetches server-to-server and
    forwards the GeoJSON back to the client.
    """
    arcgis_url = (
        "https://gis.charleston-sc.gov/arcgis2/rest/services/"
        "External/Zoning/MapServer/26/query"
    )
    params = {
        "geometry": f"{west},{south},{east},{north}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "outSR": "4326",
        "outFields": "TMS,PARCELID,OWNER,STREET,HOUSE,GENUSE,YRBUILT,APPRVAL,IMP_APPR,LAND_APPR",
        "f": "geojson",
        "resultRecordCount": str(limit),
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(arcgis_url, params=params)
            resp.raise_for_status()
            return Response(content=resp.content, media_type="application/geo+json")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"ArcGIS request failed: {e}")


@router.get("/subcontractors", response_model=SubcontractorListResponse)
async def list_subcontractors(
    source: Optional[str] = Query(None, description="Filter by source_id (e.g. charleston-permits)"),
    category: Optional[str] = Query(None, description="Filter by trade category"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all contractors with their full project portfolios, total scope value,
    and median project size. Sorted by total scope value descending."""
    conditions = [
        Project.is_active == True,
        Project.contractor != "",
        Project.contractor.is_not(None),
    ]
    if source:
        conditions.append(Project.source_id == source)
    if category:
        conditions.append(Project.category == category)

    result = await db.execute(select(Project).where(and_(*conditions)))
    projects = result.scalars().all()

    contractor_map: dict[str, list] = {}
    for p in projects:
        raw = (p.contractor or "").strip()
        if not raw:
            continue
        for name in raw.split("|"):
            name = name.strip()
            if name:
                contractor_map.setdefault(name, []).append(p)

    subcontractors = []
    for name, projs in contractor_map.items():
        values = [p.value for p in projs if p.value]
        subcontractors.append(SubcontractorOut(
            name=name,
            project_count=len(projs),
            total_scope_value=sum(values),
            median_project_value=statistics.median(values) if values else None,
            projects=[SubcontractorProjectOut.model_validate(p) for p in projs],
        ))

    subcontractors.sort(key=lambda x: x.total_scope_value, reverse=True)
    return SubcontractorListResponse(
        total_subcontractors=len(subcontractors),
        subcontractors=subcontractors,
    )


@router.get("/subcontractors/by-trade")
async def subcontractors_by_trade(
    source: str = Query("charleston-permits", description="Source to analyze"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List contractors grouped by trade/category.

    Defaults to charleston-permits. Each trade bucket lists its contractors
    sorted by total scope value descending, with individual project details,
    total scope value, and median project size per contractor.
    """
    result = await db.execute(
        select(Project).where(
            Project.is_active == True,
            Project.source_id == source,
            Project.contractor != "",
            Project.contractor.is_not(None),
        )
    )
    projects = result.scalars().all()

    # Build trade → contractor → [projects] map
    trade_map: dict[str, dict[str, list]] = {}
    for p in projects:
        raw = (p.contractor or "").strip()
        if not raw:
            continue
        trade = p.category or "unknown"
        for name in raw.split("|"):
            name = name.strip()
            if name:
                trade_map.setdefault(trade, {}).setdefault(name, []).append(p)

    trades: dict[str, list] = {}
    for trade, contractor_map in sorted(trade_map.items()):
        subs = []
        for name, projs in contractor_map.items():
            values = [p.value for p in projs if p.value]
            subs.append({
                "name": name,
                "project_count": len(projs),
                "total_scope_value": sum(values),
                "median_project_value": statistics.median(values) if values else None,
                "projects": [SubcontractorProjectOut.model_validate(p) for p in projs],
            })
        subs.sort(key=lambda x: x["total_scope_value"], reverse=True)
        trades[trade] = subs

    return {"source": source, "trades": trades}


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
    result = await db.execute(
        select(Project).where(
            Project.is_active == True,
            Project.category.not_in(["residential", "trade-permit", "electrical", "fire-sprinkler", "plumbing", "mechanical", "roofing"]),
            Project.status.not_in(["Completed", "Void", "Cancelled"]),
        )
    )
    projects = result.scalars().all()

    scores = [score_against_profile(p, user) for p in projects]
    total = len(projects)
    total_value = sum(p.value for p in projects if p.value)
    avg_match = round(sum(scores) / total, 1) if total else 0
    high_match = sum(1 for s in scores if s >= 80)

    from datetime import datetime, timezone, timedelta
    _week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    new_this_week = sum(
        1 for p in projects
        if p.posted_date and p.posted_date.replace(tzinfo=timezone.utc) > _week_ago
    )
    bids_open = sum(1 for p in projects if p.status in ("Open", "Accepting Bids"))

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
        "new_this_week": new_this_week,
        "bids_open": bids_open,
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
