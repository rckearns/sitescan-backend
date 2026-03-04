"""Scan endpoints."""
import asyncio
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.models.database import ScanLog, User, get_db
from app.models.schemas import ScanTriggerResponse, ScanLogOut
from app.auth import get_current_user
from app.services.orchestrator import run_full_scan, run_source_scan
from app.services.notifications import process_alerts
from app.config import get_settings

router = APIRouter(prefix="/scan", tags=["scan"])

async def _run_scan_background(sam_key, keywords, state, source_list):
    """Background task: run full scan then process alerts."""
    await run_full_scan(
        sam_api_key=sam_key,
        keywords=keywords,
        state=state,
        sources=source_list,
    )
    await process_alerts()


@router.post("/trigger", response_model=ScanTriggerResponse)
async def trigger_scan(
    background_tasks: BackgroundTasks,
    sources: Optional[str] = Query(None, description="Comma-separated source IDs (or all)"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    source_list = None
    if sources:
        source_list = [s.strip() for s in sources.split(",")]
    settings = get_settings()
    sam_key = user.sam_gov_api_key or settings.sam_gov_api_key or ""
    keywords = user.search_keywords or "masonry restoration structural"
    state = user.search_state or "SC"
    background_tasks.add_task(_run_scan_background, sam_key, keywords, state, source_list)
    return ScanTriggerResponse(
        message="Scan started — CHS Permits takes ~10-15 min. Check History when complete.",
        scan_id=None,
    )

@router.get("/history", response_model=list[ScanLogOut])
async def scan_history(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(ScanLog).order_by(desc(ScanLog.started_at)).limit(limit)
    )
    return result.scalars().all()

@router.get("/connectivity")
async def test_connectivity(user: User = Depends(get_current_user)):
    """Test SCBO and EnerGov connectivity directly from this server."""
    import httpx
    from datetime import datetime
    results = {}

    # Test SCBO (uses same path as the scanner — ZenRows if key set, else curl-cffi)
    try:
        import os as _os
        import re as _re
        from app.services.scanners import _fetch_scbo_html
        _zenrows_key = _os.environ.get("ZENROWS_API_KEY", "")
        today = datetime.utcnow()
        date_str = f"{today.year}-{today.month:02d}-{today.day:02d}"
        url = f"https://scbo.sc.gov/online-edition?c=3-{date_str}"
        html = await _fetch_scbo_html(url)
        # Count how many project names the parser can actually extract (not just markers)
        parsed_names = 0
        for chunk in html.split("<b>Project Name:</b>")[1:]:
            m = _re.search(r'margin-right:0\.5%["\']?>(.*?)</div>', chunk, _re.DOTALL)
            name = _re.sub(r'<[^>]+>', ' ', m.group(1)).strip() if m else ""
            if name and len(name) >= 3:
                parsed_names += 1
        results["scbo"] = {
            "via_zenrows": bool(_zenrows_key),
            "response_bytes": len(html),
            "has_project_markers": "<b>Project Name:</b>" in html,
            "raw_marker_count": html.count("<b>Project Name:</b>"),
            "parsed_project_count": parsed_names,
            "preview": html[:400],
        }
    except Exception as e:
        results["scbo"] = {"error": str(e)}

    # Test ArcGIS — Layer 20 (active permits) and Layer 21 (new construction since 2010)
    try:
        _ARCGIS_BASE = "https://gis.charleston-sc.gov/arcgis2/rest/services/External/Applications/MapServer"
        async with httpx.AsyncClient(timeout=20.0) as client:
            r20, r21 = await asyncio.gather(
                client.get(f"{_ARCGIS_BASE}/20/query", params={
                    "where": "PERMIT_TYPE = 'Building Commercial' AND PERMIT_STATUS NOT IN ('Void','Cancelled')",
                    "outFields": "OBJECTID", "resultRecordCount": "5", "f": "json",
                }),
                client.get(f"{_ARCGIS_BASE}/21/query", params={
                    "where": "PERMIT_STATUS <> 'Void' AND PERMIT_STATUS <> 'Cancelled'",
                    "outFields": "OBJECTID,PERMIT_STATUS", "resultRecordCount": "5", "f": "json",
                }),
            )
        d20 = r20.json()
        d21 = r21.json()
        results["arcgis"] = {
            "layer20_status": r20.status_code,
            "layer20_features": len(d20.get("features", [])),
            "layer20_error": d20.get("error"),
            "layer21_status": r21.status_code,
            "layer21_features": len(d21.get("features", [])),
            "layer21_error": d21.get("error"),
            "layer21_sample_statuses": [
                f.get("attributes", {}).get("PERMIT_STATUS") for f in d21.get("features", [])
            ],
        }
    except Exception as e:
        results["arcgis"] = {"error": str(e)}

    # Test EnerGov with 110 Calhoun (Emanuel Nine Memorial)
    try:
        pmpermitid = "47738947-fc93-440c-8b9e-4e3dc68b45cc"
        headers = {
            "tenantId": "1", "tenantName": "CharlestonSC",
            "Tyler-TenantUrl": "CharlestonSC", "Tyler-Tenant-Culture": "en-US",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(timeout=35.0) as client:
            resp = await client.get(
                f"https://egcss.charleston-sc.gov/EnerGov_Prod/selfservice/api/energov/permits/permit/{pmpermitid}",
                headers=headers,
            )
            data = resp.json()
            contacts = (data.get("Result") or {}).get("Contacts") or []
            results["energov"] = {
                "status_code": resp.status_code,
                "contacts_found": len(contacts),
                "contractors": [c.get("GlobalEntityName") for c in contacts
                                if (c.get("ContactTypeName") or "").lower() == "contractor"],
            }
    except Exception as e:
        results["energov"] = {"error": str(e)}

    # Test North Charleston ArcGIS PermitCustomers MapServer
    try:
        nc_arcgis = (
            "https://arc.northcharleston.org/arcgis/rest/services/Admin/PermitCustomers/MapServer"
        )
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            r = await client.get(f"{nc_arcgis}?f=json")
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            results["north_charleston_arcgis"] = {
                "status_code": r.status_code,
                "has_layers": bool(body.get("layers")),
                "error": body.get("error"),
                "preview": r.text[:200],
            }
    except Exception as e:
        results["north_charleston_arcgis"] = {"error": str(e)}

    # Test Mt. Pleasant AGOL search
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                "https://gis-tomp.maps.arcgis.com/sharing/rest/search",
                params={"q": "permit owner:gis-tomp", "num": 5, "f": "json"},
            )
            data = r.json() if r.status_code == 200 else {}
            results["mt_pleasant_agol"] = {
                "status_code": r.status_code,
                "total_results": data.get("total", 0),
                "items": [(i.get("title"), i.get("type")) for i in data.get("results", [])],
            }
    except Exception as e:
        results["mt_pleasant_agol"] = {"error": str(e)}

    return results


@router.get("/debug-permits")
async def debug_permits(
    address: str = Query("CALHOUN", description="Address substring to search"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Show DB state for permits matching an address — useful for diagnosing contractor enrichment."""
    from app.models.database import Project
    result = await db.execute(
        select(Project).where(Project.address.ilike(f"%{address}%"))
    )
    permits = result.scalars().all()
    return [
        {
            "external_id": p.external_id,
            "address": p.address,
            "title": p.title[:80],
            "contractor": p.contractor or "(empty)",
            "is_active": p.is_active,
            "category": p.category,
            "last_seen": p.last_seen.isoformat() if p.last_seen else None,
        }
        for p in permits
    ]


@router.get("/sources")
async def list_sources(user: User = Depends(get_current_user)):
    from app.services.scanners import ALL_SCANNERS
    sources = []
    for source_id, info in ALL_SCANNERS.items():
        sources.append({
            "id": source_id,
            "name": info["name"],
            "needs_api_key": info.get("needs_key", False),
            "has_key": bool(
                getattr(user, f"{source_id.replace('-', '_')}_api_key", "")
            ) if info.get("needs_key") else True,
        })
    return {"sources": sources}
