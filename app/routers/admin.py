"""Admin-only endpoints (ADMIN_EMAILS)."""
from fastapi import APIRouter, Depends, Query

from app.auth import require_admin

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/usage")
async def usage(days: int = Query(30, ge=1, le=180), _admin=Depends(require_admin)):
    """Active users, feature use, errors and slow endpoints over the last `days` days."""
    from app.services.usage import usage_report
    return await usage_report(days)
