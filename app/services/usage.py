"""Who uses what: every API request is recorded (user, route template, status, time) and
summarized for admins at GET /api/v1/admin/usage.

Requests are buffered in memory and written in batches once a minute, so recording never
slows a request down. Events older than RETENTION_DAYS are deleted daily.
"""
from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, insert, select

from app.models.database import UsageEvent, User, get_session_factory

log = logging.getLogger("sitescan.usage")

RETENTION_DAYS = 180
MAX_BUFFER = 20_000          # drop rather than grow without bound if the database is down
_buffer: list[dict] = []

# Route template -> feature name shown on the dashboard. First match wins.
FEATURES: list[tuple[str, re.Pattern]] = [(name, re.compile(rx)) for name, rx in [
    ("Sign in / sign up", r"^/api/v1/auth/(login|register|signup)"),
    ("Developers: parcels", r"^/api/v1/analyze/parcels/cached"),   # loaded with every Home visit
    ("Parcel analysis (AI)", r"^/api/v1/analyze"),
    ("Developers: parcels", r"^/api/v1/projects/home/parcels"),
    ("Map", r"^/api/v1/projects/map"),
    ("Contractors: by trade & details", r"^/api/v1/directory/trades"),
    ("Contractors: GC leaderboard", r"^/api/v1/directory/gc-leaders"),
    ("Contractors: proven subs", r"^/api/v1/directory/subs"),
    ("Contractors: state awards", r"^/api/v1/awards"),
    ("Contractors: other", r"^/api/v1/(directory|contractors)"),
    ("GC pipeline", r"^/api/v1/pipeline"),
    ("Board agendas", r"^/api/v1/boards"),
    ("Permits & projects", r"^/api/v1/projects"),
    ("Profile & settings", r"^/api/v1/(profile|auth/me)"),
    ("Scans", r"^/api/v1/scan"),
    ("Admin", r"^/api/v1/admin"),
]]


def feature_for(route: str) -> str:
    for name, rx in FEATURES:
        if rx.search(route or ""):
            return name
    return "Other"


def user_id_from_auth(header: str | None) -> int | None:
    """The user id in a Bearer token, without a database lookup; None if absent or invalid."""
    if not header or not header.lower().startswith("bearer "):
        return None
    try:
        from app.auth import decode_token
        return int(decode_token(header[7:].strip()).get("sub") or 0) or None
    except Exception:
        return None


def record(user_id: int | None, method: str, route: str, status: int, ms: float) -> None:
    if len(_buffer) < MAX_BUFFER:
        _buffer.append({"at": datetime.utcnow(), "user_id": user_id, "method": method[:10],
                        "route": route[:200], "status": status, "ms": int(ms)})


async def flush() -> int:
    """Write buffered events (never raises)."""
    global _buffer
    batch, _buffer = _buffer, []
    if not batch:
        return 0
    try:
        async with get_session_factory()() as db:
            await db.execute(insert(UsageEvent), batch)
            await db.commit()
        return len(batch)
    except Exception as exc:
        log.warning("Usage flush failed (%d events dropped): %s", len(batch), exc)
        return 0


async def prune() -> None:
    try:
        async with get_session_factory()() as db:
            await db.execute(delete(UsageEvent).where(UsageEvent.at < datetime.utcnow() - timedelta(days=RETENTION_DAYS)))
            await db.commit()
    except Exception as exc:
        log.warning("Usage prune failed: %s", exc)


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    v = sorted(values)
    return v[min(len(v) - 1, int(len(v) * 0.95))]


def summarize(events: list[Any], users: dict[int, Any], days: int, now: datetime) -> dict[str, Any]:
    """The dashboard: totals, a daily series, per-user and per-feature use, and trouble spots."""
    start = (now - timedelta(days=days - 1)).date()
    daily = {start + timedelta(days=i): {"users": set(), "requests": 0, "errors": 0, "logins": 0} for i in range(days)}
    per_user: dict[int, dict] = defaultdict(lambda: {"requests": 0, "days": set(), "features": Counter(), "last": None})
    per_feature: dict[str, dict] = defaultdict(lambda: {"requests": 0, "users": set()})
    per_route: dict[tuple, dict] = defaultdict(lambda: {"requests": 0, "errors": 0, "ms": []})
    all_ms, errors = [], 0
    for e in events:
        d = daily.get(e.at.date())
        is_err = (e.status or 0) >= 500
        errors += is_err
        all_ms.append(e.ms or 0)
        feature = feature_for(e.route)
        if d is not None:
            d["requests"] += 1
            d["errors"] += is_err
            if e.user_id:
                d["users"].add(e.user_id)
            if feature == "Sign in / sign up" and e.status == 200:
                d["logins"] += 1
        if e.user_id:
            u = per_user[e.user_id]
            u["requests"] += 1
            u["days"].add(e.at.date())
            u["features"][feature] += 1
            u["last"] = max(u["last"] or e.at, e.at)
            per_feature[feature]["users"].add(e.user_id)
        per_feature[feature]["requests"] += 1
        r = per_route[(e.method, e.route)]
        r["requests"] += 1
        r["errors"] += is_err
        r["ms"].append(e.ms or 0)
    week_ago = now - timedelta(days=7)
    return {
        "days": days,
        "totals": {
            "requests": len(events),
            "active_users": len(per_user),
            "active_users_7d": sum(1 for u in per_user.values() if u["last"] and u["last"] >= week_ago),
            "registered_users": len(users),
            "new_users": sum(1 for u in users.values() if u.created_at and u.created_at.date() >= start),
            "errors": errors,
            "error_rate": round(errors / len(events), 4) if events else 0,
            "p95_ms": _p95(all_ms),
        },
        "daily": [{"date": day.isoformat(), "active_users": len(v["users"]), "requests": v["requests"],
                   "errors": v["errors"], "logins": v["logins"]} for day, v in sorted(daily.items())],
        "users": sorted(({
            "id": uid, "email": getattr(users.get(uid), "email", f"user {uid}"),
            "name": getattr(users.get(uid), "full_name", "") or "",
            "company": getattr(users.get(uid), "company", "") or "",
            "last_seen": u["last"].isoformat() if u["last"] else None,
            "active_days": len(u["days"]), "requests": u["requests"],
            "top_features": [f for f, _ in u["features"].most_common(3)],
        } for uid, u in per_user.items()), key=lambda r: r["last_seen"] or "", reverse=True),
        "never_active": sorted(u.email for uid, u in users.items() if uid not in per_user),
        "features": sorted(({"feature": f, "requests": v["requests"], "users": len(v["users"])}
                            for f, v in per_feature.items()), key=lambda r: -r["requests"]),
        "problem_routes": sorted(({
            "route": f"{m} {r}", "requests": v["requests"], "errors": v["errors"], "p95_ms": _p95(v["ms"]),
        } for (m, r), v in per_route.items() if v["errors"] or _p95(v["ms"]) >= 3000),
            key=lambda r: (-r["errors"], -r["p95_ms"]))[:15],
    }


async def usage_report(days: int = 30) -> dict[str, Any]:
    await flush()
    now = datetime.utcnow()
    since = datetime.combine((now - timedelta(days=days - 1)).date(), datetime.min.time())
    async with get_session_factory()() as db:
        events = (await db.execute(select(UsageEvent).where(UsageEvent.at >= since))).scalars().all()
        users = {u.id: u for u in (await db.execute(select(User))).scalars().all()}
    return summarize(events, users, days, now)
