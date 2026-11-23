"""Who uses what: every API request is recorded (user, route template, status, time) and
summarized for admins at GET /api/v1/admin/usage.

Requests are buffered in memory and written in batches once a minute, so recording never
slows a request down. Events older than RETENTION_DAYS are deleted daily; their IP addresses
are cleared after IP_RETENTION_DAYS. Each new IP's approximate city and network come from
ipinfo.io (IPINFO_TOKEN raises the free limit), looked up once in the background.
"""
from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

import ipaddress

import httpx
from sqlalchemy import delete, insert, select, update

from app.models.database import IpLookup, UsageEvent, User, get_session_factory

log = logging.getLogger("sitescan.usage")

RETENTION_DAYS = 180
IP_RETENTION_DAYS = 90
IP_LOOKUPS_PER_RUN = 200
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


def client_ip(headers, peer: str | None) -> str | None:
    """The visitor's IP: Railway's edge puts it in X-Real-IP / the first X-Forwarded-For entry."""
    for raw in (headers.get("x-real-ip"), (headers.get("x-forwarded-for") or "").split(",")[0], peer):
        raw = (raw or "").strip()
        try:
            return str(ipaddress.ip_address(raw))
        except ValueError:
            continue
    return None


def record(user_id: int | None, method: str, route: str, status: int, ms: float, ip: str | None = None) -> None:
    if len(_buffer) < MAX_BUFFER:
        _buffer.append({"at": datetime.utcnow(), "user_id": user_id, "method": method[:10],
                        "route": route[:200], "status": status, "ms": int(ms), "ip": ip})


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
        now = datetime.utcnow()
        async with get_session_factory()() as db:
            await db.execute(delete(UsageEvent).where(UsageEvent.at < now - timedelta(days=RETENTION_DAYS)))
            await db.execute(update(UsageEvent).where(UsageEvent.at < now - timedelta(days=IP_RETENTION_DAYS),
                                                      UsageEvent.ip.is_not(None)).values(ip=None))
            await db.commit()
    except Exception as exc:
        log.warning("Usage prune failed: %s", exc)


def _public(ip: str) -> bool:
    try:
        a = ipaddress.ip_address(ip)
        return not (a.is_private or a.is_loopback or a.is_reserved or a.is_link_local)
    except ValueError:
        return False


async def lookup_ips(limit: int = IP_LOOKUPS_PER_RUN) -> int:
    """Look up city/region/network for recent IPs that have none yet (never raises)."""
    import os
    try:
        since = datetime.utcnow() - timedelta(days=IP_RETENTION_DAYS)
        async with get_session_factory()() as db:
            seen = {r[0] for r in await db.execute(
                select(UsageEvent.ip).where(UsageEvent.ip.is_not(None), UsageEvent.at >= since).distinct())}
            done = {r[0] for r in await db.execute(select(IpLookup.ip))}
            todo = [ip for ip in seen - done if _public(ip)][:limit]
            if not todo:
                return 0
            token = os.environ.get("IPINFO_TOKEN", "")
            async with httpx.AsyncClient(timeout=10) as client:
                for ip in todo:
                    try:
                        r = await client.get(f"https://ipinfo.io/{ip}/json", params={"token": token} if token else None)
                        if r.status_code == 429:
                            log.info("ipinfo rate limit reached; continuing next run")
                            break
                        d = r.json() if r.status_code == 200 else {}
                    except Exception as exc:
                        log.debug("ipinfo %s failed: %s", ip, exc)
                        continue
                    db.add(IpLookup(ip=ip, city=(d.get("city") or "")[:120], region=(d.get("region") or "")[:120],
                                    country=(d.get("country") or "")[:10], org=(d.get("org") or "")[:255],
                                    hostname=(d.get("hostname") or "")[:255]))
            await db.commit()
            return len(todo)
    except Exception as exc:
        log.warning("IP lookup failed: %s", exc)
        return 0


def visitors(events: list[Any], users: dict[int, Any], geo: dict[str, Any], limit: int = 300) -> list[dict]:
    """One row per IP: where it is, which accounts used it, and sign-in attempts."""
    by_ip: dict[str, dict] = {}
    for e in events:
        ip = getattr(e, "ip", None)
        if not ip:
            continue
        v = by_ip.setdefault(ip, {"first": e.at, "last": e.at, "requests": 0, "users": set(),
                                  "failed_logins": 0, "signups": 0})
        v["first"], v["last"] = min(v["first"], e.at), max(v["last"], e.at)
        v["requests"] += 1
        if e.user_id:
            v["users"].add(e.user_id)
        route = e.route or ""
        if route.endswith("/auth/login") and (e.status or 0) in (400, 401, 403):
            v["failed_logins"] += 1
        if route.endswith("/auth/register") and 200 <= (e.status or 0) < 300:
            v["signups"] += 1
    rows = []
    for ip, v in by_ip.items():
        g = geo.get(ip)
        org = getattr(g, "org", "") or ""
        rows.append({
            "ip": ip,
            "location": ", ".join(x for x in (getattr(g, "city", ""), getattr(g, "region", ""), getattr(g, "country", "")) if x),
            "network": org.split(" ", 1)[1] if org.startswith("AS") and " " in org else org,
            "first_seen": v["first"].isoformat(), "last_seen": v["last"].isoformat(),
            "requests": v["requests"], "failed_logins": v["failed_logins"], "signups": v["signups"],
            "accounts": sorted(getattr(users.get(u), "email", f"user {u}") for u in v["users"]),
        })
    rows.sort(key=lambda r: r["last_seen"], reverse=True)
    return rows[:limit]


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
        ips = {e.ip for e in events if getattr(e, "ip", None)}
        geo = {g.ip: g for g in (await db.execute(select(IpLookup).where(IpLookup.ip.in_(list(ips))))).scalars().all()} if ips else {}
    report = summarize(events, users, days, now)
    report["visitors"] = visitors(events, users, geo)
    report["ip_retention_days"] = IP_RETENTION_DAYS
    return report
