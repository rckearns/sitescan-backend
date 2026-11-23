import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import usage  # noqa: E402

NOW = datetime(2026, 11, 23, 12)


def ev(days_ago, user, route, status=200, ms=100):
    return SimpleNamespace(at=NOW - timedelta(days=days_ago), user_id=user, method="GET", route=route, status=status, ms=ms)


def test_features():
    assert usage.feature_for("/api/v1/directory/subs") == "Contractors: proven subs"
    assert usage.feature_for("/api/v1/analyze/parcel/{tms}") == "Parcel analysis (AI)"
    assert usage.feature_for("/api/v1/analyze/parcels/cached") == "Developers: parcels"
    assert usage.feature_for("/api/v1/auth/me") == "Profile & settings"
    assert usage.feature_for("/api/v1/whatever") == "Other"


def test_summary_counts_users_features_and_errors():
    users = {1: SimpleNamespace(email="a@x.com", full_name="A", company="", created_at=NOW - timedelta(days=100)),
             2: SimpleNamespace(email="b@x.com", full_name="B", company="", created_at=NOW - timedelta(days=2)),
             3: SimpleNamespace(email="c@x.com", full_name="", company="", created_at=NOW - timedelta(days=50))}
    events = [ev(0, 1, "/api/v1/directory/subs"), ev(0, 1, "/api/v1/directory/subs"), ev(10, 1, "/api/v1/projects/home/parcels"),
              ev(1, 2, "/api/v1/auth/login"), ev(1, 2, "/api/v1/analyze/parcel/{tms}", status=502, ms=9000),
              ev(1, None, "/api/v1/auth/login", status=401)]
    r = usage.summarize(events, users, 30, NOW)
    t = r["totals"]
    assert (t["requests"], t["active_users"], t["active_users_7d"], t["registered_users"], t["new_users"], t["errors"]) == (6, 2, 2, 3, 1, 1)
    assert r["users"][0]["email"] == "a@x.com" and r["users"][0]["active_days"] == 2
    assert r["users"][0]["top_features"][0] == "Contractors: proven subs"
    assert r["never_active"] == ["c@x.com"]
    assert len(r["daily"]) == 30 and r["daily"][-2]["logins"] == 1 and r["daily"][-1]["active_users"] == 1
    assert r["problem_routes"][0] == {"route": "GET /api/v1/analyze/parcel/{tms}", "requests": 1, "errors": 1, "p95_ms": 9000}


def test_middleware_records_route_template_and_user():
    import httpx
    from app.auth import create_access_token, get_current_user
    from app.main import app
    usage._buffer.clear()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=42, email="z@x.com")

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
            await c.get("/api/v1/admin/usage", headers={"Authorization": "Bearer junk"})
            await c.get("/api/v1/admin/usage", headers={"Authorization": "Bearer " + create_access_token(42, "z@x.com")})
            await c.get("/health")
    try:
        asyncio.run(run())
    finally:
        app.dependency_overrides.clear()
    got = [(e["route"], e["user_id"], e["status"] >= 400) for e in usage._buffer]
    assert got[0][:2] == ("/api/v1/admin/usage", None)   # bad token: no user
    assert got[1] == ("/api/v1/admin/usage", 42, True)     # not an admin: 403
    assert len(got) == 2   # /health isn't recorded
    usage._buffer.clear()


def test_client_ip_prefers_proxy_headers():
    assert usage.client_ip({"x-real-ip": "203.0.113.9"}, "10.0.0.1") == "203.0.113.9"
    assert usage.client_ip({"x-forwarded-for": "198.51.100.4, 10.1.1.1"}, "10.0.0.1") == "198.51.100.4"
    assert usage.client_ip({"x-forwarded-for": "junk"}, "10.0.0.1") == "10.0.0.1"
    assert usage.client_ip({}, None) is None


def test_visitors_by_ip_with_location_accounts_and_failed_logins():
    users = {1: SimpleNamespace(email="a@x.com")}
    geo = {"203.0.113.9": SimpleNamespace(city="Mount Pleasant", region="South Carolina", country="US",
                                          org="AS7922 Comcast Cable Communications, LLC")}
    def e(ip, user, route, status, h):
        return SimpleNamespace(at=NOW - timedelta(hours=h), ip=ip, user_id=user, route=route, status=status)
    rows = usage.visitors([e("203.0.113.9", None, "/api/v1/auth/login", 401, 3), e("203.0.113.9", 1, "/api/v1/auth/login", 200, 2),
                           e("198.51.100.4", None, "/api/v1/auth/register", 200, 5), e(None, 1, "/api/v1/projects", 200, 1)], users, geo)
    assert [r["ip"] for r in rows] == ["203.0.113.9", "198.51.100.4"]
    assert rows[0]["location"] == "Mount Pleasant, South Carolina, US"
    assert rows[0]["network"] == "Comcast Cable Communications, LLC"
    assert rows[0]["accounts"] == ["a@x.com"] and rows[0]["failed_logins"] == 1
    assert rows[1]["signups"] == 1 and rows[1]["location"] == ""
