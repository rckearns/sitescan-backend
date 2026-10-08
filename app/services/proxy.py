"""ZenRows proxy for public sites that don't answer Railway's datacenter IP
(SCBO, SFAA, Charleston County parcels). Used as a fallback, or always for SCBO."""

import os
from typing import Optional

import httpx

ZENROWS_URL = "https://api.zenrows.com/v1/"


def zenrows_key() -> str:
    key = os.environ.get("ZENROWS_API_KEY", "")
    if not key:
        try:
            from app.config import get_settings
            key = get_settings().zenrows_api_key or ""
        except Exception:
            key = ""
    return key


def zenrows_params(key: str, url: str, premium: bool = False, js_render: bool = False) -> dict:
    params = {"apikey": key, "url": url}
    if js_render:
        params["js_render"] = "true"   # headless browser; for sites that block plain fetches
    if premium:
        # Residential US exit IPs; needed for sites that refuse ZenRows' standard
        # datacenter proxies (SCBO answers those with 422 "could not get content").
        params.update({"premium_proxy": "true", "proxy_country": "us"})
    return params


async def proxied(client: httpx.AsyncClient, method: str, url: str, *, data: Optional[dict] = None,
                  timeout: float = 90.0, premium: bool = False) -> httpx.Response:
    """Send the request through ZenRows (it forwards POST bodies). Raises if no key."""
    key = zenrows_key()
    if not key:
        raise httpx.ConnectError(f"No ZenRows key to proxy {url}")
    try:
        resp = await client.request(method, ZENROWS_URL, params=zenrows_params(key, url, premium),
                                    data=data, timeout=timeout)
    except httpx.HTTPError as exc:
        # Don't let the request URL (which carries the API key) reach logs or responses.
        raise type(exc)(f"proxy request for {url} failed: {type(exc).__name__}") from None
    if resp.status_code >= 400:
        raise httpx.HTTPStatusError(f"proxy returned {resp.status_code} for {url}",
                                    request=httpx.Request(method, url), response=resp)
    return resp
