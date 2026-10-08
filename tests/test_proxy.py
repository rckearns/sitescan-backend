"""The ZenRows key must never appear in errors (they reach logs and API responses)."""
import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.services import proxy  # noqa: E402

KEY = "secret-key-123"


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("handler", [
    lambda r: httpx.Response(422, text="RESP001"),
    lambda r: (_ for _ in ()).throw(httpx.ReadTimeout("timed out", request=r)),
])
def test_proxy_errors_do_not_contain_the_key(monkeypatch, handler):
    monkeypatch.setattr(proxy, "zenrows_key", lambda: KEY)
    with pytest.raises(httpx.HTTPError) as info:
        asyncio.run(proxy.proxied(_client(handler), "GET", "https://scbo.sc.gov/x"))
    assert KEY not in str(info.value) and KEY not in repr(info.value)
    assert "scbo.sc.gov" in str(info.value)


def test_premium_params():
    p = proxy.zenrows_params(KEY, "https://x", premium=True)
    assert p["premium_proxy"] == "true" and p["proxy_country"] == "us"
