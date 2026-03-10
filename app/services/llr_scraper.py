"""
SC LLR (Dept. of Labor, Licensing & Regulation) contractor directory scraper.

Uses the 2captcha API to solve the reCAPTCHA v2 on verify.llronline.com, then
scrapes contractor license records by classification (trade) and city.

Requires TWOCAPTCHA_API_KEY env var.  Without it the scraper raises
LLRCaptchaRequired so callers can skip gracefully.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

_LLR_BASE = "https://verify.llronline.com/LicLookup/Contractors/Contractor.aspx?div=69"
_LLR_POST = "https://verify.llronline.com/LicLookup/Contractors/Contractor.aspx?div=69&AspxAutoDetectCookieSupport=1"
_RECAPTCHA_SITEKEY = "6Lc2X-saAAAAAPC6HatgHFOd8rCxCl-2yPTh44PN"

_2CAP_SUBMIT = "https://2captcha.com/in.php"
_2CAP_RESULT = "https://2captcha.com/res.php"

# SC LLR classification codes → human trade label
CLASSIFICATION_MAP: dict[str, str] = {
    "AC": "Air Conditioning",
    "BL": "Boiler Installation",
    "BD": "Building (General)",
    "CT": "Concrete",
    "CP": "Concrete Paving",
    "CCM": "Construction Manager",
    "EL": "Electrical",
    "GG": "Glass & Glazing",
    "GD": "Grading",
    "HT": "Heating",
    "MS": "Masonry",
    "MM": "Miscellaneous Metals",
    "NR": "Nonstructural Renovation",
    "PB": "Plumbing",
    "RF": "Roofing",
    "SF": "Structural Framing",
    "WF": "Wood Frame Structures",
    "AP": "Asphalt Paving",
    "MR": "Marine",
    "RG": "Refrigeration",
    "GE": "Grading",
}

# Classifications to scrape when doing a "full" directory refresh
DEFAULT_CLASSIFICATIONS = [
    "BD",   # Building (General Contractor)
    "CCM",  # Construction Manager
    "CT",   # Concrete
    "CP",   # Concrete Paving
    "MS",   # Masonry
    "SF",   # Structural Framing
    "WF",   # Wood Frame Structures
    "MM",   # Miscellaneous Metals
    "NR",   # Nonstructural Renovation (drywall, finishes)
    "GD",   # Grading
    "AP",   # Asphalt Paving
    "RF",   # Roofing
    "GG",   # Glass & Glazing
    "EL",   # Electrical
    "PB",   # Plumbing
    "AC",   # Air Conditioning
    "HT",   # Heating
    "RG",   # Refrigeration
]

# Charleston-area cities to query (LLR is exact-match on city name)
DEFAULT_CITIES = [
    "Charleston",
    "North Charleston",
    "Mount Pleasant",
    "Summerville",
    "Ladson",
    "Goose Creek",
    "Hanahan",
    "Johns Island",
    "James Island",
    "West Ashley",
    "Moncks Corner",
    "Ridgeville",
    "Walterboro",
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class LLRCaptchaRequired(Exception):
    """Raised when TWOCAPTCHA_API_KEY is not configured."""


class LLRSolveError(Exception):
    """Raised when 2captcha fails to solve the CAPTCHA."""


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _extract_tokens(html: str) -> dict[str, str]:
    """Pull ASP.NET hidden form field values from page HTML."""
    tokens: dict[str, str] = {}
    for field in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        m = re.search(rf'id="{re.escape(field)}" value="([^"]+)"', html)
        if m:
            tokens[field] = m.group(1)
    return tokens


def _parse_results(html: str) -> list[dict[str, Any]]:
    """
    Parse the LLR results DataGrid.

    Current grid columns (as of 2025):
      License# | Status | Type | Last | First | Suffix | Business | City | State
    """
    rows: list[dict[str, Any]] = []

    # The gv_results id is ON the table element itself
    table_m = re.search(
        r'<table(?=[^>]*id="ctl00_ContentPlaceHolder2_gv_results")[^>]*>(.*?)</table>',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not table_m:
        # Fall back: any table after the "X record(s)" count line
        table_m = re.search(
            r'record\(s\).*?<table[^>]*>(.*?)</table>',
            html,
            re.DOTALL | re.IGNORECASE,
        )
    if not table_m:
        return rows

    table_html = table_m.group(1)
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE)

    def cell_text(td: str) -> str:
        # Replace <br/> with a space before stripping all tags
        td = re.sub(r"<br\s*/?>", " ", td, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", td).strip()
        # Decode common HTML entities
        text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ").replace("&#39;", "'").replace("&quot;", '"')
        return text.strip()

    for tr in trs:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL | re.IGNORECASE)
        if len(cells) < 7:
            continue
        texts = [cell_text(c) for c in cells]
        # columns: License# | Status | Type | Last | First | Suffix | Business | City | State
        company = texts[6]  # Business name
        # Some licensees are individuals with no business name — fall back to person name
        if not company or company in ("\xa0", "&nbsp;"):
            last = texts[3] if len(texts) > 3 else ""
            first = texts[4] if len(texts) > 4 else ""
            company = f"{first} {last}".strip() if (first or last) else ""
        # Strip nbsp artifacts
        company = company.replace("\xa0", "").strip()
        row = {
            "company_name": company,
            "license_number": texts[0],
            "city": texts[7].title() if len(texts) > 7 else "",
            "state": texts[8].upper() if len(texts) > 8 else "SC",
            "classification": texts[2] if len(texts) > 2 else "",
            "license_status": texts[1].title() if len(texts) > 1 else "",
            "license_expires": "",
        }
        if row["company_name"] and row["license_number"]:
            rows.append(row)
    return rows


async def _solve_recaptcha(api_key: str, client: httpx.AsyncClient) -> str:
    """
    Submit reCAPTCHA v2 to 2captcha and return the solved g-recaptcha-response token.
    Polls up to 120 s.
    """
    # Step 1 — submit task
    resp = await client.post(
        _2CAP_SUBMIT,
        data={
            "key": api_key,
            "method": "userrecaptcha",
            "googlekey": _RECAPTCHA_SITEKEY,
            "pageurl": _LLR_BASE,
            "json": "1",
        },
        timeout=30,
    )
    data = resp.json()
    if data.get("status") != 1:
        raise LLRSolveError(f"2captcha submit error: {data}")
    task_id = data["request"]
    log.info("2captcha task submitted: %s", task_id)

    # Step 2 — poll for result
    for attempt in range(24):  # 24 × 5 s = 120 s max
        await asyncio.sleep(5)
        poll = await client.get(
            _2CAP_RESULT,
            params={"key": api_key, "action": "get", "id": task_id, "json": "1"},
            timeout=15,
        )
        pdata = poll.json()
        if pdata.get("status") == 1:
            log.info("2captcha solved after %d polls", attempt + 1)
            return pdata["request"]
        if pdata.get("request") not in ("CAPCHA_NOT_READY", "CAPTCHA_NOT_READY"):
            raise LLRSolveError(f"2captcha error: {pdata}")

    raise LLRSolveError("2captcha timed out after 120 s")


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

async def scrape_llr_contractors(
    classification: str,
    city: str,
    state: str = "SC",
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """
    Scrape SC LLR for active contractors of a given classification in a city.

    Returns a list of dicts with keys:
        company_name, license_number, city, state,
        classification, license_status, license_expires, trade_label

    Raises LLRCaptchaRequired if no api_key and TWOCAPTCHA_API_KEY env var unset.
    """
    key = api_key or os.environ.get("TWOCAPTCHA_API_KEY", "")
    if not key:
        raise LLRCaptchaRequired(
            "Set TWOCAPTCHA_API_KEY to enable SC LLR scraping"
        )

    trade_label = CLASSIFICATION_MAP.get(classification, classification)

    async with httpx.AsyncClient(
        headers=_HEADERS, follow_redirects=True, timeout=60
    ) as client:
        # 1. Fetch the page to get session cookies + ASP.NET tokens
        log.info("LLR scrape: classification=%s city=%s", classification, city)
        page_resp = await client.get(_LLR_BASE)
        tokens = _extract_tokens(page_resp.text)
        if not tokens.get("__VIEWSTATE"):
            log.warning("LLR: could not extract VIEWSTATE from page")
            return []

        # 2. Solve the reCAPTCHA via 2captcha
        captcha_token = await _solve_recaptcha(key, client)

        # 3. POST the search form
        form_data = {
            "__VIEWSTATE": tokens.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": tokens.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": tokens.get("__EVENTVALIDATION", ""),
            "ctl00$ContentPlaceHolder1$UserInputGen$txt_lastName": "",
            "ctl00$ContentPlaceHolder1$UserInputGen$txt_firstName": "",
            "ctl00$ContentPlaceHolder1$UserInputGen$txt_licNum": "",
            "ctl00$ContentPlaceHolder1$UserInputGen$txt_city": city,
            "ctl00$ContentPlaceHolder1$UserInputGen$txt_state": state,
            "ctl00$ContentPlaceHolder1$UserInputGen$ddl_type": classification,
            "ctl00$ContentPlaceHolder1$btn_find": "Find",
            "g-recaptcha-response": captcha_token,
        }
        result_resp = await client.post(
            _LLR_POST,
            data=form_data,
            headers={**_HEADERS, "Referer": _LLR_BASE, "Content-Type": "application/x-www-form-urlencoded"},
        )
        results = _parse_results(result_resp.text)

        # Enrich each result with trade_label and normalise classification to the
        # search code (e.g. "WF") rather than the HTML description ("WOOD FRAME…")
        for r in results:
            r["trade_label"] = trade_label
            r["classification"] = classification  # always store the search code
            r["source"] = "sc-llr"
            r["external_id"] = r.get("license_number", "")

        log.info(
            "LLR scrape: classification=%s city=%s → %d results",
            classification, city, len(results),
        )
        return results


async def scrape_llr_full(
    classifications: list[str] | None = None,
    cities: list[str] | None = None,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """
    Scrape SC LLR across multiple classifications and Charleston-area cities.
    Deduplicates by (license_number, classification).
    """
    classes = classifications or DEFAULT_CLASSIFICATIONS
    city_list = cities or DEFAULT_CITIES
    key = api_key or os.environ.get("TWOCAPTCHA_API_KEY", "")
    if not key:
        raise LLRCaptchaRequired("Set TWOCAPTCHA_API_KEY to enable SC LLR scraping")

    seen: set[tuple[str, str]] = set()
    all_results: list[dict[str, Any]] = []

    for cls in classes:
        for city in city_list:
            try:
                results = await scrape_llr_contractors(cls, city, api_key=key)
                for r in results:
                    key_tuple = (r.get("license_number", ""), cls)
                    if key_tuple not in seen:
                        seen.add(key_tuple)
                        all_results.append(r)
                # Small delay to be polite
                await asyncio.sleep(2)
            except LLRSolveError as e:
                log.warning("LLR captcha solve failed for %s/%s: %s", cls, city, e, exc_info=True)
                await asyncio.sleep(10)
            except Exception as e:
                log.warning("LLR scrape error for %s/%s: [%s] %s", cls, city, type(e).__name__, e, exc_info=True)

    log.info("LLR full scrape complete: %d unique records", len(all_results))
    return all_results
