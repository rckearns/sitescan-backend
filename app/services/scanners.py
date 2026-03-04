"""Data source scanners — fetch opportunities from external APIs and websites."""

import asyncio
import re
import logging
from datetime import datetime, timedelta
from typing import Optional
import httpx
try:
    from curl_cffi.requests import AsyncSession as CurlSession
    _CURL_CFFI_AVAILABLE = True
except ImportError:
    CurlSession = None
    _CURL_CFFI_AVAILABLE = False
from tenacity import retry, stop_after_attempt, wait_exponential

from app.services.scoring import classify_project
from app.services.geocode import geocode

logger = logging.getLogger("sitescan.scanners")


def _parse_date(date_str):
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(date_str)[:19], fmt)
        except (ValueError, TypeError):
            continue
    return None


def _clean_text(text, max_len=2000):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


CONSTRUCTION_NAICS = ["236220", "236210", "238140", "238110", "238190"]


async def scan_sam_gov(api_key="", state="SC", keywords=None, days_back=30):
    if not api_key:
        logger.warning("SAM.gov: No API key, skipping")
        return []

    today = datetime.utcnow()
    from_date = today - timedelta(days=days_back)
    params = {
        "api_key": api_key,
        "limit": 100,
        "postedFrom": from_date.strftime("%m/%d/%Y"),
        "postedTo": today.strftime("%m/%d/%Y"),
        "ptype": "o,p,k",
        "q": keywords or "building construction",
    }
    if state:
        params["state"] = state

    # Geocode state centroid once (cached after first call)
    state_coords = await geocode(f"{state}, USA") if state else None
    state_lat = state_coords[0] if state_coords else None
    state_lng = state_coords[1] if state_coords else None

    results = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get("https://api.sam.gov/opportunities/v2/search", params=params)
            if not resp.is_success:
                logger.error(f"SAM.gov HTTP {resp.status_code}: {resp.text[:500]}")
                resp.raise_for_status()
            data = resp.json()
            total = data.get("totalRecords", "?")
            logger.info(f"SAM.gov: {total} total records returned")
            for opp in data.get("opportunitiesData", []):
                title = _clean_text(opp.get("title", ""), 500)
                desc = ""
                if isinstance(opp.get("description"), dict):
                    desc = _clean_text(opp["description"].get("body", ""))
                elif isinstance(opp.get("description"), str):
                    desc = _clean_text(opp["description"])
                cat = classify_project(title, desc)
                ms = 50  # scored dynamically per-user at request time
                results.append({
                    "source_id": "sam-gov", "external_id": str(opp.get("noticeId", "")),
                    "title": title, "description": desc, "location": state,
                    "latitude": state_lat, "longitude": state_lng,
                    "value": None, "category": cat, "match_score": ms,
                    "status": "Open" if opp.get("active") == "Yes" else "Closed",
                    "posted_date": _parse_date(opp.get("postedDate")),
                    "deadline": _parse_date(opp.get("responseDeadLine")),
                    "agency": _clean_text(opp.get("fullParentPathName", ""), 255),
                    "solicitation_number": opp.get("solicitationNumber", ""),
                    "naics_code": ",".join(CONSTRUCTION_NAICS),
                    "source_url": f"https://sam.gov/opp/{opp.get('noticeId','')}/view",
                    "raw_data": opp,
                })
        except Exception as e:
            logger.error(f"SAM.gov error: {e}")

    seen = set()
    return [r for r in results if r["external_id"] not in seen and not seen.add(r["external_id"])]


# EnerGov CSS API tenant headers (TenantID=1, TenantName="CharlestonSC")
_ENERGOV_TENANT_HEADERS = {
    "tenantId": "1",
    "tenantName": "CharlestonSC",
    "Tyler-TenantUrl": "CharlestonSC",
    "Tyler-Tenant-Culture": "en-US",
    "Accept": "application/json",
}
_ENERGOV_PERMIT_URL = (
    "https://egcss.charleston-sc.gov/EnerGov_Prod/selfservice/api/energov/permits/permit/{}"
)


async def _fetch_energov_contractor(client: httpx.AsyncClient, pmpermitid: str) -> str:
    """Look up all contractor names for a permit via the EnerGov CSS API.
    Returns a pipe-delimited string of all Contractor contact names, or "" on any error.
    Multi-contractor permits (e.g. GC + subs listed together) will return all names
    so each contractor can be associated with this permit in the subcontractors view.
    """
    if not pmpermitid:
        return ""
    for attempt in range(3):
        try:
            resp = await client.get(
                _ENERGOV_PERMIT_URL.format(pmpermitid),
                headers=_ENERGOV_TENANT_HEADERS,
                timeout=30.0,
            )
            if resp.status_code != 200:
                return ""
            data = resp.json()
            result = data.get("Result") or {}
            contacts = result.get("Contacts") or []
            # Collect all contacts typed "Contractor"
            names: list[str] = []
            seen: set[str] = set()
            for contact in contacts:
                if (contact.get("ContactTypeName") or "").lower() == "contractor":
                    name = (contact.get("GlobalEntityName") or "").strip()
                    if name and name not in seen:
                        names.append(name)
                        seen.add(name)
            # Fall back to any contact with an entity name if no "Contractor" typed contacts
            if not names:
                for contact in contacts:
                    name = (contact.get("GlobalEntityName") or "").strip()
                    if name and name not in seen:
                        names.append(name)
                        seen.add(name)
            return "|".join(names)
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)  # 1s, 2s
            else:
                logger.debug(f"EnerGov {pmpermitid} failed after 3 attempts: {exc}")
    return ""


async def scan_charleston_permits(arcgis_url="", record_count=500):
    """Fetch permits from Charleston ArcGIS layers 20 and 21, then enrich with EnerGov.

    Layer 20 (Active Permits): current issued/applied permits of all types.
    Layer 21 (New Construction since 2010): large multi-year projects (e.g. 310 Broad St,
      Emanuel Nine Memorial) that may not appear in Layer 20.

    Both layers share the same field schema. Results are deduplicated by PMPERMITID.
    """
    _ARCGIS_BASE = "https://gis.charleston-sc.gov/arcgis2/rest/services/External/Applications/MapServer"

    # Layer 20: current active permits, filtered by relevant type
    _GC_PERMIT_TYPES = (
        "Building Commercial",
        "Building Multi-Family",
        "Demolition",
        "Foundation",
        "Construction Activity Type 2",
    )
    _TRADE_PERMIT_TYPES = (
        "Electrical - Commercial",
        "Roofing - Commercial",
        "Plumbing - Commercial",
        "Plumbing",
        "Mechanical",
        "xDNU Mechanical - Commercial",
        "Fire Protection System - Standalone",
    )
    _all_types = _GC_PERMIT_TYPES + _TRADE_PERMIT_TYPES
    type_in = ", ".join(f"'{t}'" for t in _all_types)

    layer20_params = {
        "where": f"PERMIT_TYPE IN ({type_in}) AND PERMIT_STATUS NOT IN ('Void', 'Cancelled')",
        "outFields": "*",
        "resultRecordCount": "5000",
        "f": "json",
        "outSR": "4326",
    }
    # Layer 21: new construction since 2010 (Emanuel Nine Memorial, 310 Broad St, etc.)
    # Keep this filter narrow — broader queries may exceed the layer's maxRecordCount.
    layer21_params = {
        "where": "PERMIT_STATUS = 'Issued'",
        "outFields": "*",
        "resultRecordCount": "5000",
        "f": "json",
        "outSR": "4326",
    }

    results = []

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Fetch both layers concurrently
            resp20, resp21 = await asyncio.gather(
                client.get(f"{_ARCGIS_BASE}/20/query", params=layer20_params),
                client.get(f"{_ARCGIS_BASE}/21/query", params=layer21_params),
            )
            resp20.raise_for_status()
            resp21.raise_for_status()

            data20 = resp20.json()
            data21 = resp21.json()
            if "error" in data20:
                raise RuntimeError(f"ArcGIS Layer 20 error: {data20['error']}")
            if "error" in data21:
                logger.warning(f"ArcGIS Layer 21 error (skipping): {data21['error']}")
                data21 = {}
            feats20 = data20.get("features", [])
            feats21 = data21.get("features", [])
            logger.info(f"ArcGIS Layer 20: {len(feats20)} features, Layer 21: {len(feats21)} features")

            # Merge and deduplicate by PMPERMITID then PERMIT_NUMBER (Layer 20 takes precedence).
            # Some records have an empty PMPERMITID; PERMIT_NUMBER is a reliable fallback
            # to prevent the same physical permit from appearing twice (once per layer).
            seen_pids: set[str] = set()
            seen_pnums: set[str] = set()
            features = []
            for f in feats20 + feats21:
                attrs = f.get("attributes", {})
                pid = str(attrs.get("PMPERMITID") or "")
                pnum = str(attrs.get("PERMIT_NUMBER") or "")
                if (pid and pid in seen_pids) or (pnum and pnum in seen_pnums):
                    continue
                if pid:
                    seen_pids.add(pid)
                if pnum:
                    seen_pnums.add(pnum)
                features.append(f)
            logger.info(f"Charleston ArcGIS combined: {len(features)} unique permits")

            # Build permit records from ArcGIS data
            raw_records = []
            pmpermitids = []
            for feature in features:
                a = feature.get("attributes", {})

                permit_type = str(a.get("PERMIT_TYPE") or a.get("PERMITTYPE") or "Permit")

                # Identify trade permits (electrical, plumbing, mechanical, etc.).
                # We no longer skip these entirely — we store them with category="trade-permit"
                # so they contribute to contractor discovery without appearing in the project list.
                _TRADE_PERMIT_RE = re.compile(
                    r"operational\s+permit|zoning\s+verification|fire\s+protection|"
                    r"fire\s+alarm|electrical|plumbing|mechanical|gas\s+pipe|"
                    r"low\s+voltage|sign\s+permit|temporary\s+use|"
                    r"short[\s-]*term\s+rental",
                    re.IGNORECASE,
                )
                is_trade_permit = bool(_TRADE_PERMIT_RE.search(permit_type))

                address = str(a.get("PERMIT_ADDRESS_LINE1") or a.get("ADDRESS") or "")
                description = _clean_text(a.get("DESCRIPTION"))
                work_class = str(a.get("WORK_CLASS") or "")

                # For non-trade building permits, also skip descriptions that are clearly
                # trade-specific work filed under a generic building permit type
                if not is_trade_permit:
                    _SKIP_DESC_RE = re.compile(
                        r"^(fire\s+suppression|fire\s+alarm|fire\s+sprinkler|sprinkler\s+system|"
                        r"fire\s+protection|low\s+voltage|security\s+alarm|short[\s-]*term\s+rental)",
                        re.IGNORECASE,
                    )
                    if _SKIP_DESC_RE.search(description):
                        continue
                    if _SKIP_DESC_RE.search(work_class):
                        continue

                # Build a human-readable title from description or work_class.
                # "Building Commercial" is a permit-type code, not a useful title.
                raw_desc = description.strip()
                if raw_desc:
                    # Convert ALL-CAPS descriptions to Title Case
                    alpha_chars = [c for c in raw_desc if c.isalpha()]
                    if alpha_chars and sum(c.isupper() for c in alpha_chars) / len(alpha_chars) > 0.7:
                        raw_desc = raw_desc.title()
                    if len(raw_desc) <= 160:
                        title_base = raw_desc
                    else:
                        # Truncate long descriptions at a word boundary so we keep
                        # key project names (e.g. "Emanuel Nine Memorial Project")
                        # rather than falling back to the generic work_class value.
                        title_base = raw_desc[:160].rsplit(" ", 1)[0].rstrip(":,;") + "…"
                elif work_class:
                    title_base = work_class
                else:
                    title_base = permit_type
                title = f"{title_base} — {address}" if address else title_base
                full_desc = f"{description} | {work_class} | {permit_type}".strip(" |")

                value = None
                for vfield in ("VALUATION", "JOBVALUE"):
                    if a.get(vfield):
                        try:
                            value = float(a[vfield])
                            break
                        except (ValueError, TypeError):
                            pass

                posted = None
                for dfield in ("ISSUE_DATE", "ISSUEDATE"):
                    raw = a.get(dfield)
                    if raw:
                        try:
                            if isinstance(raw, (int, float)) and raw > 1e10:
                                posted = datetime.fromtimestamp(raw / 1000)
                            else:
                                posted = _parse_date(str(raw))
                            break
                        except (ValueError, TypeError, OSError):
                            pass

                lat = a.get("LATITUDE") or (feature.get("geometry", {}) or {}).get("y")
                lng = a.get("LONGITUDE") or (feature.get("geometry", {}) or {}).get("x")

                # Prefer PERMIT_NUMBER over OBJECTID: PERMIT_NUMBER is the same across
                # Layer 20 and Layer 21 for the same physical permit, so using it as the
                # external_id prevents duplicate DB rows when the same permit appears in
                # both layers (e.g. after the PMPERMITID-based dedup passes it through).
                ext_id = str(a.get("PERMIT_NUMBER") or a.get("OBJECTID") or abs(hash(title)))
                cat = "trade-permit" if is_trade_permit else classify_project(title, full_desc)

                pmpermitid = str(a.get("PMPERMITID") or "")
                pmpermitids.append(pmpermitid)

                raw_records.append({
                    "source_id": "charleston-permits",
                    "external_id": f"chs-{ext_id}",
                    "title": title, "description": full_desc,
                    "location": f"{address}, Charleston, SC" if address else "Charleston, SC",
                    "address": address,
                    "latitude": float(lat) if lat else None,
                    "longitude": float(lng) if lng else None,
                    "value": value, "category": cat, "match_score": 50,
                    "status": str(a.get("PERMIT_STATUS") or "Active"),
                    "posted_date": posted,
                    "permit_number": str(a.get("PERMIT_NUMBER") or ""),
                    "contractor": "",
                    "source_url": "https://gis.charleston-sc.gov/interactive/permits/",
                    "raw_data": a,
                })

            # Concurrently enrich with contractor names from EnerGov
            # Use a semaphore to limit concurrent requests (avoid hammering the server)
            semaphore = asyncio.Semaphore(8)

            async def fetch_with_semaphore(pid):
                async with semaphore:
                    return await _fetch_energov_contractor(client, pid)

            logger.info(f"Fetching contractor data from EnerGov for {len(pmpermitids)} permits...")
            contractors = await asyncio.gather(
                *[fetch_with_semaphore(pid) for pid in pmpermitids]
            )

            enriched = sum(1 for c in contractors if c)
            logger.info(f"EnerGov enrichment: {enriched}/{len(pmpermitids)} permits have contractor data")

            for record, contractor in zip(raw_records, contractors):
                record["contractor"] = contractor
                results.append(record)

    except Exception as e:
        logger.error(f"Charleston Permits error: {e}")
        import traceback
        logger.error(traceback.format_exc())

    logger.info(f"Charleston Permits: Found {len(results)} active permits")
    return results


async def _fetch_scbo_html(url: str) -> str:
    """Fetch SCBO page HTML, routing through ZenRows proxy when key is configured."""
    import os
    # Read directly from env to bypass any lru_cache staleness
    zenrows_key = os.environ.get("ZENROWS_API_KEY", "")
    if not zenrows_key:
        from app.config import get_settings
        zenrows_key = get_settings().zenrows_api_key
    if zenrows_key:
        logger.info(f"SCBO fetch via ZenRows: {url}")
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get(
                    "https://api.zenrows.com/v1/",
                    params={"apikey": zenrows_key, "url": url},
                )
                resp.raise_for_status()
                return resp.text
        except httpx.HTTPStatusError as e:
            logger.warning(
                f"ZenRows returned {e.response.status_code} for {url} — "
                f"falling back to curl_cffi/direct"
            )
        except Exception as e:
            logger.warning(f"ZenRows request failed ({e}) — falling back to curl_cffi/direct")
    if _CURL_CFFI_AVAILABLE:
        async with CurlSession(impersonate="chrome120") as client:
            resp = await client.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, verify=False) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text


async def scan_scbo():
    """Scrape SC Business Opportunities - Construction category."""
    from app.config import get_settings
    settings = get_settings()
    results = []
    today = datetime.utcnow()
    for days_ago in range(7):
        d = today - timedelta(days=days_ago)
        date_str = f"{d.year}-{d.month:02d}-{d.day:02d}"
        url = f"https://scbo.sc.gov/online-edition?c=3-{date_str}"
        try:
            import os as _os
            html = await _fetch_scbo_html(url)
            _zr = bool(_os.environ.get("ZENROWS_API_KEY", "") or settings.zenrows_api_key)
            logger.info(f"SCBO {date_str}: {len(html)} bytes, zenrows={'yes' if _zr else 'no'}")
            if "<b>Project Name:</b>" not in html:
                logger.warning(f"SCBO {date_str}: no project markers — block/empty. First 200: {html[:200]!r}")
            chunks = html.split("<b>Project Name:</b>")
            for i, chunk in enumerate(chunks[1:], 1):
                def grab(label):
                    idx = chunk.find("<b>" + label + "</b>")
                    if idx == -1:
                        return ""
                    after = chunk[idx:]
                    m = re.search(r'margin-right:0\.5%["\x27]?>(.*?)</div>', after, re.DOTALL)
                    return re.sub(r'<[^>]+>', ' ', m.group(1)).strip() if m else ""
                name_m = re.search(r'margin-right:0\.5%["\x27]?>(.*?)</div>', chunk, re.DOTALL)
                name = re.sub(r'<[^>]+>', ' ', name_m.group(1)).strip() if name_m else ""
                if not name or len(name) < 3:
                    continue
                proj_num = grab("Project Number:")
                location = grab("Project Location:")
                agency = grab("Agency/Owner:")
                cost_range = grab("Construction Cost Range:")
                desc_text = ""
                dp = re.search(r'<p>(.*?)</p>', chunk, re.DOTALL)
                if dp:
                    desc_text = re.sub(r'<[^>]+>', ' ', dp.group(1)).strip()
                ext_id = proj_num or f"scbo-{date_str}-{i}"
                full_desc = f"{name}. {desc_text}. Cost: {cost_range}".strip()
                cat = classify_project(name, full_desc)
                ms = 50  # scored dynamically per-user at request time
                value = None
                val_m = re.findall(r'\$([\d,]+)', cost_range)
                if val_m:
                    try:
                        value = int(val_m[-1].replace(',', ''))
                    except (ValueError, IndexError):
                        pass
                loc_str = location or "South Carolina"
                geo_query = f"{loc_str}, South Carolina, USA" if loc_str != "South Carolina" else "Columbia, South Carolina, USA"
                coords = await geocode(geo_query)
                results.append({
                    "source_id": "scbo", "external_id": ext_id,
                    "title": _clean_text(name, 500),
                    "description": _clean_text(full_desc, 1000),
                    "location": loc_str,
                    "latitude": coords[0] if coords else None,
                    "longitude": coords[1] if coords else None,
                    "value": value, "category": cat, "match_score": ms,
                    "status": "Accepting Bids",
                    "posted_date": _parse_date(date_str),
                    "agency": _clean_text(agency, 255),
                    "solicitation_number": proj_num,
                    "source_url": url,
                    "raw_data": {"project_number": proj_num, "cost_range": cost_range},
                })
        except Exception as e:
            logger.error(f"SCBO error for {date_str}: {e}")
    seen = set()
    results = [r for r in results if r["external_id"] not in seen and not seen.add(r["external_id"])]
    logger.info(f"SCBO: Found {len(results)} construction opportunities")
    return results

async def scan_charleston_bids():
    results = []
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get("https://www.charleston-sc.gov/Bids.aspx?CatID=17")
            resp.raise_for_status()
            clean = re.sub(r"<[^>]+>", "\n", resp.text)
            lines = [l.strip() for l in clean.split("\n") if l.strip() and len(l.strip()) > 20]
            for j, line in enumerate(lines):
                m = re.match(r"(\d{2}-[A-Z]\d{3}[A-Z]?)\s+(.*)", line)
                if m:
                    bid_no, title = m.group(1), m.group(2).strip()
                    desc = " ".join(lines[j+1:j+4]) if j+1 < len(lines) else ""
                    cat = classify_project(title, desc)
                    ms = 50  # scored dynamically per-user at request time
                    results.append({
                        "source_id": "charleston-city-bids",
                        "external_id": f"chs-bid-{bid_no}",
                        "title": f"{bid_no} — {_clean_text(title, 300)}",
                        "description": _clean_text(desc, 1000),
                        "location": "Charleston, SC", "category": cat, "match_score": ms,
                        "latitude": 32.7765, "longitude": -79.9311,
                        "status": "Accepting Bids", "posted_date": datetime.utcnow(),
                        "solicitation_number": bid_no,
                        "source_url": "https://www.charleston-sc.gov/Bids.aspx?CatID=17",
                        "raw_data": {"bid_number": bid_no},
                    })
    except Exception as e:
        logger.error(f"Charleston Bids error: {e}")
    logger.info(f"Charleston Bids: Found {len(results)}")
    return results


async def scan_north_charleston_permits() -> list[dict]:
    """North Charleston building permits via ArcGIS or CustomerPortal.

    Discovery scanner: probes ArcGIS (expected 499 Token Required) then
    CustomerPortal. Returns [] on failure so the pre-clear guard protects DB.
    """
    results = []

    # ── Step 1: Probe ArcGIS PermitCustomers MapServer (likely 499 Token Required) ──
    nc_arcgis_base = (
        "https://arc.northcharleston.org/arcgis/rest/services/Admin/PermitCustomers/MapServer"
    )
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            probe = await client.get(f"{nc_arcgis_base}?f=json")
            logger.info(
                f"NC ArcGIS probe: HTTP {probe.status_code}, "
                f"preview={probe.text[:300]}"
            )
            if probe.status_code == 200:
                data = probe.json()
                if data.get("layers") and not data.get("error"):
                    logger.info(
                        f"NC ArcGIS: public access! layers="
                        f"{[(l.get('id'), l.get('name')) for l in data['layers']]}"
                    )
                    results = await _query_nc_arcgis(client, nc_arcgis_base, data["layers"])
                else:
                    logger.info(f"NC ArcGIS: 200 but error/no layers: {data.get('error')}")
    except Exception as e:
        logger.warning(f"NC ArcGIS probe failed: {e}")

    if results:
        return results

    # ── Step 2: CustomerPortal HTML probe ──
    portal_url = "https://maps.northcharleston.org/CustomerPortal/"
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(portal_url)
            logger.info(
                f"NC CustomerPortal: HTTP {resp.status_code}, "
                f"ct={resp.headers.get('content-type')}, bytes={len(resp.text)}"
            )
            if resp.status_code == 200:
                html = resp.text
                rest_pats = re.findall(
                    r'https?://[^"\'<>\s]{10,}(?:permit|search|query)[^"\'<>\s]*',
                    html, re.IGNORECASE,
                )
                logger.info(f"NC CustomerPortal REST patterns: {rest_pats[:10]}")
                for api_path in [
                    "/CustomerPortal/api/permits",
                    "/CustomerPortal/api/v1/permits",
                    "/PermitPortal/api/permits",
                ]:
                    try:
                        base = f"https://maps.northcharleston.org{api_path}"
                        api_resp = await client.get(base, headers={"Accept": "application/json"})
                        logger.info(
                            f"NC portal API {api_path}: HTTP {api_resp.status_code}, "
                            f"preview={api_resp.text[:200]}"
                        )
                    except Exception as e:
                        logger.info(f"NC portal API {api_path}: {e}")
    except Exception as e:
        logger.warning(f"NC CustomerPortal probe failed: {e}")

    logger.info("NC permits: no data source accessible yet — returning []")
    return results


async def _query_nc_arcgis(client, base_url: str, layers: list) -> list[dict]:
    """Query North Charleston ArcGIS MapServer layers if publicly accessible."""
    results = []
    layer_ids = [
        l.get("id") for l in layers
        if any(kw in (l.get("name") or "").lower() for kw in ["permit", "build", "commercial"])
    ] or [0]
    for layer_id in layer_ids[:2]:
        try:
            r = await client.get(
                f"{base_url}/{layer_id}/query",
                params={"where": "1=1", "outFields": "*", "resultRecordCount": "5",
                        "f": "json", "outSR": "4326"},
            )
            d = r.json()
            if d.get("error"):
                logger.warning(f"NC ArcGIS layer {layer_id}: {d['error']}")
                continue
            feats = d.get("features", [])
            logger.info(
                f"NC ArcGIS layer {layer_id}: sample feats={len(feats)}, "
                f"fields={[f.get('name') for f in d.get('fields', [])]}"
            )
            if feats:
                logger.info(f"NC ArcGIS layer {layer_id} first record: {feats[0].get('attributes', {})}")
            all_r = await client.get(
                f"{base_url}/{layer_id}/query",
                params={"where": "1=1", "outFields": "*", "resultRecordCount": "5000",
                        "f": "json", "outSR": "4326"},
            )
            for feat in all_r.json().get("features", []):
                rec = _parse_nc_arcgis_feature(feat)
                if rec:
                    results.append(rec)
        except Exception as e:
            logger.warning(f"NC ArcGIS layer {layer_id}: {e}")
    return results


def _parse_nc_arcgis_feature(feat: dict) -> Optional[dict]:
    """Parse a North Charleston ArcGIS feature into standard schema (field names TBD)."""
    a = feat.get("attributes", {})
    permit_number = str(
        a.get("PERMIT_NUMBER") or a.get("PermitNumber") or a.get("PERMITNUMBER") or ""
    ).strip()
    if not permit_number or permit_number == "None":
        return None
    address = _clean_text(str(
        a.get("ADDRESS") or a.get("SITE_ADDRESS") or a.get("Address") or ""
    ))
    permit_type = str(a.get("PERMIT_TYPE") or a.get("PermitType") or a.get("TYPE_DESC") or "")
    status = str(a.get("PERMIT_STATUS") or a.get("Status") or "Active")
    skip = {"electrical", "plumbing", "mechanical", "fire", "gas", "sign", "fence", "pool"}
    if any(s in permit_type.lower() for s in skip):
        return None
    value_raw = a.get("JOB_VALUE") or a.get("VALUE") or a.get("VALUATION") or 0
    try:
        value = float(str(value_raw).replace(",", "").replace("$", "") or 0) or None
    except (ValueError, TypeError):
        value = None
    desc = f"{permit_type} — {address}".strip(" —")
    geom = feat.get("geometry") or {}
    return {
        "source_id": "north-charleston-permits",
        "external_id": f"nch-{permit_number}",
        "title": _clean_text(desc, 300),
        "description": _clean_text(desc, 1000),
        "location": f"{address}, North Charleston, SC",
        "address": address,
        "latitude": geom.get("y"),
        "longitude": geom.get("x"),
        "value": value,
        "category": classify_project(desc, desc),
        "match_score": 50,
        "status": status,
        "posted_date": _parse_date(
            a.get("ISSUED_DATE") or a.get("IssuedDate") or a.get("ISSUE_DATE")
        ),
        "permit_number": permit_number,
        "contractor": "",
        "source_url": "https://maps.northcharleston.org/CustomerPortal/",
        "raw_data": dict(a),
    }


async def scan_mt_pleasant_permits() -> list[dict]:
    """Mt. Pleasant building permits via ArcGIS Online FeatureServer or Oracle OPAL.

    Discovery scanner: searches AGOL for permit FeatureServers, queries any found,
    then probes Oracle OPAL. Returns [] on failure.
    """
    results = []

    # ── Step 1: Search ArcGIS Online for Mt. Pleasant permit services ──
    try:
        results = await _scan_mtp_agol()
    except Exception as e:
        logger.warning(f"MtP AGOL search failed: {e}")

    if results:
        return results

    # ── Step 2: Oracle OPAL probe (expect SSO redirect — logs findings) ──
    try:
        await _probe_mtp_opal()
    except Exception as e:
        logger.warning(f"MtP OPAL probe failed: {e}")

    logger.info("MtP permits: no data source accessible yet — returning []")
    return results


async def _scan_mtp_agol() -> list[dict]:
    """Search ArcGIS Online for Mt. Pleasant permit FeatureServers."""
    results = []
    search_url = "https://gis-tomp.maps.arcgis.com/sharing/rest/search"
    async with httpx.AsyncClient(timeout=25.0) as client:
        for q in ["permit owner:gis-tomp", "building permits site:gis-tomp"]:
            resp = await client.get(search_url, params={"q": q, "num": 20, "f": "json"})
            logger.info(f"MtP AGOL search '{q}': HTTP {resp.status_code}")
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = data.get("results", [])
            logger.info(
                f"MtP AGOL: {data.get('total', 0)} total, "
                f"items={[(i.get('title'), i.get('type'), i.get('url')) for i in items]}"
            )
            fs_items = [
                i for i in items
                if "FeatureServer" in (i.get("url") or "")
                or i.get("type") == "Feature Service"
            ]
            for item in fs_items[:2]:
                fs_url = item.get("url", "")
                if fs_url:
                    recs = await _query_mtp_feature_server(client, fs_url)
                    results.extend(recs)
            if results:
                return results
    return results


async def _query_mtp_feature_server(client, fs_url: str) -> list[dict]:
    """Query a Mt. Pleasant ArcGIS FeatureServer and return standard records."""
    results = []
    meta = await client.get(f"{fs_url}?f=json")
    if meta.status_code != 200:
        return results
    data = meta.json()
    if data.get("error"):
        logger.warning(f"MtP FeatureServer error: {data['error']}")
        return results
    layers = data.get("layers", [])
    logger.info(f"MtP FeatureServer: layers={[(l.get('id'), l.get('name')) for l in layers]}")
    for layer in layers[:3]:
        layer_id = layer.get("id", 0)
        if any(skip in (layer.get("name") or "").lower()
               for skip in ["boundary", "parcel", "zoning", "road"]):
            continue
        try:
            r = await client.get(
                f"{fs_url}/{layer_id}/query",
                params={"where": "1=1", "outFields": "*", "resultRecordCount": "10",
                        "f": "json", "outSR": "4326"},
            )
            ld = r.json()
            if ld.get("error"):
                logger.warning(f"MtP layer {layer_id}: {ld['error']}")
                continue
            feats = ld.get("features", [])
            logger.info(
                f"MtP layer {layer_id} ({layer.get('name')}): "
                f"feats={len(feats)}, fields={[f.get('name') for f in ld.get('fields', [])]}"
            )
            if not feats:
                continue
            logger.info(f"MtP layer {layer_id} first record: {feats[0].get('attributes', {})}")
            first_keys = {k.lower() for k in feats[0].get("attributes", {})}
            if any(k in first_keys for k in
                   ("permit_number", "permitnumber", "permit_no", "permit_id")):
                all_r = await client.get(
                    f"{fs_url}/{layer_id}/query",
                    params={"where": "1=1", "outFields": "*", "resultRecordCount": "5000",
                            "f": "json", "outSR": "4326"},
                )
                for feat in all_r.json().get("features", []):
                    rec = _parse_mtp_feature(feat)
                    if rec:
                        results.append(rec)
        except Exception as e:
            logger.warning(f"MtP layer {layer_id}: {e}")
    return results


def _parse_mtp_feature(feat: dict) -> Optional[dict]:
    """Parse a Mt. Pleasant ArcGIS feature into standard schema."""
    a = feat.get("attributes", {})
    permit_number = str(
        a.get("PERMIT_NUMBER") or a.get("PermitNumber") or a.get("PERMIT_NO")
        or a.get("PERMIT_ID") or a.get("permit_number") or ""
    ).strip()
    if not permit_number or permit_number == "None":
        return None
    address = _clean_text(str(
        a.get("ADDRESS") or a.get("SITE_ADDRESS") or a.get("WORK_ADDRESS")
        or a.get("address") or ""
    ))
    permit_type = str(
        a.get("PERMIT_TYPE") or a.get("PermitType") or a.get("TYPE") or a.get("permit_type") or ""
    )
    status = str(a.get("PERMIT_STATUS") or a.get("Status") or a.get("STATUS") or "Active")
    skip = {"electrical", "plumbing", "mechanical", "fire", "gas", "sign", "fence", "pool"}
    if any(s in permit_type.lower() for s in skip):
        return None
    value_raw = a.get("JOB_VALUE") or a.get("VALUE") or a.get("VALUATION") or a.get("value") or 0
    try:
        value = float(str(value_raw).replace(",", "").replace("$", "") or 0) or None
    except (ValueError, TypeError):
        value = None
    desc = f"{permit_type} — {address}".strip(" —")
    geom = feat.get("geometry") or {}
    return {
        "source_id": "mt-pleasant-permits",
        "external_id": f"mtp-{permit_number}",
        "title": _clean_text(desc, 300),
        "description": _clean_text(desc, 1000),
        "location": f"{address}, Mt. Pleasant, SC",
        "address": address,
        "latitude": geom.get("y"),
        "longitude": geom.get("x"),
        "value": value,
        "category": classify_project(desc, desc),
        "match_score": 50,
        "status": status,
        "posted_date": _parse_date(
            a.get("ISSUED_DATE") or a.get("ISSUE_DATE") or a.get("IssuedDate")
        ),
        "permit_number": permit_number,
        "contractor": "",
        "source_url": "https://gis-tomp.hub.arcgis.com",
        "raw_data": dict(a),
    }


async def _probe_mtp_opal() -> None:
    """Probe Mt. Pleasant's Oracle OPAL portal. Logs findings for Railway diagnostics."""
    opal_url = "https://eody.fa.us6.oraclecloud.com/fscmUI/publicSector.html"
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=False) as client:
        resp = await client.get(opal_url)
        logger.info(
            f"MtP OPAL: HTTP {resp.status_code}, "
            f"ct={resp.headers.get('content-type')}, "
            f"location={resp.headers.get('location', '')[:200]}"
        )
        rest_url = (
            "https://eody.fa.us6.oraclecloud.com/fscmRestApi/resources/"
            "11.13.18.05/publicPermits"
        )
        try:
            api_resp = await client.get(
                rest_url,
                params={"limit": 10, "fields": "PermitNumber,PermitTypeName,StatusCode,AddressLine1"},
                headers={"Accept": "application/json"},
            )
            logger.info(
                f"MtP OPAL REST: HTTP {api_resp.status_code}, "
                f"preview={api_resp.text[:300]}"
            )
        except Exception as e:
            logger.info(f"MtP OPAL REST probe: {e}")


ALL_SCANNERS = {
    "sam-gov": {"name": "SAM.gov Federal Opportunities", "func": scan_sam_gov, "needs_key": True},
    "charleston-permits": {"name": "Charleston Building Permits", "func": scan_charleston_permits, "needs_key": False},
    "north-charleston-permits": {"name": "North Charleston Building Permits", "func": scan_north_charleston_permits, "needs_key": False},
    "mt-pleasant-permits": {"name": "Mt. Pleasant Building Permits", "func": scan_mt_pleasant_permits, "needs_key": False},
    "scbo": {"name": "SC Business Opportunities", "func": scan_scbo, "needs_key": False},
    "charleston-city-bids": {"name": "Charleston City Bids", "func": scan_charleston_bids, "needs_key": False},
}
