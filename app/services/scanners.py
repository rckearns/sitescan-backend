"""Data source scanners — fetch opportunities from external APIs and websites."""

import asyncio
import re
import logging
from datetime import datetime, timedelta
from typing import Optional
import httpx
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
    try:
        resp = await client.get(
            _ENERGOV_PERMIT_URL.format(pmpermitid),
            headers=_ENERGOV_TENANT_HEADERS,
            timeout=10.0,
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
    except Exception:
        pass
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
    # Layer 21: new construction since 2010 — only "Issued" (still-active) projects
    # No type filter needed: this layer only contains significant new construction
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

            feats20 = resp20.json().get("features", [])
            feats21 = resp21.json().get("features", [])
            logger.info(f"ArcGIS Layer 20: {len(feats20)} features, Layer 21: {len(feats21)} features")

            # Merge and deduplicate by PMPERMITID (Layer 20 takes precedence for duplicates)
            seen_pids: set[str] = set()
            features = []
            for f in feats20 + feats21:
                pid = str(f.get("attributes", {}).get("PMPERMITID") or "")
                if pid and pid in seen_pids:
                    continue
                seen_pids.add(pid)
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

                ext_id = str(a.get("OBJECTID") or a.get("PERMIT_NUMBER") or hash(title))
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
            semaphore = asyncio.Semaphore(15)

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


async def scan_scbo():
    """Scrape SC Business Opportunities - Construction category."""
    results = []
    today = datetime.utcnow()
    for days_ago in range(7):
        d = today - timedelta(days=days_ago)
        date_str = f"{d.year}-{d.month:02d}-{d.day:02d}"
        url = f"https://scbo.sc.gov/online-edition?c=3-{date_str}"
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, verify=False, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5"}) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text
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


ALL_SCANNERS = {
    "sam-gov": {"name": "SAM.gov Federal Opportunities", "func": scan_sam_gov, "needs_key": True},
    "charleston-permits": {"name": "Charleston Building Permits", "func": scan_charleston_permits, "needs_key": False},
    "scbo": {"name": "SC Business Opportunities", "func": scan_scbo, "needs_key": False},
    "charleston-city-bids": {"name": "Charleston City Bids", "func": scan_charleston_bids, "needs_key": False},
}
