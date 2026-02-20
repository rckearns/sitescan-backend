"""Data source scanners — fetch opportunities from external APIs and websites."""

import re
import logging
from datetime import datetime, timedelta
from typing import Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.services.scoring import classify_project, score_match, score_with_value_boost

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
        "api_key": api_key, "limit": "50",
        "postedFrom": from_date.strftime("%m/%d/%Y"),
        "postedTo": today.strftime("%m/%d/%Y"),
        "ptype": "o,p,k",
    }
    if state: params["state"] = state
    if keywords: params["title"] = keywords

    results = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for naics in CONSTRUCTION_NAICS:
            params["ncode"] = naics
            try:
                resp = await client.get("https://api.sam.gov/prod/opportunities/v2/search", params=params)
                resp.raise_for_status()
                data = resp.json()
                for opp in data.get("opportunitiesData", []):
                    title = _clean_text(opp.get("title", ""), 500)
                    desc = ""
                    if isinstance(opp.get("description"), dict):
                        desc = _clean_text(opp["description"].get("body", ""))
                    elif isinstance(opp.get("description"), str):
                        desc = _clean_text(opp["description"])
                    cat = classify_project(title, desc)
                    ms = score_with_value_boost(score_match(title, desc, keywords), None)
                    results.append({
                        "source_id": "sam-gov", "external_id": str(opp.get("noticeId", "")),
                        "title": title, "description": desc, "location": state,
                        "value": None, "category": cat, "match_score": ms,
                        "status": "Open" if opp.get("active") == "Yes" else "Closed",
                        "posted_date": _parse_date(opp.get("postedDate")),
                        "deadline": _parse_date(opp.get("responseDeadLine")),
                        "agency": _clean_text(opp.get("fullParentPathName", ""), 255),
                        "solicitation_number": opp.get("solicitationNumber", ""),
                        "naics_code": naics,
                        "source_url": f"https://sam.gov/opp/{opp.get('noticeId','')}/view",
                        "raw_data": opp,
                    })
            except Exception as e:
                logger.error(f"SAM.gov NAICS {naics}: {e}")

    seen = set()
    return [r for r in results if r["external_id"] not in seen and not seen.add(r["external_id"])]


async def scan_charleston_permits(arcgis_url="", record_count=100):
    """Fetch permits from Charleston ArcGIS. Field names:
    OBJECTID, DESCRIPTION, PERMIT_NUMBER, PERMIT_TYPE, WORK_CLASS,
    PERMIT_STATUS, ISSUE_DATE, VALUATION, ADDRESS, CONTRACTOR, LATITUDE, LONGITUDE
    """
    if not arcgis_url:
        arcgis_url = (
            "https://gis.charleston-sc.gov/arcgis2/rest/services/"
            "External/Applications/MapServer/20/query"
        )

    params = {
        "where": "1=1", "outFields": "*",
        "orderByFields": "ISSUE_DATE DESC",
        "resultRecordCount": str(record_count), "f": "json",
    }
    results = []

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(arcgis_url, params=params)
            resp.raise_for_status()
            data = resp.json()

            logger.info(f"Charleston ArcGIS returned {len(data.get('features', []))} features")

            for feature in data.get("features", []):
                a = feature.get("attributes", {})

                permit_type = str(a.get("PERMIT_TYPE") or a.get("PERMITTYPE") or "Permit")
                address = str(a.get("ADDRESS") or "")
                description = _clean_text(a.get("DESCRIPTION"))
                contractor = _clean_text(a.get("CONTRACTOR"), 255)
                work_class = str(a.get("WORK_CLASS") or "")

                title = f"{permit_type} — {address}" if address else permit_type
                full_desc = f"{description} {work_class} {contractor}".strip()

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
                cat = classify_project(title, full_desc)
                ms = score_with_value_boost(score_match(title, full_desc), value)

                results.append({
                    "source_id": "charleston-permits",
                    "external_id": f"chs-{ext_id}",
                    "title": title, "description": full_desc,
                    "location": f"{address}, Charleston, SC" if address else "Charleston, SC",
                    "address": address,
                    "latitude": float(lat) if lat else None,
                    "longitude": float(lng) if lng else None,
                    "value": value, "category": cat, "match_score": ms,
                    "status": str(a.get("PERMIT_STATUS") or "Active"),
                    "posted_date": posted,
                    "permit_number": str(a.get("PERMIT_NUMBER") or ""),
                    "contractor": contractor,
                    "source_url": "https://gis.charleston-sc.gov/interactive/permits/",
                    "raw_data": a,
                })

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
    for days_ago in range(3):
        d = today - timedelta(days=days_ago)
        date_str = f"{d.year}-{d.month:02d}-{d.day:02d}"
        url = f"https://scbo.sc.gov/online-edition?c=3-{date_str}"
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, verify=False) as client:
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
                    ms = score_match(name, full_desc)
                    value = None
                    val_m = re.findall(r'\$([\d,]+)', cost_range)
                    if val_m:
                        try:
                            value = int(val_m[-1].replace(',', ''))
                        except (ValueError, IndexError):
                            pass
                    results.append({
                        "source_id": "scbo", "external_id": ext_id,
                        "title": _clean_text(name, 500),
                        "description": _clean_text(full_desc, 1000),
                        "location": location or "South Carolina",
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
                    ms = score_match(title, desc)
                    results.append({
                        "source_id": "charleston-city-bids",
                        "external_id": f"chs-bid-{bid_no}",
                        "title": f"{bid_no} — {_clean_text(title, 300)}",
                        "description": _clean_text(desc, 1000),
                        "location": "Charleston, SC", "category": cat, "match_score": ms,
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
