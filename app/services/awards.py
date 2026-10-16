"""State award notices from Charleston-area agencies: who was shortlisted and who won.

State agencies must post OSE notices when they shortlist or award a job: SE-212/SE-412/
SE-512 selection for interview, SE-170 prequalified bidders, SE-370/SE-375 design-bid-build
awards, SE-470 CM at Risk, SE-570 design-build, SE-221 A/E, SE-621/SE-670 IDQ and task
order contracts. They're posted per agency, not centrally, and taken down after a while,
so this job reads each agency's page daily and keeps what it finds. MUSC posts notices
only on physical bulletin boards, so it isn't covered.

Notices are PDFs; some are scans and some combine several forms, so Claude Haiku reads
each PDF and returns the fields. Each PDF is read once (remembered in pipeline_documents).
"""

import asyncio
import base64
import logging
import re
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urljoin

import anthropic
from sqlalchemy import select

from app.config import get_settings
from app.models.database import ContractAward, PipelineDocument, get_session_factory
from app.services.ai_text import response_text
from app.services.contractor_match import normalize_name
from app.services.parcel_analysis import _parse_json
from app.services.pipeline.events import PipelineEvent, normalize_pip, pip_project_key

logger = logging.getLogger("sitescan.awards")

MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 2000
MAX_PDF_BYTES = 15 * 1024 * 1024
DOC_SOURCE = "award"

AGENCY_PAGES = [
    ("College of Charleston", "https://charleston.edu/facilities-management/contract-awards.php"),
    ("The Citadel", "https://www.citadel.edu/ofe/project-information/"),
    ("Trident Technical College", "https://www.tridenttech.edu/about/departments/engineering_construction/projects.html"),
]

NOTICE_TYPES = ("award", "shortlist", "selection", "prequalified")
_AWARDS_LOCK_KEY = 7_324_115_007   # Postgres advisory lock: one collection run at a time


def firm_key(name: str) -> str:
    """Normalized firm name, ignoring branch suffixes ("… - Charleston", "… of Charleston")."""
    base = re.sub(r"\s+[-–]\s+[A-Za-z .]+$", "", name or "")
    base = re.sub(r"\s+(of\s+)?charleston(,?\s*(inc|llc|ltd)\.?)?\s*$", "", base, flags=re.IGNORECASE)
    return normalize_name(base) or normalize_name(name or "")


def agency_name(raw: str) -> str:
    """One spelling per agency ("COLLEGE OF CHARLESTON, Facility Management" -> "College of Charleston")."""
    low = (raw or "").lower()
    for name, _ in AGENCY_PAGES:
        if name.lower().replace("the ", "") in low:
            return name
    return (raw or "").strip()
DELIVERIES = ("design-bid-build", "cmr", "design-build", "professional", "idq", "task-order", "other")
ROLES = ("contractor", "cm", "design-builder", "architect-engineer", "other")

SYSTEM_PROMPT = """You read South Carolina Office of State Engineer (OSE) procurement notices.
A PDF may hold one or several forms (e.g. an SE-310 invitation stapled to an SE-370 award).

Return JSON only:
{"notices": [{
  "form": "SE-370",
  "notice_type": "award" | "shortlist" | "selection" | "prequalified" | "invitation" | "other",
  "delivery": "design-bid-build" | "cmr" | "design-build" | "professional" | "idq" | "task-order" | "other",
  "agency": "...", "project_name": "...", "project_number": "H15-9689-PD-A",
  "posting_date": "YYYY-MM-DD" or null,
  "firms": [{"name": "...", "role": "contractor" | "cm" | "design-builder" | "architect-engineer" | "other",
             "amount": number or null}]
}]}

Rules:
- notice_type: "award" = notice of intent to award / notice of award; "shortlist" = selection for
  interview; "selection" = notification of selection (ranked first, before award); "prequalified" =
  prequalified bidders list; "invitation" = an invitation to bid or submit (no firms named).
- firms: every firm named as bidder, offeror, contractor, CM, design-builder or A/E in that notice,
  exactly as written. Skip empty lines. amount = total contract award (or total bid if the award is
  blank or $0) in dollars, only for the winning firm of an award.
- Leave out signatures, agency staff and anything that isn't a firm.
"""


def _fetch_text(url: str) -> str:
    from curl_cffi import requests as creq   # browser-like TLS: some agency sites block plain clients
    r = creq.get(url, impersonate="chrome", timeout=40)
    r.raise_for_status()
    return r.text


def _fetch_bytes(url: str) -> bytes:
    from curl_cffi import requests as creq
    r = creq.get(url, impersonate="chrome", timeout=60)
    r.raise_for_status()
    return r.content


def notice_links(html: str, base_url: str) -> list[str]:
    """PDF links on an agency page that look like OSE notices (not invitations or plans)."""
    out = []
    for href in re.findall(r'href="([^"]+\.pdf)"', html, re.IGNORECASE):
        url = urljoin(base_url, href.replace("&amp;", "&"))
        name = url.rsplit("/", 1)[-1].lower()
        if re.search(r"se[-_ %20]*\d{3}|award|intent|selection|prequal|notice|noi\b", name) or "contract-awards" in url:
            if url not in out:
                out.append(url)
    return out


def _date(v: Any) -> Optional[datetime]:
    try:
        return datetime.strptime(str(v)[:10], "%Y-%m-%d") if v else None
    except ValueError:
        return None


def _choice(v: Any, allowed: tuple, default: str) -> str:
    v = str(v or "").strip().lower()
    return v if v in allowed else default


def rows_from_extraction(data: dict, url: str, agency: str) -> list[dict]:
    """Flatten the model's notices into one row per firm, dropping invitations and blanks."""
    rows = []
    for n in (data or {}).get("notices") or []:
        if not isinstance(n, dict):
            continue
        kind = str(n.get("notice_type") or "").lower()
        if kind not in NOTICE_TYPES:
            continue
        number = str(n.get("project_number") or "").strip()[:60]
        for f in n.get("firms") or []:
            name = " ".join(str((f or {}).get("name") or "").split())[:255]
            if len(name) < 3:
                continue
            try:
                amount = float(f.get("amount")) if f.get("amount") not in (None, "") else None
            except (TypeError, ValueError):
                amount = None
            rows.append({
                "source_url": url[:1000],
                "agency": agency_name(str(n.get("agency") or "").strip() or agency)[:255],
                "form": str(n.get("form") or "").strip().upper()[:20],
                "notice_type": kind,
                "delivery": _choice(n.get("delivery"), DELIVERIES, "other"),
                "project_name": " ".join(str(n.get("project_name") or "").split())[:500],
                "project_number": number,
                "pip_number": normalize_pip(number),
                "firm": name,
                "firm_key": firm_key(name)[:255],
                "role": _choice(f.get("role"), ROLES, "other"),
                "amount": amount if amount and amount > 0 else None,
                "posted_date": _date(n.get("posting_date")),
            })
    return rows


async def extract_notice(pdf: bytes, client: Any) -> dict:
    message = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": [
            {"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                            "data": base64.standard_b64encode(pdf).decode()}},
            {"type": "text", "text": "Extract the notices in this PDF."},
        ]}],
    )
    return _parse_json(response_text(message))


_STAGE_LABEL = {"award": "Awarded", "shortlist": "Shortlisted", "selection": "Selected", "prequalified": "Prequalified"}


def award_events(rows: list[dict]) -> list[PipelineEvent]:
    """One pipeline event per notice on a state project, so it shows on the project's timeline."""
    by_notice: dict = {}
    for r in rows:
        if r["pip_number"]:
            by_notice.setdefault((r["source_url"], r["project_number"], r["notice_type"]), []).append(r)
    events = []
    for (url, number, kind), rs in by_notice.items():
        first = rs[0]
        firms = ", ".join(r["firm"] + (f" (${r['amount']:,.0f})" if r["amount"] else "") for r in rs)
        events.append(PipelineEvent(
            source="award",
            external_id=f"{url}#{number}#{kind}"[:255],
            project_key=pip_project_key(first["pip_number"]),
            title=f"{_STAGE_LABEL.get(kind, kind)}: {firms} – {first['project_name']}"[:500],
            source_url=url,
            event_date=first["posted_date"].date() if first["posted_date"] else None,
            stage="award",
            owner=first["agency"],
            pip_number=first["pip_number"],
            text=f"{first['form']} {kind} notice for {first['project_name']} ({number}): {firms}",
        ))
    return events


async def run_awards_job(session_factory=None, client: Optional[Any] = None) -> dict:
    """Read new notices from each agency page. Never raises."""
    summary = {"started_at": datetime.utcnow().isoformat(), "pages": {}, "new_documents": 0, "rows": 0,
               "events": 0, "failed": 0, "errors": []}
    settings = get_settings()
    if client is None:
        if not settings.anthropic_api_key:
            logger.info("Awards job skipped: ANTHROPIC_API_KEY not configured")
            summary["errors"].append("ANTHROPIC_API_KEY not configured")
            await _record_run(session_factory or get_session_factory(), summary)
            return summary
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    session_factory = session_factory or get_session_factory()
    from app.services.parcel_analysis import _single_runner_lock
    async with _single_runner_lock(session_factory, key=_AWARDS_LOCK_KEY) as got:
        if not got:
            logger.info("Awards job skipped: another run is in progress")
            return {**summary, "skipped": "another run is in progress"}
        return await _collect(summary, session_factory, client)


async def _collect(summary: dict, session_factory, client: Any) -> dict:
    try:
        async with session_factory() as db:
            seen = set((await db.execute(select(PipelineDocument.url).where(PipelineDocument.source == DOC_SOURCE))).scalars().all())
        for agency, page in AGENCY_PAGES:
            try:
                links = notice_links(await asyncio.to_thread(_fetch_text, page), page)
                summary["pages"][agency] = len(links)
            except Exception as e:
                logger.warning(f"Awards: {agency} page failed: {type(e).__name__}: {e}")
                summary["pages"][agency] = f"failed: {type(e).__name__}: {str(e)[:150]}"
                summary["failed"] += 1
                continue
            for url in links:
                if url in seen:
                    continue
                try:
                    pdf = await asyncio.to_thread(_fetch_bytes, url)
                    rows = [] if len(pdf) > MAX_PDF_BYTES else rows_from_extraction(await extract_notice(pdf, client), url, agency)
                    await _save(session_factory, url, rows)
                except Exception as e:
                    logger.warning(f"Awards: {url} failed: {type(e).__name__}: {e}")
                    summary["failed"] += 1
                    if len(summary["errors"]) < 10:
                        summary["errors"].append(f"{url.rsplit('/', 1)[-1]}: {type(e).__name__}: {str(e)[:200]}")
                    continue
                seen.add(url)
                summary["new_documents"] += 1
                summary["rows"] += len(rows)
                summary["events"] += len(award_events(rows))
    except Exception as e:
        logger.error(f"Awards job failed: {e}", exc_info=True)
        summary["errors"].append(f"job: {type(e).__name__}: {str(e)[:200]}")
    summary["finished_at"] = datetime.utcnow().isoformat()
    logger.info(f"Awards job: {summary}")
    await _record_run(session_factory, summary)
    return summary


STATUS_KEY = "awards:last-run"


async def _record_run(session_factory, summary: dict) -> None:
    """Keep the last run's summary so /awards/status can show what happened."""
    from app.models.database import CachedPayload
    try:
        async with session_factory() as db:
            row = (await db.execute(select(CachedPayload).where(CachedPayload.key == STATUS_KEY))).scalars().first()
            if row is None:
                db.add(CachedPayload(key=STATUS_KEY, payload=summary, fetched_at=datetime.utcnow()))
            else:
                row.payload, row.fetched_at = summary, datetime.utcnow()
            await db.commit()
    except Exception as e:
        logger.warning(f"Awards: couldn't record run summary: {e}")


async def _save(session_factory, url: str, rows: list[dict]) -> None:
    from app.services.pipeline.store import store_events
    async with session_factory() as db:
        keys = set()
        for r in rows:
            k = (r["project_number"], r["firm"], r["notice_type"])
            if k not in keys:
                keys.add(k)
                db.add(ContractAward(**r))
        db.add(PipelineDocument(source=DOC_SOURCE, url=url[:1000], event_count=len(rows),
                                document_date=next((r["posted_date"] for r in rows if r["posted_date"]), None)))
        events = award_events(rows)
        if events:
            await store_events(db, events)
        await db.commit()


# ─── Read side ───────────────────────────────────────────────────────────────

def firm_rankings(rows: list) -> list[dict]:
    """Firms ranked by awards, then shortlists, with what they won and where."""
    firms: dict[str, dict] = {}
    for r in rows:
        key = firm_key(r.firm)
        e = firms.setdefault(key, {"key": key, "names": set(), "awards": 0, "shortlists": 0, "prequalified": 0,
                                   "awarded_value": 0.0, "agencies": set(), "roles": set(), "last_date": None, "notices": []})
        e["names"].add(r.firm)
        if r.notice_type in ("award", "selection"):
            e["awards"] += 1
            e["awarded_value"] += r.amount or 0
        elif r.notice_type == "shortlist":
            e["shortlists"] += 1
        elif r.notice_type == "prequalified":
            e["prequalified"] += 1
        e["agencies"].add(agency_name(r.agency))
        e["roles"].add(r.role)
        if r.posted_date and (e["last_date"] is None or r.posted_date > e["last_date"]):
            e["last_date"] = r.posted_date
        e["notices"].append(r)
    out = []
    for e in firms.values():
        notices = sorted(e["notices"], key=lambda r: r.posted_date or datetime.min, reverse=True)
        out.append({
            "key": e["key"], "name": max(e["names"], key=len),
            "awards": e["awards"], "shortlists": e["shortlists"], "prequalified": e["prequalified"],
            "awarded_value": e["awarded_value"], "agencies": sorted(e["agencies"]), "roles": sorted(e["roles"]),
            "last_date": e["last_date"].isoformat() if e["last_date"] else None,
            "notices": [award_out(r) for r in notices[:8]],
        })
    out.sort(key=lambda f: (f["awards"], f["shortlists"], f["awarded_value"]), reverse=True)
    return out


def award_out(r) -> dict:
    return {"agency": agency_name(r.agency), "form": r.form, "notice_type": r.notice_type, "delivery": r.delivery,
            "project_name": r.project_name, "project_number": r.project_number, "pip_number": r.pip_number,
            "firm": r.firm, "role": r.role, "amount": r.amount, "source_url": r.source_url,
            "posted_date": r.posted_date.isoformat() if r.posted_date else None}
