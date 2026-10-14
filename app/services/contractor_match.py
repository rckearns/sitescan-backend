"""
Contractor matching and trade inference.

Three main capabilities:
1. Cross-reference LLR directory entries against permit contractor names.
2. Infer a contractor's trade from their COMPANY NAME and LLR license code.
3. Build a trade roster with permit-derived market share data.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import DirectoryEntry, Project, get_session_factory

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Name normalization
# ──────────────────────────────────────────────────────────────────────────────

# Legal entity suffixes — always strip anywhere in name
_ENTITY_SUFFIXES = re.compile(
    r"""\b(
        llc|l\.l\.c\.?|inc\.?|incorporated|corp\.?|corporation|
        co\.?|company|dba|d/b/a|of\s+sc|of\s+south\s+carolina|the
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)
# Industry words — only strip when trailing (so "Palmetto Builders Group" keeps
# "builders" but "Smith Masonry Contractors LLC" drops "contractors")
_TRAILING_INDUSTRY = re.compile(
    r"""\s+(
        enterprises?|contractors?|contracting|construction|
        builders?|building|services?|solutions?|
        group|partners?|associates?
    )\s*$""",
    re.IGNORECASE | re.VERBOSE,
)
_PUNCT = re.compile(r"[^a-z0-9\s]")
_MULTI_SPACE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """Reduce a company name to a canonical form for matching."""
    s = name.lower().strip()
    s = _ENTITY_SUFFIXES.sub(" ", s)
    s = _PUNCT.sub(" ", s)
    s = _MULTI_SPACE.sub(" ", s).strip()
    # Strip trailing industry words AFTER punctuation cleanup so "Inc." is gone first
    s = _TRAILING_INDUSTRY.sub("", s).strip()
    return s


def _name_tokens(normalized: str) -> set[str]:
    """Split a normalized name into tokens for partial matching."""
    return set(normalized.split())


def match_score(dir_name: str, permit_name: str) -> float:
    """
    Return a 0.0–1.0 similarity score between two company names.

    - 1.0 = exact normalized match
    - 0.8+ = one name is a substring of the other (e.g. "ABC" vs "ABC Construction")
    - 0.6+ = high token overlap (Jaccard)
    - Below 0.6 = no match
    """
    a = normalize_name(dir_name)
    b = normalize_name(permit_name)

    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    # Substring containment — "smith masonry" in "john smith masonry"
    if a in b or b in a:
        return 0.85

    # Token overlap (Jaccard similarity)
    ta = _name_tokens(a)
    tb = _name_tokens(b)
    if not ta or not tb:
        return 0.0
    intersection = ta & tb
    union = ta | tb
    jaccard = len(intersection) / len(union)

    # Require at least 60% overlap AND at least 2 shared tokens (or all tokens of the shorter name)
    min_len = min(len(ta), len(tb))
    if jaccard >= 0.6 and (len(intersection) >= 2 or len(intersection) >= min_len):
        return jaccard

    return 0.0


_MATCH_THRESHOLD = 0.6


# ──────────────────────────────────────────────────────────────────────────────
# Permit activity map builder
# ──────────────────────────────────────────────────────────────────────────────

def _build_permit_contractor_map(
    projects: list[Any],
) -> dict[str, dict[str, Any]]:
    """
    Build a map of normalized contractor name → aggregated permit stats.

    Returns:
        {normalized_name: {
            "original_names": {set of original names},
            "permit_count": int,
            "total_value": float,
            "last_date": datetime | None,
        }}
    """
    cmap: dict[str, dict[str, Any]] = {}

    for p in projects:
        raw = (p.contractor or "").strip()
        if not raw:
            continue
        for name in raw.split("|"):
            name = name.strip()
            if not name:
                continue
            norm = normalize_name(name)
            if not norm:
                continue
            if norm not in cmap:
                cmap[norm] = {
                    "original_names": set(),
                    "permit_count": 0,
                    "total_value": 0.0,
                    "last_date": None,
                }
            entry = cmap[norm]
            entry["original_names"].add(name)
            entry["permit_count"] += 1
            if p.value:
                entry["total_value"] += p.value
            if p.posted_date:
                if entry["last_date"] is None or p.posted_date > entry["last_date"]:
                    entry["last_date"] = p.posted_date

    return cmap


# ──────────────────────────────────────────────────────────────────────────────
# Main enrichment task
# ──────────────────────────────────────────────────────────────────────────────

async def enrich_directory_with_permits() -> dict[str, int]:
    """
    Cross-reference all DirectoryEntry rows against permit contractor names.

    Updates permit_count, total_permit_value, last_permit_date, matched_names
    on each DirectoryEntry.  Returns summary stats.
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        # 1. Load all projects that have a contractor name
        proj_result = await session.execute(
            select(Project).where(
                Project.contractor != "",
                Project.contractor.is_not(None),
            )
        )
        projects = proj_result.scalars().all()
        log.info("Contractor enrichment: loaded %d projects with contractor data", len(projects))

        # 2. Build the permit contractor map (normalized name → stats)
        permit_map = _build_permit_contractor_map(projects)
        log.info("Contractor enrichment: %d unique normalized contractor names", len(permit_map))

        # Pre-compute list of (normalized_name, stats) for fuzzy matching
        permit_entries = list(permit_map.items())

        # 3. Load all directory entries
        dir_result = await session.execute(select(DirectoryEntry))
        dir_entries = dir_result.scalars().all()
        log.info("Contractor enrichment: %d directory entries to match", len(dir_entries))

        matched = 0
        unmatched = 0
        now = datetime.utcnow()

        for de in dir_entries:
            dir_norm = normalize_name(de.company_name)
            if not dir_norm:
                de.permit_count = 0
                de.total_permit_value = 0.0
                de.last_permit_date = None
                de.matched_names = []
                de.enriched_at = now
                unmatched += 1
                continue

            # Try exact normalized match first (fast path)
            if dir_norm in permit_map:
                stats = permit_map[dir_norm]
                de.permit_count = stats["permit_count"]
                de.total_permit_value = stats["total_value"]
                de.last_permit_date = stats["last_date"]
                de.matched_names = sorted(stats["original_names"])
                de.enriched_at = now
                matched += 1
                continue

            # Fuzzy match against all permit names
            best_score = 0.0
            best_matches: list[dict[str, Any]] = []

            for perm_norm, perm_stats in permit_entries:
                sc = match_score(de.company_name, next(iter(perm_stats["original_names"])))
                if sc >= _MATCH_THRESHOLD and sc > best_score - 0.1:
                    if sc > best_score:
                        best_score = sc
                        best_matches = [(perm_stats, sc)]
                    elif sc >= best_score - 0.1:
                        best_matches.append((perm_stats, sc))

            if best_matches:
                # Aggregate across all close matches (a company might appear
                # under slightly different names across permits)
                total_count = 0
                total_value = 0.0
                last_date = None
                all_names: set[str] = set()

                for bm_stats, _ in best_matches:
                    total_count += bm_stats["permit_count"]
                    total_value += bm_stats["total_value"]
                    all_names.update(bm_stats["original_names"])
                    if bm_stats["last_date"]:
                        if last_date is None or bm_stats["last_date"] > last_date:
                            last_date = bm_stats["last_date"]

                de.permit_count = total_count
                de.total_permit_value = total_value
                de.last_permit_date = last_date
                de.matched_names = sorted(all_names)
                de.enriched_at = now
                matched += 1
            else:
                de.permit_count = 0
                de.total_permit_value = 0.0
                de.last_permit_date = None
                de.matched_names = []
                de.enriched_at = now
                unmatched += 1

        await session.commit()

        summary = {
            "total_directory_entries": len(dir_entries),
            "matched": matched,
            "unmatched": unmatched,
            "unique_permit_contractors": len(permit_map),
        }
        log.info("Contractor enrichment complete: %s", summary)
        return summary


# ──────────────────────────────────────────────────────────────────────────────
# Trade inference from COMPANY NAME (not permit text)
# ──────────────────────────────────────────────────────────────────────────────

# Patterns matched against the contractor's COMPANY NAME to determine trade.
_NAME_TRADE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("framing", re.compile(
        r"fram(?:ing|e(?:rs?)?)|truss|timber\s*frame",
        re.IGNORECASE,
    )),
    ("masonry", re.compile(
        r"mason(?:ry)?|brick|stone\s*work|tuckpoint|repoint|stucco",
        re.IGNORECASE,
    )),
    ("concrete", re.compile(
        r"concrete|precast|flatwork|post[\s-]*tension|foundation[s]?\b",
        re.IGNORECASE,
    )),
    ("roofing", re.compile(
        r"roof(?:ing|ers?)?|shingle",
        re.IGNORECASE,
    )),
    ("electrical", re.compile(
        r"electric(?:al)?|wiring|power\s*(?:systems|design|solutions|group|services)",
        re.IGNORECASE,
    )),
    ("plumbing", re.compile(
        r"plumb(?:ing|ers?)?|piping|backflow",
        re.IGNORECASE,
    )),
    ("hvac", re.compile(
        r"hvac|heating|cooling|air\s*condition|mechanical(?!\s*metals)|"
        r"refriger",
        re.IGNORECASE,
    )),
    ("drywall-finishes", re.compile(
        r"drywall|sheetrock|plaster|paint(?:ing|ers?)?|finish(?:ing|es)",
        re.IGNORECASE,
    )),
    ("siding-exterior", re.compile(
        r"siding|cladding|exterior\s*(?:finish|panel)",
        re.IGNORECASE,
    )),
    ("windows-doors", re.compile(
        r"window|glass|glazing|door(?:s|\b)",
        re.IGNORECASE,
    )),
    ("grading-sitework", re.compile(
        r"grad(?:ing|ers?)|excavat|earthwork|sitework|site\s*work|"
        r"paving|asphalt|land\s*clearing",
        re.IGNORECASE,
    )),
    ("demolition", re.compile(
        r"demol(?:ition|ish)|abatement|wrecking",
        re.IGNORECASE,
    )),
    ("fire-protection", re.compile(
        r"fire\s*(?:sprinkler|protect|suppress)|sprinkler\s*system",
        re.IGNORECASE,
    )),
    ("insulation", re.compile(
        r"insulat|spray\s*foam",
        re.IGNORECASE,
    )),
    ("general", re.compile(
        r"general\s*contract|construction|builder|develop",
        re.IGNORECASE,
    )),
]

# SC LLR classification code → trade ID
_LLR_TRADE_MAP: dict[str, str] = {
    "SF": "framing",
    "WF": "framing",
    "MS": "masonry",
    "CT": "concrete",
    "CP": "concrete",
    "RF": "roofing",
    "EL": "electrical",
    "PB": "plumbing",
    "AC": "hvac",
    "HT": "hvac",
    "RG": "hvac",
    "BL": "hvac",
    "GG": "windows-doors",
    "GD": "grading-sitework",
    "GE": "grading-sitework",
    "AP": "grading-sitework",
    "NR": "drywall-finishes",
    "MM": "general",
    "MR": "general",
    "BD": "general",
    "CCM": "general",
}

# Human-readable labels
TRADE_LABELS: dict[str, str] = {
    "framing": "Framing",
    "masonry": "Masonry",
    "concrete": "Concrete / Foundations",
    "roofing": "Roofing",
    "electrical": "Electrical",
    "plumbing": "Plumbing",
    "hvac": "HVAC / Mechanical",
    "drywall-finishes": "Drywall & Finishes",
    "siding-exterior": "Siding & Exterior",
    "windows-doors": "Windows & Doors",
    "grading-sitework": "Grading & Sitework",
    "demolition": "Demolition & Abatement",
    "fire-protection": "Fire Protection",
    "insulation": "Insulation",
    "general": "General / Unclassified",
}


def infer_trade_from_name(company_name: str) -> str | None:
    """Infer a contractor's primary trade from their company name."""
    for trade_id, pattern in _NAME_TRADE_PATTERNS:
        if pattern.search(company_name):
            return trade_id
    return None


# ──────────────────────────────────────────────────────────────────────────────
# SC LLR license lookup
# ──────────────────────────────────────────────────────────────────────────────

class LlrIndex:
    """License entries by normalized company name, with token-indexed fuzzy matching
    (comparing every name against all ~3,500 licenses one by one took ~9s per request)."""

    def __init__(self, entries: dict[str, list[dict]]):
        self.entries = entries
        self.by_token: dict[str, set] = {}
        for norm in entries:
            for tok in _name_tokens(norm):
                if len(tok) > 2:
                    self.by_token.setdefault(tok, set()).add(norm)
        self._cache: dict[str, list[dict]] = {}

    def match(self, norm: str) -> list[dict]:
        if norm in self._cache:
            return self._cache[norm]
        found = self.entries.get(norm) or self._fuzzy(norm)
        self._cache[norm] = found
        return found

    def _fuzzy(self, norm: str) -> list[dict]:
        ta = _name_tokens(norm)
        candidates = set().union(*[self.by_token.get(t, set()) for t in ta if len(t) > 2]) if ta else set()
        best, best_score = None, 0.0
        for cand in candidates:
            tb = _name_tokens(cand)
            inter, union = ta & tb, ta | tb
            score = len(inter) / len(union) if union else 0
            # One name containing the other counts only for multi-word names ("hill" ≠ "churchill").
            contained = (norm in cand or cand in norm) and min(len(ta), len(tb)) >= 2
            if (contained or (score >= 0.6 and (len(inter) >= 2 or len(inter) >= min(len(ta), len(tb))))) and score > best_score:
                best, best_score = cand, score
        return self.entries[best] if best else []


_llr_cache: dict[str, Any] = {"at": 0.0, "index": None}
LLR_CACHE_SECONDS = 3600


async def load_llr_index(session: AsyncSession) -> LlrIndex:
    import time
    if _llr_cache["index"] is not None and time.time() - _llr_cache["at"] < LLR_CACHE_SECONDS:
        return _llr_cache["index"]
    entries: dict[str, list[dict]] = {}
    for de in (await session.execute(select(DirectoryEntry))).scalars().all():
        norm = normalize_name(de.company_name)
        if norm:
            entries.setdefault(norm, []).append({
                "classification": de.classification, "trade_label": de.trade_label,
                "license_status": de.license_status, "license_number": de.external_id,
            })
    index = LlrIndex(entries)
    _llr_cache.update(at=time.time(), index=index)
    return index


def _fuzzy_match_llr(norm: str, llr_map: dict[str, list]) -> list[dict]:
    """Kept for callers that pass a plain dict."""
    return LlrIndex(llr_map).match(norm)


# ──────────────────────────────────────────────────────────────────────────────
# Jobs: permits on one site grouped together
# ──────────────────────────────────────────────────────────────────────────────
# Big projects are permitted in phases (one permit per floor or area), and each phase
# permit can repeat the whole project's value. Permits on the same parcel within a year
# of each other are one job, valued at its largest permit.

JOB_GAP = timedelta(days=365)
OPEN_RE = re.compile(r"issued|active|open|review|applied|pending|hold|approved", re.IGNORECASE)
OPEN_WITHIN = timedelta(days=3 * 365)


def _names(contractor_field: str | None) -> list[str]:
    return [n.strip() for n in (contractor_field or "").split("|") if n.strip()]


def _site(p) -> str | None:
    """Parcel number when the permit has one, else the street address without unit/floor suffixes."""
    raw = getattr(p, "raw_data", None) or {}
    parcel = str(raw.get("MAIN_PARCEL_NUMBER") or "").strip().upper() if isinstance(raw, dict) else ""
    if parcel:
        return "P:" + parcel
    addr = re.sub(r"\s+", " ", (getattr(p, "address", "") or "").upper()).strip()
    m = re.match(r"(\d+[A-Z]?\s+(?:[NSEW]\s+)?[A-Z0-9.'-]+(?:\s+(?:ST|AVE|RD|DR|LN|PL|BLVD|HWY|WAY|CT|CIR|PKWY|TRL|SQ))?)", addr)
    addr = m.group(1) if m else addr
    return "A:" + addr if addr else None


class Job:
    """One project on one site: its permits, largest value and everyone named on them."""

    def __init__(self, permits: list):
        permits = sorted(permits, key=lambda p: p.posted_date or datetime.min)
        main = max(permits, key=lambda p: p.value or 0)
        self.permits = permits
        self.id = main.id
        self.value = main.value or 0
        self.category = main.category
        self.address = re.sub(r"\s+(MASTER\b.*|/.*)$", "", (main.address or "").strip(), flags=re.IGNORECASE)
        self.title = getattr(main, "title", "")
        self.source_id = getattr(main, "source_id", "")
        self.source_url = getattr(main, "source_url", "")
        self.permit_number = getattr(main, "permit_number", "")
        self.latitude, self.longitude = getattr(main, "latitude", None), getattr(main, "longitude", None)
        self.city = getattr(main, "location", "")
        dates = [p.posted_date for p in permits if p.posted_date]
        self.first_date = min(dates) if dates else None
        self.last_date = max(dates) if dates else None
        self.posted_date = self.first_date
        names: dict[str, str] = {}
        for p in permits:
            for n in _names(p.contractor):
                k = normalize_name(n)
                if k and (k not in names or len(n) > len(names[k])):
                    names[k] = n
        self.names = names            # normalized → display name
        self.is_open = any(OPEN_RE.search(getattr(p, "status", "") or "") for p in permits)

    def out(self, gcs: list | None = None) -> dict[str, Any]:
        return {
            "id": self.id, "title": self.title, "address": self.address, "city": self.city,
            "value": self.value, "category": self.category,
            "date": self.first_date.isoformat() if self.first_date else None,
            "last_date": self.last_date.isoformat() if self.last_date else None,
            "permit_number": self.permit_number, "permit_count": len(self.permits),
            "latitude": self.latitude, "longitude": self.longitude,
            "source_id": self.source_id, "source_url": self.source_url,
            "open": self.is_open, **({"gcs": gcs} if gcs is not None else {}),
        }


def group_jobs(permits: list) -> list[Job]:
    by_site: dict[str, list] = {}
    for p in permits:
        by_site.setdefault(_site(p) or f"ID:{p.id}", []).append(p)
    jobs = []
    for group in by_site.values():
        group.sort(key=lambda p: p.posted_date or datetime.min)
        current: list = []
        for p in group:
            if current and p.posted_date and current[-1].posted_date and p.posted_date - current[-1].posted_date > JOB_GAP:
                jobs.append(Job(current))
                current = []
            current.append(p)
        if current:
            jobs.append(Job(current))
    return jobs


def _is_open_job(job: Job, now: datetime) -> bool:
    return job.is_open and bool(job.last_date) and now - job.last_date <= OPEN_WITHIN


# ──────────────────────────────────────────────────────────────────────────────
# Who's the GC on a permit
# ──────────────────────────────────────────────────────────────────────────────
# Building permits list everyone with a contractor role, so a hotel permit can name the
# GC plus its radio, hood and fire alarm installers.

GC_LICENSES = {"BD", "CCM"}
NOT_GC_NAME_RE = re.compile(
    r"fire|sprinkler|security|alarm|radio|communicat|utilit|logistic|controls|hood|elevator|"
    r"steel|iron\s*works|\bsigns?\b|survey|engineer|testing|crane|rental|supply|landscap|irrigat|"
    r"\bpools?\b|solar|low\s*voltage|audio|cabling|waterproof|drilling|abatement|insulation|"
    r"millwork|cabinet|flooring|tile\b|marble|granite|kitchen",
    re.IGNORECASE,
)
GC_NAME_RE = re.compile(r"construct|builder|building|contracting|general\s+contract|\bgc\b", re.IGNORECASE)


def is_gc(name: str, licenses: list[dict]) -> bool:
    if NOT_GC_NAME_RE.search(name) or infer_trade_from_name(name) not in (None, "general"):
        return False
    classes = {str(lic.get("classification") or "").upper() for lic in licenses}
    if classes & GC_LICENSES:
        return True
    if classes:                    # licensed, but only for a trade
        return False
    return bool(GC_NAME_RE.search(name))


# Trades for the sub lists: from the permit type on trade permits, or the company's
# name / license when a sub is named on a building permit.
SUB_TRADES = {
    "electrical": "Electrical", "plumbing": "Plumbing & gas", "mechanical": "Mechanical / HVAC",
    "fire-sprinkler": "Fire protection & alarm", "roofing": "Roofing", "masonry": "Masonry",
    "concrete": "Concrete", "framing": "Framing", "drywall-finishes": "Drywall, paint & finishes",
    "windows-doors": "Glass, windows & doors", "siding-exterior": "Siding & exterior",
    "grading-sitework": "Sitework & utilities", "insulation": "Insulation", "demolition": "Demolition",
    "painting": "Painting", "other": "Other specialty",
}
TRADE_CATEGORIES = {k: SUB_TRADES[k] for k in ("electrical", "plumbing", "mechanical", "fire-sprinkler", "roofing", "painting")}
_NAME_TRADE_TO_SUB = {"hvac": "mechanical", "fire-protection": "fire-sprinkler"}
_SUB_NAME_HINTS = [
    ("fire-sprinkler", re.compile(r"fire|sprinkler|alarm|security|radio|communicat|low\s*voltage", re.IGNORECASE)),
    ("grading-sitework", re.compile(r"utilit|site\s*logistic|paving|grading|excavat", re.IGNORECASE)),
]
NOT_BUILDING_CATEGORIES = {"residential", "trade-permit", *TRADE_CATEGORIES}


def sub_trade(name: str, licenses: list[dict]) -> str:
    t = infer_trade_from_name(name)
    if t and t != "general":
        return _NAME_TRADE_TO_SUB.get(t, t if t in SUB_TRADES else "other")
    for trade, rx in _SUB_NAME_HINTS:
        if rx.search(name):
            return trade
    for lic in licenses:
        t = _LLR_TRADE_MAP.get(str(lic.get("classification") or "").upper())
        if t and t != "general":
            return _NAME_TRADE_TO_SUB.get(t, t if t in SUB_TRADES else "other")
    return "other"


# ──────────────────────────────────────────────────────────────────────────────
# Trade roster (By trade view)
# ──────────────────────────────────────────────────────────────────────────────

def roster_from(projects: list, llr: LlrIndex, *, trade: str | None = None, min_value: float = 0,
                now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.utcnow()
    by_contractor: dict[str, dict[str, Any]] = {}
    for p in projects:
        for name in _names(p.contractor):
            norm = normalize_name(name)
            if not norm:
                continue
            e = by_contractor.setdefault(norm, {"names": set(), "permits": []})
            e["names"].add(name)
            e["permits"].append(p)

    trade_roster: dict[str, list] = {}
    for norm, cdata in by_contractor.items():
        jobs = group_jobs(cdata["permits"])
        total = sum(j.value for j in jobs)
        if total < min_value:
            continue
        display_name = max(cdata["names"], key=len)
        licenses = llr.match(norm)
        assigned = infer_trade_from_name(display_name)
        if not assigned:
            for lic in licenses:
                assigned = _LLR_TRADE_MAP.get(lic["classification"])
                if assigned:
                    break
        assigned = assigned or "general"
        if trade and assigned != trade:
            continue
        dates = [j.last_date for j in jobs if j.last_date]
        trade_roster.setdefault(assigned, []).append({
            "key": norm,
            "name": display_name,
            "also_known_as": sorted(cdata["names"] - {display_name}),
            "trade": assigned,
            "permit_count": len(cdata["permits"]),
            "job_count": len(jobs),
            "open_jobs": sum(1 for j in jobs if _is_open_job(j, now)),
            "total_permit_value": total,
            "last_permit_date": max(dates).isoformat() if dates else None,
            "licenses": licenses,
        })

    result: dict[str, Any] = {}
    for tid in sorted(trade_roster):
        contractors = sorted(trade_roster[tid], key=lambda c: c["total_permit_value"], reverse=True)
        total_value = sum(c["total_permit_value"] for c in contractors)
        for c in contractors:
            c["market_share_pct"] = round(c["total_permit_value"] / total_value * 100, 1) if total_value > 0 else 0.0
        result[tid] = {
            "label": TRADE_LABELS.get(tid, tid),
            "total_contractors": len(contractors),
            "total_permit_value": total_value,
            "contractors": contractors,
        }
    return {
        "total_trades": len(result),
        "total_contractors": sum(len(t["contractors"]) for t in result.values()),
        "trades": result,
    }


async def build_permit_trade_roster(trade: str | None = None, source: str | None = None,
                                    min_value: float = 0) -> dict[str, Any]:
    """Contractors grouped by trade, ranked by the value of their jobs (phase permits counted once)."""
    async with get_session_factory()() as session:
        conds = [Project.contractor != "", Project.contractor.is_not(None)]
        if source:
            conds.append(Project.source_id == source)
        projects = (await session.execute(select(Project).where(*conds))).scalars().all()
        llr = await load_llr_index(session)
    return roster_from(projects, llr, trade=trade, min_value=min_value)


# ──────────────────────────────────────────────────────────────────────────────
# One contractor's work
# ──────────────────────────────────────────────────────────────────────────────

PERMIT_SOURCES = ("charleston-permits", "north-charleston-permits", "mt-pleasant-permits")
CONTRACTOR_PERMIT_LIMIT = 200


def _prefilter_word(key: str) -> str:
    """The longest word of a normalized name, for a cheap SQL ILIKE before exact matching."""
    words = sorted(key.split(), key=len, reverse=True)
    return words[0] if words else key


async def _contractor_rows(db: AsyncSession, key: str) -> list:
    rows = (await db.execute(
        select(Project).where(Project.contractor.ilike(f"%{_prefilter_word(key)}%"))
    )).scalars().all()
    return [p for p in rows if any(normalize_name(n) == key for n in _names(p.contractor))]


async def contractor_permits(key: str, session: AsyncSession | None = None) -> dict[str, Any]:
    """A contractor's jobs (phase permits grouped), newest first, with how many are still open."""
    key = (key or "").strip().lower()
    if not key:
        return {"key": key, "permits": [], "total_value": 0, "permit_count": 0, "job_count": 0, "open_jobs": 0}

    async def run(db: AsyncSession) -> dict[str, Any]:
        jobs = group_jobs(await _contractor_rows(db, key))
        jobs.sort(key=lambda j: j.last_date or datetime.min, reverse=True)
        now = datetime.utcnow()
        return {
            "key": key,
            "permit_count": sum(len(j.permits) for j in jobs),
            "job_count": len(jobs),
            "open_jobs": sum(1 for j in jobs if _is_open_job(j, now)),
            "total_value": sum(j.value for j in jobs),
            "permits": [j.out() for j in jobs[:CONTRACTOR_PERMIT_LIMIT]],
        }

    if session is not None:
        return await run(session)
    async with get_session_factory()() as db:
        return await run(db)


def _median(values: list[float]) -> float:
    v = sorted(values)
    if not v:
        return 0.0
    mid = len(v) // 2
    return v[mid] if len(v) % 2 else (v[mid - 1] + v[mid]) / 2


# ──────────────────────────────────────────────────────────────────────────────
# GC leaderboard
# ──────────────────────────────────────────────────────────────────────────────

def _building_permits(projects: list) -> list:
    return [p for p in projects if (p.category or "") not in NOT_BUILDING_CATEGORIES]


def gc_leaderboard_from(projects: list, *, min_value: float, since: datetime | None,
                        categories: set | None = None, llr: LlrIndex | None = None) -> list[dict[str, Any]]:
    """General contractors on commercial jobs of at least `min_value`, ranked by total job value."""
    llr = llr or LlrIndex({})
    leaders: dict[str, dict[str, Any]] = {}
    for job in group_jobs(_building_permits(projects)):
        if job.value < min_value:
            continue
        if since and (job.last_date is None or job.last_date < since):
            continue
        if categories and (job.category or "") not in categories:
            continue
        for key, name in job.names.items():
            if not is_gc(name, llr.match(key)):
                continue
            e = leaders.setdefault(key, {"key": key, "names": set(), "jobs": [], "categories": {}})
            e["names"].add(name)
            e["jobs"].append(job)
            cat = job.category or "other"
            e["categories"][cat] = e["categories"].get(cat, 0) + 1

    out = []
    for e in leaders.values():
        jobs = sorted(e["jobs"], key=lambda j: j.value, reverse=True)
        values = [j.value for j in jobs]
        dates = [j.last_date for j in jobs if j.last_date]
        out.append({
            "key": e["key"],
            "name": max(e["names"], key=len),
            "job_count": len(jobs),
            "total_value": sum(values),
            "median_value": _median(values),
            "largest_value": values[0] if values else 0,
            "last_date": max(dates).isoformat() if dates else None,
            "categories": dict(sorted(e["categories"].items(), key=lambda kv: -kv[1])),
            "top_jobs": [j.out() for j in jobs[:3]],
        })
    out.sort(key=lambda c: c["total_value"], reverse=True)
    return out


async def gc_leaderboard(min_value: float = 1_000_000, days: int = 730, limit: int = 50,
                         categories: list | None = None) -> dict[str, Any]:
    since = datetime.utcnow() - timedelta(days=days) if days else None
    async with get_session_factory()() as db:
        conds = [Project.source_id.in_(PERMIT_SOURCES), Project.contractor.is_not(None), Project.contractor != ""]
        if since:
            conds.append(Project.posted_date >= since - JOB_GAP)
        projects = (await db.execute(select(Project).where(*conds))).scalars().all()
        llr = await load_llr_index(db)
    leaders = gc_leaderboard_from(projects, min_value=min_value, since=since,
                                  categories=set(categories or []) or None, llr=llr)
    return {"min_value": min_value, "days": days, "categories": categories or [],
            "total": len(leaders), "contractors": leaders[:limit]}


# ──────────────────────────────────────────────────────────────────────────────
# Subs on jobs
# ──────────────────────────────────────────────────────────────────────────────
# Two signals: (1) companies other than the GC named on the job's own building permits,
# and (2) City of Charleston trade permits (electrical, plumbing, mechanical, fire,
# roofing) on the same parcel from 90 days before to two years after the job started.
# Charleston is the only source that stores trade permits.

SUB_WINDOW_BEFORE = timedelta(days=90)
SUB_WINDOW_AFTER = timedelta(days=730)
SUB_SOURCE = "charleston-permits"


def link_subs(jobs: list[Job], trade_permits: list, llr: LlrIndex | None = None) -> list[tuple]:
    """(job, trade, sub name, date) for every sub found on each job."""
    llr = llr or LlrIndex({})
    by_site: dict[str, list] = {}
    for t in trade_permits:
        site = _site(t)
        if site and t.posted_date:
            by_site.setdefault(site, []).append(t)
    links = []
    for job in jobs:
        gc_keys = {k for k, n in job.names.items() if is_gc(n, llr.match(k))}
        for key, name in job.names.items():
            if key not in gc_keys:
                links.append((job, sub_trade(name, llr.match(key)), name, job.first_date))
        site = _site(job.permits[0])
        if not site or not job.first_date:
            continue
        lo, hi = job.first_date - SUB_WINDOW_BEFORE, (job.last_date or job.first_date) + SUB_WINDOW_AFTER
        for t in by_site.get(site, []):
            if not (lo <= t.posted_date <= hi) or t.category not in TRADE_CATEGORIES:
                continue
            for name in _names(t.contractor):
                k = normalize_name(name)
                if k and k not in gc_keys:
                    links.append((job, t.category, name, t.posted_date))
    return links


def aggregate_subs(links: list[tuple], llr: LlrIndex | None = None, per_trade: int = 15) -> dict[str, Any]:
    """Group linked subs by trade, ranked by how many distinct jobs they worked."""
    llr = llr or LlrIndex({})
    trades: dict[str, dict[str, dict]] = {}
    for job, trade, name, when in links:
        key = normalize_name(name)
        if not key or trade not in SUB_TRADES:
            continue
        e = trades.setdefault(trade, {}).setdefault(key, {
            "key": key, "names": set(), "jobs": {}, "gcs": set(), "last_date": None})
        e["names"].add(name)
        e["jobs"][job.id] = job
        e["gcs"].update(n for k, n in job.names.items() if is_gc(n, llr.match(k)))
        if when and (e["last_date"] is None or when > e["last_date"]):
            e["last_date"] = when
    out = {}
    for trade, subs in trades.items():
        rows = []
        for e in subs.values():
            jobs = sorted(e["jobs"].values(), key=lambda j: j.value, reverse=True)
            rows.append({
                "key": e["key"],
                "name": max(e["names"], key=len),
                "job_count": len(jobs),
                "job_value": sum(j.value for j in jobs),
                "gcs": sorted(e["gcs"] - e["names"])[:6],
                "last_date": e["last_date"].isoformat() if e["last_date"] else None,
                "jobs": [j.out() for j in jobs[:5]],
            })
        rows.sort(key=lambda r: (r["job_count"], r["job_value"]), reverse=True)
        out[trade] = {"label": SUB_TRADES[trade], "subs": rows[:per_trade], "total": len(rows)}
    order = list(SUB_TRADES)
    return dict(sorted(out.items(), key=lambda kv: order.index(kv[0])))


async def _trade_permits(db: AsyncSession, since: datetime | None) -> list:
    conds = [Project.source_id == SUB_SOURCE, Project.category.in_(list(TRADE_CATEGORIES)),
             Project.contractor.is_not(None), Project.contractor != ""]
    if since:
        conds.append(Project.posted_date >= since)
    return (await db.execute(select(Project).where(*conds))).scalars().all()


async def subs_for_gc(key: str) -> dict[str, Any]:
    """The subs who've worked on this GC's jobs."""
    key = (key or "").strip().lower()
    async with get_session_factory()() as db:
        jobs = group_jobs(_building_permits(await _contractor_rows(db, key)))
        dates = [j.first_date for j in jobs if j.first_date]
        trade = await _trade_permits(db, min(dates) - SUB_WINDOW_BEFORE) if dates else []
        llr = await load_llr_index(db)
    links = [l for l in link_subs(jobs, trade, llr) if normalize_name(l[2]) != key]
    return {"key": key, "jobs_checked": len(jobs), "trades": aggregate_subs(links, llr)}


async def proven_subs(min_value: float = 5_000_000, days: int = 1825, categories: list | None = None) -> dict[str, Any]:
    """Subs on commercial jobs of at least `min_value` (optionally of given building types)."""
    since = datetime.utcnow() - timedelta(days=days) if days else None
    async with get_session_factory()() as db:
        conds = [Project.source_id.in_(PERMIT_SOURCES), Project.category.not_in(list(NOT_BUILDING_CATEGORIES)),
                 Project.contractor.is_not(None), Project.contractor != ""]
        if since:
            conds.append(Project.posted_date >= since - JOB_GAP)
        jobs = [j for j in group_jobs((await db.execute(select(Project).where(*conds))).scalars().all())
                if j.value >= min_value and (not since or (j.last_date and j.last_date >= since))
                and (not categories or (j.category or "") in categories)]
        trade = await _trade_permits(db, (since - SUB_WINDOW_BEFORE) if since else None)
        llr = await load_llr_index(db)
    return {"min_value": min_value, "days": days, "jobs_checked": len(jobs),
            "trades": aggregate_subs(link_subs(jobs, trade, llr), llr)}
