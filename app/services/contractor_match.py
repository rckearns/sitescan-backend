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
from datetime import datetime
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
        r"electric(?:al)?|wiring|power\s*systems",
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


def _fuzzy_match_llr(norm: str, llr_map: dict[str, list]) -> list[dict]:
    """Find LLR license entries for a normalized contractor name."""
    # Exact
    if norm in llr_map:
        return llr_map[norm]
    # Fuzzy
    for llr_norm, entries in llr_map.items():
        a, b = norm, llr_norm
        if a == b:
            return entries
        if a in b or b in a:
            return entries
        ta, tb = _name_tokens(a), _name_tokens(b)
        if ta and tb:
            inter = ta & tb
            union = ta | tb
            if union and len(inter) / len(union) >= 0.6 and (
                len(inter) >= 2 or len(inter) >= min(len(ta), len(tb))
            ):
                return entries
    return []


async def build_permit_trade_roster(
    trade: str | None = None,
    source: str | None = None,
    min_value: float = 0,
) -> dict[str, Any]:
    """
    Build a contractor roster grouped by trade, ranked by permit activity.

    Trade is determined by (in priority order):
    1. The contractor's company name (e.g. "ABC Framing LLC" → framing)
    2. Their SC LLR license classification (e.g. SF → framing, MS → masonry)
    3. Falls back to "general" if neither signal is present

    Permit data provides the market share signal: total value, permit count,
    most recent activity.

    Args:
        trade: Filter to one trade (e.g. "framing"). None = all trades.
        source: Filter by permit source_id. None = all sources.
        min_value: Minimum total permit value to include a contractor.
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        # 1. Load all projects with contractor names
        conditions = [
            Project.contractor != "",
            Project.contractor.is_not(None),
        ]
        if source:
            conditions.append(Project.source_id == source)

        proj_result = await session.execute(
            select(Project).where(*conditions)
        )
        projects = proj_result.scalars().all()

        # 2. Build contractor → permit stats (keyed by normalized name)
        contractor_map: dict[str, dict[str, Any]] = {}

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

                if norm not in contractor_map:
                    contractor_map[norm] = {
                        "original_names": set(),
                        "permit_count": 0,
                        "total_value": 0.0,
                        "last_date": None,
                    }
                entry = contractor_map[norm]
                entry["original_names"].add(name)
                entry["permit_count"] += 1
                if p.value:
                    entry["total_value"] += p.value
                if p.posted_date:
                    if entry["last_date"] is None or p.posted_date > entry["last_date"]:
                        entry["last_date"] = p.posted_date

        # 3. Load LLR directory for license-based trade + enrichment
        llr_map: dict[str, list[dict]] = {}
        dir_result = await session.execute(select(DirectoryEntry))
        for de in dir_result.scalars().all():
            de_norm = normalize_name(de.company_name)
            if de_norm:
                llr_map.setdefault(de_norm, []).append({
                    "classification": de.classification,
                    "trade_label": de.trade_label,
                    "license_status": de.license_status,
                    "license_number": de.external_id,
                })

        # 4. Assign each contractor a trade and build the roster
        trade_roster: dict[str, list] = {}

        for norm, cdata in contractor_map.items():
            # Skip below minimum value
            if cdata["total_value"] < min_value:
                continue

            display_name = max(cdata["original_names"], key=len)

            # Determine trade: name first, then LLR, then "general"
            assigned_trade = infer_trade_from_name(display_name)

            licenses = _fuzzy_match_llr(norm, llr_map)

            if not assigned_trade and licenses:
                # Use the first LLR classification that maps to a trade
                for lic in licenses:
                    llr_trade = _LLR_TRADE_MAP.get(lic["classification"])
                    if llr_trade:
                        assigned_trade = llr_trade
                        break

            if not assigned_trade:
                assigned_trade = "general"

            # Apply trade filter
            if trade and assigned_trade != trade:
                continue

            if assigned_trade not in trade_roster:
                trade_roster[assigned_trade] = []

            trade_roster[assigned_trade].append({
                "name": display_name,
                "also_known_as": sorted(cdata["original_names"] - {display_name}),
                "trade": assigned_trade,
                "permit_count": cdata["permit_count"],
                "total_permit_value": cdata["total_value"],
                "last_permit_date": (
                    cdata["last_date"].isoformat() if cdata["last_date"] else None
                ),
                "licenses": licenses,
            })

        # 5. Sort by total_permit_value desc, compute market share
        result: dict[str, Any] = {}
        for tid in sorted(trade_roster.keys()):
            contractors = trade_roster[tid]
            contractors.sort(key=lambda c: c["total_permit_value"], reverse=True)
            total_value = sum(c["total_permit_value"] for c in contractors)
            for c in contractors:
                c["market_share_pct"] = round(
                    (c["total_permit_value"] / total_value * 100), 1
                ) if total_value > 0 else 0.0
            result[tid] = {
                "label": TRADE_LABELS.get(tid, tid),
                "total_contractors": len(contractors),
                "total_permit_value": total_value,
                "contractors": contractors,
            }

        return {
            "total_trades": len(result),
            "total_contractors": sum(
                len(t["contractors"]) for t in result.values()
            ),
            "trades": result,
        }
