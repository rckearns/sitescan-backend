"""Project classification and relevance scoring engine.

Classifies projects into categories and computes a match score
based on how well each project satisfies the user's saved criteria.

Scoring model:
  - User defines criteria: min value, preferred categories, statuses, sources
  - Each active criterion is worth an equal share of 100 points
  - A project that meets ALL criteria scores 100%
  - A project meeting none scores 0%
  - If no criteria are set, projects score 50 (neutral)
"""

import re
from typing import Optional


# ─── CONSTRUCTION TYPE CLASSIFICATION ────────────────────────────────────────

# Maps WORK_CLASS values from Charleston ArcGIS to our construction types
_WORK_CLASS_MAP = {
    "new construction": "new-construction",
    "addition": "addition",
    "alteration": "renovation",
    "repair": "structural-repair",
    "demolition": "demolition",
    "tenant improvement": "renovation",
    "interior": "renovation",
}

_CONSTRUCTION_TYPE_PATTERNS = [
    ("new-construction", re.compile(
        r"new\s+construction|ground[\s-]*up|new\s+build|erect\s+new|"
        r"new\s+structure|new\s+building|greenfield",
        re.IGNORECASE,
    )),
    ("historic-renovation", re.compile(
        r"historic|heritage|preservation|landmark|antebellum|"
        r"national\s*register|colonial|victorian|greek\s*revival|"
        r"period\s+correct|restoration\s+of",
        re.IGNORECASE,
    )),
    ("structural-repair", re.compile(
        r"structural\s+repair|foundation\s+repair|underpin|shoring|"
        r"crack\s+repair|remediat|seismic\s+retrofit|"
        r"concrete\s+repair|spall",
        re.IGNORECASE,
    )),
    ("addition", re.compile(
        r"\baddition\b|expand\s+existing|building\s+expansion|"
        r"annex|new\s+wing",
        re.IGNORECASE,
    )),
    ("renovation", re.compile(
        r"renovation|renovate|remodel|alteration|interior\s+fit|"
        r"fit[\s-]*up|up[\s-]*fit|tenant\s+improvement|retrofit|"
        r"refurbish|rehab|upgrade\s+existing|interior\s+demo",
        re.IGNORECASE,
    )),
]


def classify_construction_type(title: str, description: str = "", work_class: str = "") -> Optional[str]:
    """Return one of: new-construction, renovation, historic-renovation,
    structural-repair, addition, demolition, or None if unclear."""

    # WORK_CLASS from permit systems is the strongest signal
    if work_class:
        wc = work_class.strip().lower()
        for key, val in _WORK_CLASS_MAP.items():
            if key in wc:
                # Promotion: alteration/repair + historic signals → historic-renovation
                if val in ("renovation", "structural-repair"):
                    text = f"{title} {description}"
                    if _CONSTRUCTION_TYPE_PATTERNS[1][1].search(text):
                        return "historic-renovation"
                return val

    text = f"{title} {description}"
    for ct, pattern in _CONSTRUCTION_TYPE_PATTERNS:
        if pattern.search(text):
            return ct

    return None


# ─── BUILDING TYPE CLASSIFICATION ─────────────────────────────────────────────

_BUILDING_TYPE_PATTERNS = [
    ("single-family", re.compile(
        r"single[\s-]*family|sfr|single\s+family\s+dwelling|"
        r"\bdwelling\b|residence(?!\s+inn)|\bhouse\b",
        re.IGNORECASE,
    )),
    ("multi-family", re.compile(
        r"multi[\s-]*family|multifamily|apartment|condo(?:minium)?|"
        r"duplex|triplex|quadplex|townhome|townhouse|"
        r"assisted\s+living|senior\s+housing",
        re.IGNORECASE,
    )),
    ("hotel", re.compile(
        r"\bhotel\b|\bmotel\b|\binn\b|lodging|hospitality\s+facility|"
        r"\bresort\b",
        re.IGNORECASE,
    )),
    ("restaurant", re.compile(
        r"restaurant|food\s+service|dining|\bcafe\b|cafeteria|"
        r"commercial\s+kitchen|bar\s+and\s+grill",
        re.IGNORECASE,
    )),
    ("industrial", re.compile(
        r"warehouse|industrial|manufacturing|factory|distribution\s+center|"
        r"logistics|cold\s+storage|data\s+center",
        re.IGNORECASE,
    )),
    ("institutional", re.compile(
        r"\bschool\b|university|college|\bhospital\b|medical\s+center|"
        r"\bchurch\b|courthouse|library|museum|fire\s+station|"
        r"civic\s+center|police\s+station",
        re.IGNORECASE,
    )),
    ("retail", re.compile(
        r"\bretail\b|storefront|shopping\s+center|strip\s+mall|"
        r"mercantile|big\s+box|grocery\s+store",
        re.IGNORECASE,
    )),
    ("mixed-use", re.compile(
        r"mixed[\s-]*use",
        re.IGNORECASE,
    )),
    ("office", re.compile(
        r"\boffice\b|office\s+space|office\s+building|corporate\s+campus|"
        r"professional\s+building",
        re.IGNORECASE,
    )),
]


def classify_building_type(title: str, description: str = "") -> Optional[str]:
    """Return one of: office, retail, industrial, multi-family, single-family,
    mixed-use, institutional, hotel, restaurant, or None if unclear."""
    text = f"{title} {description}"
    for bt, pattern in _BUILDING_TYPE_PATTERNS:
        if pattern.search(text):
            return bt
    return None


# ─── CATEGORY CLASSIFICATION ────────────────────────────────────────────────

CATEGORY_PATTERNS = [
    ("historic-restoration", re.compile(
        r"historic|heritage|preservation|landmark|antebellum|restoration|"
        r"national\s*register|adaptive\s*reuse|period|century|colonial|"
        r"victorian|greek\s*revival|art\s*deco",
        re.IGNORECASE,
    )),
    ("masonry", re.compile(
        r"masonry|mortar|brick|stone|concrete\s*block|stucco|repoint|"
        r"tuckpoint|grout|cmu|veneer|parapet|chimney|lime\s*mortar",
        re.IGNORECASE,
    )),
    ("structural", re.compile(
        r"structur|foundation|reinforc|load[\s-]*bear|steel\s*beam|"
        r"shoring|underpin|seismic|retaining\s*wall|pile|micropile|"
        r"helical|shotcrete|carbon\s*fiber",
        re.IGNORECASE,
    )),
    ("government", re.compile(
        r"government|municipal|federal|state\s*of|county\s*of|city\s*of|"
        r"public\s*works|department\s*of|u\.?s\.?\s*army|corps\s*of\s*engineer|"
        r"gsa|va\s*hospital|courthouse|post\s*office",
        re.IGNORECASE,
    )),
    ("commercial", re.compile(
        r"commercial|office|retail|mixed[\s-]*use|warehouse|industrial|"
        r"hotel|hospital|school|university|church|tenant\s*improvement",
        re.IGNORECASE,
    )),
]


def classify_project(title: str, description: str = "") -> str:
    """Classify a project into a category based on title and description.

    Priority order:
    1. Specific building types (hotel, multi-family, office, mixed-use, etc.)
    2. Work-type categories (historic-restoration, masonry, structural, government)
    3. Generic commercial catch-all
    4. Residential (default — filtered out of the UI)
    """
    # 1. Specific building type — gives us hotel / multi-family / office / etc.
    building_type = classify_building_type(title, description)
    if building_type and building_type != "single-family":
        return building_type

    # 2. Work-type categories (ordered by specificity)
    work_type_patterns = [
        ("historic-restoration", CATEGORY_PATTERNS[0][1]),
        ("masonry", CATEGORY_PATTERNS[1][1]),
        ("structural", CATEGORY_PATTERNS[2][1]),
        ("government", CATEGORY_PATTERNS[3][1]),
    ]
    for cat_id, pattern in work_type_patterns:
        if pattern.search(f"{title} {description}"):
            return cat_id

    # 3. Generic commercial (broad catch-all)
    if CATEGORY_PATTERNS[4][1].search(f"{title} {description}"):
        return "commercial"

    # 4. Default — residential (excluded from project list display)
    return "residential"


# ─── PROFILE-BASED MATCH SCORING ────────────────────────────────────────────

def score_against_profile(project, user) -> int:
    """Score a project 0–100 based on how many of the user's criteria it meets.

    Each criterion the user has configured counts equally.
    Meeting all criteria → 100. Meeting none → 0.
    No criteria configured → neutral score of 50.
    """
    criteria_total = 0
    criteria_met = 0

    # ── Value criterion ──────────────────────────────────────────────────────
    min_val = getattr(user, "criteria_min_value", None)
    if min_val:
        criteria_total += 1
        if project.value and project.value >= min_val:
            criteria_met += 1

    # ── Category criterion ───────────────────────────────────────────────────
    cat_criteria = getattr(user, "criteria_categories", None) or []
    if cat_criteria:
        criteria_total += 1
        if project.category in cat_criteria:
            criteria_met += 1

    # ── Status criterion ─────────────────────────────────────────────────────
    status_criteria = getattr(user, "criteria_statuses", None) or []
    if status_criteria:
        criteria_total += 1
        if project.status in status_criteria:
            criteria_met += 1

    # ── Source criterion ─────────────────────────────────────────────────────
    source_criteria = getattr(user, "criteria_sources", None) or []
    if source_criteria:
        criteria_total += 1
        if project.source_id in source_criteria:
            criteria_met += 1

    if criteria_total == 0:
        return 50  # no criteria set — neutral

    return round((criteria_met / criteria_total) * 100)
