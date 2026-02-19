"""Project classification and relevance scoring engine.

Classifies projects into categories and computes a match score
based on keyword relevance to masonry, historic restoration,
and structural work profiles.
"""

import re
from typing import Optional


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
    """Classify a project into a category based on title and description."""
    text = f"{title} {description}"
    
    # Score each category
    scores = {}
    for cat_id, pattern in CATEGORY_PATTERNS:
        matches = pattern.findall(text)
        scores[cat_id] = len(matches)
    
    # Return highest-scoring category, default to residential
    if not scores or max(scores.values()) == 0:
        return "residential"
    return max(scores, key=scores.get)


# ─── MATCH SCORING ──────────────────────────────────────────────────────────

# Weighted keyword groups — higher weight = more relevant to target profile
SCORE_GROUPS = [
    # Core specialties (high value)
    (18, re.compile(
        r"historic\s*(masonry|brick|restoration|facade)|"
        r"lime[\s-]*mortar|heritage\s*mortar|repoint|tuckpoint|"
        r"parexlanko|sikamur|european\s*mortar|specialty\s*mortar",
        re.IGNORECASE,
    )),
    # Primary trades
    (14, re.compile(
        r"masonry|brick\s*(repair|replac|restor)|stone\s*(repair|restor)|"
        r"stucco\s*(repair|remov|replac)|mortar\s*joint|facade\s*restor",
        re.IGNORECASE,
    )),
    (12, re.compile(
        r"structur\s*(reinforc|repair|modif)|load[\s-]*bear|"
        r"foundation\s*(repair|reinforc|underpin)|"
        r"steel\s*beam|shoring|seismic\s*brac",
        re.IGNORECASE,
    )),
    (10, re.compile(
        r"historic|preservation|restoration|heritage|landmark|"
        r"national\s*register|adaptive\s*reuse",
        re.IGNORECASE,
    )),
    # Location bonuses
    (8, re.compile(
        r"charleston|mt\.?\s*pleasant|james\s*island|summerville|"
        r"north\s*charleston|west\s*ashley|johns?\s*island|"
        r"folly\s*beach|sullivan|daniel\s*island|goose\s*creek",
        re.IGNORECASE,
    )),
    # Adjacent trades
    (6, re.compile(
        r"concrete|block|retaining\s*wall|waterproof|"
        r"envelope|exterior\s*repair|caulk|sealant|flashing",
        re.IGNORECASE,
    )),
    # Environmental/code factors
    (4, re.compile(
        r"hurricane|wind\s*uplift|coastal|flood|moisture|"
        r"bar\s*review|board\s*of\s*architectural|"
        r"building\s*code|ibc|fema",
        re.IGNORECASE,
    )),
    # General construction
    (2, re.compile(
        r"repair|rehabilitat|renovat|restor|abatement|demolition|"
        r"construction|building|renovation",
        re.IGNORECASE,
    )),
]

# Negative signals (reduce score)
NEGATIVE_PATTERNS = re.compile(
    r"software|IT\s*service|janitorial|landscap|paving|asphalt|"
    r"electrical\s*only|plumbing\s*only|hvac\s*only|roofing\s*only|"
    r"painting\s*only|carpet|flooring\s*only",
    re.IGNORECASE,
)


def score_match(title: str, description: str = "", user_keywords: Optional[str] = None) -> int:
    """Compute a 0-99 relevance match score for a project.
    
    Higher scores indicate stronger alignment with the user's
    masonry/restoration/structural profile.
    """
    text = f"{title} {description}"
    score = 35  # baseline
    
    # Apply weighted keyword groups
    for weight, pattern in SCORE_GROUPS:
        matches = pattern.findall(text)
        if matches:
            # Diminishing returns for multiple matches in same group
            score += weight + min(len(matches) - 1, 3) * (weight // 4)
    
    # User custom keywords boost
    if user_keywords:
        for kw in user_keywords.split():
            if kw.strip() and re.search(re.escape(kw.strip()), text, re.IGNORECASE):
                score += 3
    
    # Negative signals penalty
    neg_matches = NEGATIVE_PATTERNS.findall(text)
    score -= len(neg_matches) * 10
    
    # Value-based boost (higher value projects get slight bump)
    # This is handled externally since we don't have value here
    
    return max(1, min(score, 99))


def score_with_value_boost(base_score: int, value: Optional[float]) -> int:
    """Apply a small boost for higher-value projects."""
    if not value:
        return base_score
    if value >= 500_000:
        return min(base_score + 4, 99)
    if value >= 100_000:
        return min(base_score + 2, 99)
    return base_score
