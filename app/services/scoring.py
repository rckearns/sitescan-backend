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

    scores = {}
    for cat_id, pattern in CATEGORY_PATTERNS:
        matches = pattern.findall(text)
        scores[cat_id] = len(matches)

    if not scores or max(scores.values()) == 0:
        return "residential"
    return max(scores, key=scores.get)


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
