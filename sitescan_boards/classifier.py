"""Score agenda items for commercial construction relevance (0-100)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import config
from .parser import AgendaItem


@dataclass
class Classification:
    score: int
    stage: str | None            # conceptual / preliminary / final / ...
    tags: list[str] = field(default_factory=list)


def detect_stage(text: str) -> str | None:
    for stage, pattern in config.STAGE_KEYWORDS.items():
        if re.search(pattern, text, re.IGNORECASE):
            return stage
    return None


def institutional_owner(item: AgendaItem) -> str | None:
    """Canonical institution name if the owner/applicant/request is one."""
    hay = " ".join(
        filter(None, [item.owner, item.applicant, item.request_text])
    )
    for name, pattern in config.INSTITUTIONAL_OWNERS.items():
        if re.search(pattern, hay, re.IGNORECASE):
            return name
    return None


def _institutional_score(item: AgendaItem, text: str,
                         tags: list[str]) -> float:
    """Extra points for university / hospital / school work.

    Items with none of these signals get 0, so developer scoring is
    unchanged for them.
    """
    points = 0.0
    # Building type comes from the request itself, not the metadata or the
    # owner's name ("Medical University ..." is scored as an owner below).
    request = f"{item.address} {item.request_text}"
    best = None
    for pat, pts in config.INSTITUTIONAL_USE_SCORES.items():
        m = re.search(pat, request, re.IGNORECASE)
        if m and (best is None or pts > best[0]):
            best = (pts, m.group(0).lower())
    if best:
        points += best[0]
        tags.append(f"inst_use:{best[1]}")

    # Signage, mock-up panels, lighting... on an institutional building are
    # worth the owner bump but not the "big new building" bonuses.
    minor = bool(re.search(config.MINOR_SCOPE_RE, item.request_text,
                           re.IGNORECASE))

    owner = institutional_owner(item)
    if owner:
        points += config.INSTITUTIONAL_OWNER_SCORE
        tags.append(f"inst_owner:{owner}")
        if not minor and re.search(config.INSTITUTIONAL_REDEVELOPMENT_RE,
                                   text, re.IGNORECASE):
            points += config.INSTITUTIONAL_REDEVELOPMENT_SCORE
            tags.append("inst_redevelopment")

    if (best or owner) and not minor and re.search(
            config.NEW_CONSTRUCTION_HEIGHT_RE, text, re.IGNORECASE):
        points += config.NEW_CONSTRUCTION_HEIGHT_SCORE
        tags.append("new_construction_height")
    return points


def classify(item: AgendaItem, board_code: str) -> Classification:
    text = f"{item.address} {item.request_text} {item.raw_text}"
    score = 0.0
    tags: list[str] = []

    # Rezoning to a commercial/mixed-use code is a strong early signal.
    commercial_targets = [
        z for z in item.rezoning_to if z in config.COMMERCIAL_ZONING_CODES
    ]

    for pattern, points in config.KEYWORD_SCORES.items():
        if re.search(pattern, text, re.IGNORECASE):
            # Don't penalize residential keywords when the item is a rezone
            # to a commercial code -- "from Single Family Residential (SR-2)
            # to Job Center (JC)" is a commercial signal, not noise.
            if points < 0 and commercial_targets:
                continue
            score += points
            if points > 0:
                tags.append(pattern.replace("\\b", "").split("|")[0])
    if commercial_targets:
        score += 25
        tags.append(f"rezone_to:{','.join(commercial_targets)}")

    if item.acreage and item.acreage >= config.ACREAGE_SCORE_THRESHOLD:
        score += config.ACREAGE_SCORE
        tags.append(f"acreage:{item.acreage}")

    score += _institutional_score(item, text, tags)

    applicant = item.applicant or ""
    for firm in config.KNOWN_FIRMS:
        if firm.lower() in applicant.lower():
            score += config.KNOWN_FIRM_SCORE
            tags.append(f"firm:{firm}")
            break

    # Weight by board signal value, clamp to 0-100.
    weight = config.BOARDS.get(board_code, {}).get("signal_weight", 0.5)
    final = max(0, min(100, round(score * weight)))

    return Classification(
        score=final,
        stage=detect_stage(item.request_text or item.raw_text),
        tags=tags,
    )
