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
