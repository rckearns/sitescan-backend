"""AI classification of pipeline projects: what is it, how will it be procured,
is it likely wood frame, how big, and is it in the Charleston area.

Facts stated in a document (delivery method, budget) are set from the events
in store.refresh_rollup and are never overwritten by the AI's inference.
"""

import json
import logging
from typing import Any, Optional

import anthropic

from app.config import get_settings
from app.models.database import PipelineEventRow, PipelineProject
from app.services.parcel_analysis import _parse_json

logger = logging.getLogger("sitescan.pipeline")

CLASSIFY_VERSION = 1
MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 900
MAX_INPUT_CHARS = 14000

BUILDING_TYPES = ("higher-ed", "k12", "healthcare", "government", "commercial", "hospitality",
                  "multifamily", "industrial", "infrastructure", "other")
DELIVERY = ("cmr", "design-build", "qualifications", "design-bid-build", "unknown")
CONSTRUCTION = ("non-wood", "wood", "unknown")

SYSTEM_PROMPT = """You classify construction projects for a general contractor in Charleston, SC.
You are given dated documents about ONE project: state budget approvals (JBRC/SFAA), state
procurement ads (SCBO), and City of Charleston board agendas (BAR, Planning, TRC).

Respond with JSON only, exactly this shape:
{
  "title": "short project name, e.g. 'College of Charleston Project 205 student housing'",
  "owner": "owner / agency",
  "building_type": one of ["higher-ed","k12","healthcare","government","commercial","hospitality","multifamily","industrial","infrastructure","other"],
  "construction_type": one of ["non-wood","wood","unknown"],
  "construction_reason": "one short sentence",
  "delivery_method": one of ["cmr","design-build","qualifications","design-bid-build","unknown"],
  "delivery_basis": one of ["stated","inferred","unknown"],
  "estimated_construction_value": number in USD or null,
  "city": "city / area",
  "in_charleston_area": true or false,
  "summary": "1-2 sentences a GC would want: scope, size, where it stands"
}

Rules:
- construction_type: "wood" only when wood framing is stated or the project is clearly low-rise
  light construction (single-family, townhomes, garden apartments, small wood-frame buildings).
  "non-wood" when steel/concrete/masonry is stated or the type virtually never uses wood framing
  (labs, hospitals, classroom/academic buildings, parking decks, buildings over 5 stories,
  most state institutional buildings). Mid-rise student or multifamily housing without stated
  structure -> "unknown". Never guess wood from cost alone.
- delivery_method: "stated" only if a document names it (Construction Manager at Risk / CM-R,
  design-build, design-bid-build / sealed bid / IFB). An A/E ad's "Anticipated Project Delivery
  Method" counts as stated. Otherwise infer only with good reason, else "unknown".
- estimated_construction_value: a stated construction cost/range or project estimate; else null.
- in_charleston_area: Charleston, Berkeley or Dorchester counties (Charleston, North Charleston,
  Mount Pleasant, Summerville, Goose Creek, the islands, etc.).
"""


def build_input(project: PipelineProject, events: list) -> str:
    parts = [f"Project key: {project.project_key}", f"Owner (from documents): {project.owner}"]
    budget = MAX_INPUT_CHARS
    for e in sorted(events, key=lambda e: (e.event_date is None, e.event_date)):
        head = f"\n--- {e.event_date.date() if e.event_date else 'undated'} | {e.source} | {e.stage} | {e.title}"
        facts = []
        if e.delivery_method:
            facts.append(f"delivery method stated: {e.delivery_method}")
        if e.estimate:
            facts.append(f"estimate: ${e.estimate:,.0f}")
        if e.cost_high:
            facts.append(f"cost range: ${e.cost_low or 0:,.0f}-${e.cost_high:,.0f}")
        if e.address:
            facts.append(f"address: {e.address}")
        body = (e.text or "")[: max(0, min(4000, budget))]
        chunk = head + ("\n" + "; ".join(facts) if facts else "") + "\n" + body
        parts.append(chunk)
        budget -= len(chunk)
        if budget <= 0:
            break
    return "\n".join(parts)


def _choice(v: Any, allowed: tuple, default: str) -> str:
    v = str(v or "").strip().lower()
    return v if v in allowed else default


def apply_classification(project: PipelineProject, data: dict) -> None:
    """Copy AI output onto the project without overriding stated facts."""
    # State projects keep their official name; board items get the AI's cleaner one.
    if data.get("title") and not project.pip_number:
        project.title = str(data["title"])[:500]
    project.building_type = _choice(data.get("building_type"), BUILDING_TYPES, "other")
    project.construction_type = _choice(data.get("construction_type"), CONSTRUCTION, "unknown")
    project.construction_reason = str(data.get("construction_reason") or "")[:500]
    if project.delivery_basis != "stated":
        method = _choice(data.get("delivery_method"), DELIVERY, "unknown")
        basis = _choice(data.get("delivery_basis"), ("stated", "inferred", "unknown"), "unknown")
        project.delivery_method = "" if method == "unknown" else method
        project.delivery_basis = "unknown" if method == "unknown" else ("inferred" if basis == "stated" else basis)
    if project.estimate_basis != "stated":
        try:
            value = float(data.get("estimated_construction_value") or 0) or None
        except (TypeError, ValueError):
            value = None
        project.estimate = value
        project.estimate_basis = "inferred" if value else "unknown"
    if not project.owner and data.get("owner"):
        project.owner = str(data["owner"])[:255]
    if data.get("city"):
        project.city = str(data["city"])[:120]
    if isinstance(data.get("in_charleston_area"), bool):
        project.in_charleston_area = data["in_charleston_area"]
    project.summary = str(data.get("summary") or "")[:1000]
    project.ai_version = CLASSIFY_VERSION
    project.needs_classification = False


async def classify_project(project: PipelineProject, events: list, client: Optional[Any] = None) -> dict:
    """Ask the AI about one project and apply the result. Raises on API/JSON failure."""
    if client is None:
        client = anthropic.AsyncAnthropic(api_key=get_settings().anthropic_api_key)
    message = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_input(project, events)}],
    )
    data = _parse_json(message.content[0].text)
    apply_classification(project, data)
    return data
