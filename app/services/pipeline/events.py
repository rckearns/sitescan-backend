"""The common shape every pipeline source produces.

Each source module (state approvals, SCBO, city boards) turns documents into
PipelineEvent objects. They never write to the database; the pipeline job
stores events, links them into projects by `project_key`, and has the AI
read `text` to fill in delivery method, construction type and value.
"""

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

# Stages, roughly in the order a project moves through them.
STAGES = (
    "land",                # state land acquisition approval
    "phase1",              # state Phase I pre-design budget
    "ae-selection",        # architect/engineer solicitation (often states anticipated delivery)
    "board-concept",       # city board: concept / preliminary review, rezoning, PUD
    "board-final",         # city board: final / site plan approval
    "phase2",              # state Phase II full construction budget
    "cmr-solicitation",    # CM at Risk RFQ/RFP
    "design-build-solicitation",
    "bid",                 # design-bid-build / IFB / ITB
    "other",
)

DELIVERY_METHODS = ("cmr", "design-build", "qualifications", "design-bid-build", "")


@dataclass
class PipelineEvent:
    source: str                 # "jbrc", "sfaa", "scbo-ae", "scbo-construction", "board"
    external_id: str            # unique per event within its source (used to dedupe)
    project_key: str            # links events for the same project, e.g. "PIP:H15.9689"
    title: str
    source_url: str = ""
    event_date: Optional[date] = None
    stage: str = "other"        # one of STAGES
    owner: str = ""             # agency / owner, e.g. "College of Charleston"
    pip_number: str = ""        # state project number, normalized "H15.9689"
    delivery_method: str = ""   # only when the document states it explicitly; one of DELIVERY_METHODS
    estimate: Optional[float] = None       # project / construction estimate in dollars, if stated
    cost_range: Optional[tuple] = None     # (low, high) dollars, if stated
    location: str = ""          # city / area text as written
    address: str = ""
    deadline: Optional[datetime] = None    # response / bid due date, if any
    text: str = ""              # excerpt for the AI to read (keep under ~6,000 chars)
    extra: dict = field(default_factory=dict)


_PIP_RE = re.compile(r"\b([A-Z]\d{2})[.\-](\d{4})\b")


def normalize_pip(text: str) -> str:
    """'H15-9689-ML', 'H15.9689' or 'h15 9689' → 'H15.9689'; '' if none found."""
    m = _PIP_RE.search((text or "").upper().replace(" ", "."))
    return f"{m.group(1)}.{m.group(2)}" if m else ""


def pip_project_key(pip: str) -> str:
    return f"PIP:{pip}" if pip else ""
