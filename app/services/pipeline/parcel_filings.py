"""Public filings on a parcel, for the AI parcel analysis.

A parcel analysis is underwritten from assessor data and zoning alone, so on a
parcel with a real project in review it guesses a program (and guesses what
review will allow). When the pipeline holds city board items for that parcel,
this module hands them to the analysis so it can price the project as filed.

Matching is by parcel: city board items are stored under "BOARD:<tms digits>"
(see boards.board_project_key), and research notes sit on the same project.
Not matched yet: a parcel listed second or later on a multi-parcel item, and
board items filed under a state project by street address.

Only shared pipeline data is read (projects and their events). Per-user saves
(pipeline_saves) are private and never enter an analysis, which every user sees.

  lookup_filings(db, tms)            -> (ParcelFilings | None, fingerprint | None); never raises
  filings_fingerprints(db, tms_list) -> {tms: fingerprint} | None; bulk, ids only; never raises
  filings_fingerprint(ids)           changes when a filing is added (or FILINGS_REV is bumped)
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import PipelineEventRow, PipelineProject
from app.services.pipeline.boards import normalize_tms
from app.services.pipeline.events import NOTE_SOURCE

logger = logging.getLogger("sitescan.pipeline.parcel_filings")

# Bump when the prompt section below changes shape: analyses made with an older
# revision then show as out of date for parcels that have filings.
FILINGS_REV = 1
MAX_FILINGS = 12            # filings quoted to the model
FILING_TEXT_LIMIT = 1500    # characters quoted per filing
BLOCK_LIMIT = 9000          # characters for the whole section

STAGE_LABELS = {
    "land": "state land acquisition",
    "phase1": "state Phase I (pre-design)",
    "ae-selection": "architect/engineer selection",
    "board-concept": "city board review (concept or preliminary)",
    "board-final": "city board final or site plan review",
    "phase2": "state Phase II (construction budget)",
    "cmr-solicitation": "CM at Risk solicitation",
    "design-build-solicitation": "design-build solicitation",
    "bid": "out to bid",
    "award": "award notice",
    "other": "other",
}
_SOURCE_LABELS = {
    NOTE_SOURCE: "Research note",
    "jbrc": "Joint Bond Review Committee",
    "sfaa": "State Fiscal Accountability Authority",
    "scbo-ae": "SCBO A/E advertisement",
    "scbo-construction": "SCBO construction advertisement",
    "award": "Agency notice",
}


def board_key(tms) -> str:
    """Pipeline project key for a parcel's city board items; '' if the TMS is unusable."""
    digits = normalize_tms(str(tms or ""))
    return f"BOARD:{digits}" if digits else ""


def filings_fingerprint(event_ids: Iterable[Tuple[str, str]]) -> str:
    """Short hash of a parcel's filings, as (source, external_id) pairs; '' when there are none."""
    ids = sorted(f"{source}:{external_id}" for source, external_id in event_ids)
    if not ids:
        return ""
    return hashlib.sha1("|".join([f"rev{FILINGS_REV}", *ids]).encode()).hexdigest()[:16]


def listed_parcels(raw_tms) -> List[str]:
    """Parcels named in a board item's TMS field: '4590503136, 9' -> ['4590503136', '9']."""
    return [p.strip() for p in re.split(r"[,&/;]|\band\b", str(raw_tms or "")) if re.search(r"\d", p)]


def _clean(text: str, limit: int) -> str:
    """Agenda text arrives with PDF spacing; collapse it and cap the length."""
    t = re.sub(r"[ \t\r\f\v]+", " ", text or "")
    t = re.sub(r" ?\n ?", "\n", t)
    t = re.sub(r"\n{2,}", "\n", t).strip()
    return t if len(t) <= limit else t[:limit].rstrip() + " [...]"


def _day(d: Optional[datetime]) -> Optional[str]:
    return d.date().isoformat() if d else None


@dataclass
class Filing:
    source: str
    external_id: str
    stage: str
    date: Optional[datetime]
    title: str
    url: str
    text: str
    label: str      # where it came from, for people: "Board of Architectural Review - Large", "Research note"


@dataclass
class ParcelFilings:
    """One parcel's pipeline project and every public filing on it."""
    tms: str
    project: dict
    filings: List[Filing] = field(default_factory=list)
    parcels: List[str] = field(default_factory=list)   # parcels the board items name, when more than one

    @property
    def fingerprint(self) -> str:
        return filings_fingerprint((f.source, f.external_id) for f in self.filings)

    @property
    def multi_parcel(self) -> bool:
        return len(self.parcels) > 1

    def quoted(self) -> List[Filing]:
        """The filings shown to the model and listed on the analysis, newest first.

        Research notes are kept ahead of agenda items when there are more than
        MAX_FILINGS: they carry the program (keys, floors, square feet).
        """
        newest = sorted(self.filings, key=lambda f: f.date or datetime.min, reverse=True)
        notes = [f for f in newest if f.source == NOTE_SOURCE]
        rest = [f for f in newest if f.source != NOTE_SOURCE]
        keep = (notes + rest)[:MAX_FILINGS]
        return sorted(keep, key=lambda f: f.date or datetime.min, reverse=True)

    def prompt_block(self) -> str:
        """The "Public filings on this parcel" section of the analysis prompt."""
        p = self.project
        dates = [f.date for f in self.filings if f.date]
        span = f", {min(dates):%b %Y} to {max(dates):%b %Y}" if dates else ""
        head = [
            "Public filings on this parcel (quoted from city board agendas and research notes; newest first):",
            " | ".join(filter(None, [
                f"Project on file: {p.get('title') or 'untitled'}",
                f"Owner on file: {p['owner']}" if p.get("owner") else "",
                f"Stage: {STAGE_LABELS.get(p.get('stage'), p.get('stage') or 'unknown')}",
                f"{len(self.filings)} filing{'' if len(self.filings) == 1 else 's'}{span}",
            ])),
        ]
        if self.multi_parcel:
            head.append(f"The board items name {len(self.parcels)} parcels: {', '.join(self.parcels)}.")
        lines, used, shown = list(head), sum(len(h) for h in head), 0
        for f in self.quoted():
            entry = (f"[{shown + 1}] {_day(f.date) or 'undated'} | {f.label} | {_clean(f.title, 300)}\n"
                     f"{_clean(f.text, FILING_TEXT_LIMIT)}")
            if shown and used + len(entry) > BLOCK_LIMIT:
                break
            lines.append(entry)
            used += len(entry)
            shown += 1
        if shown < len(self.filings):
            lines.append(f"({len(self.filings) - shown} more filings not shown.)")
        lines.append("End of filings.")
        return "\n".join(lines)

    def summary(self) -> dict:
        """What the stored analysis records about the filings it was given (also shown to users)."""
        return {
            "rev": FILINGS_REV,
            "fingerprint": self.fingerprint,
            "project": self.project,
            "count": len(self.filings),
            "multi_parcel": self.multi_parcel,
            "parcels": self.parcels if self.multi_parcel else [],
            "items": [
                {"date": _day(f.date), "label": f.label, "title": f.title, "url": f.url,
                 "stage": f.stage, "source": f.source}
                for f in self.quoted()
            ],
        }


def _filing(e: PipelineEventRow) -> Filing:
    extra = e.extra or {}
    label = (extra.get("board_name") or "City board agenda") if e.source == "board" \
        else _SOURCE_LABELS.get(e.source, e.source)
    return Filing(source=e.source, external_id=e.external_id, stage=e.stage or "other", date=e.event_date,
                  title=e.title or "", url=e.source_url or "", text=e.text or "", label=label)


async def parcel_filings(db: AsyncSession, tms) -> Optional[ParcelFilings]:
    """The pipeline project keyed to this parcel, with its filings; None if there is none. May raise."""
    key = board_key(tms)
    if not key:
        return None
    project = (await db.execute(
        select(PipelineProject).where(PipelineProject.project_key == key)
    )).scalars().first()
    if project is None:
        return None
    events = (await db.execute(
        select(PipelineEventRow).where(PipelineEventRow.project_id == project.id)
    )).scalars().all()
    if not events:
        return None
    parcels: List[str] = []
    for e in events:
        if e.source == "board":
            named = listed_parcels((e.extra or {}).get("tms"))
            if len(named) > len(parcels):
                parcels = named
    return ParcelFilings(
        tms=key.split(":", 1)[1],
        project={
            "id": project.id, "key": project.project_key, "title": project.title or "",
            "owner": project.owner or "", "address": project.address or "",
            "stage": project.current_stage or "other",
            "first_event_date": _day(project.first_event_date),
            "last_event_date": _day(project.last_event_date),
        },
        filings=[_filing(e) for e in events],
        parcels=parcels if len(parcels) > 1 else [],
    )


async def lookup_filings(db: AsyncSession, tms) -> Tuple[Optional[ParcelFilings], Optional[str]]:
    """(filings, fingerprint) for one parcel. Never raises.

    The fingerprint is '' when the parcel has no filings and None when the
    lookup failed, so a database hiccup neither blocks an analysis nor marks a
    stored one out of date.
    """
    if not board_key(tms):
        return None, ""
    try:
        # Savepoint: a failed read must not poison the caller's transaction.
        async with db.begin_nested():
            filings = await parcel_filings(db, tms)
    except Exception as e:  # noqa: BLE001 - the analysis works without filings
        logger.warning("Parcel filings lookup failed for TMS %s: %s", tms, e)
        return None, None
    return filings, (filings.fingerprint if filings else "")


async def filings_fingerprints(db: AsyncSession, tms_list: Iterable) -> Optional[dict]:
    """{tms: fingerprint} for the parcels that have filings. None if the lookup failed. Never raises.

    Two queries however many parcels are asked about, and no filing text is
    loaded: this runs on the cached-analyses endpoint with every Home visit.
    """
    by_key: dict = {}
    for tms in tms_list:
        key = board_key(tms)
        if key:
            by_key.setdefault(key, []).append(tms)
    if not by_key:
        return {}
    try:
        async with db.begin_nested():
            projects = (await db.execute(
                select(PipelineProject.id, PipelineProject.project_key)
                .where(PipelineProject.project_key.in_(list(by_key)))
            )).all()
            if not projects:
                return {}
            key_of = {pid: key for pid, key in projects}
            rows = (await db.execute(
                select(PipelineEventRow.project_id, PipelineEventRow.source, PipelineEventRow.external_id)
                .where(PipelineEventRow.project_id.in_(list(key_of)))
            )).all()
    except Exception as e:  # noqa: BLE001
        logger.warning("Parcel filings fingerprints failed: %s", e)
        return None
    ids: dict = {}
    for pid, source, external_id in rows:
        ids.setdefault(key_of[pid], []).append((source, external_id))
    out = {}
    for key, event_ids in ids.items():
        fp = filings_fingerprint(event_ids)
        for tms in by_key[key]:
            out[tms] = fp
    return out
