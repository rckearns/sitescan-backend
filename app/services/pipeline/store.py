"""Store PipelineEvents and roll them up into PipelineProjects.

Events link to a project by `project_key` (e.g. "PIP:H15.9689"). A city board
item has its own key ("BOARD:<address>"), so it is also linked to an existing
state project when that project's documents mention the item's street address
(Project 205's BAR items at 106 Coming St, for example).
"""

import re
from datetime import date, datetime
from typing import Iterable, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import PipelineEventRow, PipelineProject
from app.services.pipeline.events import STAGES, PipelineEvent

# When several documents state a delivery method, the main-project methods win
# over a design-bid-build enabling package.
_DELIVERY_PRIORITY = ["cmr", "design-build", "qualifications", "design-bid-build"]


def _as_datetime(d) -> Optional[datetime]:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.replace(tzinfo=None)
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)
    return None


def project_title(event_title: str) -> str:
    """'Project 205 New Construction (College of Charleston) – Establish Phase I…' -> 'Project 205 New Construction'."""
    t = re.sub(r"\s+[–—-]\s+.*$", "", event_title or "").strip()
    t = re.sub(r"\s*\([^()]*\)\s*$", "", t).strip()
    return t or (event_title or "").strip()


def street_pattern(address: str) -> Optional[str]:
    """'106 Coming Street, Charleston' -> '106 Coming' (house number + first street word)."""
    m = re.match(r"\s*(\d+[A-Za-z]?)\s+([A-Za-z][A-Za-z.'-]+)", address or "")
    if not m or m.group(2).lower() in ("n", "s", "e", "w", "north", "south", "east", "west"):
        return None
    return f"{m.group(1)} {m.group(2)}"


# Words that don't identify a particular project when pairing a land purchase
# with its construction project ("Project 205 Land Acquisition" ↔ "Project 205 New Construction").
_GENERIC_TITLE_WORDS = {
    "land", "acquisition", "preliminary", "final", "project", "new", "construction", "renovation",
    "renovations", "phase", "the", "of", "and", "at", "for", "college", "charleston", "university",
    "building", "buildings", "campus", "facility", "facilities", "improvements", "medical", "south",
    "carolina", "musc", "citadel", "trident", "technical", "st", "street",
}


def _title_words(title: str) -> set:
    return {w for w in re.findall(r"[a-z0-9]+", (title or "").lower()) if w not in _GENERIC_TITLE_WORDS}


async def _event_titles(db: AsyncSession, project_id: int) -> list:
    return (await db.execute(
        select(PipelineEventRow.title).where(PipelineEventRow.project_id == project_id)
    )).scalars().all()


async def _stages(db: AsyncSession, project_id: int) -> set:
    return set((await db.execute(
        select(PipelineEventRow.stage).where(PipelineEventRow.project_id == project_id)
    )).scalars().all())


async def _construction_companion(db: AsyncSession, land: PipelineProject) -> Optional[PipelineProject]:
    """The same agency's construction project for a land purchase: a nearby state
    project number whose name shares a distinctive word (e.g. '205')."""
    m = re.match(r"([A-Z]\d{2})\.(\d{4})$", land.pip_number or "")
    if not m:
        return None
    agency, number = m.group(1), int(m.group(2))
    land_words = set().union(*[_title_words(t) for t in await _event_titles(db, land.id)] or [set()])
    candidates = (await db.execute(
        select(PipelineProject).where(and_(
            PipelineProject.pip_number.like(f"{agency}.%"), PipelineProject.id != land.id,
        ))
    )).scalars().all()
    best = None
    for c in candidates:
        try:
            distance = abs(int(c.pip_number.split(".")[1]) - number)
        except (IndexError, ValueError):
            continue
        if distance > 5 or (await _stages(db, c.id)) <= {"land"}:
            continue
        shared = land_words & set().union(*[_title_words(t) for t in await _event_titles(db, c.id)] or [set()])
        if shared and (best is None or distance < best[0]):
            best = (distance, c)
    return best[1] if best else None


async def _find_project_by_address(db: AsyncSession, address: str) -> Optional[PipelineProject]:
    """A state (PIP) project whose documents mention this street address.

    Prefers a construction project; if only a land purchase mentions the
    address, the item goes to that purchase's construction companion.
    """
    pattern = street_pattern(address)
    if not pattern:
        return None
    matches = (await db.execute(
        select(PipelineProject).distinct()
        .join(PipelineEventRow, PipelineEventRow.project_id == PipelineProject.id)
        .where(and_(PipelineProject.pip_number != "", PipelineEventRow.text.ilike(f"%{pattern}%")))
    )).scalars().all()
    for p in matches:
        if not (await _stages(db, p.id)) <= {"land"}:
            return p
    for p in matches:
        companion = await _construction_companion(db, p)
        if companion:
            return companion
    return matches[0] if matches else None


async def _get_or_create_project(db: AsyncSession, ev: PipelineEvent) -> PipelineProject:
    project = (await db.execute(
        select(PipelineProject).where(PipelineProject.project_key == ev.project_key)
    )).scalars().first()
    if project is None and ev.source == "board" and ev.address:
        project = await _find_project_by_address(db, ev.address)
    if project is None:
        project = PipelineProject(project_key=ev.project_key[:120], title=(ev.title or "")[:500])
        db.add(project)
    # Set right away (not only in the rollup) so a board item later in the same
    # batch can find this state project by address.
    if ev.pip_number and not project.pip_number:
        project.pip_number = ev.pip_number[:20]
    await db.flush()
    return project


async def store_events(db: AsyncSession, events: Iterable[PipelineEvent]) -> set:
    """Insert new events (skipping ones already stored); return ids of projects that changed."""
    changed = set()
    for ev in events:
        if not ev.project_key or not ev.external_id:
            continue
        exists = (await db.execute(
            select(PipelineEventRow.id).where(and_(
                PipelineEventRow.source == ev.source, PipelineEventRow.external_id == ev.external_id[:255],
            ))
        )).first()
        if exists:
            continue
        project = await _get_or_create_project(db, ev)
        low, high = (ev.cost_range or (None, None))[:2] if ev.cost_range else (None, None)
        db.add(PipelineEventRow(
            project_id=project.id,
            source=ev.source[:40],
            external_id=ev.external_id[:255],
            stage=ev.stage if ev.stage in STAGES else "other",
            event_date=_as_datetime(ev.event_date),
            title=(ev.title or "")[:500],
            source_url=(ev.source_url or "")[:1000],
            delivery_method=(ev.delivery_method or "")[:30],
            estimate=ev.estimate,
            cost_low=low,
            cost_high=high,
            deadline=_as_datetime(ev.deadline),
            location=(ev.location or "")[:255],
            address=(ev.address or "")[:500],
            text=(ev.text or "")[:6000],
            extra={**(ev.extra or {}), "owner": ev.owner, "pip_number": ev.pip_number},
        ))
        await db.flush()
        changed.add(project.id)
    for project_id in changed:
        await refresh_rollup(db, project_id)
    return changed


async def refresh_rollup(db: AsyncSession, project_id: int) -> None:
    """Recompute a project's summary fields from its events and flag it for the AI."""
    project = await db.get(PipelineProject, project_id)
    events = (await db.execute(
        select(PipelineEventRow).where(PipelineEventRow.project_id == project_id)
    )).scalars().all()
    if not events:
        return
    dated = sorted([e for e in events if e.event_date], key=lambda e: e.event_date)
    latest = dated[-1] if dated else events[-1]

    by_date = sorted(events, key=lambda e: (e.event_date is None, e.event_date or datetime.min))
    pip_events = [e for e in by_date if (e.extra or {}).get("pip_number")]
    title_src = pip_events[-1] if pip_events else latest   # newest official name wins
    project.title = (project_title(title_src.title) or project.title or "")[:500]
    # Trim to column sizes: Postgres rejects over-long values (SQLite in tests doesn't).
    project.pip_number = (next(((e.extra or {}).get("pip_number") for e in reversed(pip_events)), project.pip_number or "") or "")[:20]
    project.owner = (next(((e.extra or {}).get("owner") for e in reversed(events) if (e.extra or {}).get("owner")), project.owner or "") or "")[:255]
    project.address = (next((e.address for e in events if e.address), project.address or "") or "")[:500]
    project.city = (next((e.location for e in reversed(events) if e.location), project.city or "") or "")[:120]
    project.current_stage = latest.stage
    project.first_event_date = dated[0].event_date if dated else None
    project.last_event_date = latest.event_date
    now = datetime.utcnow()
    future = [e.deadline for e in events if e.deadline and e.deadline >= now]
    project.next_deadline = min(future) if future else None

    stated = {e.delivery_method for e in events if e.delivery_method}
    method = next((m for m in _DELIVERY_PRIORITY if m in stated), "")
    if method:
        project.delivery_method, project.delivery_basis = method, "stated"
    amounts = [v for e in events for v in (e.estimate, e.cost_high) if v]
    if amounts:
        project.estimate, project.estimate_basis = max(amounts), "stated"
    project.needs_classification = True
