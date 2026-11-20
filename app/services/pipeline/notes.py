"""Research notes: hand-entered pipeline events.

Some facts about a project never appear in a document the pipeline polls: a
Board of Zoning Appeals ruling, numbers from a design packet, press coverage.
A research note records one of them as a PipelineEvent with source "intel", so
it lands on the project's timeline and feeds the AI summary.

Notes are shared with every user, like the rest of the pipeline. Keep private
leads (who called you, who is pricing) in per-user saves instead.

  ResearchNote            validated input (API body / JSON file entry)
  note_event(note)        -> PipelineEvent
  add_notes(db, notes)    store notes; returns per-note results
"""

import hashlib
from datetime import date as Date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import PipelineEventRow, PipelineProject
from app.services.pipeline.events import DELIVERY_METHODS, NOTE_SOURCE, STAGES, PipelineEvent
from app.services.pipeline.store import store_events


class ResearchNote(BaseModel):
    project_key: str = Field(..., min_length=3, max_length=120,
                             description='Pipeline project key, e.g. "BOARD:4600802001"')
    date: Date = Field(..., description="Date of the fact (hearing, article), not the day you typed it")
    title: str = Field(..., min_length=3, max_length=500,
                       description='Shown on the timeline; text before " – " becomes the project name')
    text: str = Field(..., min_length=3, max_length=5000, description="The facts, for people and the AI summary")
    stage: str = Field("other", description="Pipeline stage the fact belongs to; notes never move the project stage")
    source_url: str = Field("", max_length=1000)
    owner: str = Field("", max_length=255, description="Owner as named in the source, if any")
    address: str = Field("", max_length=500)
    estimate: Optional[float] = Field(None, ge=0, description="Only a figure the source states; shown as a stated budget")
    delivery_method: str = Field("", description="Only if the source states it")
    id: str = Field("", max_length=80, description="Optional stable id; defaults to a hash of key, date and title")

    @field_validator("stage")
    @classmethod
    def _stage(cls, v: str) -> str:
        if v not in STAGES:
            raise ValueError(f"stage must be one of {', '.join(STAGES)}")
        return v

    @field_validator("delivery_method")
    @classmethod
    def _delivery(cls, v: str) -> str:
        if v not in DELIVERY_METHODS:
            raise ValueError(f"delivery_method must be one of {', '.join(m for m in DELIVERY_METHODS if m)}")
        return v

    @field_validator("project_key", "title", "text", "source_url", "owner", "address", "id")
    @classmethod
    def _strip(cls, v: str) -> str:
        return (v or "").strip()


def note_external_id(note: ResearchNote) -> str:
    """Stable per note, so loading the same file twice doesn't duplicate it."""
    if note.id:
        return f"{NOTE_SOURCE}:{note.id}"
    digest = hashlib.sha1(f"{note.project_key}|{note.date.isoformat()}|{note.title}".encode()).hexdigest()
    return f"{NOTE_SOURCE}:{digest[:16]}"


def note_event(note: ResearchNote) -> PipelineEvent:
    return PipelineEvent(
        source=NOTE_SOURCE,
        external_id=note_external_id(note),
        project_key=note.project_key,
        title=note.title,
        source_url=note.source_url,
        event_date=note.date,
        stage=note.stage,
        owner=note.owner,
        delivery_method=note.delivery_method,
        estimate=note.estimate,
        address=note.address,
        text=f"Research note: {note.text}",
    )


async def add_notes(db: AsyncSession, notes: List[ResearchNote], *, allow_new_project: bool = False) -> List[dict]:
    """Store notes. Each result: {project_key, status, project_id}.

    status is "added", "duplicate" (already stored) or "unknown_project" (no
    project with that key and allow_new_project is False; nothing stored).
    The caller commits.
    """
    results = []
    for note in notes:
        project = (await db.execute(
            select(PipelineProject).where(PipelineProject.project_key == note.project_key)
        )).scalars().first()
        if project is None and not allow_new_project:
            results.append({"project_key": note.project_key, "status": "unknown_project", "project_id": None})
            continue
        ev = note_event(note)
        exists = (await db.execute(
            select(PipelineEventRow.id).where(
                PipelineEventRow.source == ev.source, PipelineEventRow.external_id == ev.external_id)
        )).first()
        if exists:
            results.append({"project_key": note.project_key, "status": "duplicate",
                            "project_id": project.id if project else None})
            continue
        changed = await store_events(db, [ev])
        project_id = project.id if project else next(iter(changed), None)
        results.append({"project_key": note.project_key, "status": "added", "project_id": project_id})
    return results
