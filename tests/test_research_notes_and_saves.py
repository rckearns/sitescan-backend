"""Research notes (shared, admin-only), per-user pipeline saves, and the board backfill script.

In-memory SQLite, no network.
"""
import asyncio
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.models.database import (  # noqa: E402
    Base, PipelineDocument, PipelineEventRow, PipelineProject, PipelineSave, User,
)
from app.services.pipeline.classify import apply_classification  # noqa: E402
from app.services.pipeline.events import NOTE_SOURCE, PipelineEvent  # noqa: E402
from app.services.pipeline.notes import ResearchNote, add_notes, note_event  # noqa: E402
from app.services.pipeline.store import store_events  # noqa: E402

KEY = "BOARD:4600802001"
AI = {"delivery_method": "unknown", "construction_type": "non-wood", "construction_reason": "8-story concrete",
      "building_type": "hospitality", "estimate": None, "in_charleston_area": True,
      "is_building_project": True, "summary": "Eight-story hotel."}


def run(coro):
    return asyncio.run(coro)


async def _factory():
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool,
                                 connect_args={"check_same_thread": False})
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[
            User.__table__, PipelineProject.__table__, PipelineEventRow.__table__,
            PipelineDocument.__table__, PipelineSave.__table__]))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _load_router():
    spec = importlib.util.spec_from_file_location("pipeline_router_notes_test", ROOT / "app/routers/pipeline.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def board_events():
    return [
        PipelineEvent(source="board", external_id="bar:2026-04-08:BAR2026-002510", project_key=KEY,
                      title="Board of Architectural Review - Large: 131 Columbus Street - preliminary",
                      event_date=date(2026, 4, 8), stage="board-concept", owner="131 Columbus NPV MCZ SPE LLC",
                      address="131 Columbus Street", location="Charleston"),
        PipelineEvent(source="board", external_id="trc:2026-03-26:TRC-SP2023-000616", project_key=KEY,
                      title="Technical Review Committee: 131 Columbus St - site plan",
                      event_date=date(2026, 3, 26), stage="board-final", owner="131 Columbus NPV MCZ SPE LLC",
                      address="131 Columbus St", location="Charleston"),
    ]


def bza_note(**over):
    data = dict(project_key=KEY, date=date(2026, 9, 15),
                title="Lowline Hotel – parking requirement cut to 82 spaces",
                text="BZA approved 82 spaces (98 with stackers) for the 174-room hotel, 4-2.",
                stage="board-concept", source_url="https://example.com/pc", owner="")
    data.update(over)
    return ResearchNote(**data)


# ─── notes ───────────────────────────────────────────────────────────────────

def test_note_validation():
    with pytest.raises(ValidationError):
        bza_note(stage="groundbreaking")
    with pytest.raises(ValidationError):
        bza_note(delivery_method="handshake")
    with pytest.raises(ValidationError):
        bza_note(project_key="")
    assert bza_note(title="  padded  ").title == "padded"


def test_note_event_ids_are_stable():
    a, b = note_event(bza_note()), note_event(bza_note())
    assert a.source == NOTE_SOURCE and a.external_id == b.external_id
    assert note_event(bza_note(title="Different")).external_id != a.external_id
    assert note_event(bza_note(id="hoxton-bza")).external_id == "intel:hoxton-bza"
    assert a.text.startswith("Research note: ")


def test_notes_add_facts_without_moving_the_stage():
    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, board_events())
            await db.commit()
        async with f() as db:
            before = (await db.execute(select(PipelineProject))).scalars().one()
            stage_before = before.current_stage
            first = await add_notes(db, [bza_note(), bza_note(date=date(2026, 11, 2), stage="other",
                                                                title="Lowline Hotel – press coverage")])
            again = await add_notes(db, [bza_note()])
            await db.commit()
            p = (await db.execute(select(PipelineProject))).scalars().one()
            n_events = len((await db.execute(select(PipelineEventRow))).scalars().all())
            return stage_before, first, again, p, n_events
    stage_before, first, again, p, n_events = run(go())
    assert stage_before == "board-concept"            # newest document: BAR-L April 8
    assert [r["status"] for r in first] == ["added", "added"]
    assert again[0]["status"] == "duplicate"
    assert n_events == 4
    assert p.current_stage == "board-concept"         # the newer notes didn't move it
    assert p.last_event_date.date() == date(2026, 11, 2)
    assert p.title == "Lowline Hotel"                 # curated note title, request text stripped
    assert p.owner == "131 Columbus NPV MCZ SPE LLC"  # notes without an owner don't blank it
    assert p.needs_classification is True


def test_notes_need_an_existing_project_unless_allowed():
    async def go():
        f = await _factory()
        async with f() as db:
            skipped = await add_notes(db, [bza_note(project_key="BOARD:999")])
            created = await add_notes(db, [bza_note(project_key="BOARD:999")], allow_new_project=True)
            await db.commit()
            p = (await db.execute(select(PipelineProject))).scalars().one()
            return skipped, created, p
    skipped, created, p = run(go())
    assert skipped[0]["status"] == "unknown_project" and skipped[0]["project_id"] is None
    assert created[0]["status"] == "added" and created[0]["project_id"] == p.id
    assert p.current_stage == "board-concept"         # only notes: stage comes from the note


def test_notes_endpoint_stores_and_reports():
    router = _load_router()

    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, board_events())
            await db.commit()
        body = router.ResearchNotesIn(notes=[bza_note()])
        async with f() as db:
            return await router.create_research_notes(body=body, db=db, admin=SimpleNamespace(id=1))
    out = run(go())
    assert out["results"][0]["status"] == "added"


def test_require_admin(monkeypatch):
    from app import auth
    from app.config import get_settings

    monkeypatch.setenv("ADMIN_EMAILS", "Ryan@Example.com, other@example.com")
    get_settings.cache_clear()
    try:
        assert auth.is_admin(SimpleNamespace(email="ryan@example.com"))
        assert run(auth.require_admin(user=SimpleNamespace(email="RYAN@example.com"))).email == "RYAN@example.com"
        with pytest.raises(HTTPException) as e:
            run(auth.require_admin(user=SimpleNamespace(email="someone@else.com")))
        assert e.value.status_code == 403
        monkeypatch.setenv("ADMIN_EMAILS", "")
        get_settings.cache_clear()
        assert not auth.is_admin(SimpleNamespace(email=""))
    finally:
        get_settings.cache_clear()


# ─── saves ───────────────────────────────────────────────────────────────────

def _user(uid, **over):
    base = dict(id=uid, gc_delivery_methods=["cmr", "design-build", "qualifications"], gc_exclude_wood_frame=True,
                gc_min_value=1_000_000, gc_project_types=[], gc_show_unconfirmed=True)
    base.update(over)
    return SimpleNamespace(**base)


def test_saves_are_private_and_keep_excluded_projects_visible():
    router = _load_router()

    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, board_events())
            for proj in (await db.execute(select(PipelineProject))).scalars().all():
                apply_classification(proj, AI)
            await db.commit()
            pid = (await db.execute(select(PipelineProject.id))).scalar_one()
        # This profile excludes hospitality, so the hotel is "excluded" for user 1.
        u1, u2 = _user(1, gc_project_types=["higher-ed"]), _user(2)
        async with f() as db:
            hidden = await router.list_pipeline_projects(include_excluded=False, db=db, user=u1)
        async with f() as db:
            saved = await router.save_pipeline_project(
                project_id=pid, body=router.PipelineSaveIn(status="contacted", notes="Owner called 11/20"),
                db=db, user=u1)
        async with f() as db:
            updated = await router.save_pipeline_project(
                project_id=pid, body=router.PipelineSaveIn(status="pursuing", notes="Call Monday"), db=db, user=u1)
        async with f() as db:
            mine = await router.list_pipeline_projects(include_excluded=False, db=db, user=u1)
        async with f() as db:
            theirs = await router.list_pipeline_projects(include_excluded=False, db=db, user=u2)
        async with f() as db:
            only_saved = await router.list_pipeline_projects(include_excluded=True, db=db, user=u2, saved_only=True)
        async with f() as db:
            n_rows = len((await db.execute(select(PipelineSave))).scalars().all())
        return hidden, saved, updated, mine, theirs, only_saved, n_rows
    hidden, saved, updated, mine, theirs, only_saved, n_rows = run(go())
    assert hidden["projects"] == []
    assert saved["saved"]["status"] == "contacted"
    assert updated["saved"] == {**updated["saved"], "status": "pursuing", "notes": "Call Monday"}
    assert n_rows == 1                                       # upsert, not a second row
    assert len(mine["projects"]) == 1 and mine["projects"][0]["match"] == "excluded"
    assert mine["projects"][0]["saved"]["notes"] == "Call Monday"
    assert theirs["projects"][0]["saved"] is None           # user 2 can't see user 1's notes
    assert only_saved["projects"] == []


def test_save_validation_404_and_delete():
    router = _load_router()

    with pytest.raises(ValidationError):
        router.PipelineSaveIn(status="maybe")
    with pytest.raises(ValidationError):
        router.PipelineSaveIn(notes="x" * (router.NOTES_MAX_CHARS + 1))

    async def go():
        f = await _factory()
        async with f() as db:
            with pytest.raises(HTTPException) as e:
                await router.save_pipeline_project(project_id=404, body=router.PipelineSaveIn(), db=db, user=_user(1))
            await store_events(db, board_events())
            await db.commit()
            pid = (await db.execute(select(PipelineProject.id))).scalar_one()
        async with f() as db:
            await router.save_pipeline_project(project_id=pid, body=router.PipelineSaveIn(), db=db, user=_user(1))
        async with f() as db:
            gone = await router.unsave_pipeline_project(project_id=pid, db=db, user=_user(1))
            again = await router.unsave_pipeline_project(project_id=pid, db=db, user=_user(1))
            left = len((await db.execute(select(PipelineSave))).scalars().all())
        return e.value.status_code, gone, again, left
    code, gone, again, left = run(go())
    assert code == 404
    assert gone["saved"] is None and again["saved"] is None and left == 0


def test_list_still_works_for_callers_without_an_id():
    router = _load_router()

    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, board_events())
            for proj in (await db.execute(select(PipelineProject))).scalars().all():
                apply_classification(proj, AI)
            await db.commit()
        user = _user(None)
        del user.id
        async with f() as db:
            return await router.list_pipeline_projects(include_excluded=True, db=db, user=user)
    out = run(go())
    assert out["projects"][0]["saved"] is None


# ─── scripts ─────────────────────────────────────────────────────────────────

def test_backfill_runs_the_board_source_over_the_window():
    from scripts import backfill_boards as bf

    calls = []

    async def fake_job(sources, today):
        calls.append((sources, today))
        return {"sources": {"board": 12}}

    summary = run(bf.backfill(date(2024, 1, 1), ["BAR-L", "TRC"], today=date(2026, 11, 20), run_job=fake_job))
    (sources, today), = calls
    name, module, func, first_days, regular_days, kwargs = sources[0]
    assert (name, module, func) == ("board", "app.services.pipeline.boards", "fetch_board_events")
    assert first_days == regular_days == (date(2026, 11, 20) - date(2024, 1, 1)).days
    assert kwargs == {"min_score": 50, "boards": {"BAR-L", "TRC"}}
    assert summary == {"sources": {"board": 12}}
    with pytest.raises(ValueError):
        run(bf.backfill(date(2030, 1, 1), today=date(2026, 11, 20), run_job=fake_job))
    assert bf.parse_args(["--since", "2024-01-01"]).boards is None
    with pytest.raises(SystemExit):
        bf.parse_args(["--since", "2024-01-01", "--boards", "BZA"])


def test_add_research_notes_cli(tmp_path, capsys):
    from scripts import add_research_notes as cli

    note = {"project_key": KEY, "date": "2026-09-15", "title": "Lowline Hotel – parking cut to 82",
            "text": "BZA approved 82 spaces.", "stage": "board-concept"}
    as_list, as_dict, bad = tmp_path / "a.json", tmp_path / "b.json", tmp_path / "c.json"
    as_list.write_text(json.dumps([note]))
    as_dict.write_text(json.dumps({"notes": [note]}))
    bad.write_text(json.dumps([{**note, "stage": "nope"}]))

    assert len(cli.load_notes(as_list)) == len(cli.load_notes(as_dict)) == 1
    assert cli.main([str(as_list), "--dry-run"]) == 0
    assert "1 note(s) valid" in capsys.readouterr().out
    assert cli.main([str(bad), "--dry-run"]) == 2

    async def go():
        f = await _factory()
        async with f() as db:
            await store_events(db, board_events())
            await db.commit()
        first = await cli.store(cli.load_notes(as_list), False, session_factory=f)
        second = await cli.store(cli.load_notes(as_list), False, session_factory=f)
        return first, second
    first, second = run(go())
    assert first[0]["status"] == "added" and second[0]["status"] == "duplicate"
