"""Orchestration: discover -> fetch new -> parse -> classify -> upsert.

Storage uses the app's async SQLAlchemy engine (app.models.database).
The rest of the package (poller, parser, classifier) has no DB knowledge.

Entry point: run_boards_scrape(). Schedule it daily; agendas post roughly a
week before meetings, BAR meets twice monthly and PC monthly, so daily is
more than enough.
"""
from __future__ import annotations

import asyncio
import logging
import os

from sqlalchemy import text

from app.models.database import get_session_factory
from . import config, poller
from .classifier import classify
from .parser import AgendaItem, parse_agenda_pdf

log = logging.getLogger(__name__)

ALERT_SCORE_THRESHOLD = int(os.environ.get("BOARDS_ALERT_THRESHOLD", "50"))


def _project_key(item: AgendaItem) -> str | None:
    if item.case_number:
        return item.case_number
    if item.tms:
        return f"tms:{item.tms}"
    return None


async def _known_external_ids(conn) -> set[str]:
    result = await conn.execute(text("SELECT external_id FROM board_agendas"))
    return {row[0] for row in result.fetchall()}


async def _insert_agenda(conn, ref: poller.AgendaRef, item_count: int,
                         parse_ok: bool) -> int:
    result = await conn.execute(
        text("""
        INSERT INTO board_agendas
            (external_id, board_code, meeting_date, pdf_url, title,
             parse_ok, item_count)
        VALUES (:external_id, :board_code, :meeting_date, :pdf_url, :title,
                :parse_ok, :item_count)
        ON CONFLICT (external_id) DO UPDATE SET
            parse_ok = EXCLUDED.parse_ok, item_count = EXCLUDED.item_count
        RETURNING id
        """),
        {
            "external_id": ref.external_id,
            "board_code": ref.board_code,
            "meeting_date": ref.meeting_date,
            "pdf_url": ref.pdf_url,
            "title": ref.title,
            "parse_ok": parse_ok,
            "item_count": item_count,
        },
    )
    return result.fetchone()[0]


async def _upsert_project(conn, item: AgendaItem, stage: str | None,
                          score: int, agenda_id: int) -> int | None:
    key = _project_key(item)
    if not key:
        return None

    row = (await conn.execute(
        text(
            "SELECT id, current_stage, max_score FROM board_projects "
            "WHERE project_key = :key"
        ),
        {"key": key},
    )).fetchone()

    if row is None:
        project_id = (await conn.execute(
            text("""
            INSERT INTO board_projects
                (project_key, case_number, tms, address, neighborhood,
                 council_district, acreage, owner, applicant,
                 current_stage, max_score)
            VALUES (:project_key,:case_number,:tms,:address,:neighborhood,
                    :council_district,:acreage,:owner,:applicant,
                    :current_stage,:max_score)
            RETURNING id
            """),
            {
                "project_key": key,
                "case_number": item.case_number,
                "tms": item.tms,
                "address": item.address,
                "neighborhood": item.neighborhood,
                "council_district": item.council_district,
                "acreage": item.acreage,
                "owner": item.owner,
                "applicant": item.applicant,
                "current_stage": stage,
                "max_score": score,
            },
        )).fetchone()[0]
        await conn.execute(
            text("""
            INSERT INTO board_project_events
                (project_id, event_type, to_stage, agenda_id, score)
            VALUES (:project_id, 'new_project', :to_stage, :agenda_id, :score)
            """),
            {"project_id": project_id, "to_stage": stage,
             "agenda_id": agenda_id, "score": score},
        )
        return project_id

    project_id, prev_stage, _ = row
    await conn.execute(
        text("""
        UPDATE board_projects SET
            current_stage = COALESCE(:stage, current_stage),
            max_score = GREATEST(max_score, :score),
            owner = COALESCE(:owner, owner),
            applicant = COALESCE(:applicant, applicant),
            acreage = COALESCE(:acreage, acreage),
            last_seen_at = now()
        WHERE id = :id
        """),
        {"stage": stage, "score": score, "owner": item.owner,
         "applicant": item.applicant, "acreage": item.acreage,
         "id": project_id},
    )
    if stage and stage != prev_stage:
        await conn.execute(
            text("""
            INSERT INTO board_project_events
                (project_id, event_type, from_stage, to_stage,
                 agenda_id, score)
            VALUES (:project_id, 'stage_change', :from_stage, :to_stage,
                    :agenda_id, :score)
            """),
            {"project_id": project_id, "from_stage": prev_stage,
             "to_stage": stage, "agenda_id": agenda_id, "score": score},
        )
    return project_id


async def _insert_item(conn, agenda_id: int, project_id: int | None,
                       item: AgendaItem, stage: str | None, score: int,
                       tags: list[str]) -> None:
    await conn.execute(
        text("""
        INSERT INTO board_agenda_items
            (agenda_id, project_id, item_number, section, address,
             case_number, tms, neighborhood, council_district, acreage,
             request_text, owner, applicant, stage, score, tags, raw_text)
        VALUES (:agenda_id,:project_id,:item_number,:section,:address,
                :case_number,:tms,:neighborhood,:council_district,:acreage,
                :request_text,:owner,:applicant,:stage,:score,:tags,:raw_text)
        ON CONFLICT (agenda_id, item_number) DO NOTHING
        """),
        {
            "agenda_id": agenda_id,
            "project_id": project_id,
            "item_number": item.item_number,
            "section": item.section,
            "address": item.address,
            "case_number": item.case_number,
            "tms": item.tms,
            "neighborhood": item.neighborhood,
            "council_district": item.council_district,
            "acreage": item.acreage,
            "request_text": item.request_text,
            "owner": item.owner,
            "applicant": item.applicant,
            "stage": stage,
            "score": score,
            "tags": tags,
            "raw_text": item.raw_text,
        },
    )


async def run_boards_scrape() -> dict:
    """Daily job. Returns a summary dict for logging/monitoring."""
    # Network I/O (requests library) runs in a thread to avoid blocking the
    # event loop; DB operations use the app's async SQLAlchemy engine.
    refs = await asyncio.to_thread(poller.discover)
    summary = {"discovered": len(refs), "new_agendas": 0,
               "items": 0, "alerts": 0}

    session_factory = get_session_factory()
    async with session_factory() as session:
        async with session.begin():
            async with session.connection() as conn:
                known = await _known_external_ids(conn)
        new_refs = [r for r in refs if r.external_id not in known]
        summary["new_agendas"] = len(new_refs)

        for ref in new_refs:
            try:
                pdf = await asyncio.to_thread(poller.fetch_pdf, ref.pdf_url)
                items = await asyncio.to_thread(parse_agenda_pdf, pdf)
                parse_ok = True
            except Exception as exc:
                log.exception("Failed to fetch/parse %s: %s", ref.pdf_url, exc)
                items, parse_ok = [], False

            async with session.begin():
                async with session.connection() as conn:
                    agenda_id = await _insert_agenda(
                        conn, ref, len(items), parse_ok
                    )
                    for item in items:
                        c = classify(item, ref.board_code)
                        project_id = await _upsert_project(
                            conn, item, c.stage, c.score, agenda_id
                        )
                        await _insert_item(
                            conn, agenda_id, project_id, item,
                            c.stage, c.score, c.tags,
                        )
                        summary["items"] += 1
                        if c.score >= ALERT_SCORE_THRESHOLD:
                            summary["alerts"] += 1

            await asyncio.sleep(config.REQUEST_DELAY_SECONDS)

    log.info("Boards scrape complete: %s", summary)
    return summary
