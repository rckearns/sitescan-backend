"""Daily GC pipeline job: pull every source, store events, classify changed projects."""

import asyncio
import importlib
import inspect
import logging
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Optional

import anthropic
from sqlalchemy import func, select

from app.config import get_settings
from app.models.database import PipelineDocument, PipelineEventRow, PipelineProject, get_session_factory
from app.services.parcel_analysis import _single_runner_lock
from app.services.pipeline.classify import CLASSIFY_VERSION, classify_project
from app.services.pipeline.store import store_events

logger = logging.getLogger("sitescan.pipeline")

_JOB_LOCK_KEY = 7_324_115_003
CLASSIFY_CONCURRENCY = 3
CLASSIFY_PER_RUN = 150

# Sources that read whole documents remember them in pipeline_documents so daily
# runs only download and parse new postings.
DOCUMENT_SOURCES = {"jbrc", "board"}

# (event source name, module, function, first-run lookback days, regular lookback days[, kwargs])
# State approvals backfill far enough to catch projects approved over the last ~2 years.
# Board items below score 50 (mostly residential / minor exterior work) are left out
# so the AI isn't asked about hundreds of irrelevant items.
SOURCES = [
    ("jbrc", "app.services.pipeline.state_approvals", "fetch_state_approval_events", 830, 60),
    ("scbo-ae", "app.services.pipeline.scbo", "fetch_scbo_pipeline_events", 30, 4),
    ("board", "app.services.pipeline.boards", "fetch_board_events", 120, 30, {"min_score": 50}),
]


def _load(module: str, func_name: str) -> Optional[Callable[..., Awaitable[list]]]:
    try:
        return getattr(importlib.import_module(module), func_name)
    except (ImportError, AttributeError) as e:
        logger.warning(f"Pipeline source {module}.{func_name} unavailable: {e}")
        return None


async def _has_events(session_factory, source: str) -> bool:
    async with session_factory() as db:
        n = await db.scalar(select(func.count(PipelineEventRow.id)).where(PipelineEventRow.source == source))
        return bool(n)


async def run_pipeline_job(
    sources: Optional[list] = None,
    client: Optional[Any] = None,
    session_factory=None,
    today: Optional[date] = None,
) -> dict:
    """Never raises; returns counts for logging."""
    summary = {"events": 0, "projects_changed": 0, "classified": 0, "classify_failed": 0, "sources": {}}
    session_factory = session_factory or get_session_factory()
    today = today or date.today()
    try:
        async with _single_runner_lock(session_factory, key=_JOB_LOCK_KEY) as got:
            if not got:
                logger.info("Pipeline: another replica is running the job; skipping")
                return summary
            await _run(summary, sources or SOURCES, client, session_factory, today)
    except Exception as e:  # never take the scheduler down
        logger.error(f"Pipeline job failed: {e}")
    logger.info(f"Pipeline job: {summary}")
    return summary


async def _known_document_urls(session_factory) -> set:
    async with session_factory() as db:
        return set((await db.execute(select(PipelineDocument.url))).scalars().all())


async def _run(summary, sources, client, session_factory, today):
    for name, module, func_name, first_days, regular_days, *rest in sources:
        kwargs = rest[0] if rest else {}
        fetch = _load(module, func_name) if isinstance(module, str) else module
        if fetch is None:
            summary["sources"][name] = "unavailable"
            continue
        days = regular_days if await _has_events(session_factory, name) else first_days
        processed = []
        if name in DOCUMENT_SOURCES and "on_document" in inspect.signature(fetch).parameters:
            kwargs = {**kwargs, "skip_urls": await _known_document_urls(session_factory),
                      "on_document": lambda src, url, d, n: processed.append((src, url, d, n))}
        try:
            events = await fetch(today - timedelta(days=days), **kwargs)
        except Exception as e:
            logger.error(f"Pipeline source {name} failed: {e}")
            summary["sources"][name] = f"failed: {str(e)[:120]}"
            continue
        try:
            async with session_factory() as db:
                changed = await store_events(db, events)
                known = kwargs.get("skip_urls") or set()
                for src, url, d, n in processed:
                    if url in known:   # re-read upcoming agenda: already recorded
                        continue
                    known.add(url)
                    db.add(PipelineDocument(source=src, url=url[:1000], event_count=n,
                                            document_date=datetime(d.year, d.month, d.day) if d else None))
                await db.commit()
            if processed:
                summary.setdefault("documents", {})[name] = len(processed)
        except Exception as e:   # one source's bad data must not stop the others or classification
            logger.error(f"Pipeline: storing {name} events failed: {str(e)[:300]}")
            summary["sources"][name] = f"store failed: {str(e)[:120]}"
            continue
        summary["sources"][name] = len(events)
        summary["events"] += len(events)
        summary["projects_changed"] += len(changed)

    settings = get_settings()
    if client is None and not settings.anthropic_api_key:
        logger.info("Pipeline: no Anthropic API key; skipping classification")
        return
    client = client or anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    async with session_factory() as db:
        ids = (await db.execute(
            select(PipelineProject.id)
            .where((PipelineProject.needs_classification == True) | (PipelineProject.ai_version < CLASSIFY_VERSION))  # noqa: E712
            .order_by(PipelineProject.last_event_date.desc().nullslast())
            .limit(CLASSIFY_PER_RUN)
        )).scalars().all()

    sem = asyncio.Semaphore(CLASSIFY_CONCURRENCY)

    async def one(pid):
        async with sem:
            async with session_factory() as db:
                project = await db.get(PipelineProject, pid)
                events = (await db.execute(
                    select(PipelineEventRow).where(PipelineEventRow.project_id == pid)
                )).scalars().all()
                try:
                    await classify_project(project, events, client=client)
                    await db.commit()
                    summary["classified"] += 1
                except Exception as e:
                    await db.rollback()
                    summary["classify_failed"] += 1
                    logger.warning(f"Pipeline: classify failed for {project.project_key}: {e}")

    await asyncio.gather(*(one(pid) for pid in ids))
