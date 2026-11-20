"""Read past city board agendas into the GC pipeline.

The daily job's board source looks back only 120 days on its first run, so a
project whose last hearing came earlier (a BAR-L preliminary in April, say)
never reached the pipeline. This runs the same board source once over a longer
window:

    python -m scripts.backfill_boards --since 2024-01-01
    python -m scripts.backfill_boards --since 2024-01-01 --boards BAR-L TRC

It goes through the normal pipeline job, so agendas already read are skipped,
events are de-duplicated, every agenda read is recorded, and changed projects
are classified (up to the job's per-run limit; the daily job classifies the
rest). It needs the same environment as the app (DATABASE_URL and, for
classification, ANTHROPIC_API_KEY). Expect a few seconds per agenda.
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import date
from typing import Iterable, Optional

BOARD_CODES = ("PC", "BAR-L", "TRC", "DRB", "BAR-S")
MIN_SCORE = 50  # same cut-off as the daily job


def board_source(since: date, today: date, boards: Optional[Iterable[str]] = None) -> tuple:
    """A pipeline job source entry that reads board agendas from `since` on."""
    days = (today - since).days
    kwargs = {"min_score": MIN_SCORE}
    if boards:
        kwargs["boards"] = set(boards)
    # First-run and regular lookback are the same, so the window is honored
    # whether or not board events already exist.
    return ("board", "app.services.pipeline.boards", "fetch_board_events", days, days, kwargs)


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="python -m scripts.backfill_boards", description=__doc__.split("\n\n")[0])
    p.add_argument("--since", type=date.fromisoformat, required=True, help="earliest meeting date, YYYY-MM-DD")
    p.add_argument("--boards", nargs="+", choices=BOARD_CODES, help="limit to these boards (default: all)")
    return p.parse_args(argv)


async def backfill(since: date, boards=None, today: Optional[date] = None, run_job=None) -> dict:
    if run_job is None:
        from app.services.pipeline.job import run_pipeline_job as run_job
    today = today or date.today()
    if since >= today:
        raise ValueError("--since must be in the past")
    return await run_job(sources=[board_source(since, today, boards)], today=today)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        summary = asyncio.run(backfill(args.since, args.boards))
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, default=str))
    result = summary.get("sources", {}).get("board")
    if result is None:
        print("Nothing ran: another pipeline job may hold the lock. Try again when it finishes.", file=sys.stderr)
        return 1
    if isinstance(result, str):   # "unavailable", "failed: ...", "store failed: ..."
        print(f"Board source {result}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
