"""Add research notes to pipeline projects from a JSON file.

    python -m scripts.add_research_notes notes.json
    python -m scripts.add_research_notes notes.json --dry-run

The file is a list of notes (or {"notes": [...]}), each shaped like the
POST /api/v1/pipeline/notes body entry:

    [{"project_key": "BOARD:4600802001", "date": "2026-09-15",
      "title": "Lowline Hotel – parking requirement cut to 82",
      "text": "...", "stage": "board-concept",
      "source_url": "https://..."}]

Notes are shared with every user: public facts only. Loading the same file
again is safe (stored notes come back as "duplicate"). A note whose
project_key matches no project is skipped unless --allow-new-project.
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from pydantic import TypeAdapter, ValidationError


def load_notes(path: Path) -> list:
    from app.services.pipeline.notes import ResearchNote

    data = json.loads(path.read_text())
    if isinstance(data, dict):
        data = data.get("notes", [])
    return TypeAdapter(list[ResearchNote]).validate_python(data)


async def store(notes: list, allow_new_project: bool, session_factory=None) -> list:
    from app.models.database import get_session_factory
    from app.services.pipeline.notes import add_notes

    session_factory = session_factory or get_session_factory()
    async with session_factory() as db:
        results = await add_notes(db, notes, allow_new_project=allow_new_project)
        await db.commit()
    return results


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m scripts.add_research_notes", description=__doc__.split("\n\n")[0])
    p.add_argument("file", type=Path, help="JSON file of notes")
    p.add_argument("--dry-run", action="store_true", help="validate and print the notes without storing them")
    p.add_argument("--allow-new-project", action="store_true", help="create a project for an unknown project_key")
    args = p.parse_args(argv)
    try:
        notes = load_notes(args.file)
    except (OSError, json.JSONDecodeError, ValidationError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if args.dry_run:
        for n in notes:
            print(f"{n.project_key}  {n.date}  [{n.stage}]  {n.title}")
        print(f"{len(notes)} note(s) valid; nothing stored (dry run)")
        return 0
    results = asyncio.run(store(notes, args.allow_new_project))
    for n, r in zip(notes, results):
        print(f"{r['status']:<16} {n.project_key}  {n.date}  {n.title}")
    return 1 if any(r["status"] == "unknown_project" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
