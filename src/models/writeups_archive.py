from __future__ import annotations

import csv
import sqlite3
from pathlib import Path


DEFAULT_WRITEUPS_MANIFEST_PATH = Path("data/processed/writeups_manifest.csv")
DEFAULT_WRITEUPS_SOURCE = "writeups_manifest"


def ensure_writeups_manifest(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "season",
                "week_label",
                "phase",
                "title",
                "markdown_path",
                "source",
            ],
        )
        writer.writeheader()


def import_writeups_manifest(
    connection: sqlite3.Connection,
    csv_path: Path,
    *,
    root_path: Path | None = None,
) -> int:
    ensure_writeups_manifest(csv_path)
    rows_imported = 0
    base_path = root_path or csv_path.parent

    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            season = (row.get("season") or "").strip()
            week_label = (row.get("week_label") or "").strip()
            phase = (row.get("phase") or "").strip().lower()
            if not season or not week_label or not phase:
                continue

            markdown_path_raw = (row.get("markdown_path") or "").strip()
            if not markdown_path_raw:
                continue

            markdown_path = Path(markdown_path_raw)
            if not markdown_path.is_absolute():
                markdown_path = base_path / markdown_path
            markdown = markdown_path.read_text(encoding="utf-8").strip()
            if markdown:
                markdown += "\n"

            title = (row.get("title") or "").strip() or _title_from_markdown(markdown) or f"{week_label} {phase.title()}"
            source = (row.get("source") or "").strip() or csv_path.name or DEFAULT_WRITEUPS_SOURCE

            connection.execute(
                """
                INSERT INTO writeups (
                    season,
                    week_label,
                    phase,
                    title,
                    markdown,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(season, week_label, phase) DO UPDATE SET
                    title = excluded.title,
                    markdown = excluded.markdown,
                    source = excluded.source,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    season,
                    week_label,
                    phase,
                    title,
                    markdown,
                    source,
                ),
            )
            rows_imported += 1

    connection.commit()
    return rows_imported


def _title_from_markdown(markdown: str) -> str:
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
    return ""
