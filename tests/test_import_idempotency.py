"""Re-running any weekly import must not change row counts.

These tests lock in the idempotency of the import pipeline (upsert_game,
delete-then-insert player_game_batting, INSERT OR REPLACE / delete-by-key for
the schedule and standings) so a future change can't silently reintroduce the
double-application class of bug.
"""
from __future__ import annotations

from pathlib import Path

from src.ingest.manual_boxscore import import_manual_boxscore_bundle
from src.models.schedule import (
    DEFAULT_LEAGUE_SCHEDULE_PATH,
    DEFAULT_SCHEDULE_PATH,
    DEFAULT_STANDINGS_PATH,
    import_league_schedule_csv,
    import_schedule_csv,
    import_standings_csv,
)
from src.utils.db import connect_db, initialize_database


def _counts(connection, tables: list[str]) -> dict[str, int]:
    return {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tables}


def test_manual_boxscore_bundle_import_is_idempotent(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "idem_boxscore.sqlite")
    tables = ["games", "player_game_batting", "schedule_games", "players"]
    try:
        initialize_database(connection)
        import_manual_boxscore_bundle(connection)
        first = _counts(connection, tables)
        import_manual_boxscore_bundle(connection)  # re-run the exact same import
        second = _counts(connection, tables)
    finally:
        connection.close()

    assert first["games"] > 0 and first["player_game_batting"] > 0
    assert first == second, f"re-import changed row counts: {first} -> {second}"


def test_schedule_csv_imports_are_idempotent(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "idem_schedule.sqlite")
    importers = [
        (import_schedule_csv, DEFAULT_SCHEDULE_PATH),
        (import_standings_csv, DEFAULT_STANDINGS_PATH),
        (import_league_schedule_csv, DEFAULT_LEAGUE_SCHEDULE_PATH),
    ]
    tables = ["schedule_games", "standings_snapshot", "league_schedule_games"]
    try:
        initialize_database(connection)
        for importer, path in importers:
            importer(connection, path)
        connection.commit()
        first = _counts(connection, tables)
        for importer, path in importers:
            importer(connection, path)
        connection.commit()
        second = _counts(connection, tables)
    finally:
        connection.close()

    assert all(count > 0 for count in first.values()), first
    assert first == second, f"re-import changed row counts: {first} -> {second}"


def test_league_schedule_csv_has_no_duplicate_ids() -> None:
    """The source-of-truth CSV should hold one row per league_game_id."""
    import csv

    with DEFAULT_LEAGUE_SCHEDULE_PATH.open(encoding="utf-8-sig") as handle:
        ids = [row["league_game_id"] for row in csv.DictReader(handle)]

    assert len(ids) == len(set(ids)), "league_schedule_games.csv has duplicate league_game_id rows"
