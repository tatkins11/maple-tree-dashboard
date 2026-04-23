from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from src.ingest.season_csv import import_season_stats_csv
from src.utils.audit import write_audit_report
from src.utils.db import (
    connect_db,
    fetch_counts,
    initialize_database,
    replace_season_batting_stats,
)
from src.utils.player_identity import (
    DEFAULT_ALIAS_OVERRIDE_PATH,
    apply_manual_alias_overrides,
    resolve_player,
)


def sync_sources(
    db_path: Path,
    audit_dir: Path,
    season_csv_paths: Iterable[Path],
    alias_override_path: Path = DEFAULT_ALIAS_OVERRIDE_PATH,
) -> Path:
    connection = connect_db(db_path)
    uncertainties: list[str] = []
    identity_notes: list[str] = []

    try:
        initialize_database(connection)
        identity_notes.extend(apply_manual_alias_overrides(connection, alias_override_path))
        _load_season_csvs(connection, season_csv_paths, uncertainties, identity_notes)
        counts = fetch_counts(connection)
    finally:
        connection.close()

    return write_audit_report(audit_dir, counts, uncertainties, identity_notes)


def _load_season_csvs(
    connection: sqlite3.Connection,
    season_csv_paths: Iterable[Path],
    uncertainties: list[str],
    identity_notes: list[str],
) -> None:
    for csv_path in season_csv_paths:
        records, issues = import_season_stats_csv(csv_path)
        resolved_records = []
        for record in records:
            resolution = resolve_player(
                connection=connection,
                source_name=record.player_name,
                source_file=record.raw_source_file,
                source_type="season_csv",
            )
            if resolution.status in {"manual_review", "new_identity_created"}:
                identity_notes.append(resolution.message)
            if resolution.player_id is None:
                uncertainties.append(resolution.message)
                continue
            resolved_records.append((record, resolution.player_id))
        replace_season_batting_stats(connection, resolved_records)
        uncertainties.extend(f"{csv_path.name}: {issue}" for issue in issues)
