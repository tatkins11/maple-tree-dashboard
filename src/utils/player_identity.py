from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from src.utils.names import normalize_player_name


DEFAULT_ALIAS_OVERRIDE_PATH = Path("data/processed/player_alias_overrides.csv")


@dataclass
class PlayerResolution:
    player_id: int | None
    status: str
    message: str


def ensure_alias_override_file(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["source_name", "player_name", "canonical_name", "notes"],
        )
        writer.writeheader()


def apply_manual_alias_overrides(connection: sqlite3.Connection, csv_path: Path) -> list[str]:
    ensure_alias_override_file(csv_path)
    notes: list[str] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_name = (row.get("source_name") or "").strip()
            player_name = (row.get("player_name") or "").strip()
            canonical_name = (row.get("canonical_name") or "").strip()
            if not source_name:
                continue
            target_name = player_name or source_name
            target_canonical = canonical_name or normalize_player_name(target_name)
            if not target_canonical:
                notes.append(f"alias override skipped for '{source_name}': missing canonical target")
                continue
            player_id = _get_identity_by_canonical(connection, target_canonical)
            if player_id is None:
                player_id = _create_identity(connection, target_name, target_canonical)
            _upsert_alias(
                connection=connection,
                player_id=player_id,
                source_name=source_name,
                normalized_source_name=normalize_player_name(source_name),
                source_type="manual_override",
                source_file=csv_path.name,
                match_method="manual_override",
                approved_flag=1,
            )
    connection.commit()
    return notes


def resolve_player(
    connection: sqlite3.Connection,
    source_name: str,
    source_file: str,
    source_type: str = "season_csv",
) -> PlayerResolution:
    normalized_source_name = normalize_player_name(source_name)
    if not normalized_source_name:
        return PlayerResolution(
            player_id=None,
            status="manual_review",
            message=f"{source_file}: blank player name requires manual review",
        )

    alias_match = connection.execute(
        """
        SELECT player_id FROM player_aliases
        WHERE source_name = ?
        ORDER BY approved_flag DESC, alias_id ASC
        LIMIT 1
        """,
        (source_name,),
    ).fetchone()
    if alias_match:
        return PlayerResolution(
            player_id=int(alias_match["player_id"]),
            status="exact_alias_match",
            message=f"{source_file}: exact alias match for '{source_name}'",
        )

    canonical_match = connection.execute(
        """
        SELECT player_id FROM player_identity
        WHERE canonical_name = ?
        LIMIT 1
        """,
        (normalized_source_name,),
    ).fetchone()
    if canonical_match:
        player_id = int(canonical_match["player_id"])
        _upsert_alias(
            connection=connection,
            player_id=player_id,
            source_name=source_name,
            normalized_source_name=normalized_source_name,
            source_type=source_type,
            source_file=source_file,
            match_method="exact_canonical_name_match",
            approved_flag=0,
        )
        connection.commit()
        return PlayerResolution(
            player_id=player_id,
            status="exact_canonical_name_match",
            message=f"{source_file}: exact canonical name match for '{source_name}'",
        )

    safe_matches = connection.execute(
        """
        SELECT DISTINCT player_id FROM player_aliases
        WHERE normalized_source_name = ?
        """,
        (normalized_source_name,),
    ).fetchall()
    if len(safe_matches) == 1:
        player_id = int(safe_matches[0]["player_id"])
        _upsert_alias(
            connection=connection,
            player_id=player_id,
            source_name=source_name,
            normalized_source_name=normalized_source_name,
            source_type=source_type,
            source_file=source_file,
            match_method="safe_normalized_match",
            approved_flag=0,
        )
        connection.commit()
        return PlayerResolution(
            player_id=player_id,
            status="safe_normalized_match",
            message=f"{source_file}: safe normalized match for '{source_name}'",
        )

    if len(safe_matches) > 1:
        return PlayerResolution(
            player_id=None,
            status="manual_review",
            message=(
                f"{source_file}: ambiguous normalized match for '{source_name}' "
                f"matched {len(safe_matches)} player identities"
            ),
        )

    player_id = _create_identity(connection, source_name, normalized_source_name)
    _upsert_alias(
        connection=connection,
        player_id=player_id,
        source_name=source_name,
        normalized_source_name=normalized_source_name,
        source_type=source_type,
        source_file=source_file,
        match_method="new_identity_created",
        approved_flag=0,
    )
    connection.commit()
    return PlayerResolution(
        player_id=player_id,
        status="new_identity_created",
        message=f"{source_file}: created new player identity for '{source_name}'",
    )


def fetch_identity_review_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            i.player_id,
            i.player_name,
            i.canonical_name,
            a.source_name,
            a.source_type,
            a.match_method,
            a.approved_flag
        FROM player_identity i
        LEFT JOIN player_aliases a ON a.player_id = i.player_id
        ORDER BY LOWER(i.player_name), LOWER(COALESCE(a.source_name, ''))
        """
    ).fetchall()


def _get_identity_by_canonical(
    connection: sqlite3.Connection, canonical_name: str
) -> int | None:
    row = connection.execute(
        """
        SELECT player_id FROM player_identity
        WHERE canonical_name = ?
        LIMIT 1
        """,
        (canonical_name,),
    ).fetchone()
    return None if row is None else int(row["player_id"])


def _create_identity(
    connection: sqlite3.Connection, player_name: str, canonical_name: str
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO players (player_name, canonical_name, active_flag)
        VALUES (?, ?, 1)
        """,
        (player_name, canonical_name),
    )
    player_id = int(cursor.lastrowid)
    connection.execute(
        """
        INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag)
        VALUES (?, ?, ?, 1)
        """,
        (player_id, player_name, canonical_name),
    )
    return player_id


def _upsert_alias(
    connection: sqlite3.Connection,
    player_id: int,
    source_name: str,
    normalized_source_name: str,
    source_type: str,
    source_file: str,
    match_method: str,
    approved_flag: int,
) -> None:
    connection.execute(
        """
        INSERT INTO player_aliases (
            player_id,
            source_name,
            normalized_source_name,
            source_type,
            source_file,
            match_method,
            approved_flag
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name, source_type)
        DO UPDATE SET
            player_id = excluded.player_id,
            normalized_source_name = excluded.normalized_source_name,
            source_file = excluded.source_file,
            match_method = excluded.match_method,
            approved_flag = excluded.approved_flag
        """,
        (
            player_id,
            source_name,
            normalized_source_name,
            source_type,
            source_file,
            match_method,
            approved_flag,
        ),
    )
