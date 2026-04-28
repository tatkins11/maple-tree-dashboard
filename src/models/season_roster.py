from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.utils.names import normalize_player_name


DEFAULT_ACTIVE_ROSTER_SEASON = "Current Spring"
DEFAULT_SEASON_ROSTER_PATH = Path("data/processed/current_spring_roster.csv")
DEFAULT_AVAILABILITY_PATH = Path("data/processed/game_day_availability.csv")


@dataclass
class SeasonRosterImportResult:
    matched_count: int
    review_items: list[str]


def ensure_season_roster_file(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["season_name", "player_name", "active_flag", "notes"],
        )
        writer.writeheader()


def import_season_roster(
    connection: sqlite3.Connection,
    csv_path: Path,
    season_name: str,
) -> SeasonRosterImportResult:
    ensure_season_roster_file(csv_path)
    matched_count = 0
    review_items: list[str] = []
    connection.execute("DELETE FROM season_rosters WHERE season_name = ?", (season_name,))

    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_season = (row.get("season_name") or "").strip()
            if row_season and row_season != season_name:
                continue
            player_name = (row.get("player_name") or "").strip()
            if not player_name:
                continue
            active_flag = _to_bool_int(row.get("active_flag"), default=1)
            notes = (row.get("notes") or "").strip() or None
            player_id = _resolve_roster_player_id(connection, player_name)
            if player_id is None:
                review_items.append(
                    f"{season_name}: roster name '{player_name}' did not match a known player identity"
                )
                continue
            connection.execute(
                """
                INSERT INTO season_rosters (
                    season_name,
                    player_id,
                    source_name,
                    active_flag,
                    notes
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (season_name, player_id, player_name, active_flag, notes),
            )
            matched_count += 1

    connection.commit()
    return SeasonRosterImportResult(matched_count=matched_count, review_items=review_items)


def sync_season_roster_additive(
    connection: sqlite3.Connection,
    csv_path: Path,
    season_name: str,
) -> SeasonRosterImportResult:
    ensure_season_roster_file(csv_path)
    matched_count = 0
    review_items: list[str] = []

    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_season = (row.get("season_name") or "").strip()
            if row_season and row_season != season_name:
                continue
            player_name = (row.get("player_name") or "").strip()
            if not player_name:
                continue
            active_flag = _to_bool_int(row.get("active_flag"), default=1)
            notes = (row.get("notes") or "").strip() or None
            player_id = _resolve_roster_player_id(connection, player_name)
            if player_id is None:
                review_items.append(
                    f"{season_name}: roster name '{player_name}' did not match a known player identity"
                )
                continue
            cursor = connection.execute(
                """
                UPDATE season_rosters
                SET source_name = ?, active_flag = ?, notes = ?
                WHERE season_name = ? AND player_id = ?
                """,
                (player_name, active_flag, notes, season_name, player_id),
            )
            if getattr(cursor, "rowcount", 0) == 0:
                connection.execute(
                    """
                    INSERT INTO season_rosters (
                        season_name,
                        player_id,
                        source_name,
                        active_flag,
                        notes
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (season_name, player_id, player_name, active_flag, notes),
                )
            matched_count += 1

    connection.commit()
    return SeasonRosterImportResult(matched_count=matched_count, review_items=review_items)


def fetch_active_roster_rows(
    connection: sqlite3.Connection, season_name: str
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            sr.season_name,
            sr.player_id,
            sr.source_name,
            sr.active_flag,
            COALESCE(sr.notes, '') AS roster_notes,
            pm.preferred_display_name,
            pm.is_fixed_dhh,
            pm.active_flag AS metadata_active_flag,
            pi.canonical_name
        FROM season_rosters sr
        JOIN player_identity pi ON pi.player_id = sr.player_id
        JOIN player_metadata pm ON pm.player_id = sr.player_id
        WHERE sr.season_name = ?
          AND sr.active_flag = 1
        ORDER BY pm.is_fixed_dhh DESC, pm.preferred_display_name COLLATE NOCASE
        """,
        (season_name,),
    ).fetchall()


def seed_availability_from_active_roster(
    connection: sqlite3.Connection,
    season_name: str,
    game_date: str,
    availability_path: Path = DEFAULT_AVAILABILITY_PATH,
) -> int:
    _ensure_availability_file(availability_path)
    existing_names = set(load_availability_rows(availability_path, game_date))
    if existing_names:
        return 0

    roster_rows = fetch_active_roster_rows(connection, season_name)
    needs_newline = (
        availability_path.exists()
        and availability_path.stat().st_size > 0
        and not availability_path.read_text(encoding="utf-8").endswith("\n")
    )
    with availability_path.open("a", newline="", encoding="utf-8") as handle:
        if needs_newline:
            handle.write("\n")
        writer = csv.DictWriter(
            handle,
            fieldnames=["game_date", "player_name", "available_flag", "notes"],
        )
        for row in roster_rows:
            writer.writerow(
                {
                    "game_date": game_date,
                    "player_name": row["preferred_display_name"],
                    "available_flag": "yes",
                    "notes": f"defaulted from active roster: {season_name}",
                }
            )
    return len(roster_rows)


def load_availability_rows(csv_path: Path, game_date: str) -> list[str]:
    _ensure_availability_file(csv_path)
    names: list[str] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if (row.get("game_date") or "").strip() == game_date:
                player_name = (row.get("player_name") or "").strip()
                if player_name:
                    names.append(player_name)
    return names


def _resolve_roster_player_id(connection: sqlite3.Connection, player_name: str) -> int | None:
    direct = connection.execute(
        "SELECT player_id FROM player_identity WHERE canonical_name = ?",
        (normalize_player_name(player_name),),
    ).fetchone()
    if direct:
        return int(direct["player_id"])

    alias = connection.execute(
        """
        SELECT player_id FROM player_aliases
        WHERE normalized_source_name = ?
        LIMIT 1
        """,
        (normalize_player_name(player_name),),
    ).fetchone()
    if alias:
        return int(alias["player_id"])
    return None


def _to_bool_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    return default


def _ensure_availability_file(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["game_date", "player_name", "available_flag", "notes"],
        )
        writer.writeheader()
