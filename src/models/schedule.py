from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path


DEFAULT_SCHEDULE_PATH = Path("data/processed/team_schedule.csv")
DEFAULT_STANDINGS_PATH = Path("data/processed/standings_snapshot.csv")
DEFAULT_LEAGUE_SCHEDULE_PATH = Path("data/processed/league_schedule_games.csv")
DEFAULT_SCHEDULE_TEAM_NAME = "Maple Tree"
DEFAULT_SCHEDULE_SOURCE = "local_csv"


@dataclass
class ScheduleImportResult:
    games_imported: int
    standings_rows_imported: int
    league_games_imported: int


def ensure_schedule_csv(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "game_id",
                "season",
                "league_name",
                "division_name",
                "week_label",
                "game_date",
                "game_time",
                "team_name",
                "opponent_name",
                "home_away",
                "location_or_field",
                "status",
                "completed_flag",
                "is_bye",
                "result",
                "runs_for",
                "runs_against",
                "notes",
                "source",
            ],
        )
        writer.writeheader()


def ensure_standings_csv(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "season",
                "league_name",
                "division_name",
                "snapshot_date",
                "team_name",
                "wins",
                "losses",
                "ties",
                "win_pct",
                "games_back",
                "notes",
                "source",
            ],
        )
        writer.writeheader()


def ensure_league_schedule_csv(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "league_game_id",
                "season",
                "league_name",
                "division_name",
                "week_label",
                "game_date",
                "game_time",
                "location_or_field",
                "home_team",
                "away_team",
                "status",
                "completed_flag",
                "home_runs",
                "away_runs",
                "result_summary",
                "notes",
                "source",
            ],
        )
        writer.writeheader()


def import_schedule_csv(
    connection: sqlite3.Connection,
    csv_path: Path,
) -> int:
    ensure_schedule_csv(csv_path)
    rows_imported = 0
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            season = (row.get("season") or "").strip()
            game_date = (row.get("game_date") or "").strip()
            team_name = (row.get("team_name") or "").strip()
            if not season or not game_date or not team_name:
                continue

            game_id = (row.get("game_id") or "").strip() or _build_game_id(row, index)
            opponent_name = (row.get("opponent_name") or "").strip() or None
            status = (row.get("status") or "").strip().lower() or "scheduled"
            is_bye = _to_bool_int(row.get("is_bye"), default=0)
            runs_for = _to_optional_int(row.get("runs_for"))
            runs_against = _to_optional_int(row.get("runs_against"))
            completed_flag = _derive_completed_flag(
                explicit_flag=row.get("completed_flag"),
                status=status,
                runs_for=runs_for,
                runs_against=runs_against,
                result=row.get("result"),
            )
            result = _derive_game_result(
                runs_for=runs_for,
                runs_against=runs_against,
                explicit_result=row.get("result"),
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO schedule_games (
                    game_id,
                    season,
                    league_name,
                    division_name,
                    week_label,
                    game_date,
                    game_time,
                    team_name,
                    opponent_name,
                    home_away,
                    location_or_field,
                    status,
                    completed_flag,
                    is_bye,
                    result,
                    runs_for,
                    runs_against,
                    notes,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    game_id,
                    season,
                    (row.get("league_name") or "").strip() or None,
                    (row.get("division_name") or "").strip() or None,
                    (row.get("week_label") or "").strip() or None,
                    game_date,
                    (row.get("game_time") or "").strip() or None,
                    team_name,
                    opponent_name,
                    (row.get("home_away") or "").strip() or None,
                    (row.get("location_or_field") or "").strip() or None,
                    status,
                    completed_flag,
                    is_bye,
                    result,
                    runs_for,
                    runs_against,
                    (row.get("notes") or "").strip() or None,
                    (row.get("source") or "").strip() or csv_path.name or DEFAULT_SCHEDULE_SOURCE,
                ),
            )
            rows_imported += 1

    connection.commit()
    return rows_imported


def import_standings_csv(
    connection: sqlite3.Connection,
    csv_path: Path,
) -> int:
    ensure_standings_csv(csv_path)
    rows_imported = 0
    normalized_rows: list[tuple[str, str | None, str | None, str, str, int, int, int, float, float | None, str | None, str]] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            season = (row.get("season") or "").strip()
            snapshot_date = (row.get("snapshot_date") or "").strip()
            team_name = (row.get("team_name") or "").strip()
            if not season or not snapshot_date or not team_name:
                continue

            normalized_rows.append(
                (
                    season,
                    (row.get("league_name") or "").strip() or None,
                    (row.get("division_name") or "").strip() or None,
                    snapshot_date,
                    team_name,
                    _to_optional_int(row.get("wins")) or 0,
                    _to_optional_int(row.get("losses")) or 0,
                    _to_optional_int(row.get("ties")) or 0,
                    _to_optional_float(row.get("win_pct")) or 0.0,
                    _to_optional_float(row.get("games_back")),
                    (row.get("notes") or "").strip() or None,
                    (row.get("source") or "").strip() or csv_path.name or DEFAULT_SCHEDULE_SOURCE,
                )
            )

    if normalized_rows:
        snapshot_keys = {
            (season, league_name, division_name, snapshot_date)
            for season, league_name, division_name, snapshot_date, *_ in normalized_rows
        }
        for season, league_name, division_name, snapshot_date in snapshot_keys:
            connection.execute(
                """
                DELETE FROM standings_snapshot
                WHERE season = ?
                  AND COALESCE(league_name, '') = COALESCE(?, '')
                  AND COALESCE(division_name, '') = COALESCE(?, '')
                  AND snapshot_date = ?
                """,
                (season, league_name, division_name, snapshot_date),
            )

        connection.executemany(
            """
            INSERT INTO standings_snapshot (
                season,
                league_name,
                division_name,
                snapshot_date,
                team_name,
                wins,
                losses,
                ties,
                win_pct,
                games_back,
                notes,
                source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
        rows_imported = len(normalized_rows)

    connection.commit()
    return rows_imported


def import_league_schedule_csv(
    connection: sqlite3.Connection,
    csv_path: Path,
) -> int:
    ensure_league_schedule_csv(csv_path)
    rows_imported = 0
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            season = (row.get("season") or "").strip()
            game_date = (row.get("game_date") or "").strip()
            home_team = (row.get("home_team") or "").strip()
            away_team = (row.get("away_team") or "").strip()
            if not season or not game_date or not home_team or not away_team:
                continue

            league_game_id = (row.get("league_game_id") or "").strip() or _build_league_game_id(row, index)
            status = (row.get("status") or "").strip().lower() or "scheduled"
            home_runs = _to_optional_int(row.get("home_runs"))
            away_runs = _to_optional_int(row.get("away_runs"))
            completed_flag = _derive_completed_flag(
                explicit_flag=row.get("completed_flag"),
                status=status,
                runs_for=home_runs,
                runs_against=away_runs,
                result=row.get("result_summary"),
            )
            result_summary = _derive_league_result_summary(
                home_team=home_team,
                away_team=away_team,
                home_runs=home_runs,
                away_runs=away_runs,
                explicit_summary=row.get("result_summary"),
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO league_schedule_games (
                    league_game_id,
                    season,
                    league_name,
                    division_name,
                    week_label,
                    game_date,
                    game_time,
                    location_or_field,
                    home_team,
                    away_team,
                    status,
                    completed_flag,
                    home_runs,
                    away_runs,
                    result_summary,
                    notes,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    league_game_id,
                    season,
                    (row.get("league_name") or "").strip() or None,
                    (row.get("division_name") or "").strip() or None,
                    (row.get("week_label") or "").strip() or None,
                    game_date,
                    (row.get("game_time") or "").strip() or None,
                    (row.get("location_or_field") or "").strip() or None,
                    home_team,
                    away_team,
                    status,
                    completed_flag,
                    home_runs,
                    away_runs,
                    result_summary,
                    (row.get("notes") or "").strip() or None,
                    (row.get("source") or "").strip() or csv_path.name or DEFAULT_SCHEDULE_SOURCE,
                ),
            )
            rows_imported += 1

    connection.commit()
    return rows_imported


def import_schedule_bundle(
    connection: sqlite3.Connection,
    schedule_csv_path: Path,
    standings_csv_path: Path | None = None,
    league_schedule_csv_path: Path | None = None,
) -> ScheduleImportResult:
    games_imported = import_schedule_csv(connection, schedule_csv_path)
    standings_rows_imported = 0
    league_games_imported = 0
    if standings_csv_path is not None and standings_csv_path.exists():
        standings_rows_imported = import_standings_csv(connection, standings_csv_path)
    if league_schedule_csv_path is not None and league_schedule_csv_path.exists():
        league_games_imported = import_league_schedule_csv(connection, league_schedule_csv_path)
    return ScheduleImportResult(
        games_imported=games_imported,
        standings_rows_imported=standings_rows_imported,
        league_games_imported=league_games_imported,
    )


def update_game_result(
    connection: sqlite3.Connection,
    *,
    game_id: str,
    runs_for: int,
    runs_against: int,
    result: str | None = None,
    notes: str | None = None,
    status: str = "completed",
) -> int:
    derived_result = _derive_game_result(
        runs_for=runs_for,
        runs_against=runs_against,
        explicit_result=result,
    )
    cursor = connection.execute(
        """
        UPDATE schedule_games
        SET
            completed_flag = 1,
            status = ?,
            result = ?,
            runs_for = ?,
            runs_against = ?,
            notes = COALESCE(?, notes)
        WHERE game_id = ?
        """,
        (
            (status or "completed").strip().lower(),
            derived_result,
            runs_for,
            runs_against,
            notes.strip() if notes else None,
            game_id,
        ),
    )
    connection.commit()
    return int(cursor.rowcount)


def _build_game_id(row: dict[str, object], index: int) -> str:
    season = str(row.get("season") or "").strip().lower().replace(" ", "-")
    game_date = str(row.get("game_date") or "").strip()
    game_time = str(row.get("game_time") or "").strip().lower().replace(" ", "")
    team_name = str(row.get("team_name") or "").strip().lower().replace(" ", "-")
    opponent_name = str(row.get("opponent_name") or "bye").strip().lower().replace(" ", "-")
    week_label = str(row.get("week_label") or f"week-{index}").strip().lower().replace(" ", "-")
    return f"{season}|{week_label}|{game_date}|{game_time}|{team_name}|{opponent_name}|{index}"


def _build_league_game_id(row: dict[str, object], index: int) -> str:
    season = str(row.get("season") or "").strip().lower().replace(" ", "-")
    week_label = str(row.get("week_label") or f"week-{index}").strip().lower().replace(" ", "-")
    game_date = str(row.get("game_date") or "").strip()
    game_time = str(row.get("game_time") or "").strip().lower().replace(" ", "")
    home_team = str(row.get("home_team") or "").strip().lower().replace(" ", "-")
    away_team = str(row.get("away_team") or "").strip().lower().replace(" ", "-")
    return f"{season}|{week_label}|{game_date}|{game_time}|{away_team}|{home_team}|{index}"


def _to_bool_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    return default


def _to_optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(float(text))


def _to_optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _derive_game_result(
    *,
    runs_for: int | None,
    runs_against: int | None,
    explicit_result: object | None = None,
) -> str | None:
    explicit = str(explicit_result or "").strip().upper()
    if explicit in {"W", "L", "T"}:
        return explicit
    if runs_for is None or runs_against is None:
        return None
    if runs_for > runs_against:
        return "W"
    if runs_for < runs_against:
        return "L"
    return "T"


def _derive_league_result_summary(
    *,
    home_team: str,
    away_team: str,
    home_runs: int | None,
    away_runs: int | None,
    explicit_summary: object | None = None,
) -> str | None:
    explicit = str(explicit_summary or "").strip()
    if explicit:
        return explicit
    if home_runs is None or away_runs is None:
        return None
    return f"{away_team} {away_runs} - {home_team} {home_runs}"


def _derive_completed_flag(
    *,
    explicit_flag: object | None,
    status: str,
    runs_for: int | None,
    runs_against: int | None,
    result: object | None,
) -> int:
    explicit = _to_bool_int(explicit_flag, default=-1)
    if explicit in {0, 1}:
        return explicit
    if status in {"completed", "final"}:
        return 1
    if _derive_game_result(runs_for=runs_for, runs_against=runs_against, explicit_result=result):
        return 1
    return 0
