from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.models.records import LeagueRulesRecord
from src.models.season_roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    seed_availability_from_active_roster,
)
from src.utils.names import normalize_player_name


DEFAULT_AVAILABILITY_PATH = Path("data/processed/game_day_availability.csv")
DEFAULT_LEAGUE_RULES_PATH = Path("data/processed/league_rules.json")


@dataclass
class GameDayProjectionRow:
    player_id: int
    player_name: str
    canonical_name: str
    preferred_display_name: str
    projection_source: str
    is_fixed_dhh: bool
    baserunning_grade: str
    consistency_grade: str
    speed_flag: bool
    active_flag: bool
    notes: str
    projection_season: str
    current_plate_appearances: int
    career_plate_appearances: int
    current_season_weight: float
    baserunning_adjustment: float
    p_single: float
    p_double: float
    p_triple: float
    p_home_run: float
    p_walk: float
    projected_strikeout_rate: float
    p_hit_by_pitch: float
    p_reached_on_error: float
    p_fielder_choice: float
    p_grounded_into_double_play: float
    p_out: float
    projected_on_base_rate: float
    projected_total_base_rate: float
    projected_run_rate: float
    projected_rbi_rate: float
    projected_extra_base_hit_rate: float


def ensure_availability_file(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["game_date", "player_name", "available_flag", "notes"],
        )
        writer.writeheader()


def ensure_league_rules_file(config_path: Path) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    if config_path.exists():
        return
    config_path.write_text(
        json.dumps(LeagueRulesRecord().model_dump(), indent=2) + "\n",
        encoding="utf-8",
    )


def load_league_rules(config_path: Path) -> LeagueRulesRecord:
    ensure_league_rules_file(config_path)
    data = json.loads(config_path.read_text(encoding="utf-8"))
    return LeagueRulesRecord(**data)


def load_available_player_names(csv_path: Path, game_date: str) -> list[str]:
    ensure_availability_file(csv_path)
    available_names: list[str] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if (row.get("game_date") or "").strip() != game_date:
                continue
            available_flag = (row.get("available_flag") or "").strip().lower()
            if available_flag not in {"1", "true", "yes", "y"}:
                continue
            player_name = (row.get("player_name") or "").strip()
            if player_name:
                available_names.append(player_name)
    return available_names


def load_available_player_names_with_active_roster_defaults(
    connection: sqlite3.Connection,
    csv_path: Path,
    game_date: str,
    season_name: str = DEFAULT_ACTIVE_ROSTER_SEASON,
) -> list[str]:
    available_names = load_available_player_names(csv_path, game_date)
    if available_names:
        return available_names
    seed_availability_from_active_roster(
        connection=connection,
        season_name=season_name,
        game_date=game_date,
        availability_path=csv_path,
    )
    return load_available_player_names(csv_path, game_date)


def select_game_day_projections(
    connection: sqlite3.Connection,
    projection_season: str,
    available_player_names: list[str],
) -> list[GameDayProjectionRow]:
    if not available_player_names:
        return []

    normalized_names = [normalize_player_name(name) for name in available_player_names]
    placeholders = ",".join("?" for _ in normalized_names)
    try:
        rows = connection.execute(
            f"""
        SELECT
            hp.player_id,
            pi.player_name,
            pi.canonical_name,
            pm.preferred_display_name,
            hp.projection_source,
            pm.is_fixed_dhh,
            pm.baserunning_grade,
            pm.consistency_grade,
            pm.speed_flag,
            pm.active_flag,
            COALESCE(pm.notes, '') AS notes,
            hp.projection_season,
            hp.current_plate_appearances,
            hp.career_plate_appearances,
            hp.current_season_weight,
            hp.baserunning_adjustment,
            hp.p_single,
            hp.p_double,
            hp.p_triple,
            hp.p_home_run,
            hp.p_walk,
            hp.projected_strikeout_rate,
            hp.p_hit_by_pitch,
            hp.p_reached_on_error,
            hp.p_fielder_choice,
            hp.p_grounded_into_double_play,
            hp.p_out,
            hp.projected_on_base_rate,
            hp.projected_total_base_rate,
            hp.projected_run_rate,
            hp.projected_rbi_rate,
            hp.projected_extra_base_hit_rate
        FROM hitter_projections hp
        JOIN player_identity pi ON pi.player_id = hp.player_id
        JOIN player_metadata pm ON pm.player_id = hp.player_id
        WHERE hp.projection_season = ?
          AND pm.active_flag = 1
          AND (
              pi.canonical_name IN ({placeholders})
              OR EXISTS (
                  SELECT 1
                  FROM player_aliases pa
                  WHERE pa.player_id = hp.player_id
                    AND pa.normalized_source_name IN ({placeholders})
              )
          )
        ORDER BY pm.is_fixed_dhh DESC, LOWER(pm.preferred_display_name)
        """,
            (projection_season, *normalized_names, *normalized_names),
        ).fetchall()
    except Exception as exc:
        if not _is_missing_projection_alias_object(exc):
            raise
        rows = connection.execute(
            f"""
        SELECT
            hp.player_id,
            pi.player_name,
            pi.canonical_name,
            pm.preferred_display_name,
            hp.projection_source,
            pm.is_fixed_dhh,
            pm.baserunning_grade,
            pm.consistency_grade,
            pm.speed_flag,
            pm.active_flag,
            COALESCE(pm.notes, '') AS notes,
            hp.projection_season,
            hp.current_plate_appearances,
            hp.career_plate_appearances,
            hp.current_season_weight,
            hp.baserunning_adjustment,
            hp.p_single,
            hp.p_double,
            hp.p_triple,
            hp.p_home_run,
            hp.p_walk,
            hp.projected_strikeout_rate,
            hp.p_hit_by_pitch,
            hp.p_reached_on_error,
            hp.p_fielder_choice,
            hp.p_grounded_into_double_play,
            hp.p_out,
            hp.projected_on_base_rate,
            hp.projected_total_base_rate,
            hp.projected_run_rate,
            hp.projected_rbi_rate,
            hp.projected_extra_base_hit_rate
        FROM hitter_projections hp
        JOIN player_identity pi ON pi.player_id = hp.player_id
        JOIN player_metadata pm ON pm.player_id = hp.player_id
        WHERE hp.projection_season = ?
          AND pm.active_flag = 1
          AND pi.canonical_name IN ({placeholders})
        ORDER BY pm.is_fixed_dhh DESC, LOWER(pm.preferred_display_name)
        """,
            (projection_season, *normalized_names),
        ).fetchall()
    return [
        GameDayProjectionRow(
            player_id=int(row["player_id"]),
            player_name=str(row["player_name"]),
            canonical_name=str(row["canonical_name"]),
            preferred_display_name=str(row["preferred_display_name"]),
            projection_source=str(row["projection_source"]),
            is_fixed_dhh=bool(row["is_fixed_dhh"]),
            baserunning_grade=str(row["baserunning_grade"]),
            consistency_grade=str(row["consistency_grade"]),
            speed_flag=bool(row["speed_flag"]),
            active_flag=bool(row["active_flag"]),
            notes=str(row["notes"]),
            projection_season=str(row["projection_season"]),
            current_plate_appearances=int(row["current_plate_appearances"]),
            career_plate_appearances=int(row["career_plate_appearances"]),
            current_season_weight=float(row["current_season_weight"]),
            baserunning_adjustment=float(row["baserunning_adjustment"]),
            p_single=float(row["p_single"]),
            p_double=float(row["p_double"]),
            p_triple=float(row["p_triple"]),
            p_home_run=float(row["p_home_run"]),
            p_walk=float(row["p_walk"]),
            projected_strikeout_rate=float(row["projected_strikeout_rate"]),
            p_hit_by_pitch=float(row["p_hit_by_pitch"]),
            p_reached_on_error=float(row["p_reached_on_error"]),
            p_fielder_choice=float(row["p_fielder_choice"]),
            p_grounded_into_double_play=float(row["p_grounded_into_double_play"]),
            p_out=float(row["p_out"]),
            projected_on_base_rate=float(row["projected_on_base_rate"]),
            projected_total_base_rate=float(row["projected_total_base_rate"]),
            projected_run_rate=float(row["projected_run_rate"]),
            projected_rbi_rate=float(row["projected_rbi_rate"]),
            projected_extra_base_hit_rate=float(row["projected_extra_base_hit_rate"]),
        )
        for row in rows
    ]


def _is_missing_projection_alias_object(exc: Exception) -> bool:
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate in {"42P01", "42703"}:
        return True
    return exc.__class__.__name__ in {
        "UndefinedObject",
        "UndefinedTable",
        "UndefinedColumn",
    }
