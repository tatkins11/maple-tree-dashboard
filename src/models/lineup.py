from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_AVAILABILITY_PATH,
    GameDayProjectionRow,
    load_available_player_names_with_active_roster_defaults,
    select_game_day_projections,
)
from src.utils.names import normalize_player_name


DEFAULT_LINEUP_PATH = Path("data/processed/game_day_lineup.csv")


@dataclass
class SimulationLineupRow:
    player_id: int
    player_name: str
    projection_source: str
    lineup_spot: int
    is_fixed_dhh: bool
    baserunning_adjustment: float
    p_single: float
    p_double: float
    p_triple: float
    p_home_run: float
    p_walk: float
    p_hit_by_pitch: float
    p_reached_on_error: float
    p_fielder_choice: float
    p_grounded_into_double_play: float
    projected_strikeout_rate: float
    p_out: float
    projected_on_base_rate: float
    projected_total_base_rate: float
    projected_run_rate: float
    projected_rbi_rate: float


def ensure_lineup_file(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["game_date", "lineup_spot", "player_name", "notes"],
        )
        writer.writeheader()


def build_simulation_lineup(
    connection: sqlite3.Connection,
    projection_season: str,
    game_date: str,
    availability_path: Path = DEFAULT_AVAILABILITY_PATH,
    lineup_path: Path = DEFAULT_LINEUP_PATH,
    roster_season: str = DEFAULT_ACTIVE_ROSTER_SEASON,
) -> list[SimulationLineupRow]:
    available_names = load_available_player_names_with_active_roster_defaults(
        connection=connection,
        csv_path=availability_path,
        game_date=game_date,
        season_name=roster_season,
    )
    available_rows = select_game_day_projections(
        connection=connection,
        projection_season=projection_season,
        available_player_names=available_names,
    )
    available_by_name = {
        normalize_player_name(row.preferred_display_name): row for row in available_rows
    }
    for row in available_rows:
        available_by_name.setdefault(normalize_player_name(row.player_name), row)

    lineup_assignments = load_lineup_assignments(lineup_path, game_date)
    lineup_rows: list[SimulationLineupRow] = []
    for lineup_spot, player_name in lineup_assignments:
        normalized_name = normalize_player_name(player_name)
        row = available_by_name.get(normalized_name)
        if row is None:
            raise ValueError(
                f"Lineup player '{player_name}' for {game_date} is not available or has no usable projection."
            )
        lineup_rows.append(_to_simulation_row(row, lineup_spot))

    _validate_lineup(
        lineup_rows,
        expected_player_names=[row.preferred_display_name for row in available_rows],
    )
    return lineup_rows


def build_simulation_lineup_from_order(
    connection: sqlite3.Connection,
    projection_season: str,
    ordered_player_names: list[str],
    available_player_names: list[str],
) -> list[SimulationLineupRow]:
    available_rows = select_game_day_projections(
        connection=connection,
        projection_season=projection_season,
        available_player_names=available_player_names,
    )
    available_by_name = {
        normalize_player_name(row.preferred_display_name): row for row in available_rows
    }
    for row in available_rows:
        available_by_name.setdefault(normalize_player_name(row.player_name), row)

    lineup_rows: list[SimulationLineupRow] = []
    for index, player_name in enumerate(ordered_player_names, start=1):
        normalized_name = normalize_player_name(player_name)
        row = available_by_name.get(normalized_name)
        if row is None:
            raise ValueError(
                f"Lineup player '{player_name}' is not available or has no usable projection."
            )
        lineup_rows.append(_to_simulation_row(row, index))

    _validate_lineup(
        lineup_rows,
        expected_player_names=[row.preferred_display_name for row in available_rows],
    )
    return lineup_rows


def load_lineup_assignments(csv_path: Path, game_date: str) -> list[tuple[int, str]]:
    ensure_lineup_file(csv_path)
    assignments: list[tuple[int, str]] = []
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if (row.get("game_date") or "").strip() != game_date:
                continue
            player_name = (row.get("player_name") or "").strip()
            lineup_spot = int((row.get("lineup_spot") or "").strip())
            assignments.append((lineup_spot, player_name))
    assignments.sort(key=lambda item: item[0])
    return assignments


def _to_simulation_row(
    row: GameDayProjectionRow, lineup_spot: int
) -> SimulationLineupRow:
    return SimulationLineupRow(
        player_id=row.player_id,
        player_name=row.preferred_display_name,
        projection_source=row.projection_source,
        lineup_spot=lineup_spot,
        is_fixed_dhh=row.is_fixed_dhh,
        baserunning_adjustment=row.baserunning_adjustment,
        p_single=row.p_single,
        p_double=row.p_double,
        p_triple=row.p_triple,
        p_home_run=row.p_home_run,
        p_walk=row.p_walk,
        p_hit_by_pitch=row.p_hit_by_pitch,
        p_reached_on_error=row.p_reached_on_error,
        p_fielder_choice=row.p_fielder_choice,
        p_grounded_into_double_play=row.p_grounded_into_double_play,
        projected_strikeout_rate=row.projected_strikeout_rate,
        p_out=row.p_out,
        projected_on_base_rate=row.projected_on_base_rate,
        projected_total_base_rate=row.projected_total_base_rate,
        projected_run_rate=row.projected_run_rate,
        projected_rbi_rate=row.projected_rbi_rate,
    )


def _validate_lineup(
    lineup_rows: list[SimulationLineupRow],
    expected_player_names: list[str] | None = None,
) -> None:
    if not lineup_rows:
        raise ValueError("No lineup rows found for the selected game date.")
    seen_spots = set()
    seen_players = set()
    expected_spot = 1
    for row in lineup_rows:
        if row.lineup_spot in seen_spots:
            raise ValueError(f"Duplicate lineup spot found: {row.lineup_spot}")
        if row.player_id in seen_players:
            raise ValueError(f"Duplicate player found in lineup: {row.player_name}")
        if row.lineup_spot != expected_spot:
            raise ValueError(
                f"Lineup spots must be consecutive starting at 1. Found {row.lineup_spot} when expecting {expected_spot}."
            )
        seen_spots.add(row.lineup_spot)
        seen_players.add(row.player_id)
        expected_spot += 1

    if expected_player_names is None:
        return

    expected_count = len(expected_player_names)
    if len(lineup_rows) != expected_count:
        lineup_names = {normalize_player_name(row.player_name) for row in lineup_rows}
        missing_names = [
            name
            for name in expected_player_names
            if normalize_player_name(name) not in lineup_names
        ]
        missing_label = ", ".join(missing_names) if missing_names else "unknown"
        raise ValueError(
            "Lineup must include every available player exactly once. "
            f"Expected {expected_count} hitters but found {len(lineup_rows)}. "
            f"Missing players: {missing_label}."
        )
