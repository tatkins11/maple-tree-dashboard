from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from src.models.records import ParsedGame, PlayerGameBattingRecord
from src.utils.db import upsert_game
from src.utils.player_identity import (
    DEFAULT_ALIAS_OVERRIDE_PATH,
    apply_manual_alias_overrides,
    resolve_player,
)
from src.utils.validation import validate_player_game_record


DEFAULT_GAME_BOXSCORE_GAMES_PATH = Path("data/processed/game_boxscore_games.csv")
DEFAULT_GAME_BOXSCORE_BATTING_PATH = Path("data/processed/game_boxscore_batting.csv")
DEFAULT_GAME_BOXSCORE_SOURCE_TYPE = "manual_boxscore"


@dataclass
class ManualBoxscoreImportResult:
    games_imported: int
    batting_rows_imported: int
    schedule_rows_imported: int
    uncertainties: list[str] = field(default_factory=list)
    identity_notes: list[str] = field(default_factory=list)


def ensure_game_boxscore_games_csv(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "game_key",
                "season",
                "team_name",
                "game_date",
                "game_time",
                "opponent_name",
                "team_score",
                "opponent_score",
                "notes",
                "source",
            ],
        )
        writer.writeheader()


def ensure_game_boxscore_batting_csv(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "game_key",
                "lineup_spot",
                "player_name",
                "pa",
                "ab",
                "h",
                "1b",
                "2b",
                "3b",
                "hr",
                "rbi",
                "r",
                "bb",
                "so",
                "sf",
                "fc",
                "gidp",
                "outs",
                "notes",
            ],
        )
        writer.writeheader()


def import_manual_boxscore_bundle(
    connection: sqlite3.Connection,
    *,
    games_csv_path: Path = DEFAULT_GAME_BOXSCORE_GAMES_PATH,
    batting_csv_path: Path = DEFAULT_GAME_BOXSCORE_BATTING_PATH,
    alias_override_path: Path = DEFAULT_ALIAS_OVERRIDE_PATH,
) -> ManualBoxscoreImportResult:
    ensure_game_boxscore_games_csv(games_csv_path)
    ensure_game_boxscore_batting_csv(batting_csv_path)

    uncertainties: list[str] = []
    identity_notes = apply_manual_alias_overrides(connection, alias_override_path)

    games_by_key = _read_games_csv(games_csv_path, uncertainties)
    batting_rows_by_key = _read_batting_csv(batting_csv_path, uncertainties)

    games_imported = 0
    batting_rows_imported = 0
    schedule_rows_imported = 0

    for game_key, game_row in games_by_key.items():
        parsed_game = ParsedGame(
            team_name=game_row["team_name"],
            game_date=game_row["game_date"],
            game_time=game_row["game_time"],
            opponent_name=game_row["opponent_name"],
            team_score=game_row["team_score"],
            opponent_score=game_row["opponent_score"],
            source_file=game_key,
            season=game_row["season"],
            notes=game_row["notes"],
            player_rows=[],
        )
        game_id = upsert_game(connection, parsed_game)
        games_imported += 1

        _upsert_schedule_game(connection, game_key=game_key, game_row=game_row)
        schedule_rows_imported += 1

        connection.execute("DELETE FROM player_game_batting WHERE game_id = ?", (game_id,))
        imported_for_game = 0
        for batting_row in batting_rows_by_key.get(game_key, []):
            player_name = str(batting_row["player_name"]).strip()
            resolution = resolve_player(
                connection=connection,
                source_name=player_name,
                source_file=game_key,
                source_type=DEFAULT_GAME_BOXSCORE_SOURCE_TYPE,
            )
            if resolution.status in {"manual_review", "new_identity_created"}:
                identity_notes.append(resolution.message)
            if resolution.player_id is None:
                uncertainties.append(resolution.message)
                continue

            record = _build_player_game_record(
                game_key=game_key,
                batting_row=batting_row,
                uncertainties=uncertainties,
            )
            issues = validate_player_game_record(record)
            uncertainties.extend(f"{game_key}: {issue}" for issue in issues)
            _insert_player_game_row(connection, game_id=game_id, player_id=resolution.player_id, row=record)
            imported_for_game += 1

        if imported_for_game == 0:
            uncertainties.append(f"{game_key}: no batting rows were imported for this game")
        batting_rows_imported += imported_for_game

    connection.commit()
    return ManualBoxscoreImportResult(
        games_imported=games_imported,
        batting_rows_imported=batting_rows_imported,
        schedule_rows_imported=schedule_rows_imported,
        uncertainties=uncertainties,
        identity_notes=identity_notes,
    )


def _read_games_csv(csv_path: Path, uncertainties: list[str]) -> dict[str, dict[str, object]]:
    games_by_key: dict[str, dict[str, object]] = {}
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=2):
            game_key = str(row.get("game_key") or "").strip()
            season = str(row.get("season") or "").strip()
            team_name = str(row.get("team_name") or "").strip()
            game_date = str(row.get("game_date") or "").strip()
            opponent_name = str(row.get("opponent_name") or "").strip()
            if not game_key or not season or not team_name or not game_date or not opponent_name:
                uncertainties.append(f"{csv_path.name}:{index}: skipped game row with missing required fields")
                continue

            games_by_key[game_key] = {
                "season": season,
                "team_name": team_name,
                "game_date": game_date,
                "game_time": str(row.get("game_time") or "").strip() or None,
                "opponent_name": opponent_name,
                "team_score": _to_optional_int(row.get("team_score")),
                "opponent_score": _to_optional_int(row.get("opponent_score")),
                "notes": str(row.get("notes") or "").strip() or None,
                "source": str(row.get("source") or "").strip() or csv_path.name,
            }
    return games_by_key


def _read_batting_csv(csv_path: Path, uncertainties: list[str]) -> dict[str, list[dict[str, object]]]:
    rows_by_key: dict[str, list[dict[str, object]]] = defaultdict(list)
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=2):
            game_key = str(row.get("game_key") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            lineup_spot = _to_optional_int(row.get("lineup_spot"))
            if not game_key or not player_name or lineup_spot is None:
                uncertainties.append(f"{csv_path.name}:{index}: skipped batting row with missing game_key, lineup_spot, or player_name")
                continue
            rows_by_key[game_key].append(
                {
                    "lineup_spot": lineup_spot,
                    "player_name": player_name,
                    "pa": _to_int(row.get("pa")),
                    "ab": _to_int(row.get("ab")),
                    "h": _to_optional_int(row.get("h")),
                    "1b": _to_int(row.get("1b")),
                    "2b": _to_int(row.get("2b")),
                    "3b": _to_int(row.get("3b")),
                    "hr": _to_int(row.get("hr")),
                    "rbi": _to_int(row.get("rbi")),
                    "r": _to_int(row.get("r")),
                    "bb": _to_int(row.get("bb")),
                    "so": _to_int(row.get("so")),
                    "sf": _to_int(row.get("sf")),
                    "fc": _to_int(row.get("fc")),
                    "gidp": _to_int(row.get("gidp")),
                    "outs": _to_optional_int(row.get("outs")),
                }
            )
    for game_key, game_rows in rows_by_key.items():
        game_rows.sort(key=lambda row: (int(row["lineup_spot"]), str(row["player_name"]).lower()))
    return rows_by_key


def _build_player_game_record(
    *,
    game_key: str,
    batting_row: dict[str, object],
    uncertainties: list[str],
) -> PlayerGameBattingRecord:
    singles = int(batting_row["1b"])
    doubles = int(batting_row["2b"])
    triples = int(batting_row["3b"])
    home_runs = int(batting_row["hr"])
    strikeouts = int(batting_row["so"])
    fielder_choice = int(batting_row["fc"])
    double_plays = int(batting_row["gidp"])
    derived_hits = singles + doubles + triples + home_runs
    listed_hits = batting_row["h"]
    if listed_hits is not None and int(listed_hits) != derived_hits:
        uncertainties.append(
            f"{game_key}: hits mismatch for {batting_row['player_name']}: listed {listed_hits}, derived {derived_hits}"
        )

    explicit_outs = batting_row["outs"]
    derived_outs = int(batting_row["ab"]) - derived_hits - strikeouts - fielder_choice - double_plays
    outs = explicit_outs if explicit_outs is not None else max(0, derived_outs)

    return PlayerGameBattingRecord(
        lineup_spot=int(batting_row["lineup_spot"]),
        player_name=str(batting_row["player_name"]),
        canonical_name=str(batting_row["player_name"]).strip().lower(),
        plate_appearances=int(batting_row["pa"]),
        at_bats=int(batting_row["ab"]),
        singles=singles,
        doubles=doubles,
        triples=triples,
        home_runs=home_runs,
        walks=int(batting_row["bb"]),
        strikeouts=strikeouts,
        sacrifice_flies=int(batting_row["sf"]),
        fielder_choice=fielder_choice,
        double_plays=double_plays,
        outs=int(outs),
        runs=int(batting_row["r"]),
        rbi=int(batting_row["rbi"]),
        raw_scorebook_file=game_key,
    )


def _insert_player_game_row(
    connection: sqlite3.Connection,
    *,
    game_id: int,
    player_id: int,
    row: PlayerGameBattingRecord,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO player_game_batting (
            game_id,
            player_id,
            lineup_spot,
            plate_appearances,
            at_bats,
            singles,
            doubles,
            triples,
            home_runs,
            walks,
            strikeouts,
            sacrifice_flies,
            fielder_choice,
            double_plays,
            outs,
            runs,
            rbi,
            raw_scorebook_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            game_id,
            player_id,
            row.lineup_spot,
            row.plate_appearances,
            row.at_bats,
            row.singles,
            row.doubles,
            row.triples,
            row.home_runs,
            row.walks,
            row.strikeouts,
            row.sacrifice_flies,
            row.fielder_choice,
            row.double_plays,
            row.outs,
            row.runs,
            row.rbi,
            row.raw_scorebook_file,
        ),
    )


def _upsert_schedule_game(
    connection: sqlite3.Connection,
    *,
    game_key: str,
    game_row: dict[str, object],
) -> None:
    team_score = game_row["team_score"]
    opponent_score = game_row["opponent_score"]
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
            game_key,
            game_row["season"],
            None,
            None,
            None,
            game_row["game_date"],
            game_row["game_time"],
            game_row["team_name"],
            game_row["opponent_name"],
            None,
            None,
            "final" if team_score is not None and opponent_score is not None else "scheduled",
            1 if team_score is not None and opponent_score is not None else 0,
            0,
            _derive_result(team_score, opponent_score),
            team_score,
            opponent_score,
            game_row["notes"],
            game_row["source"],
        ),
    )


def _derive_result(team_score: int | None, opponent_score: int | None) -> str | None:
    if team_score is None or opponent_score is None:
        return None
    if team_score > opponent_score:
        return "W"
    if team_score < opponent_score:
        return "L"
    return "T"


def _to_optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(float(text))


def _to_int(value: object) -> int:
    optional_value = _to_optional_int(value)
    return 0 if optional_value is None else int(optional_value)
