"""Tests for the player-form / head-to-head / pregame data layer."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.dashboard.data import (
    fetch_player_recent_form,
    fetch_player_vs_opponent,
    fetch_pregame_hot_bats,
    fetch_team_recent_form,
    fetch_team_vs_opponent,
)
from src.models.schedule import import_schedule_csv
from src.utils.db import connect_db, initialize_database


def _insert_player(connection, player_id: int, player_name: str, canonical: str) -> None:
    connection.execute(
        "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical),
    )
    connection.execute(
        "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical),
    )
    connection.execute(
        """
        INSERT INTO player_metadata (
            player_id, preferred_display_name, is_fixed_dhh, baserunning_grade,
            consistency_grade, speed_flag, active_flag, notes
        ) VALUES (?, ?, 0, 'C', 'C', 0, 1, '')
        """,
        (player_id, player_name),
    )


def _insert_game(
    connection,
    *,
    game_id: int,
    game_date: str,
    opponent: str,
    season: str,
    game_time: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO games (game_id, game_date, game_time, opponent_name, source_file, season, notes)
        VALUES (?, ?, ?, ?, ?, ?, '')
        """,
        (game_id, game_date, game_time, opponent, f"game_{game_id}.png", season),
    )


def _insert_player_game(
    connection,
    *,
    game_id: int,
    player_id: int,
    pa: int,
    ab: int,
    singles: int = 0,
    doubles: int = 0,
    triples: int = 0,
    hr: int = 0,
    walks: int = 0,
    strikeouts: int = 0,
    runs: int = 0,
    rbi: int = 0,
) -> None:
    connection.execute(
        """
        INSERT INTO player_game_batting (
            game_id, player_id, lineup_spot, plate_appearances, at_bats, singles, doubles, triples,
            home_runs, walks, strikeouts, sacrifice_flies, fielder_choice, double_plays, outs,
            runs, rbi, raw_scorebook_file
        ) VALUES (?, ?, 3, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, ?)
        """,
        (
            game_id,
            player_id,
            pa,
            ab,
            singles,
            doubles,
            triples,
            hr,
            walks,
            strikeouts,
            runs,
            rbi,
            f"game_{game_id}.png",
        ),
    )


def _insert_season_row(
    connection,
    *,
    season: str,
    player_id: int,
    games: int,
    pa: int,
    ab: int,
    hits: int,
    singles: int,
    doubles: int,
    triples: int,
    hr: int,
    walks: int,
    runs: int,
    rbi: int,
    tb: int,
    raw_source_file: str,
) -> None:
    avg = hits / ab if ab else 0
    obp = (hits + walks) / (ab + walks) if (ab + walks) else 0
    slg = tb / ab if ab else 0
    ops = obp + slg
    connection.execute(
        """
        INSERT INTO season_batting_stats (
            season, player_id, games, plate_appearances, at_bats, hits, singles, doubles, triples,
            home_runs, walks, strikeouts, hit_by_pitch, sacrifice_hits, sacrifice_flies,
            reached_on_error, fielder_choice, grounded_into_double_play, runs, rbi, total_bases,
            batting_average, on_base_percentage, slugging_percentage, ops,
            batting_average_risp, two_out_rbi, left_on_base, raw_source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?)
        """,
        (
            season,
            player_id,
            games,
            pa,
            ab,
            hits,
            singles,
            doubles,
            triples,
            hr,
            walks,
            runs,
            rbi,
            tb,
            avg,
            obp,
            slg,
            ops,
            raw_source_file,
        ),
    )


def _seed_player_with_games(connection) -> None:
    _insert_player(connection, 1, "Jane Smith", "jane smith")
    season = "Maple Tree Spring 2026"

    # Game 1 (oldest): cold — 0-for-3, no walks
    _insert_game(connection, game_id=1, game_date="2026-04-01", opponent="Eagles", season=season)
    _insert_player_game(connection, game_id=1, player_id=1, pa=3, ab=3, walks=0, strikeouts=2)

    # Game 2: average — 1-for-3 single
    _insert_game(connection, game_id=2, game_date="2026-04-08", opponent="Hawks", season=season)
    _insert_player_game(connection, game_id=2, player_id=1, pa=3, ab=3, singles=1, runs=1)

    # Game 3: very hot — 3-for-3, 2HR, 1BB
    _insert_game(connection, game_id=3, game_date="2026-04-15", opponent="Eagles", season=season)
    _insert_player_game(
        connection,
        game_id=3,
        player_id=1,
        pa=4,
        ab=3,
        singles=1,
        hr=2,
        walks=1,
        runs=3,
        rbi=5,
    )

    # Game 4 (most recent): hot — 2-for-3 with a double
    _insert_game(connection, game_id=4, game_date="2026-04-22", opponent="Eagles", season=season)
    _insert_player_game(
        connection,
        game_id=4,
        player_id=1,
        pa=4,
        ab=3,
        singles=1,
        doubles=1,
        walks=1,
        runs=2,
        rbi=1,
    )

    # Season totals row (for baseline)
    totals = {
        "pa": 14,
        "ab": 12,
        "hits": 6,
        "singles": 3,
        "doubles": 1,
        "triples": 0,
        "hr": 2,
        "walks": 2,
        "runs": 6,
        "rbi": 6,
        "tb": 13,
    }
    _insert_season_row(
        connection,
        season=season,
        player_id=1,
        games=4,
        raw_source_file="season.csv",
        **totals,
    )
    connection.commit()


def _seed_schedule_rows(connection, tmp_path: Path) -> None:
    csv_path = tmp_path / "team_schedule.csv"
    csv_path.write_text(
        "\n".join(
            [
                "game_id,season,league_name,division_name,week_label,game_date,game_time,team_name,opponent_name,home_away,location_or_field,status,completed_flag,is_bye,result,runs_for,runs_against,notes,source",
                "g1,Maple Tree Spring 2026,Wed,Blue,Week 1,2026-04-01,7:00 PM,Maple Tree,Eagles,home,Boncosky,completed,1,0,W,12,8,,seed.csv",
                "g2,Maple Tree Spring 2026,Wed,Blue,Week 2,2026-04-08,7:00 PM,Maple Tree,Hawks,away,Other,completed,1,0,L,5,9,,seed.csv",
                "g3,Maple Tree Spring 2026,Wed,Blue,Week 3,2026-04-15,7:00 PM,Maple Tree,Eagles,home,Boncosky,completed,1,0,W,14,7,,seed.csv",
                "g4,Maple Tree Spring 2026,Wed,Blue,Week 4,2026-04-22,7:00 PM,Maple Tree,Eagles,away,Other,completed,1,0,W,10,6,,seed.csv",
                "g5,Maple Tree Spring 2026,Wed,Blue,Week 5,2026-04-29,7:00 PM,Maple Tree,Eagles,home,Boncosky,scheduled,0,0,,,,Upcoming,seed.csv",
            ]
        ),
        encoding="utf-8",
    )
    import_schedule_csv(connection, csv_path)


def test_recent_form_returns_last_n_games_with_baseline_delta(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "form.sqlite")
    try:
        initialize_database(connection)
        _seed_player_with_games(connection)

        form = fetch_player_recent_form(connection, "jane smith", window=2, season="Maple Tree Spring 2026")
    finally:
        connection.close()

    assert form["window"] == 2
    assert form["games_available"] == 4
    recent = form["recent"]
    # Games 3 and 4 combined: pa=8, ab=6, hits=5 (1 single + 2 hr + 1 single + 1 double),
    # walks=2, tb = 1+8+1+2 = 12
    assert recent["pa"] == 8
    assert recent["ab"] == 6
    assert recent["hits"] == 5
    assert recent["bb"] == 2
    assert recent["tb"] == 12
    baseline = form["baseline"]
    # Season baseline mirrors all 4 games
    assert baseline["pa"] == 14
    # OPS over the recent window should be (much) higher than the season baseline
    assert recent["ops"] > baseline["ops"]
    assert form["deltas"]["ops_delta"] > 0
    assert form["trend"] in {"hot", "steady"}


def test_recent_form_falls_back_to_all_games_when_window_too_large(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "form_all.sqlite")
    try:
        initialize_database(connection)
        _seed_player_with_games(connection)
        form = fetch_player_recent_form(connection, "jane smith", window=50, season="Maple Tree Spring 2026")
    finally:
        connection.close()

    assert form["window"] == 4
    # When window == total games, recent matches baseline → tiny delta
    assert abs(form["deltas"]["ops_delta"]) < 1e-6
    assert form["trend"] == "steady"


def test_player_vs_opponent_aggregates_only_matching_games(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "h2h.sqlite")
    try:
        initialize_database(connection)
        _seed_player_with_games(connection)
        result = fetch_player_vs_opponent(connection, "jane smith", opponent="Eagles")
    finally:
        connection.close()

    # Games 1, 3, 4 are vs Eagles
    assert result["games_played"] == 3
    totals = result["totals"]
    # Combined: pa=11, ab=9, hits=5 (game 1: 0, game 3: 1B+2HR=3, game 4: 1B+2B=2)
    # walks=2, tb = 0 + 9 + 3 = 12
    assert totals["pa"] == 11
    assert totals["ab"] == 9
    assert totals["hits"] == 5
    assert totals["bb"] == 2
    assert totals["tb"] == 12
    assert not result["recent_games"].empty


def test_team_vs_opponent_uses_schedule_results(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "team_h2h.sqlite")
    try:
        initialize_database(connection)
        _seed_schedule_rows(connection, tmp_path)
        result = fetch_team_vs_opponent(connection, opponent="Eagles")
    finally:
        connection.close()

    # Three completed games vs Eagles (W, W, W); the upcoming one is not counted as completed
    assert result["games_played"] == 3
    assert result["wins"] == 3
    assert result["losses"] == 0
    assert result["runs_for_total"] == 12 + 14 + 10
    assert result["runs_against_total"] == 8 + 7 + 6
    assert not result["recent_meetings"].empty


def test_team_recent_form_window(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "team_form.sqlite")
    try:
        initialize_database(connection)
        _seed_schedule_rows(connection, tmp_path)
        form = fetch_team_recent_form(
            connection,
            season="Maple Tree Spring 2026",
            window=3,
        )
    finally:
        connection.close()

    # 4 completed games total; window 3 should grab the most recent 3 (g4 W, g3 W, g2 L)
    assert form["games_played"] == 3
    assert form["wins"] == 2
    assert form["losses"] == 1
    # Sorted most-recent first
    most_recent = form["recent"].iloc[0]
    assert most_recent["game_date"] == "2026-04-22"


def test_pregame_hot_bats_flags_recent_riser(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "hot_bats.sqlite")
    try:
        initialize_database(connection)
        # Active roster: Jane is on it
        _seed_player_with_games(connection)
        connection.execute(
            "INSERT INTO season_rosters (season_name, player_id, source_name, active_flag, notes) VALUES (?, ?, ?, 1, NULL)",
            ("Maple Tree Spring 2026", 1, "Jane Smith"),
        )
        connection.commit()
        hot_bats = fetch_pregame_hot_bats(
            connection,
            season="Maple Tree Spring 2026",
            window=2,
            min_recent_pa=2,
            limit=5,
        )
    finally:
        connection.close()

    assert not hot_bats.empty
    top = hot_bats.iloc[0]
    assert top["canonical_name"] == "jane smith"
    assert top["recent_pa"] >= 2
    assert top["ops_delta"] > 0
