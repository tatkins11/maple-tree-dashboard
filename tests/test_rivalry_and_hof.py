"""Tests for the Rivalry Ledger and Single-Game Hall of Fame data layer.

Both pages treat every team-name era (Soviet Sluggers, Smoking Bunts, Maple Tree
Tappers, Maple Tree) as one franchise and read the ``games`` / ``player_game_batting``
tables directly, so these tests seed a small multi-era franchise and assert the
franchise-wide aggregations.
"""
from __future__ import annotations

from pathlib import Path

from src.dashboard.data import (
    fetch_franchise_opponent_ledger,
    fetch_franchise_opponents,
    fetch_franchise_vs_opponent,
    fetch_single_game_feats,
    fetch_single_game_score_leaders,
)
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
    team_name: str,
    team_score: int,
    opponent_score: int,
    game_time: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO games (
            game_id, team_name, game_date, game_time, opponent_name,
            team_score, opponent_score, source_file, season, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '')
        """,
        (
            game_id,
            team_name,
            game_date,
            game_time,
            opponent,
            team_score,
            opponent_score,
            f"game_{game_id}.png",
            season,
        ),
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
    rbi: int = 0,
    runs: int = 0,
) -> None:
    connection.execute(
        """
        INSERT INTO player_game_batting (
            game_id, player_id, lineup_spot, plate_appearances, at_bats, singles, doubles, triples,
            home_runs, walks, strikeouts, sacrifice_flies, fielder_choice, double_plays, outs,
            runs, rbi, raw_scorebook_file
        ) VALUES (?, ?, 3, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)
        """,
        (game_id, player_id, pa, ab, singles, doubles, triples, hr, walks, runs, rbi, f"game_{game_id}.png"),
    )


def _seed_franchise(connection) -> None:
    _insert_player(connection, 1, "Tristan", "tristan")
    _insert_player(connection, 2, "Glove", "glove")
    _insert_player(connection, 3, "Joey", "snaxx")  # canonical 'snaxx' displays 'Joey'

    # g1: 2021 era, win vs Refuse To Win; Tristan goes 3-HR / 13 TB / 3R / 6RBI
    _insert_game(connection, game_id=1, game_date="2021-06-23", opponent="Refuse To Win",
                 season="Soviet Sluggers Summer 2021", team_name="Soviet Sluggers", team_score=18, opponent_score=12)
    _insert_player_game(connection, game_id=1, player_id=1, pa=4, ab=4, singles=1, hr=3, runs=3, rbi=6)

    # g2: 2025 era, loss vs Yard Goats; Glove goes 5-hit / 3-HR / 14 TB / 3R / 8RBI
    _insert_game(connection, game_id=2, game_date="2025-10-01", opponent="Yard Goats",
                 season="Maple Tree Fall 2025", team_name="Maple Tree", team_score=21, opponent_score=29)
    _insert_player_game(connection, game_id=2, player_id=2, pa=5, ab=5, singles=2, hr=3, runs=3, rbi=8)

    # g3: 2026 era, loss vs 'no dice' (lowercase); Joey quiet 2-for-3
    _insert_game(connection, game_id=3, game_date="2026-06-03", opponent="no dice",
                 season="Maple Tree Spring 2026", team_name="Maple Tree", team_score=5, opponent_score=17)
    _insert_player_game(connection, game_id=3, player_id=3, pa=3, ab=3, singles=2)

    # g4: 2022 era, loss vs 'No Dice' (different casing -> must merge with g3)
    _insert_game(connection, game_id=4, game_date="2022-06-29", opponent="No Dice",
                 season="Smoking Bunts Summer 2022", team_name="Smoking Bunts", team_score=9, opponent_score=11)
    _insert_player_game(connection, game_id=4, player_id=1, pa=3, ab=3, singles=1)

    # g5: tie vs Foes (exercises explicit tie counting)
    _insert_game(connection, game_id=5, game_date="2026-05-20", opponent="Foes",
                 season="Maple Tree Spring 2026", team_name="Maple Tree", team_score=10, opponent_score=10)
    _insert_player_game(connection, game_id=5, player_id=2, pa=3, ab=3, singles=1)
    connection.commit()


def test_franchise_opponent_ledger_merges_eras_and_casing(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "ledger.sqlite")
    try:
        initialize_database(connection)
        _seed_franchise(connection)
        ledger = fetch_franchise_opponent_ledger(connection)
    finally:
        connection.close()

    # Four distinct opponents (No Dice merged across casing + era), five games total.
    assert int(ledger["games"].sum()) == 5
    assert len(ledger) == 4

    no_dice = ledger[ledger["opponent"] == "No Dice"].iloc[0]
    assert int(no_dice["games"]) == 2  # 'no dice' + 'No Dice' merged via LOWER()
    assert int(no_dice["wins"]) == 0
    assert int(no_dice["losses"]) == 2
    assert int(no_dice["ties"]) == 0
    assert int(no_dice["runs_for"]) == 14  # 5 + 9
    assert int(no_dice["runs_against"]) == 28  # 17 + 11
    assert int(no_dice["run_diff"]) == -14
    assert no_dice["win_pct"] == 0.0

    foes = ledger[ledger["opponent"] == "Foes"].iloc[0]
    assert int(foes["ties"]) == 1  # tie counted explicitly, not folded into losses
    assert int(foes["losses"]) == 0
    assert int(foes["run_diff"]) == 0


def test_franchise_opponents_orders_by_games_then_name(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "opponents.sqlite")
    try:
        initialize_database(connection)
        _seed_franchise(connection)
        opponents = fetch_franchise_opponents(connection)
    finally:
        connection.close()

    # Most-played first (No Dice, 2 games), then the single-game rivals alphabetically.
    assert opponents[0] == "No Dice"
    assert opponents == ["No Dice", "Foes", "Refuse To Win", "Yard Goats"]


def test_franchise_vs_opponent_is_case_insensitive_and_spans_eras(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "vs.sqlite")
    try:
        initialize_database(connection)
        _seed_franchise(connection)
        detail = fetch_franchise_vs_opponent(connection, opponent="NO DICE")  # mixed case input
    finally:
        connection.close()

    assert detail["opponent"] == "No Dice"
    assert detail["games"] == 2
    assert (detail["wins"], detail["losses"], detail["ties"]) == (0, 2, 0)
    assert detail["run_diff"] == -14
    assert detail["first_played"] == "2022-06-29"
    assert detail["last_played"] == "2026-06-03"

    meetings = detail["meetings"]
    # Newest meeting first.
    assert meetings.iloc[0]["game_date"] == "2026-06-03"
    # Both franchise eras represented.
    assert set(meetings["era"]) == {"Maple Tree", "Smoking Bunts"}


def test_single_game_feats_thresholds_and_identity(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "feats.sqlite")
    try:
        initialize_database(connection)
        _seed_franchise(connection)
        feats = fetch_single_game_feats(connection)
    finally:
        connection.close()

    five_hit = feats["5-Hit Games"]
    assert len(five_hit) == 1
    assert five_hit.iloc[0]["player"] == "Glove"
    assert int(five_hit.iloc[0]["hits"]) == 5
    assert int(five_hit.iloc[0]["tb"]) == 14

    three_hr = feats["3-HR Games"]
    assert len(three_hr) == 2
    # Sorted by HR then TB desc -> Glove (14 TB) ahead of Tristan (13 TB).
    assert list(three_hr["player"]) == ["Glove", "Tristan"]

    big_tb = feats["11+ Total Base Games"]
    assert set(big_tb["player"]) == {"Glove", "Tristan"}
    assert int(big_tb["tb"].max()) == 14

    # Quiet 2-for-3 (Joey) is not a feat anywhere.
    for board in feats.values():
        assert "Joey" not in set(board["player"])


def test_single_game_score_leaders_rank_by_linear_weights(tmp_path: Path) -> None:
    from src.dashboard.data import GAME_SCORE_CONTEXT_WEIGHT
    from src.models.advanced_analytics import LINEAR_WEIGHTS as W

    connection = connect_db(tmp_path / "gamescore.sqlite")
    try:
        initialize_database(connection)
        _seed_franchise(connection)
        leaders = fetch_single_game_score_leaders(connection, limit=10)
    finally:
        connection.close()

    w = GAME_SCORE_CONTEXT_WEIGHT
    # Glove: 2x1B + 3xHR batting value, plus 0.20 * (3 R + 8 RBI) context bonus.
    expected_glove = W["1b"] * 2 + W["hr"] * 3 + w * (3 + 8)
    # Tristan: 1x1B + 3xHR, plus 0.20 * (3 R + 6 RBI).
    expected_tristan = W["1b"] * 1 + W["hr"] * 3 + w * (3 + 6)

    assert leaders.iloc[0]["player"] == "Glove"
    assert round(float(leaders.iloc[0]["game_score"]), 2) == round(expected_glove, 2)
    assert leaders.iloc[1]["player"] == "Tristan"
    assert round(float(leaders.iloc[1]["game_score"]), 2) == round(expected_tristan, 2)
    # The context bonus lifts the score above the pure batting-event value.
    assert expected_glove > W["1b"] * 2 + W["hr"] * 3
    # Score is monotonically non-increasing (sorted best-first).
    scores = [float(v) for v in leaders["game_score"]]
    assert scores == sorted(scores, reverse=True)
    # Identity column is present for player links.
    assert "canonical_name" in leaders.columns
    # Columns needed to render the full box-score line are present.
    assert {"ab", "r", "rbi", "1b", "2b", "3b", "hr", "bb"}.issubset(leaders.columns)


def test_single_game_score_leaders_ascending_is_worst_first(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "worst.sqlite")
    try:
        initialize_database(connection)
        _seed_franchise(connection)
        best = fetch_single_game_score_leaders(connection, limit=20)
        worst = fetch_single_game_score_leaders(connection, limit=20, ascending=True)
    finally:
        connection.close()

    worst_scores = [float(v) for v in worst["game_score"]]
    # Ascending order, worst game first.
    assert worst_scores == sorted(worst_scores)
    # With the full set in view, worst-first is exactly best-first reversed.
    assert worst_scores == list(reversed([float(v) for v in best["game_score"]]))


def test_game_score_penalizes_at_bat_outs(tmp_path: Path) -> None:
    """A 4-for-4 should edge a 4-for-5 with the same hits by exactly the out weight."""
    from src.dashboard.data import GAME_SCORE_OUT_WEIGHT

    connection = connect_db(tmp_path / "outs.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Efficient", "efficient")
        _insert_player(connection, 2, "Hacker", "hacker")
        # Same game, same 4 singles / no HR / no R / no RBI — only the extra out differs.
        _insert_game(connection, game_id=1, game_date="2026-05-01", opponent="Foes",
                     season="Maple Tree Spring 2026", team_name="Maple Tree", team_score=10, opponent_score=9)
        _insert_player_game(connection, game_id=1, player_id=1, pa=4, ab=4, singles=4)  # 4-for-4, 0 outs
        _insert_player_game(connection, game_id=1, player_id=2, pa=5, ab=5, singles=4)  # 4-for-5, 1 out
        connection.commit()
        leaders = fetch_single_game_score_leaders(connection, limit=10)
    finally:
        connection.close()

    efficient = leaders[leaders["player"] == "Efficient"].iloc[0]["game_score"]
    hacker = leaders[leaders["player"] == "Hacker"].iloc[0]["game_score"]
    # The 4-for-4 ranks ahead, and the gap is exactly one out's penalty.
    assert leaders.iloc[0]["player"] == "Efficient"
    assert round(float(efficient) - float(hacker), 2) == round(abs(GAME_SCORE_OUT_WEIGHT), 2)


def test_franchise_helpers_handle_empty_database(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "empty.sqlite")
    try:
        initialize_database(connection)
        ledger = fetch_franchise_opponent_ledger(connection)
        opponents = fetch_franchise_opponents(connection)
        detail = fetch_franchise_vs_opponent(connection, opponent="No Dice")
        feats = fetch_single_game_feats(connection)
    finally:
        connection.close()

    # The games-table-backed helpers are genuinely empty with no games loaded.
    assert ledger.empty
    assert opponents == []
    assert detail["games"] == 0
    assert detail["meetings"].empty
    # fetch_single_game_feats delegates to fetch_single_game_stats, which falls back
    # to the on-disk box-score CSV when player_game_batting is empty; just assert the
    # board structure is intact rather than emptiness.
    assert set(feats.keys()) == {"5-Hit Games", "3-HR Games", "11+ Total Base Games"}
