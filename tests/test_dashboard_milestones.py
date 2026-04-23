from pathlib import Path

import pandas as pd

from src.dashboard.data import (
    calculate_next_milestone_state,
    fetch_career_milestones,
    fetch_passed_milestones_summary,
    select_first_to_milestones,
    select_in_play_milestones,
)
from src.models.roster import DEFAULT_ACTIVE_ROSTER_SEASON
from src.utils.db import connect_db, initialize_database


def _insert_player(connection, player_id: int, player_name: str, canonical_name: str) -> None:
    connection.execute(
        "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )
    connection.execute(
        "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
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


def test_calculate_next_milestone_state_skips_exactly_cleared_milestone() -> None:
    state = calculate_next_milestone_state(50, (25, 50, 75, 100))

    assert state["highest_cleared_milestone"] == 50
    assert state["next_milestone"] == 75
    assert state["remaining"] == 25
    assert state["progress_to_next"] == 50 / 75


def test_calculate_next_milestone_state_handles_all_cleared() -> None:
    state = calculate_next_milestone_state(45, (5, 10, 15, 20, 25, 30, 40))

    assert state["next_milestone"] is None
    assert state["remaining"] is None
    assert state["status"] == "All listed milestones cleared"


def test_calculate_next_milestone_state_uses_total_vs_next_target_progress() -> None:
    state = calculate_next_milestone_state(11, (5, 10, 15, 20))

    assert state["highest_cleared_milestone"] == 10
    assert state["next_milestone"] == 15
    assert state["progress_to_next"] == 11 / 15


def test_fetch_career_milestones_respects_canonical_identity_and_aliases(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "milestones.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        connection.execute(
            """
            INSERT INTO player_aliases (
                player_id, source_name, normalized_source_name, source_type, source_file, match_method, approved_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "Teo", "teo", "manual_override", "aliases.csv", "manual_override", 1),
        )
        _insert_season_row(
            connection,
            season="Soviet Sluggers Summer 2021",
            player_id=1,
            games=18,
            pa=63,
            ab=58,
            hits=36,
            singles=18,
            doubles=8,
            triples=2,
            hr=8,
            walks=5,
            runs=20,
            rbi=25,
            tb=72,
            raw_source_file="2021.csv",
        )
        _insert_season_row(
            connection,
            season="Smoking Bunts Summer 2022",
            player_id=1,
            games=17,
            pa=62,
            ab=58,
            hits=34,
            singles=15,
            doubles=8,
            triples=1,
            hr=10,
            walks=4,
            runs=19,
            rbi=28,
            tb=74,
            raw_source_file="2022.csv",
        )
        connection.commit()

        milestones = fetch_career_milestones(connection, categories=["HR"], sort_by="player name")
    finally:
        connection.close()

    assert len(milestones) == 1
    row = milestones.iloc[0]
    assert row["player"] == "Tristan"
    assert row["current_total"] == 18
    assert row["next_milestone"] == 20
    assert row["remaining"] == 2
    assert row["club_size"] == 0
    assert row["club_label"] == "First to 20"


def test_fetch_career_milestones_filters_active_roster(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "milestones.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Glove", "glove")
        _insert_player(connection, 2, "Stevie", "stevie")
        connection.execute(
            "INSERT INTO season_rosters (season_name, player_id, source_name, active_flag, notes) VALUES (?, ?, ?, 1, '')",
            (DEFAULT_ACTIVE_ROSTER_SEASON, 1, "Glove"),
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=11,
            pa=49,
            ab=43,
            hits=27,
            singles=15,
            doubles=4,
            triples=1,
            hr=7,
            walks=4,
            runs=30,
            rbi=31,
            tb=54,
            raw_source_file="glove.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=2,
            games=11,
            pa=41,
            ab=39,
            hits=15,
            singles=10,
            doubles=3,
            triples=1,
            hr=1,
            walks=2,
            runs=10,
            rbi=12,
            tb=23,
            raw_source_file="stevie.csv",
        )
        connection.commit()

        milestones = fetch_career_milestones(connection, categories=["Hits"], active_only=True)
    finally:
        connection.close()

    assert set(milestones["player"]) == {"Glove"}


def test_fetch_career_milestones_remaining_calculation_is_correct(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "milestones.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Kives", "kives")
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=13,
            pa=49,
            ab=45,
            hits=30,
            singles=20,
            doubles=4,
            triples=1,
            hr=5,
            walks=4,
            runs=19,
            rbi=25,
            tb=51,
            raw_source_file="kives.csv",
        )
        connection.commit()

        milestones = fetch_career_milestones(connection, categories=["Walks"])
    finally:
        connection.close()

    row = milestones.iloc[0]
    assert row["current_total"] == 4
    assert row["next_milestone"] == 10
    assert row["remaining"] == 6
    assert row["urgency"] == "6-10 away"
    assert row["club_label"] == "First to 10"


def test_fetch_passed_milestones_summary_uses_highest_cleared_milestone(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "milestones.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Jj", "jj")
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=13,
            pa=54,
            ab=45,
            hits=28,
            singles=19,
            doubles=7,
            triples=1,
            hr=1,
            walks=9,
            runs=23,
            rbi=14,
            tb=40,
            raw_source_file="jj.csv",
        )
        connection.commit()

        passed = fetch_passed_milestones_summary(connection, categories=["Hits"])
    finally:
        connection.close()

    row = passed.iloc[0]
    assert row["player"] == "Jj"
    assert row["current_total"] == 28
    assert row["highest_cleared_milestone"] == 25


def test_select_in_play_milestones_respects_threshold_and_priority() -> None:
    milestones = [
        {"player": "Corey", "stat": "Runs", "current_total": 49, "next_milestone": 50, "next_milestone_display": 50, "remaining": 1, "urgency": "1 away", "club_size": 2},
        {"player": "Tim", "stat": "HR", "current_total": 13, "next_milestone": 15, "next_milestone_display": 15, "remaining": 2, "urgency": "2-5 away", "club_size": 1},
        {"player": "Duff", "stat": "HR", "current_total": 1, "next_milestone": 5, "next_milestone_display": 5, "remaining": 4, "urgency": "2-5 away", "club_size": 0},
        {"player": "Joel", "stat": "Walks", "current_total": 8, "next_milestone": 10, "next_milestone_display": 10, "remaining": 2, "urgency": "2-5 away", "club_size": 0},
        {"player": "Jason", "stat": "AB", "current_total": 90, "next_milestone": 100, "next_milestone_display": 100, "remaining": 10, "urgency": "6-10 away", "club_size": 3},
        {"player": "Tim", "stat": "Runs", "current_total": 48, "next_milestone": 50, "next_milestone_display": 50, "remaining": 2, "urgency": "2-5 away", "club_size": 0},
    ]

    dataframe = select_in_play_milestones(
        pd.DataFrame(milestones),
        distance_threshold=5,
        limit=10,
    )

    assert list(dataframe["player"]) == ["Corey", "Tim", "Joel", "Duff"]
    assert dataframe["remaining"].max() <= 5
    assert len(dataframe["player"].unique()) == len(dataframe)
    assert dataframe.loc[dataframe["player"] == "Tim", "stat"].iloc[0] == "Runs"


def test_fetch_career_milestones_club_label_counts_existing_members(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "milestones.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Glove", "glove")
        _insert_player(connection, 2, "Jj", "jj")
        _insert_player(connection, 3, "Porter", "porter")
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=13,
            pa=54,
            ab=50,
            hits=30,
            singles=18,
            doubles=6,
            triples=1,
            hr=5,
            walks=4,
            runs=20,
            rbi=22,
            tb=63,
            raw_source_file="glove.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=2,
            games=13,
            pa=53,
            ab=48,
            hits=27,
            singles=17,
            doubles=6,
            triples=1,
            hr=4,
            walks=5,
            runs=19,
            rbi=17,
            tb=57,
            raw_source_file="jj.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=3,
            games=13,
            pa=49,
            ab=46,
            hits=24,
            singles=14,
            doubles=5,
            triples=1,
            hr=3,
            walks=3,
            runs=14,
            rbi=16,
            tb=44,
            raw_source_file="porter.csv",
        )
        connection.commit()

        milestones = fetch_career_milestones(connection, categories=["HR"], sort_by="nearest milestone")
    finally:
        connection.close()

    porter_row = milestones[milestones["player"] == "Porter"].iloc[0]
    assert porter_row["next_milestone"] == 5
    assert porter_row["club_size"] == 1
    assert porter_row["club_label"] == "1 in club"


def test_select_first_to_milestones_returns_only_brand_new_clubs() -> None:
    milestones = [
        {
            "player": "Glove",
            "stat": "Singles",
            "current_total": 73,
            "next_milestone": 75,
            "next_milestone_display": 75,
            "remaining": 2,
            "urgency": "2-5 away",
            "club_size": 0,
            "progress_to_next": 73 / 75,
        },
        {
            "player": "Jason",
            "stat": "Walks",
            "current_total": 23,
            "next_milestone": 25,
            "next_milestone_display": 25,
            "remaining": 2,
            "urgency": "2-5 away",
            "club_size": 0,
            "progress_to_next": 23 / 25,
        },
        {
            "player": "Porter",
            "stat": "HR",
            "current_total": 9,
            "next_milestone": 10,
            "next_milestone_display": 10,
            "remaining": 1,
            "urgency": "1 away",
            "club_size": 4,
            "progress_to_next": 9 / 10,
        },
        {
            "player": "Joel",
            "stat": "Games",
            "current_total": 24,
            "next_milestone": 25,
            "next_milestone_display": 25,
            "remaining": 1,
            "urgency": "1 away",
            "club_size": 12,
            "progress_to_next": 24 / 25,
        },
        {
            "player": "Kives",
            "stat": "Triples",
            "current_total": 9,
            "next_milestone": 10,
            "next_milestone_display": 10,
            "remaining": 1,
            "urgency": "1 away",
            "club_size": 0,
            "progress_to_next": 9 / 10,
        },
        {
            "player": "Duff",
            "stat": "Triples",
            "current_total": 11,
            "next_milestone": 15,
            "next_milestone_display": 15,
            "remaining": 4,
            "urgency": "2-5 away",
            "club_size": 0,
            "progress_to_next": 11 / 15,
        },
    ]

    dataframe = select_first_to_milestones(
        pd.DataFrame(milestones),
        progress_threshold=0.85,
        max_remaining=10,
        limit=10,
    )

    assert list(dataframe["player"]) == ["Kives", "Glove", "Jason"]
    assert dataframe["club_size"].eq(0).all()
    assert dataframe["remaining"].max() <= 10
    assert "Duff" not in set(dataframe["player"])
