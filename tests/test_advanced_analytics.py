from pathlib import Path

import pandas as pd

from src.dashboard.data import (
    fetch_advanced_analytics_archetype_summary,
    fetch_advanced_analytics_view,
    fetch_advanced_archetype_order,
    fetch_advanced_methodology_summary,
)
from src.models.advanced_analytics import calculate_advanced_analytics
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
    strikeouts: int,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?)
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
            strikeouts,
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


def test_calculate_advanced_analytics_core_metrics() -> None:
    dataframe = pd.DataFrame(
        [
            {
                "player": "A",
                "pa": 12,
                "ab": 10,
                "hits": 6,
                "1b": 3,
                "2b": 1,
                "3b": 1,
                "hr": 1,
                "bb": 2,
                "so": 1,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 1,
                "fc": 1,
                "gidp": 0,
                "r": 5,
                "rbi": 6,
                "tb": 12,
                "two_out_rbi": 1,
                "lob": 2,
                "ba_risp": 0.5,
            }
        ]
    )

    analytics, metadata = calculate_advanced_analytics(
        dataframe,
        mode="Season",
        comparison_group_label="Test Season",
    )

    row = analytics.iloc[0]
    assert row["iso"] == 0.6
    assert row["xbh_rate"] == 0.25
    assert row["hr_rate"] == 1 / 12
    assert row["tb_per_pa"] == 1.0
    assert row["non_out_rate"] == 0.75
    assert row["walk_rate"] == 2 / 12
    assert row["runs_per_on_base_event"] == 0.5
    assert row["team_relative_obp"] == 100.0
    assert row["team_relative_slg"] == 100.0
    assert row["team_relative_ops"] == 100.0
    assert round(row["raa"], 6) == 0.0
    assert round(row["rar"], 6) == round((0.75 - 0.6) * 12, 6)
    assert round(row["owar"], 6) == round(row["rar"] / metadata.runs_per_win, 6)


def test_calculate_advanced_analytics_handles_zero_denominators() -> None:
    dataframe = pd.DataFrame(
        [
            {
                "player": "Zero",
                "pa": 0,
                "ab": 0,
                "hits": 0,
                "1b": 0,
                "2b": 0,
                "3b": 0,
                "hr": 0,
                "bb": 0,
                "so": 0,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 0,
                "fc": 0,
                "gidp": 0,
                "r": 0,
                "rbi": 0,
                "tb": 0,
                "two_out_rbi": 0,
                "lob": 0,
                "ba_risp": 0.0,
            }
        ]
    )

    analytics, _ = calculate_advanced_analytics(
        dataframe,
        mode="Season",
        comparison_group_label="Zero",
    )

    row = analytics.iloc[0]
    assert row["iso"] == 0.0
    assert row["obp"] == 0.0
    assert row["tb_per_pa"] == 0.0
    assert row["walk_rate"] == 0.0
    assert row["owar"] == 0.0


def test_calculate_advanced_analytics_assigns_expected_archetypes() -> None:
    dataframe = pd.DataFrame(
        [
            {
                "player": "Slugger",
                "pa": 40,
                "ab": 38,
                "hits": 20,
                "1b": 5,
                "2b": 5,
                "3b": 1,
                "hr": 9,
                "bb": 2,
                "so": 1,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 0,
                "fc": 0,
                "gidp": 0,
                "r": 18,
                "rbi": 20,
                "tb": 48,
                "two_out_rbi": 4,
                "lob": 3,
                "ba_risp": 0.0,
            },
            {
                "player": "Setter",
                "pa": 50,
                "ab": 42,
                "hits": 18,
                "1b": 16,
                "2b": 2,
                "3b": 0,
                "hr": 0,
                "bb": 6,
                "so": 2,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 2,
                "fc": 0,
                "gidp": 0,
                "r": 14,
                "rbi": 7,
                "tb": 20,
                "two_out_rbi": 1,
                "lob": 4,
                "ba_risp": 0.0,
            },
            {
                "player": "Weak",
                "pa": 40,
                "ab": 38,
                "hits": 10,
                "1b": 9,
                "2b": 1,
                "3b": 0,
                "hr": 0,
                "bb": 1,
                "so": 6,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 0,
                "fc": 1,
                "gidp": 1,
                "r": 5,
                "rbi": 4,
                "tb": 11,
                "two_out_rbi": 0,
                "lob": 6,
                "ba_risp": 0.0,
            },
                {
                    "player": "Balanced",
                    "pa": 44,
                    "ab": 40,
                    "hits": 18,
                    "1b": 12,
                    "2b": 3,
                    "3b": 1,
                    "hr": 2,
                    "bb": 4,
                    "so": 2,
                    "hbp": 0,
                    "sac": 0,
                    "sf": 0,
                    "roe": 0,
                    "fc": 0,
                    "gidp": 0,
                    "r": 10,
                    "rbi": 8,
                    "tb": 29,
                    "two_out_rbi": 2,
                    "lob": 4,
                    "ba_risp": 0.0,
                },
        ]
    )

    analytics, _ = calculate_advanced_analytics(
        dataframe,
        mode="Season",
        comparison_group_label="Archetypes",
    )
    archetypes = dict(zip(analytics["player"], analytics["archetype"]))
    assert archetypes["Slugger"] == "HR Threat"
    assert archetypes["Setter"] in {"Table Setter", "Low-Damage OBP Bat", "Balanced Bat"}
    assert archetypes["Balanced"] in {"Balanced Bat", "Table Setter"}
    assert archetypes["Weak"] == "Bottom-Order Bat"


def test_advanced_helpers_return_methodology_and_sorted_archetypes() -> None:
    dataframe = pd.DataFrame(
        [
            {
                "player": "One",
                "pa": 50,
                "ab": 45,
                "hits": 20,
                "1b": 12,
                "2b": 4,
                "3b": 0,
                "hr": 4,
                "bb": 5,
                "so": 2,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 0,
                "fc": 0,
                "gidp": 0,
                "r": 15,
                "rbi": 18,
                "tb": 36,
                "two_out_rbi": 2,
                "lob": 3,
                "ba_risp": 0.0,
            },
            {
                "player": "Two",
                "pa": 45,
                "ab": 42,
                "hits": 12,
                "1b": 10,
                "2b": 2,
                "3b": 0,
                "hr": 0,
                "bb": 3,
                "so": 4,
                "hbp": 0,
                "sac": 0,
                "sf": 0,
                "roe": 0,
                "fc": 1,
                "gidp": 0,
                "r": 7,
                "rbi": 5,
                "tb": 14,
                "two_out_rbi": 0,
                "lob": 5,
                "ba_risp": 0.0,
            },
        ]
    )

    analytics, metadata = calculate_advanced_analytics(
        dataframe,
        mode="Season",
        comparison_group_label="Helper Test",
    )
    summary = fetch_advanced_methodology_summary(metadata)
    archetype_summary = fetch_advanced_analytics_archetype_summary(analytics)

    assert summary["Comparison group"] == "Helper Test"
    assert summary["Runs per win"] == "10.0"
    assert fetch_advanced_archetype_order()[0] == "HR Threat"
    assert list(archetype_summary["archetype"]) == sorted(
        archetype_summary["archetype"],
        key=lambda value: fetch_advanced_archetype_order().index(value),
    )


def test_fetch_advanced_analytics_view_respects_canonical_identity(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "advanced.sqlite")
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
            strikeouts=1,
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
            strikeouts=2,
            runs=19,
            rbi=28,
            tb=74,
            raw_source_file="2022.csv",
        )
        connection.commit()

        analytics, metadata = fetch_advanced_analytics_view(
            connection,
            view_mode="Career",
            selected_seasons=None,
            min_pa=0,
            active_only=False,
        )
    finally:
        connection.close()

    assert len(analytics) == 1
    row = analytics.iloc[0]
    assert row["player"] == "Tristan"
    assert row["pa"] == 125
    assert row["hr"] == 18
    assert metadata.baseline_player_count == 1
