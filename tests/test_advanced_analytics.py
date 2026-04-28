from pathlib import Path

import pandas as pd

from src.dashboard.data import (
    fetch_advanced_analytics_archetype_summary,
    fetch_advanced_analytics_view,
    fetch_advanced_archetype_order,
    fetch_career_milestones,
    fetch_career_leader_snapshot,
    fetch_career_stats,
    fetch_career_summary,
    fetch_current_season_leader_snapshot,
    fetch_current_season_stats,
    fetch_advanced_methodology_summary,
    fetch_passed_milestones_summary,
    fetch_player_advanced_history,
    fetch_player_milestone_context,
    fetch_player_profile_summary,
    fetch_player_record_context,
    fetch_player_season_history,
    fetch_team_summary,
    fetch_top_hitters,
    format_player_season_label,
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


def test_current_season_stats_helpers_support_team_facing_page(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "current_stats.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        _insert_player(connection, 2, "Jj", "jj")
        _insert_player(connection, 3, "Glove", "glove")

        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=1,
            games=2,
            pa=8,
            ab=5,
            hits=4,
            singles=1,
            doubles=0,
            triples=0,
            hr=3,
            walks=2,
            strikeouts=0,
            runs=5,
            rbi=6,
            tb=13,
            raw_source_file="tristan.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=2,
            games=2,
            pa=8,
            ab=6,
            hits=4,
            singles=2,
            doubles=0,
            triples=1,
            hr=1,
            walks=2,
            strikeouts=0,
            runs=3,
            rbi=3,
            tb=9,
            raw_source_file="jj.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=3,
            games=2,
            pa=8,
            ab=8,
            hits=5,
            singles=4,
            doubles=0,
            triples=0,
            hr=1,
            walks=0,
            strikeouts=0,
            runs=5,
            rbi=5,
            tb=8,
            raw_source_file="glove.csv",
        )
        connection.commit()

        stats = fetch_current_season_stats(connection, "Maple Tree Spring 2026")
        summary = fetch_team_summary(connection, "Maple Tree Spring 2026")
        leaders = fetch_current_season_leader_snapshot(connection, "Maple Tree Spring 2026")
        advanced, _ = fetch_advanced_analytics_view(
            connection,
            view_mode="Season",
            selected_season="Maple Tree Spring 2026",
            min_pa=0,
            active_only=False,
        )
    finally:
        connection.close()

    assert "canonical_name" in stats.columns
    assert list(stats["player"]) == ["Tristan", "Jj", "Glove"]
    assert summary["runs"] == 13
    assert summary["home_runs"] == 5
    assert round(summary["ops"], 3) == 2.318
    assert leaders["ops_leader"] == "Tristan (OPS 3.457)"
    assert leaders["hr_leader"] == "Tristan (HR 3)"
    assert leaders["rbi_leader"] == "Tristan (RBI 6)"
    assert leaders["avg_leader"] == "Tristan (AVG 0.800)"
    assert set(["player", "iso", "xbh_rate", "hr_rate", "tb_per_pa", "team_relative_ops", "rar", "owar", "archetype"]).issubset(
        advanced.columns
    )


def test_career_stats_helpers_support_team_facing_page(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "career_stats.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        _insert_player(connection, 2, "Jj", "jj")
        _insert_player(connection, 3, "Glove", "glove")

        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=1,
            games=2,
            pa=8,
            ab=5,
            hits=4,
            singles=1,
            doubles=0,
            triples=0,
            hr=3,
            walks=2,
            strikeouts=0,
            runs=5,
            rbi=6,
            tb=13,
            raw_source_file="tristan-2026.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=4,
            pa=15,
            ab=12,
            hits=7,
            singles=3,
            doubles=2,
            triples=0,
            hr=2,
            walks=3,
            strikeouts=1,
            runs=6,
            rbi=7,
            tb=15,
            raw_source_file="tristan-2025.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=2,
            games=2,
            pa=8,
            ab=6,
            hits=4,
            singles=2,
            doubles=0,
            triples=1,
            hr=1,
            walks=2,
            strikeouts=0,
            runs=3,
            rbi=3,
            tb=9,
            raw_source_file="jj-2026.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=2,
            games=3,
            pa=8,
            ab=7,
            hits=3,
            singles=2,
            doubles=1,
            triples=0,
            hr=0,
            walks=1,
            strikeouts=1,
            runs=2,
            rbi=2,
            tb=4,
            raw_source_file="jj-2025.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=3,
            games=2,
            pa=5,
            ab=5,
            hits=1,
            singles=1,
            doubles=0,
            triples=0,
            hr=0,
            walks=0,
            strikeouts=1,
            runs=1,
            rbi=0,
            tb=1,
            raw_source_file="glove-2026.csv",
        )
        connection.commit()

        seasons = ["Maple Tree Spring 2026", "Maple Tree Fall 2025"]
        summary = fetch_career_summary(connection, seasons=seasons, min_pa=8)
        leaders = fetch_career_leader_snapshot(connection, seasons=seasons, min_pa=8)
        stats = fetch_career_stats(connection, seasons=seasons, min_pa=8)
        advanced, _ = fetch_advanced_analytics_view(
            connection,
            view_mode="Career",
            selected_seasons=seasons,
            min_pa=8,
            active_only=False,
        )
    finally:
        connection.close()

    assert "canonical_name" in stats.columns
    assert list(stats["player"]) == ["Tristan", "Jj"]
    assert summary["players"] == 2
    assert summary["seasons"] == 2
    assert summary["pa"] == 39
    assert summary["runs"] == 16
    assert summary["home_runs"] == 6
    assert round(summary["avg"], 3) == 0.600
    assert round(summary["obp"], 3) == 0.684
    assert round(summary["slg"], 3) == 1.367
    assert round(summary["ops"], 3) == 2.051
    assert leaders["ops_leader"] == "Tristan (OPS 2.374)"
    assert leaders["hr_leader"] == "Tristan (HR 5)"
    assert leaders["rbi_leader"] == "Tristan (RBI 13)"
    assert leaders["avg_leader"] == "Tristan (AVG 0.647)"
    assert leaders["most_seasons"] == "Tristan (Seasons 2)"
    assert set(["player", "iso", "xbh_rate", "hr_rate", "tb_per_pa", "team_relative_ops", "rar", "owar", "archetype"]).issubset(
        advanced.columns
    )


def test_career_helpers_handle_empty_filtered_views(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "career_empty.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=1,
            games=2,
            pa=8,
            ab=5,
            hits=4,
            singles=1,
            doubles=0,
            triples=0,
            hr=3,
            walks=2,
            strikeouts=0,
            runs=5,
            rbi=6,
            tb=13,
            raw_source_file="tristan.csv",
        )
        connection.commit()

        summary = fetch_career_summary(connection, seasons=["Maple Tree Spring 2026"], min_pa=50)
        leaders = fetch_career_leader_snapshot(connection, seasons=["Maple Tree Spring 2026"], min_pa=50)
    finally:
        connection.close()

    assert summary["players"] == 0
    assert summary["seasons"] == 1
    assert summary["pa"] == 0
    assert summary["ops"] == 0.0
    assert leaders["ops_leader"] == ""
    assert leaders["most_seasons"] == ""


def test_all_time_page_standard_table_filters_out_canonical_name() -> None:
    page_path = Path("C:/Slowpitch/slowpitch_optimizer/pages/2_All_Time_Career_Stats.py")
    contents = page_path.read_text(encoding="utf-8")

    assert "STANDARD_CAREER_COLUMNS" in contents
    assert 'column != "canonical_name"' in contents


def test_format_player_season_label_uses_short_suffixes() -> None:
    assert format_player_season_label("Maple Tree Spring 2026") == "2026 Sp"
    assert format_player_season_label("Maple Tree Summer 2025") == "2025 S"
    assert format_player_season_label("Maple Tree Fall 2024") == "2024 F"
    assert format_player_season_label("Maple Tree Winter 2024") == "Maple Tree Winter 2024"


def test_fetch_top_hitters_keeps_canonical_name_for_linking(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "top_hitters_links.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=1,
            games=2,
            pa=8,
            ab=5,
            hits=4,
            singles=1,
            doubles=0,
            triples=0,
            hr=3,
            walks=2,
            strikeouts=0,
            runs=5,
            rbi=6,
            tb=13,
            raw_source_file="tristan.csv",
        )
        connection.commit()

        top_hitters = fetch_top_hitters(connection, "Maple Tree Spring 2026", min_pa=0, limit=5)
    finally:
        connection.close()

    assert "canonical_name" in top_hitters.columns
    assert top_hitters.iloc[0]["canonical_name"] == "tristan"


def test_player_card_helpers_return_summary_and_histories(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "player_card_helpers.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        _insert_player(connection, 2, "Jj", "jj")
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
            season="Maple Tree Fall 2025",
            player_id=1,
            games=10,
            pa=28,
            ab=24,
            hits=14,
            singles=8,
            doubles=3,
            triples=1,
            hr=2,
            walks=4,
            strikeouts=1,
            runs=10,
            rbi=11,
            tb=25,
            raw_source_file="fall.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=1,
            games=8,
            pa=24,
            ab=20,
            hits=12,
            singles=6,
            doubles=2,
            triples=1,
            hr=3,
            walks=4,
            strikeouts=0,
            runs=9,
            rbi=10,
            tb=25,
            raw_source_file="spring.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=2,
            games=8,
            pa=22,
            ab=20,
            hits=8,
            singles=6,
            doubles=1,
            triples=0,
            hr=1,
            walks=2,
            strikeouts=1,
            runs=5,
            rbi=4,
            tb=12,
            raw_source_file="jj.csv",
        )
        connection.commit()

        summary = fetch_player_profile_summary(connection, "tristan")
        season_history = fetch_player_season_history(connection, "tristan")
        advanced_history = fetch_player_advanced_history(connection, "tristan")
    finally:
        connection.close()

    assert summary is not None
    assert summary["player"] == "Tristan"
    assert summary["canonical_name"] == "tristan"
    assert summary["seasons_played"] == 2
    assert summary["pa"] == 52
    assert summary["hits"] == 26
    assert summary["hr"] == 5
    assert "Teo" in summary["aliases"]
    assert set(season_history["season_label"]) == {"2025 F", "2026 Sp"}
    assert set(advanced_history["season_label"]) == {"2025 F", "2026 Sp"}
    assert {"iso", "team_relative_ops", "rar", "owar", "archetype"}.issubset(advanced_history.columns)


def test_player_card_helpers_filter_milestones_and_records_to_one_player(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "player_card_context.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Tristan", "tristan")
        _insert_player(connection, 2, "Jj", "jj")
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=10,
            pa=30,
            ab=26,
            hits=18,
            singles=9,
            doubles=4,
            triples=1,
            hr=4,
            walks=4,
            strikeouts=1,
            runs=12,
            rbi=14,
            tb=36,
            raw_source_file="fall.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=1,
            games=8,
            pa=24,
            ab=20,
            hits=14,
            singles=6,
            doubles=3,
            triples=1,
            hr=4,
            walks=4,
            strikeouts=0,
            runs=10,
            rbi=12,
            tb=31,
            raw_source_file="spring.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Spring 2026",
            player_id=2,
            games=8,
            pa=22,
            ab=20,
            hits=7,
            singles=6,
            doubles=1,
            triples=0,
            hr=0,
            walks=2,
            strikeouts=1,
            runs=3,
            rbi=4,
            tb=8,
            raw_source_file="jj.csv",
        )
        connection.commit()

        milestone_context = fetch_player_milestone_context(connection, "tristan")
        record_context = fetch_player_record_context(connection, "tristan")
        passed_summary = fetch_passed_milestones_summary(connection, categories=["Hits"], active_only=False, min_current_total=0, limit=10)
        all_milestones = fetch_career_milestones(connection, categories=["Hits"], active_only=False, min_current_total=0)
    finally:
        connection.close()

    assert not all_milestones.empty
    assert not passed_summary.empty
    if not milestone_context["upcoming"].empty:
        assert set(milestone_context["upcoming"]["canonical_name"]) == {"tristan"}
    if not milestone_context["cleared"].empty:
        assert set(milestone_context["cleared"]["canonical_name"]) == {"tristan"}
    assert not record_context["placements"].empty
    assert set(record_context["placements"]["scope"]).issubset({"Career", "Single Season"})
    assert any(label in {"2025 F", "2026 Sp", ""} for label in record_context["placements"]["season_label"].tolist())
