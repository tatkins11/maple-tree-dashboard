from pathlib import Path

from src.models.projections import build_hitter_projections
from src.models.roster import (
    load_available_player_names,
    load_league_rules,
    select_game_day_projections,
)
from src.utils.db import connect_db, initialize_database, replace_hitter_projections


def test_load_league_rules_defaults(tmp_path: Path) -> None:
    rules = load_league_rules(tmp_path / "league_rules.json")

    assert rules.innings_per_game == 7
    assert rules.steals_allowed is False
    assert rules.fixed_dhh_enabled is True
    assert rules.max_home_runs_non_dhh == 3
    assert rules.ignore_slaughter_rule is True


def test_load_available_player_names_filters_by_date_and_flag(tmp_path: Path) -> None:
    csv_path = tmp_path / "availability.csv"
    csv_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                "2026-04-20,Tristan,yes,",
                "2026-04-20,Corey,no,",
                "2026-04-21,Glove,yes,",
            ]
        ),
        encoding="utf-8",
    )

    names = load_available_player_names(csv_path, "2026-04-20")

    assert names == ["Tristan"]


def test_select_game_day_projections_returns_metadata_and_projection_fields(
    tmp_path: Path,
) -> None:
    connection = connect_db(tmp_path / "roster.sqlite")
    try:
        initialize_database(connection)
        connection.execute(
            "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan", "tristan"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan", "tristan"),
        )
        connection.execute(
            """
            INSERT INTO player_metadata (
                player_id, preferred_display_name, is_fixed_dhh, baserunning_grade,
                consistency_grade, speed_flag, active_flag, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "Tristan", 1, "B", "A", 1, 1, "fixed DHH"),
        )
        connection.execute(
            """
            INSERT INTO season_batting_stats (
                season, player_id, games, plate_appearances, at_bats, hits, singles, doubles, triples,
                home_runs, walks, strikeouts, hit_by_pitch, sacrifice_hits, sacrifice_flies,
                reached_on_error, fielder_choice, grounded_into_double_play, runs, rbi, total_bases,
                batting_average, on_base_percentage, slugging_percentage, ops,
                batting_average_risp, two_out_rbi, left_on_base, raw_source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Maple Tree Fall 2025",
                1,
                10,
                20,
                18,
                8,
                5,
                1,
                0,
                2,
                3,
                2,
                0,
                0,
                0,
                1,
                1,
                0,
                6,
                7,
                18,
                0.444,
                0.550,
                1.000,
                1.550,
                0.500,
                3,
                4,
                "2025.csv",
            ),
        )
        projections = build_hitter_projections(connection, "Maple Tree Fall 2025")
        replace_hitter_projections(connection, "Maple Tree Fall 2025", projections)
        rows = select_game_day_projections(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            available_player_names=["Tristan"],
        )
    finally:
        connection.close()

    assert len(rows) == 1
    row = rows[0]
    assert row.preferred_display_name == "Tristan"
    assert row.projection_source == "season_blended"
    assert row.is_fixed_dhh is True
    assert row.baserunning_grade == "B"
    assert row.consistency_grade == "A"
    assert row.speed_flag is True
    assert row.projected_on_base_rate >= 0
    assert row.projected_strikeout_rate >= 0


def test_tristan_identity_defaults_to_fixed_dhh_metadata(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "tristan.sqlite")
    try:
        initialize_database(connection)
        connection.execute(
            "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan", "tristan"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan", "tristan"),
        )
        initialize_database(connection)
        row = connection.execute(
            """
            SELECT preferred_display_name, is_fixed_dhh, notes
            FROM player_metadata
            WHERE player_id = 1
            """
        ).fetchone()
    finally:
        connection.close()

    assert row["preferred_display_name"] == "Tristan"
    assert row["is_fixed_dhh"] == 1
    assert "fixed DHH" in row["notes"]
