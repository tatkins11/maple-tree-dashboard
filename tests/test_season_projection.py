from pathlib import Path

from project_season_stats import build_parser, main
from src.models.lineup import SimulationLineupRow
from src.models.records import LeagueRulesRecord
from src.models.season_projection import simulate_season_projection
from src.utils.db import connect_db, initialize_database


def test_simulated_season_stats_reconcile_hits_and_slugging() -> None:
    lineup = [
        _make_lineup_row("SingleGuy", 1, p_single=1.0),
        _make_lineup_row("Out1", 2, p_out=1.0),
        _make_lineup_row("Out2", 3, p_out=1.0),
        _make_lineup_row("Out3", 4, p_out=1.0),
    ]

    result = simulate_season_projection(
        lineup=lineup,
        league_rules=LeagueRulesRecord(innings_per_game=1),
        season_games=3,
        simulated_seasons=10,
        seed=7,
    )

    hitter = result.player_summaries[0]
    assert hitter.mean_plate_appearances == 3.0
    assert hitter.mean_at_bats == 3.0
    assert hitter.mean_singles == 3.0
    assert hitter.mean_home_runs == 0.0
    assert hitter.mean_walks == 0.0
    assert hitter.mean_avg == 1.0
    assert hitter.mean_obp == 1.0
    assert hitter.mean_slg == 1.0
    assert hitter.mean_ops == 2.0


def test_simulated_season_is_stable_for_fixed_seed() -> None:
    lineup = [
        _make_lineup_row("Lead", 1, p_single=0.3, p_walk=0.1, p_out=0.6),
        _make_lineup_row("Power", 2, p_double=0.1, p_home_run=0.1, p_out=0.8),
        _make_lineup_row("Out", 3, p_out=1.0),
    ]

    result_a = simulate_season_projection(lineup, LeagueRulesRecord(), season_games=4, simulated_seasons=25, seed=99)
    result_b = simulate_season_projection(lineup, LeagueRulesRecord(), season_games=4, simulated_seasons=25, seed=99)

    assert result_a.team_summary.average_runs_per_game == result_b.team_summary.average_runs_per_game
    assert [row.mean_runs for row in result_a.player_summaries] == [row.mean_runs for row in result_b.player_summaries]


def test_project_season_stats_cli_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "season_projection.sqlite"
    availability_path = tmp_path / "availability.csv"
    lineup_path = tmp_path / "lineup.csv"
    output_csv = tmp_path / "season_projection.csv"
    report_path = tmp_path / "season_projection_report.txt"

    connection = connect_db(db_path)
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
            (1, "Tristan", 1, "B", "A", 0, 1, None),
        )
        connection.execute(
            """
            INSERT INTO hitter_projections (
                projection_season, player_id, projection_source, current_plate_appearances, career_plate_appearances,
                baseline_plate_appearances, current_season_weight, p_single, p_double, p_triple,
                p_home_run, p_walk, projected_strikeout_rate, p_hit_by_pitch, p_reached_on_error,
                p_fielder_choice, p_grounded_into_double_play, p_out, projected_on_base_rate,
                projected_total_base_rate, projected_run_rate, projected_rbi_rate,
                projected_extra_base_hit_rate, fixed_dhh_flag, baserunning_adjustment,
                secondary_batting_average_risp, secondary_two_out_rbi_rate, secondary_left_on_base_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Maple Tree Fall 2025",
                1,
                "season_blended",
                20,
                100,
                80,
                0.2,
                0.30,
                0.05,
                0.0,
                0.05,
                0.10,
                0.08,
                0.0,
                0.02,
                0.03,
                0.01,
                0.36,
                0.55,
                0.60,
                0.12,
                0.13,
                0.10,
                1,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    availability_path.write_text(
        "game_date,player_name,available_flag,notes\n2026-04-20,Tristan,yes,\n",
        encoding="utf-8",
    )
    lineup_path.write_text(
        "game_date,lineup_spot,player_name,notes\n2026-04-20,1,Tristan,\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "project_season_stats.py",
            "--db-path",
            str(db_path),
            "--projection-season",
            "Maple Tree Fall 2025",
            "--game-date",
            "2026-04-20",
            "--availability-path",
            str(availability_path),
            "--lineup-path",
            str(lineup_path),
            "--season-games",
            "3",
            "--simulated-seasons",
            "15",
            "--seed",
            "11",
            "--output-csv",
            str(output_csv),
            "--report-path",
            str(report_path),
        ],
    )

    exit_code = main()

    assert exit_code == 0
    assert output_csv.exists()
    assert report_path.exists()
    csv_text = output_csv.read_text(encoding="utf-8")
    assert "player_name,lineup_spot,projection_source" in csv_text
    assert "mean_bb" in csv_text
    assert "mean_obp" in csv_text
    assert "Tristan" in csv_text
    report_text = report_path.read_text(encoding="utf-8")
    assert "season_games: 3" in report_text
    assert "Runs and RBI are tracked directly from simulated scoring events." in report_text


def test_parser_defaults_to_twelve_game_season() -> None:
    parser = build_parser()
    args = parser.parse_args(["--projection-season", "Maple Tree Fall 2025", "--game-date", "2026-04-20"])
    assert args.season_games == 12


def _make_lineup_row(
    name: str,
    lineup_spot: int,
    *,
    p_single: float = 0.0,
    p_double: float = 0.0,
    p_triple: float = 0.0,
    p_home_run: float = 0.0,
    p_walk: float = 0.0,
    p_hit_by_pitch: float = 0.0,
    p_reached_on_error: float = 0.0,
    p_fielder_choice: float = 0.0,
    p_grounded_into_double_play: float = 0.0,
    projected_strikeout_rate: float = 0.0,
    p_out: float = 0.0,
) -> SimulationLineupRow:
    return SimulationLineupRow(
        player_id=lineup_spot,
        player_name=name,
        projection_source="season_blended",
        lineup_spot=lineup_spot,
        is_fixed_dhh=False,
        baserunning_adjustment=0.0,
        p_single=p_single,
        p_double=p_double,
        p_triple=p_triple,
        p_home_run=p_home_run,
        p_walk=p_walk,
        p_hit_by_pitch=p_hit_by_pitch,
        p_reached_on_error=p_reached_on_error,
        p_fielder_choice=p_fielder_choice,
        p_grounded_into_double_play=p_grounded_into_double_play,
        projected_strikeout_rate=projected_strikeout_rate,
        p_out=p_out,
        projected_on_base_rate=0.0,
        projected_total_base_rate=0.0,
        projected_run_rate=0.0,
        projected_rbi_rate=0.0,
    )
