from pathlib import Path

import pytest

from src.models.lineup import SimulationLineupRow, build_simulation_lineup
from src.models.records import LeagueRulesRecord
from src.models.simulator import _apply_event, simulate_game, simulate_lineup
from src.utils.db import connect_db, initialize_database


def test_simulator_all_outs_produces_zero_runs() -> None:
    lineup = [_make_lineup_row("A", 1, p_out=1.0), _make_lineup_row("B", 2, p_out=1.0), _make_lineup_row("C", 3, p_out=1.0)]

    summary = simulate_lineup(lineup, LeagueRulesRecord(), simulations=25, seed=1)

    assert summary.average_runs == 0.0
    assert summary.median_runs == 0
    assert summary.run_distribution == {0: 25}


def test_team_non_dhh_home_run_cap_applies_after_three_team_home_runs() -> None:
    lineup = [
        _make_lineup_row("SluggerA", 1, p_home_run=1.0, is_fixed_dhh=False),
        _make_lineup_row("SluggerB", 2, p_home_run=1.0, is_fixed_dhh=False),
        _make_lineup_row("SluggerC", 3, p_home_run=1.0, is_fixed_dhh=False),
        _make_lineup_row("SluggerD", 4, p_home_run=1.0, is_fixed_dhh=False),
        _make_lineup_row("Out1", 5, p_out=1.0),
        _make_lineup_row("Out2", 6, p_out=1.0),
        _make_lineup_row("Out3", 7, p_out=1.0),
    ]

    summary = simulate_lineup(lineup, LeagueRulesRecord(innings_per_game=1), simulations=1, seed=1)

    assert summary.average_runs == 3.0
    assert summary.player_event_averages["SluggerA"]["home_run"] == 1.0
    assert summary.player_event_averages["SluggerB"]["home_run"] == 1.0
    assert summary.player_event_averages["SluggerC"]["home_run"] == 1.0
    assert summary.player_event_averages["SluggerD"].get("home_run", 0.0) == 0.0
    assert (
        summary.player_event_averages["SluggerD"].get("single", 0.0)
        + summary.player_event_averages["SluggerD"].get("double", 0.0)
        + summary.player_event_averages["SluggerD"].get("other_out", 0.0)
    ) == 1.0
    assert summary.average_team_non_dhh_home_runs == 3.0


def test_fixed_dhh_is_exempt_from_home_run_cap() -> None:
    lineup = [
        _make_lineup_row("DHH", 1, p_home_run=1.0, is_fixed_dhh=True),
        _make_lineup_row("Out1", 2, p_out=1.0),
        _make_lineup_row("Out2", 3, p_out=1.0),
        _make_lineup_row("Out3", 4, p_out=1.0),
    ]

    summary = simulate_lineup(lineup, LeagueRulesRecord(), simulations=5, seed=1)

    assert summary.average_runs == 7.0
    assert summary.player_event_averages["DHH"]["home_run"] == 7.0


def test_tristan_identity_as_fixed_dhh_is_exempt_from_home_run_cap() -> None:
    lineup = [
        _make_lineup_row("Tristan", 1, p_home_run=1.0, is_fixed_dhh=True),
        _make_lineup_row("Out1", 2, p_out=1.0),
        _make_lineup_row("Out2", 3, p_out=1.0),
        _make_lineup_row("Out3", 4, p_out=1.0),
    ]

    summary = simulate_lineup(lineup, LeagueRulesRecord(), simulations=5, seed=1)

    assert summary.average_runs == 7.0
    assert summary.player_event_averages["Tristan"]["home_run"] == 7.0


def test_dhh_home_runs_do_not_count_toward_team_non_dhh_cap() -> None:
    lineup = [
        _make_lineup_row("Tristan", 1, p_home_run=1.0, is_fixed_dhh=True),
        _make_lineup_row("Slugger", 2, p_home_run=1.0),
        _make_lineup_row("Out1", 3, p_out=1.0),
        _make_lineup_row("Out2", 4, p_out=1.0),
        _make_lineup_row("Out3", 5, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1))

    assert result.player_stats[1].home_runs == 1
    assert result.player_stats[2].home_runs == 1
    assert result.team_non_dhh_home_runs == 1


def test_seeded_simulation_is_stable() -> None:
    lineup = [
        _make_lineup_row("Lead", 1, p_single=0.3, p_walk=0.1, projected_strikeout_rate=0.1, p_out=0.5),
        _make_lineup_row("Power", 2, p_double=0.1, p_home_run=0.1, projected_strikeout_rate=0.2, p_out=0.6),
        _make_lineup_row("Table", 3, p_single=0.2, p_walk=0.2, projected_strikeout_rate=0.1, p_out=0.5),
    ]

    summary_a = simulate_lineup(lineup, LeagueRulesRecord(), simulations=200, seed=99)
    summary_b = simulate_lineup(lineup, LeagueRulesRecord(), simulations=200, seed=99)

    assert summary_a.average_runs == summary_b.average_runs
    assert summary_a.run_distribution == summary_b.run_distribution
    assert summary_a.player_event_averages == summary_b.player_event_averages


def test_post_cap_non_dhh_hr_without_exemption_is_soft_suppressed() -> None:
    lineup = [
        _make_lineup_row("SluggerA", 1, p_home_run=1.0),
        _make_lineup_row("SluggerB", 2, p_home_run=1.0),
        _make_lineup_row("SluggerC", 3, p_home_run=1.0),
        _make_lineup_row("SluggerD", 4, p_home_run=1.0),
        _make_lineup_row("Out1", 5, p_out=1.0),
        _make_lineup_row("Out2", 6, p_out=1.0),
        _make_lineup_row("Out3", 7, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1), rng=__import__("random").Random(1))

    assert result.team_non_dhh_home_runs == 3
    assert result.player_stats[4].home_runs == 0
    assert result.player_stats[4].singles + result.player_stats[4].doubles + result.player_stats[4].other_outs == 1


def test_dhh_walk_creates_single_use_exemption_and_consumes_it_on_first_hr() -> None:
    lineup = [
        _make_lineup_row("Tristan", 1, is_fixed_dhh=True, p_walk=1.0),
        _make_lineup_row("SluggerA", 2, p_home_run=1.0),
        _make_lineup_row("SluggerB", 3, p_home_run=1.0),
        _make_lineup_row("SluggerC", 4, p_home_run=1.0),
        _make_lineup_row("SluggerD", 5, p_home_run=1.0),
        _make_lineup_row("SluggerE", 6, p_home_run=1.0),
        _make_lineup_row("Out1", 7, p_out=1.0),
        _make_lineup_row("Out2", 8, p_out=1.0),
        _make_lineup_row("Out3", 9, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1), rng=__import__("random").Random(1))

    assert result.dhh_exemption_used is True
    assert result.team_non_dhh_home_runs == 3
    assert result.player_stats[5].home_runs == 1
    assert result.player_stats[6].home_runs == 0


def test_dhh_single_does_not_create_exemption() -> None:
    lineup = [
        _make_lineup_row("Tristan", 1, is_fixed_dhh=True, p_single=1.0),
        _make_lineup_row("SluggerA", 2, p_home_run=1.0),
        _make_lineup_row("SluggerB", 3, p_home_run=1.0),
        _make_lineup_row("SluggerC", 4, p_home_run=1.0),
        _make_lineup_row("SluggerD", 5, p_home_run=1.0),
        _make_lineup_row("Out1", 6, p_out=1.0),
        _make_lineup_row("Out2", 7, p_out=1.0),
        _make_lineup_row("Out3", 8, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1), rng=__import__("random").Random(1))

    assert result.dhh_exemption_used is False
    assert result.player_stats[5].home_runs == 0


def test_unused_exemption_expires_when_dhh_bats_again() -> None:
    lineup = [
        _make_lineup_row("Tristan", 1, is_fixed_dhh=True, p_walk=1.0),
        _make_lineup_row("Out1", 2, p_out=1.0),
        _make_lineup_row("Out2", 3, p_out=1.0),
        _make_lineup_row("Out3", 4, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=2), rng=__import__("random").Random(1))

    assert result.dhh_exemption_used is False
    assert result.team_non_dhh_home_runs == 0


def test_only_first_hr_before_dhh_returns_uses_exemption() -> None:
    lineup = [
        _make_lineup_row("Tristan", 1, is_fixed_dhh=True, p_walk=1.0),
        _make_lineup_row("SluggerA", 2, p_home_run=1.0),
        _make_lineup_row("SluggerB", 3, p_home_run=1.0),
        _make_lineup_row("SluggerC", 4, p_home_run=1.0),
        _make_lineup_row("SluggerD", 5, p_home_run=1.0),
        _make_lineup_row("SluggerE", 6, p_home_run=1.0),
        _make_lineup_row("SluggerF", 7, p_home_run=1.0),
        _make_lineup_row("Out1", 8, p_out=1.0),
        _make_lineup_row("Out2", 9, p_out=1.0),
        _make_lineup_row("Out3", 10, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1), rng=__import__("random").Random(1))

    assert result.dhh_exemption_used is True
    assert result.player_stats[5].home_runs == 1
    assert result.player_stats[6].home_runs == 0
    assert result.player_stats[7].home_runs == 0


def test_low_hr_hitter_post_cap_behavior_stays_as_out() -> None:
    lineup = [
        _make_lineup_row("SluggerA", 1, p_home_run=1.0),
        _make_lineup_row("SluggerB", 2, p_home_run=1.0),
        _make_lineup_row("SluggerC", 3, p_home_run=1.0),
        _make_lineup_row("LowPower", 4, p_home_run=0.04, p_out=0.96),
        _make_lineup_row("Out1", 5, p_out=1.0),
        _make_lineup_row("Out2", 6, p_out=1.0),
        _make_lineup_row("Out3", 7, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1), rng=__import__("random").Random(2))

    assert result.team_non_dhh_home_runs == 3
    assert result.player_stats[4].home_runs == 0
    assert result.player_stats[4].other_outs == 1


def test_reached_on_error_uses_weaker_forced_advancement() -> None:
    lineup = [
        _make_lineup_row("ROE", 1, p_reached_on_error=1.0),
        _make_lineup_row("Out1", 2, p_out=1.0),
        _make_lineup_row("Out2", 3, p_out=1.0),
        _make_lineup_row("Out3", 4, p_out=1.0),
    ]

    summary = simulate_lineup(lineup, LeagueRulesRecord(innings_per_game=1), simulations=1, seed=1)

    assert summary.average_runs == 0.0
    assert summary.player_event_averages["ROE"]["reached_on_error"] == 1.0


def test_fielder_choice_with_bases_loaded_records_out_without_forced_run() -> None:
    runs, scored_runner_ids, outs, bases = _apply_event("fielder_choice", [1, 2, 3], 9)

    assert runs == 0
    assert scored_runner_ids == []
    assert outs == 1
    assert bases == [9, 2, 3]


def test_home_run_credits_batter_run_and_rbi() -> None:
    lineup = [
        _make_lineup_row("Slugger", 1, p_home_run=1.0),
        _make_lineup_row("Out1", 2, p_out=1.0),
        _make_lineup_row("Out2", 3, p_out=1.0),
        _make_lineup_row("Out3", 4, p_out=1.0),
    ]

    result = simulate_game(lineup, LeagueRulesRecord(innings_per_game=1))

    slugger = result.player_stats[1]
    assert result.runs == 1
    assert slugger.plate_appearances == 1
    assert slugger.at_bats == 1
    assert slugger.home_runs == 1
    assert slugger.runs == 1
    assert slugger.rbi == 1
    assert slugger.total_bases == 4


def test_bases_loaded_walk_credits_one_run_and_one_rbi() -> None:
    runs, scored_runner_ids, outs, bases = _apply_event("walk", [1, 2, 3], 4)

    assert runs == 1
    assert scored_runner_ids == [3]
    assert outs == 0
    assert bases == [4, 1, 2]


def test_gidp_and_other_out_do_not_credit_rbi() -> None:
    gidp_lineup = [
        _make_lineup_row("Single1", 1, p_single=1.0),
        _make_lineup_row("GIDP", 2, p_grounded_into_double_play=1.0),
        _make_lineup_row("Out3", 3, p_out=1.0),
        _make_lineup_row("Out4", 4, p_out=1.0),
    ]
    other_out_lineup = [
        _make_lineup_row("Single1", 1, p_single=1.0),
        _make_lineup_row("Single2", 2, p_single=1.0),
        _make_lineup_row("Single3", 3, p_single=1.0),
        _make_lineup_row("OutBatter", 4, p_out=1.0),
    ]

    gidp_result = simulate_game(gidp_lineup, LeagueRulesRecord(innings_per_game=1))
    other_out_result = simulate_game(other_out_lineup, LeagueRulesRecord(innings_per_game=1))

    assert gidp_result.player_stats[2].rbi == 0
    assert other_out_result.player_stats[4].rbi == 0


def test_build_simulation_lineup_joins_order_and_availability(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "sim_input.sqlite")
    availability_path = tmp_path / "availability.csv"
    lineup_path = tmp_path / "lineup.csv"
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
            (1, "Tristan", 1, "B", "A", 1, 1, None),
        )
        connection.execute(
            """
            INSERT INTO hitter_projections (
                projection_season, player_id, current_plate_appearances, career_plate_appearances,
                baseline_plate_appearances, current_season_weight, p_single, p_double, p_triple,
                p_home_run, p_walk, projected_strikeout_rate, p_hit_by_pitch, p_reached_on_error,
                p_fielder_choice, p_grounded_into_double_play, p_out, projected_on_base_rate,
                projected_total_base_rate, projected_run_rate, projected_rbi_rate,
                projected_extra_base_hit_rate, fixed_dhh_flag, baserunning_adjustment,
                secondary_batting_average_risp, secondary_two_out_rbi_rate, secondary_left_on_base_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Maple Tree Fall 2025",
                1,
                20,
                100,
                80,
                0.2,
                0.25,
                0.05,
                0.0,
                0.05,
                0.10,
                0.08,
                0.0,
                0.02,
                0.03,
                0.01,
                0.41,
                0.50,
                0.60,
                0.12,
                0.13,
                0.10,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
        )
        availability_path.write_text(
            "\n".join(
                [
                    "game_date,player_name,available_flag,notes",
                    "2026-04-20,Teo,yes,alias works",
                ]
            ),
            encoding="utf-8",
        )
        lineup_path.write_text(
            "\n".join(
                [
                    "game_date,lineup_spot,player_name,notes",
                    "2026-04-20,1,Tristan,",
                ]
            ),
            encoding="utf-8",
        )
        connection.execute(
            """
            INSERT INTO player_aliases (
                player_id, source_name, normalized_source_name, source_type, source_file, match_method, approved_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "Teo", "teo", "manual_override", "aliases.csv", "manual_override", 1),
        )
        lineup = build_simulation_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            availability_path=availability_path,
            lineup_path=lineup_path,
        )
    finally:
        connection.close()

    assert len(lineup) == 1
    assert lineup[0].player_name == "Tristan"
    assert lineup[0].lineup_spot == 1
    assert lineup[0].is_fixed_dhh is True


def test_build_simulation_lineup_requires_all_available_players_to_bat(
    tmp_path: Path,
) -> None:
    connection = connect_db(tmp_path / "full_lineup.sqlite")
    availability_path = tmp_path / "availability.csv"
    lineup_path = tmp_path / "lineup.csv"
    try:
        initialize_database(connection)
        for player_id, player_name, canonical_name in [
            (1, "Tristan", "tristan"),
            (2, "Corey", "corey"),
        ]:
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
            ) VALUES
                (?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "Tristan",
                1,
                "B",
                "A",
                1,
                1,
                None,
                2,
                "Corey",
                0,
                "C",
                "B",
                0,
                1,
                None,
            ),
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
            ) VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Maple Tree Fall 2025",
                1,
                "season_blended",
                20,
                100,
                80,
                0.2,
                0.25,
                0.05,
                0.0,
                0.05,
                0.10,
                0.08,
                0.0,
                0.02,
                0.03,
                0.01,
                0.41,
                0.50,
                0.60,
                0.12,
                0.13,
                0.10,
                1,
                0.0,
                0.0,
                0.0,
                0.0,
                "Maple Tree Fall 2025",
                2,
                "season_blended",
                20,
                100,
                80,
                0.2,
                0.20,
                0.04,
                0.0,
                0.02,
                0.08,
                0.07,
                0.0,
                0.01,
                0.02,
                0.01,
                0.55,
                0.35,
                0.44,
                0.10,
                0.11,
                0.06,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
        )
        availability_path.write_text(
            "\n".join(
                [
                    "game_date,player_name,available_flag,notes",
                    "2026-04-20,Tristan,yes,",
                    "2026-04-20,Corey,yes,",
                ]
            ),
            encoding="utf-8",
        )
        lineup_path.write_text(
            "\n".join(
                [
                    "game_date,lineup_spot,player_name,notes",
                    "2026-04-20,1,Tristan,",
                ]
            ),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="include every available player exactly once"):
            build_simulation_lineup(
                connection=connection,
                projection_season="Maple Tree Fall 2025",
                game_date="2026-04-20",
                availability_path=availability_path,
                lineup_path=lineup_path,
            )
    finally:
        connection.close()


def _make_lineup_row(
    name: str,
    lineup_spot: int,
    *,
    is_fixed_dhh: bool = False,
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
        is_fixed_dhh=is_fixed_dhh,
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
