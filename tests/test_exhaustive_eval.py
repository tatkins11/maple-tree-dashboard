from pathlib import Path

from src.models.exhaustive_eval import (
    EvaluationStage,
    filter_survivor_orders,
    generate_bottom_six_lineups,
    generate_top_five_lineups,
    rank_aggregates,
    run_definitive_lineup_evaluation,
)
from src.models.records import LeagueRulesRecord
from src.utils.db import connect_db, initialize_database


def test_generate_top_five_lineups_returns_120_permutations() -> None:
    lineups = generate_top_five_lineups(
        top_pool=["A", "B", "C", "D", "E"],
        fixed_bottom_suffix=["F", "G", "H", "I", "J", "K", "L"],
    )

    assert len(lineups) == 120
    assert all(lineup[5:] == ["F", "G", "H", "I", "J", "K", "L"] for lineup in lineups)


def test_generate_bottom_six_lineups_returns_720_permutations_and_preserves_jason_last() -> None:
    lineups = generate_bottom_six_lineups(
        fixed_top_prefix=["A", "B", "C", "D", "E"],
        middle_pool=["F", "G", "H", "I", "J", "K"],
        fixed_last="Jason",
    )

    assert len(lineups) == 720
    assert all(lineup[:5] == ["A", "B", "C", "D", "E"] for lineup in lineups)
    assert all(lineup[-1] == "Jason" for lineup in lineups)


def test_filter_survivor_orders_preserves_minimum_keep() -> None:
    ranked = [
        _result(["A"], 10.0),
        _result(["B"], 9.7),
        _result(["C"], 9.6),
        _result(["D"], 9.5),
    ]

    survivors = filter_survivor_orders(
        ranked_results=ranked,
        delta_threshold=0.05,
        min_keep=3,
    )

    assert survivors == [("A",), ("B",), ("C",)]


def test_rank_aggregates_sorts_descending() -> None:
    aggregates = [
        _aggregate(["A"], [10, 11]),
        _aggregate(["B"], [12, 13]),
        _aggregate(["C"], [9, 10]),
    ]

    ranked = rank_aggregates(aggregates, stage_name="A")

    assert [result.ordered_player_names for result in ranked] == [["B"], ["A"], ["C"]]


def test_run_definitive_lineup_evaluation_is_deterministic_on_toy_case(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "definitive.sqlite")
    try:
        initialize_database(connection)
        players = [
            ("A", "a", False, 0.30, 0.08, 0.02, 0.06, 0.06, 0.01, 0.35, 0.52, 0.82, 0.22, 0.25),
            ("B", "b", False, 0.31, 0.07, 0.01, 0.04, 0.05, 0.01, 0.38, 0.48, 0.72, 0.19, 0.22),
            ("C", "c", True, 0.20, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.65, 1.26, 0.43, 0.60),
            ("D", "d", False, 0.28, 0.06, 0.01, 0.03, 0.03, 0.01, 0.45, 0.41, 0.54, 0.18, 0.20),
            ("E", "e", False, 0.27, 0.05, 0.01, 0.02, 0.04, 0.01, 0.47, 0.39, 0.49, 0.16, 0.17),
            ("F", "f", False, 0.24, 0.04, 0.00, 0.01, 0.03, 0.01, 0.55, 0.32, 0.36, 0.11, 0.12),
            ("G", "g", False, 0.23, 0.03, 0.00, 0.00, 0.02, 0.01, 0.60, 0.28, 0.29, 0.08, 0.09),
            ("H", "h", False, 0.22, 0.03, 0.00, 0.00, 0.02, 0.01, 0.61, 0.27, 0.28, 0.08, 0.08),
            ("I", "i", False, 0.21, 0.03, 0.00, 0.00, 0.02, 0.01, 0.62, 0.26, 0.27, 0.07, 0.08),
            ("J", "j", False, 0.20, 0.02, 0.00, 0.00, 0.02, 0.01, 0.64, 0.24, 0.24, 0.06, 0.07),
            ("K", "k", False, 0.20, 0.02, 0.00, 0.00, 0.02, 0.01, 0.64, 0.24, 0.24, 0.06, 0.07),
            ("Jason", "jason", False, 0.18, 0.01, 0.00, 0.00, 0.02, 0.01, 0.68, 0.21, 0.22, 0.05, 0.06),
        ]
        for index, (player_name, canonical_name, is_fixed_dhh, p_single, p_double, p_triple, p_home_run, p_walk, projected_strikeout_rate, p_out, projected_on_base_rate, projected_total_base_rate, projected_run_rate, projected_rbi_rate) in enumerate(players, start=1):
            _insert_projection_player(
                connection=connection,
                player_id=index,
                player_name=player_name,
                canonical_name=canonical_name,
                is_fixed_dhh=is_fixed_dhh,
                p_single=p_single,
                p_double=p_double,
                p_triple=p_triple,
                p_home_run=p_home_run,
                p_walk=p_walk,
                projected_strikeout_rate=projected_strikeout_rate,
                p_out=p_out,
                projected_on_base_rate=projected_on_base_rate,
                projected_total_base_rate=projected_total_base_rate,
                projected_run_rate=projected_run_rate,
                projected_rbi_rate=projected_rbi_rate,
            )

        kwargs = dict(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            fixed_bottom_suffix=["F", "G", "H", "I", "J", "K", "Jason"],
            top_pool=["A", "B", "C", "D", "E"],
            middle_pool=["F", "G", "H", "I", "J", "K"],
            fixed_last="Jason",
                block_size=100,
                base_seed=123,
                near_tie_delta=0.03,
                available_player_names_override=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "Jason"],
                phase_1_stages=(
                    EvaluationStage("A", total_simulations=100, delta_threshold=0.15, min_keep=10),
                    EvaluationStage("B", total_simulations=200, delta_threshold=0.08, min_keep=5),
                    EvaluationStage("C", total_simulations=400),
                ),
                phase_2_stages=(
                    EvaluationStage("A", total_simulations=100, delta_threshold=0.20, min_keep=20),
                    EvaluationStage("B", total_simulations=200, delta_threshold=0.08, min_keep=8),
                    EvaluationStage("C", total_simulations=400, keep_top_n=3),
                    EvaluationStage("D", total_simulations=800),
                ),
            )
        result_a = run_definitive_lineup_evaluation(**kwargs)
        result_b = run_definitive_lineup_evaluation(**kwargs)
    finally:
        connection.close()

    assert result_a.full_best_lineup.ordered_player_names == result_b.full_best_lineup.ordered_player_names
    assert result_a.full_best_lineup.average_runs == result_b.full_best_lineup.average_runs


def _result(order: list[str], average_runs: float):
    from src.models.exhaustive_eval import LineupEvaluationResult

    return LineupEvaluationResult(
        ordered_player_names=order,
        total_simulations=1000,
        average_runs=average_runs,
        median_runs=average_runs,
        stddev_runs=1.0,
        standard_error=0.1,
        ci_lower=average_runs - 0.2,
        ci_upper=average_runs + 0.2,
        stage_name="A",
    )


def _aggregate(order: list[str], runs: list[int]):
    from src.models.exhaustive_eval import _LineupAggregate

    return _LineupAggregate(ordered_player_names=order, runs_by_game=runs)


def _insert_projection_player(
    connection,
    player_id: int,
    player_name: str,
    canonical_name: str,
    is_fixed_dhh: bool,
    p_single: float,
    p_double: float,
    p_triple: float,
    p_home_run: float,
    p_walk: float,
    projected_strikeout_rate: float,
    p_out: float,
    projected_on_base_rate: float,
    projected_total_base_rate: float,
    projected_run_rate: float,
    projected_rbi_rate: float,
) -> None:
    connection.execute(
        "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )
    connection.execute(
        "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )
    initialize_database(connection)
    connection.execute(
        """
        UPDATE player_metadata
        SET preferred_display_name = ?, is_fixed_dhh = ?
        WHERE player_id = ?
        """,
        (player_name, int(is_fixed_dhh), player_id),
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
            player_id,
            "season_blended",
            50,
            150,
            100,
            0.3,
            p_single,
            p_double,
            p_triple,
            p_home_run,
            p_walk,
            projected_strikeout_rate,
            0.0,
            0.0,
            0.0,
            0.0,
            p_out,
            projected_on_base_rate,
            projected_total_base_rate,
            projected_run_rate,
            projected_rbi_rate,
            p_double + p_triple + p_home_run,
            int(is_fixed_dhh),
            0.0,
            0.0,
            0.0,
            0.0,
        ),
    )
    connection.commit()
