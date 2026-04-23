from __future__ import annotations

import argparse
from pathlib import Path

from src.models.exhaustive_eval import (
    EvaluationStage,
    default_report_path,
    run_definitive_lineup_evaluation,
    write_definitive_lineup_report,
)
from src.models.roster import DEFAULT_ACTIVE_ROSTER_SEASON, DEFAULT_AVAILABILITY_PATH, DEFAULT_LEAGUE_RULES_PATH, load_league_rules
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the definitive exhaustive lineup evaluation workflow."
    )
    parser.add_argument(
        "--db-path",
        default="db/all_seasons_identity.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--projection-season",
        required=True,
        help="Projection season to use for hitter projections.",
    )
    parser.add_argument(
        "--game-date",
        required=True,
        help="Game date key used for availability defaults.",
    )
    parser.add_argument(
        "--availability-path",
        default=str(DEFAULT_AVAILABILITY_PATH),
        help="CSV file with game-day player availability.",
    )
    parser.add_argument(
        "--roster-season",
        default=DEFAULT_ACTIVE_ROSTER_SEASON,
        help="Season roster name used to default availability when needed.",
    )
    parser.add_argument(
        "--league-rules-path",
        default=str(DEFAULT_LEAGUE_RULES_PATH),
        help="JSON file with league rules.",
    )
    parser.add_argument(
        "--top-pool",
        default="Jj,Glove,Tristan,Tim,Kives",
        help="Comma-separated top-5 pool for phase 1.",
    )
    parser.add_argument(
        "--fixed-bottom",
        default="Porter,Walsh,Joel,Corey,Duff,Joey,Jason",
        help="Comma-separated fixed 6-12 suffix for phase 1.",
    )
    parser.add_argument(
        "--middle-pool",
        default="Porter,Walsh,Joel,Corey,Duff,Joey",
        help="Comma-separated pool for spots 6-11 in phase 2.",
    )
    parser.add_argument(
        "--fixed-last",
        default="Jason",
        help="Fixed player for spot 12 in phase 2.",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=1000,
        help="Simulation block size used for repeated block evaluation.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base seed for deterministic stage/block evaluation.",
    )
    parser.add_argument(
        "--near-tie-delta",
        type=float,
        default=0.03,
        help="Treat final differences below this expected-runs gap as a practical near-tie.",
    )
    parser.add_argument(
        "--report-path",
        default="",
        help="Optional report output path. Defaults to data/audits timestamped report.",
    )
    parser.add_argument(
        "--phase1-stage-a",
        type=int,
        default=5_000,
        help="Total simulations for phase 1 stage A.",
    )
    parser.add_argument(
        "--phase1-stage-b",
        type=int,
        default=25_000,
        help="Total simulations for phase 1 stage B.",
    )
    parser.add_argument(
        "--phase1-stage-c",
        type=int,
        default=100_000,
        help="Total simulations for phase 1 stage C.",
    )
    parser.add_argument(
        "--phase2-stage-a",
        type=int,
        default=2_000,
        help="Total simulations for phase 2 stage A.",
    )
    parser.add_argument(
        "--phase2-stage-b",
        type=int,
        default=10_000,
        help="Total simulations for phase 2 stage B.",
    )
    parser.add_argument(
        "--phase2-stage-c",
        type=int,
        default=50_000,
        help="Total simulations for phase 2 stage C.",
    )
    parser.add_argument(
        "--phase2-stage-d",
        type=int,
        default=100_000,
        help="Total simulations for phase 2 stage D.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    top_pool = _parse_csv_list(args.top_pool)
    fixed_bottom = _parse_csv_list(args.fixed_bottom)
    middle_pool = _parse_csv_list(args.middle_pool)
    report_path = Path(args.report_path) if args.report_path else default_report_path()
    phase_1_stages = (
        EvaluationStage("A", total_simulations=args.phase1_stage_a, delta_threshold=0.15, min_keep=10),
        EvaluationStage("B", total_simulations=args.phase1_stage_b, delta_threshold=0.08, min_keep=5),
        EvaluationStage("C", total_simulations=args.phase1_stage_c),
    )
    phase_2_stages = (
        EvaluationStage("A", total_simulations=args.phase2_stage_a, delta_threshold=0.20, min_keep=20),
        EvaluationStage("B", total_simulations=args.phase2_stage_b, delta_threshold=0.08, min_keep=8),
        EvaluationStage("C", total_simulations=args.phase2_stage_c, keep_top_n=3),
        EvaluationStage("D", total_simulations=args.phase2_stage_d),
    )

    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        result = run_definitive_lineup_evaluation(
            connection=connection,
            projection_season=args.projection_season,
            game_date=args.game_date,
            league_rules=load_league_rules(Path(args.league_rules_path)),
            fixed_bottom_suffix=fixed_bottom,
            top_pool=top_pool,
            middle_pool=middle_pool,
            fixed_last=args.fixed_last,
            block_size=args.block_size,
            base_seed=args.seed,
            near_tie_delta=args.near_tie_delta,
            phase_1_stages=phase_1_stages,
            phase_2_stages=phase_2_stages,
        )
    finally:
        connection.close()

    write_definitive_lineup_report(
        result=result,
        report_path=report_path,
        projection_season=args.projection_season,
        game_date=args.game_date,
        fixed_bottom_suffix=fixed_bottom,
        fixed_last=args.fixed_last,
        near_tie_delta=args.near_tie_delta,
    )

    print(f"Report written: {report_path}")
    print("Phase 1 winner:")
    print(" / ".join(result.phase_1.winner.ordered_player_names))
    print(f"avg={result.phase_1.winner.average_runs:.3f} | median={result.phase_1.winner.median_runs:.3f}")
    print("")
    print("Final best lineup:")
    print(" / ".join(result.full_best_lineup.ordered_player_names))
    print(f"avg={result.full_best_lineup.average_runs:.3f} | median={result.full_best_lineup.median_runs:.3f}")
    if result.runner_up_lineup is not None:
        delta = result.runner_up_lineup.average_runs - result.full_best_lineup.average_runs
        print(f"runner_up_delta={delta:.3f}")
    print("decision=" + ("effectively tied" if result.effectively_tied else "decisive winner"))
    return 0


def _parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
