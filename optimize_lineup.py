from __future__ import annotations

import argparse
from pathlib import Path

from src.models.optimizer import optimize_lineup
from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_AVAILABILITY_PATH,
    DEFAULT_LEAGUE_RULES_PATH,
)
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Optimize a full slowpitch batting order from the available roster."
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
        help="Game date key used for availability defaults, for example 2026-04-23.",
    )
    parser.add_argument(
        "--availability-path",
        default=str(DEFAULT_AVAILABILITY_PATH),
        help="CSV file with game-day player availability.",
    )
    parser.add_argument(
        "--league-rules-path",
        default=str(DEFAULT_LEAGUE_RULES_PATH),
        help="JSON file with league rules.",
    )
    parser.add_argument(
        "--roster-season",
        default=DEFAULT_ACTIVE_ROSTER_SEASON,
        help="Season roster name used to default availability when needed.",
    )
    parser.add_argument(
        "--simulations",
        type=int,
        default=1500,
        help="Number of Monte Carlo simulations for each final candidate lineup.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base random seed for reproducible optimizer runs.",
    )
    parser.add_argument(
        "--mode",
        choices=["unconstrained", "team_aware"],
        default="unconstrained",
        help="Optimizer search mode.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        from src.models.roster import load_league_rules

        result = optimize_lineup(
            connection=connection,
            projection_season=args.projection_season,
            game_date=args.game_date,
            league_rules=load_league_rules(Path(args.league_rules_path)),
            simulations=args.simulations,
            seed=args.seed,
            availability_path=Path(args.availability_path),
            roster_season=args.roster_season,
            mode=args.mode,
        )
    finally:
        connection.close()

    print(f"Available hitters: {len(result.available_player_names)}")
    print(f"Optimizer mode: {args.mode}")
    print(f"Candidate lineups evaluated: {result.evaluated_lineups}")
    print("")
    print("Best lineup:")
    print(
        f"expected_runs={result.best_lineup.summary.average_runs:.3f} | "
        f"median_runs={result.best_lineup.summary.median_runs:.3f} | "
        f"dhh_slot={result.best_lineup.dhh_slot}"
    )
    for index, name in enumerate(result.best_lineup.ordered_player_names, start=1):
        marker = " (DHH)" if index == result.best_lineup.dhh_slot else ""
        print(f"{index}. {name}{marker}")
    print(f"why_it_likely_won: {result.best_lineup.reason}")
    if result.near_tie_lineups:
        print(
            f"near_ties_within_0.05_runs: {len(result.near_tie_lineups)} alternate lineup(s)"
        )
    print("")
    print("Alternate lineups:")
    for rank, alternate in enumerate(result.alternate_lineups, start=2):
        order = " / ".join(
            f"{index}:{name}{'(*)' if index == alternate.dhh_slot else ''}"
            for index, name in enumerate(alternate.ordered_player_names, start=1)
        )
        print(
            f"{rank}. avg={alternate.summary.average_runs:.3f} | "
            f"median={alternate.summary.median_runs:.3f} | order={order}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
