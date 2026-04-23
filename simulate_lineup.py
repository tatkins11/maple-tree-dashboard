from __future__ import annotations

import argparse
from pathlib import Path

from src.models.lineup import DEFAULT_LINEUP_PATH, build_simulation_lineup
from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_AVAILABILITY_PATH,
    DEFAULT_LEAGUE_RULES_PATH,
    load_league_rules,
)
from src.models.simulator import simulate_lineup
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Simulate offensive run scoring for one manually specified batting order."
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
        help="Game date key used in availability and lineup CSV files.",
    )
    parser.add_argument(
        "--availability-path",
        default=str(DEFAULT_AVAILABILITY_PATH),
        help="CSV file with game-day player availability.",
    )
    parser.add_argument(
        "--lineup-path",
        default=str(DEFAULT_LINEUP_PATH),
        help="CSV file with manual batting order.",
    )
    parser.add_argument(
        "--league-rules-path",
        default=str(DEFAULT_LEAGUE_RULES_PATH),
        help="JSON file with league rules.",
    )
    parser.add_argument(
        "--roster-season",
        default=DEFAULT_ACTIVE_ROSTER_SEASON,
        help="Season roster name used to default availability when no rows exist for the game date.",
    )
    parser.add_argument(
        "--simulations",
        type=int,
        default=5000,
        help="Number of simulated games to run.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible results.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        lineup = build_simulation_lineup(
            connection=connection,
            projection_season=args.projection_season,
            game_date=args.game_date,
            availability_path=Path(args.availability_path),
            lineup_path=Path(args.lineup_path),
            roster_season=args.roster_season,
        )
    finally:
        connection.close()

    rules = load_league_rules(Path(args.league_rules_path))
    summary = simulate_lineup(
        lineup=lineup,
        league_rules=rules,
        simulations=args.simulations,
        seed=args.seed,
    )

    print("Lineup:")
    for row in lineup:
        dhh_label = "DHH" if row.is_fixed_dhh else "BAT"
        print(
            f"{row.lineup_spot}. {row.player_name} | role={dhh_label} | source={row.projection_source} | fixed_dhh={row.is_fixed_dhh} | "
            f"obp={row.projected_on_base_rate:.3f} | tb_rate={row.projected_total_base_rate:.3f}"
        )

    print("")
    print("Simulation summary:")
    print(f"simulations={summary.simulations}")
    print(f"average_runs={summary.average_runs:.3f}")
    print(f"median_runs={summary.median_runs:.3f}")
    print(f"expected_runs_per_game={summary.expected_runs_per_game:.3f}")
    print(f"average_team_non_dhh_home_runs={summary.average_team_non_dhh_home_runs:.3f}")
    print(f"dhh_exemption_usage_rate={summary.dhh_exemption_usage_rate:.3f}")

    print("")
    print("Run distribution:")
    for runs, count in summary.run_distribution.items():
        print(f"{runs}: {count}")

    print("")
    print("Player event averages:")
    for player, events in summary.player_event_averages.items():
        compact = ", ".join(f"{event}={value:.3f}" for event, value in events.items())
        print(f"{player}: {compact}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
