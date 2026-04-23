from __future__ import annotations

import argparse
import csv
from pathlib import Path

from src.models.lineup import build_simulation_lineup_from_order
from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_LEAGUE_RULES_PATH,
    load_league_rules,
)
from src.models.season_roster import fetch_active_roster_rows
from src.models.simulator import simulate_lineup
from src.utils.db import connect_db, initialize_database


DEFAULT_SCENARIOS_PATH = Path("data/processed/manual_lineup_scenarios.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare multiple manual batting orders with the offensive simulator."
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
        "--scenarios-path",
        default=str(DEFAULT_SCENARIOS_PATH),
        help="CSV file containing manual lineup scenarios.",
    )
    parser.add_argument(
        "--league-rules-path",
        default=str(DEFAULT_LEAGUE_RULES_PATH),
        help="JSON file with league rules.",
    )
    parser.add_argument(
        "--roster-season",
        default=DEFAULT_ACTIVE_ROSTER_SEASON,
        help="Season roster used to default availability when needed.",
    )
    parser.add_argument(
        "--simulations",
        type=int,
        default=3000,
        help="Number of simulated games per lineup.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base random seed for reproducible lineup comparisons.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    scenarios = load_lineup_scenarios(Path(args.scenarios_path))
    rules = load_league_rules(Path(args.league_rules_path))

    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        roster_rows = fetch_active_roster_rows(connection, args.roster_season)
        available_names = [str(row["source_name"]) for row in roster_rows]
        results = []
        for index, (lineup_name, ordered_names) in enumerate(scenarios.items()):
            lineup = build_simulation_lineup_from_order(
                connection=connection,
                projection_season=args.projection_season,
                ordered_player_names=ordered_names,
                available_player_names=available_names,
            )
            summary = simulate_lineup(
                lineup=lineup,
                league_rules=rules,
                simulations=args.simulations,
                seed=args.seed + index,
            )
            results.append((lineup_name, lineup, summary))
    finally:
        connection.close()

    print(f"Compared {len(results)} manual lineups")
    print("")
    print("Ranked by average runs:")
    ranked = sorted(results, key=lambda item: item[2].average_runs, reverse=True)
    for lineup_name, lineup, summary in ranked:
        compact_order = " / ".join(
            f"{row.lineup_spot}:{row.player_name}{'*' if row.is_fixed_dhh else ''}"
            for row in lineup
        )
        top_dist = ", ".join(
            f"{runs}:{count}"
            for runs, count in sorted(summary.run_distribution.items())[:8]
        )
        print(
            f"{lineup_name} | avg={summary.average_runs:.3f} | median={summary.median_runs:.3f} | "
            f"order={compact_order}"
        )
        print(f"  distribution(sample)={top_dist}")
        print(
            "  team_events="
            + ", ".join(
                f"{event}={value:.3f}"
                for event, value in summary.total_event_averages.items()
                if event in {"home_run", "walk", "strikeout", "other_out", "single", "double"}
            )
        )
    return 0


def load_lineup_scenarios(csv_path: Path) -> dict[str, list[str]]:
    scenarios: dict[str, list[tuple[int, str]]] = {}
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            lineup_name = (row.get("lineup_name") or "").strip()
            player_name = (row.get("player_name") or "").strip()
            lineup_spot = int((row.get("lineup_spot") or "").strip())
            scenarios.setdefault(lineup_name, []).append((lineup_spot, player_name))
    return {
        lineup_name: [name for _, name in sorted(assignments, key=lambda item: item[0])]
        for lineup_name, assignments in scenarios.items()
    }


if __name__ == "__main__":
    raise SystemExit(main())
