from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from src.models.lineup import DEFAULT_LINEUP_PATH, build_simulation_lineup
from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_AVAILABILITY_PATH,
    DEFAULT_LEAGUE_RULES_PATH,
    load_league_rules,
)
from src.models.season_projection import (
    simulate_season_projection,
    write_season_projection_csv,
    write_season_projection_report,
)
from src.utils.db import connect_db, initialize_database


DEFAULT_OUTPUT_CSV = Path("data/processed/season_projected_stats.csv")
DEFAULT_OUTPUT_REPORT_DIR = Path("data/audits")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project player season stat lines by simulating repeated full seasons from the current lineup."
    )
    parser.add_argument("--db-path", default="db/all_seasons_identity.sqlite", help="SQLite database path.")
    parser.add_argument("--projection-season", required=True, help="Projection season to use for hitter projections.")
    parser.add_argument("--game-date", required=True, help="Game date key used in availability and lineup CSV files.")
    parser.add_argument("--availability-path", default=str(DEFAULT_AVAILABILITY_PATH), help="CSV file with game-day player availability.")
    parser.add_argument("--lineup-path", default=str(DEFAULT_LINEUP_PATH), help="CSV file with manual batting order.")
    parser.add_argument("--league-rules-path", default=str(DEFAULT_LEAGUE_RULES_PATH), help="JSON file with league rules.")
    parser.add_argument("--roster-season", default=DEFAULT_ACTIVE_ROSTER_SEASON, help="Season roster name used to default availability when no rows exist for the game date.")
    parser.add_argument("--season-games", type=int, default=12, help="Number of games in the simulated season.")
    parser.add_argument("--simulated-seasons", type=int, default=5000, help="Number of simulated seasons to run.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible season projections.")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV), help="CSV output path for projected season stats.")
    parser.add_argument("--report-path", default="", help="Optional explicit audit report path.")
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
    result = simulate_season_projection(
        lineup=lineup,
        league_rules=rules,
        season_games=args.season_games,
        simulated_seasons=args.simulated_seasons,
        seed=args.seed,
    )

    output_csv = Path(args.output_csv)
    write_season_projection_csv(result, output_csv)

    report_path = (
        Path(args.report_path)
        if args.report_path
        else DEFAULT_OUTPUT_REPORT_DIR
        / f"season_projection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    write_season_projection_report(result, report_path)

    print("Lineup:")
    for row in result.lineup:
        dhh_label = "DHH" if row.is_fixed_dhh else "BAT"
        print(
            f"{row.lineup_spot}. {row.player_name} | role={dhh_label} | source={row.projection_source}"
        )

    print("")
    print("Team season summary:")
    print(f"season_games={result.season_games}")
    print(f"simulated_seasons={result.simulated_seasons}")
    print(f"average_runs_per_game={result.team_summary.average_runs_per_game:.3f}")
    print(f"median_runs_per_game={result.team_summary.median_runs_per_game:.3f}")
    print(f"average_runs_per_season={result.team_summary.average_runs_per_season:.3f}")
    print(f"p10_runs_per_season={result.team_summary.p10_runs_per_season:.3f}")
    print(f"p90_runs_per_season={result.team_summary.p90_runs_per_season:.3f}")

    print("")
    print("Projected player season stats:")
    for summary in result.player_summaries:
        print(
            f"{summary.lineup_spot}. {summary.player_name} | source={summary.projection_source} | "
            f"PA={summary.mean_plate_appearances:.2f} | AB={summary.mean_at_bats:.2f} | "
            f"1B={summary.mean_singles:.2f} | 2B={summary.mean_doubles:.2f} | 3B={summary.mean_triples:.2f} | "
            f"HR={summary.mean_home_runs:.2f} | BB={summary.mean_walks:.2f} | R={summary.mean_runs:.2f} | RBI={summary.mean_rbi:.2f} | "
            f"AVG={summary.mean_avg:.3f} | OBP={summary.mean_obp:.3f} | SLG={summary.mean_slg:.3f} | OPS={summary.mean_ops:.3f}"
        )

    print("")
    print(f"csv_output={output_csv}")
    print(f"report_output={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
