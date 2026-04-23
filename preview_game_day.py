from __future__ import annotations

import argparse
from pathlib import Path

from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_AVAILABILITY_PATH,
    DEFAULT_LEAGUE_RULES_PATH,
    ensure_availability_file,
    load_available_player_names_with_active_roster_defaults,
    load_league_rules,
    select_game_day_projections,
)
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preview the usable hitter projection set for one game day."
    )
    parser.add_argument(
        "--db-path",
        default="db/slowpitch_optimizer.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--projection-season",
        required=True,
        help="Season name used to select hitter projections.",
    )
    parser.add_argument(
        "--game-date",
        required=True,
        help="Game date key used in the availability CSV, for example 2026-04-20.",
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
        help="Season roster name used to default availability when no rows exist for the game date.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    availability_path = Path(args.availability_path)
    league_rules_path = Path(args.league_rules_path)
    ensure_availability_file(availability_path)
    rules = load_league_rules(league_rules_path)

    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        available_names = load_available_player_names_with_active_roster_defaults(
            connection=connection,
            csv_path=availability_path,
            game_date=args.game_date,
            season_name=args.roster_season,
        )
        rows = select_game_day_projections(
            connection=connection,
            projection_season=args.projection_season,
            available_player_names=available_names,
        )
    finally:
        connection.close()

    print("League rules:")
    print(
        f"innings={rules.innings_per_game}, steals_allowed={rules.steals_allowed}, "
        f"fixed_dhh_enabled={rules.fixed_dhh_enabled}, "
        f"max_home_runs_non_dhh={rules.max_home_runs_non_dhh}, "
        f"ignore_slaughter_rule={rules.ignore_slaughter_rule}"
    )
    print("")
    print(f"Available players for {args.game_date}: {len(available_names)}")
    print(f"Usable projections returned: {len(rows)}")
    if not rows:
        return 0

    for row in rows:
        dhh_label = "DHH" if row.is_fixed_dhh else "BAT"
        print(
            f"{row.preferred_display_name} | role={dhh_label} | source={row.projection_source} | fixed_dhh={row.is_fixed_dhh} | "
            f"speed={row.speed_flag} | base={row.baserunning_grade} | "
            f"consistency={row.consistency_grade} | obp={row.projected_on_base_rate:.3f} | "
            f"tb_rate={row.projected_total_base_rate:.3f} | k_rate={row.projected_strikeout_rate:.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
