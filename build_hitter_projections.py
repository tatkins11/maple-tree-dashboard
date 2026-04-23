from __future__ import annotations

import argparse
from pathlib import Path

from src.models.projections import build_hitter_projection_table, build_hitter_projections
from src.models.season_metadata import (
    DEFAULT_SEASON_METADATA_PATH,
    ensure_player_season_metadata_file,
    sync_player_season_metadata,
)
from src.utils.db import connect_db, initialize_database, replace_hitter_projections


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build projection-ready hitter profiles for a selected season."
    )
    parser.add_argument(
        "--db-path",
        default="db/slowpitch_optimizer.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--projection-season",
        required=True,
        help="Season name to treat as the current season for blending.",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=25,
        help="Number of projection rows to print after building.",
    )
    parser.add_argument(
        "--season-metadata-path",
        default=str(DEFAULT_SEASON_METADATA_PATH),
        help="CSV file with player-season injury and weighting metadata.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        ensure_player_season_metadata_file(Path(args.season_metadata_path))
        sync_player_season_metadata(connection, Path(args.season_metadata_path))
        projections = build_hitter_projections(
            connection=connection,
            projection_season=args.projection_season,
        )
        replace_hitter_projections(connection, args.projection_season, projections)
    finally:
        connection.close()

    table = build_hitter_projection_table(projections)
    if table.empty:
        print("No projection rows were built.")
        return 0

    preview_columns = [
        "player_name",
        "current_plate_appearances",
        "career_plate_appearances",
        "current_season_weight",
        "weighted_prior_plate_appearances",
        "season_count_used",
        "consistency_score",
        "volatility_score",
        "trend_score",
        "p_single",
        "p_double",
        "p_triple",
        "p_home_run",
        "p_walk",
        "projected_strikeout_rate",
        "p_reached_on_error",
        "p_fielder_choice",
        "p_grounded_into_double_play",
        "p_out",
        "projected_on_base_rate",
        "projected_total_base_rate",
    ]
    print(table[preview_columns].head(args.show).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
