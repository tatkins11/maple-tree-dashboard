from __future__ import annotations

import argparse
from pathlib import Path

from src.models.season_metadata import (
    DEFAULT_SEASON_METADATA_PATH,
    ensure_player_season_metadata_file,
    sync_player_season_metadata,
)
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ensure and sync player-season metadata used by the hitter projection model."
    )
    parser.add_argument("--db-path", default="db/all_seasons_identity.sqlite", help="SQLite database path.")
    parser.add_argument("--metadata-path", default=str(DEFAULT_SEASON_METADATA_PATH), help="CSV path for player-season metadata.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    metadata_path = Path(args.metadata_path)
    ensure_player_season_metadata_file(metadata_path)
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        synced = sync_player_season_metadata(connection, metadata_path)
    finally:
        connection.close()
    print(f"metadata_path={metadata_path}")
    print(f"rows_synced={synced}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
