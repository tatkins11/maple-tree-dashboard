from __future__ import annotations

import argparse
from pathlib import Path

from src.models.season_roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_SEASON_ROSTER_PATH,
    fetch_active_roster_rows,
    import_season_roster,
)
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import or inspect the active season roster."
    )
    parser.add_argument(
        "--db-path",
        default="db/all_seasons_identity.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--season-name",
        default=DEFAULT_ACTIVE_ROSTER_SEASON,
        help="Season roster name.",
    )
    parser.add_argument(
        "--csv-path",
        default=str(DEFAULT_SEASON_ROSTER_PATH),
        help="CSV file with the season roster.",
    )
    parser.add_argument(
        "--mode",
        choices=["import", "inspect"],
        required=True,
        help="Import the season roster or inspect the current active roster.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        if args.mode == "import":
            result = import_season_roster(
                connection=connection,
                csv_path=Path(args.csv_path),
                season_name=args.season_name,
            )
            print(f"Imported matched roster rows: {result.matched_count}")
            if result.review_items:
                print("Roster review items:")
                for item in result.review_items:
                    print(f"- {item}")
        else:
            rows = fetch_active_roster_rows(connection, args.season_name)
            print(f"Active roster for {args.season_name}: {len(rows)} matched players")
            for row in rows:
                role = "DHH" if row["is_fixed_dhh"] else "BAT"
                print(
                    f"{row['preferred_display_name']} | canonical={row['canonical_name']} | "
                    f"role={role} | source_name={row['source_name']}"
                )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
