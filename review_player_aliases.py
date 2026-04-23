from __future__ import annotations

import argparse
from pathlib import Path

from src.utils.db import connect_db
from src.utils.player_identity import fetch_identity_review_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review current player identities and alias mappings."
    )
    parser.add_argument(
        "--db-path",
        default="db/slowpitch_optimizer.sqlite",
        help="SQLite database path.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        rows = fetch_identity_review_rows(connection)
    finally:
        connection.close()

    current_player_id = None
    for row in rows:
        if row["player_id"] != current_player_id:
            current_player_id = row["player_id"]
            print(
                f"[{row['player_id']}] {row['player_name']} "
                f"(canonical: {row['canonical_name']})"
            )
        if row["source_name"] is not None:
            approved = "approved" if row["approved_flag"] else "review"
            print(
                f"  - alias: {row['source_name']} | source_type={row['source_type']} "
                f"| method={row['match_method']} | {approved}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
