from __future__ import annotations

import argparse
import csv
from pathlib import Path

from src.utils.db import connect_db, initialize_database


DEFAULT_METADATA_CSV = Path("data/processed/player_metadata.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export or import player metadata for roster setup."
    )
    parser.add_argument(
        "--db-path",
        default="db/slowpitch_optimizer.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--csv-path",
        default=str(DEFAULT_METADATA_CSV),
        help="CSV path for player metadata export/import.",
    )
    parser.add_argument(
        "--mode",
        choices=["export", "import"],
        required=True,
        help="Export metadata to CSV or import edited CSV back into SQLite.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    db_path = Path(args.db_path)
    csv_path = Path(args.csv_path)
    connection = connect_db(db_path)
    try:
        initialize_database(connection)
        if args.mode == "export":
            export_player_metadata(connection, csv_path)
            print(f"Exported player metadata to {csv_path}")
        else:
            import_player_metadata(connection, csv_path)
            print(f"Imported player metadata from {csv_path}")
    finally:
        connection.close()
    return 0


def export_player_metadata(connection, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows = connection.execute(
        """
        SELECT
            pm.player_id,
            pm.preferred_display_name,
            pm.is_fixed_dhh,
            pm.baserunning_grade,
            pm.consistency_grade,
            pm.speed_flag,
            pm.active_flag,
            COALESCE(pm.notes, '') AS notes,
            pi.player_name,
            pi.canonical_name
        FROM player_metadata pm
        JOIN player_identity pi ON pi.player_id = pm.player_id
        ORDER BY pm.preferred_display_name COLLATE NOCASE
        """
    ).fetchall()
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "player_id",
                "preferred_display_name",
                "is_fixed_dhh",
                "baserunning_grade",
                "consistency_grade",
                "speed_flag",
                "active_flag",
                "notes",
                "player_name",
                "canonical_name",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def import_player_metadata(connection, csv_path: Path) -> None:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            connection.execute(
                """
                UPDATE player_metadata
                SET
                    preferred_display_name = ?,
                    is_fixed_dhh = ?,
                    baserunning_grade = ?,
                    consistency_grade = ?,
                    speed_flag = ?,
                    active_flag = ?,
                    notes = ?
                WHERE player_id = ?
                """,
                (
                    (row.get("preferred_display_name") or "").strip(),
                    _to_bool_int(row.get("is_fixed_dhh")),
                    (row.get("baserunning_grade") or "C").strip() or "C",
                    (row.get("consistency_grade") or "C").strip() or "C",
                    _to_bool_int(row.get("speed_flag")),
                    _to_bool_int(row.get("active_flag"), default=1),
                    (row.get("notes") or "").strip() or None,
                    int(row["player_id"]),
                ),
            )
    connection.commit()


def _to_bool_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    return default


if __name__ == "__main__":
    raise SystemExit(main())
