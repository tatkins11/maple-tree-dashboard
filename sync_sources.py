from __future__ import annotations

import argparse
from pathlib import Path

from src.ingest.pipeline import sync_sources
from src.utils.player_identity import DEFAULT_ALIAS_OVERRIDE_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync GameChanger season stats CSV sources into SQLite."
    )
    parser.add_argument(
        "--season-csv",
        action="append",
        default=[],
        help="Path to a GameChanger season stats CSV export. Repeat for multiple files.",
    )
    parser.add_argument(
        "--db-path",
        default="db/slowpitch_optimizer.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--audit-dir",
        default="data/audits",
        help="Directory where audit reports are written.",
    )
    parser.add_argument(
        "--alias-overrides",
        default=str(DEFAULT_ALIAS_OVERRIDE_PATH),
        help="CSV file containing approved player alias overrides.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    season_csv_paths = [Path(path) for path in args.season_csv]

    if not season_csv_paths:
        parser.error("Provide at least one --season-csv input.")

    report_path = sync_sources(
        db_path=Path(args.db_path),
        audit_dir=Path(args.audit_dir),
        season_csv_paths=season_csv_paths,
        alias_override_path=Path(args.alias_overrides),
    )
    print(f"Audit report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
