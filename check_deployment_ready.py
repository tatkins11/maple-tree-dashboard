from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.dashboard.data import (
    DEFAULT_DASHBOARD_SEASON,
    fetch_saved_writeups,
    fetch_schedule_season_summary,
    fetch_team_summary,
)
from src.utils.db import connect_db, initialize_database


def _mask_secret(value: str) -> str:
    if not value:
        return "missing"
    if len(value) <= 8:
        return "set"
    return f"{value[:4]}...{value[-4:]}"


def check_local_database(sqlite_path: Path) -> list[str]:
    messages: list[str] = []
    if not sqlite_path.exists():
        return [f"FAIL local database not found: {sqlite_path}"]

    connection = connect_db(sqlite_path)
    try:
        initialize_database(connection)
        team_summary = fetch_team_summary(connection, DEFAULT_DASHBOARD_SEASON)
        schedule_summary = fetch_schedule_season_summary(connection, season=DEFAULT_DASHBOARD_SEASON)
        saved_postgames = fetch_saved_writeups(
            connection,
            season=DEFAULT_DASHBOARD_SEASON,
            phase="postgame",
        )
    finally:
        connection.close()

    messages.append(
        f"OK local stats: {int(team_summary['team_games'])} games, "
        f"{int(team_summary['hitters'])} hitters, {int(team_summary['plate_appearances'])} PA"
    )
    messages.append(
        f"OK local schedule: record {schedule_summary['record']}, "
        f"{int(schedule_summary['games_completed'])} completed"
    )
    messages.append(f"OK saved postgames: {len(saved_postgames)}")
    return messages


def check_secrets() -> list[str]:
    database_url = os.getenv("DATABASE_URL", "")
    viewer_password = os.getenv("VIEWER_PASSWORD", "")
    admin_password = os.getenv("ADMIN_PASSWORD", "")
    app_mode = os.getenv("APP_MODE", "")
    messages = [
        f"APP_MODE: {app_mode or 'missing'}",
        f"DATABASE_URL: {_mask_secret(database_url)}",
        f"VIEWER_PASSWORD: {_mask_secret(viewer_password)}",
        f"ADMIN_PASSWORD: {_mask_secret(admin_password)}",
    ]
    if not database_url:
        messages.append("NEXT set DATABASE_URL before running sync_to_supabase.py")
    if not viewer_password or not admin_password:
        messages.append("NEXT set VIEWER_PASSWORD and ADMIN_PASSWORD in Streamlit secrets before sharing")
    return messages


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check local readiness for hosted team dashboard deployment.")
    parser.add_argument(
        "--sqlite-path",
        default="db/all_seasons_identity.sqlite",
        help="Local SQLite dashboard database path.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    for message in check_local_database(Path(args.sqlite_path)):
        print(message)
    for message in check_secrets():
        print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
