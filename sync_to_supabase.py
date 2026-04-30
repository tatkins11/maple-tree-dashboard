from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.utils.db import connect_db, connect_postgres_db, initialize_database


SYNC_TABLES = [
    "players",
    "player_identity",
    "player_aliases",
    "player_metadata",
    "season_rosters",
    "player_season_metadata",
    "games",
    "season_batting_stats",
    "player_game_batting",
    "hitter_projections",
    "schedule_games",
    "standings_snapshot",
    "league_schedule_games",
    "writeups",
]


def build_insert_sql(table: str, columns: list[str]) -> str:
    column_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"


def build_truncate_sql(tables: list[str]) -> str:
    return "TRUNCATE TABLE " + ", ".join(tables) + " RESTART IDENTITY CASCADE"


def sync_sqlite_to_postgres(
    *,
    sqlite_path: Path,
    database_url: str,
    replace: bool = True,
) -> dict[str, int]:
    sqlite_connection = connect_db(sqlite_path)
    postgres_connection = connect_postgres_db(database_url, autocommit=False)
    counts: dict[str, int] = {}
    try:
        initialize_database(sqlite_connection)
        if replace:
            postgres_connection.execute(build_truncate_sql(SYNC_TABLES))

        for table in SYNC_TABLES:
            columns = [
                str(row["name"])
                for row in sqlite_connection.execute(f"PRAGMA table_info({table})").fetchall()
            ]
            if not columns:
                counts[table] = 0
                continue
            rows = sqlite_connection.execute(
                f"SELECT {', '.join(columns)} FROM {table}"
            ).fetchall()
            counts[table] = len(rows)
            if not rows:
                continue
            postgres_connection.executemany(
                build_insert_sql(table, columns),
                [tuple(row[column] for column in columns) for row in rows],
            )

        postgres_connection.commit()
    except Exception:
        postgres_connection.rollback()
        raise
    finally:
        sqlite_connection.close()
        postgres_connection.close()
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync the local SQLite dashboard database to Supabase/Postgres.")
    parser.add_argument(
        "--sqlite-path",
        default="db/all_seasons_identity.sqlite",
        help="Local SQLite database path to copy from.",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Supabase/Postgres DATABASE_URL. Defaults to the DATABASE_URL environment variable.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append rows instead of replacing the hosted database. Replacement is safer for full weekly syncs.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Pass --database-url or set the DATABASE_URL environment variable.")
    counts = sync_sqlite_to_postgres(
        sqlite_path=Path(args.sqlite_path),
        database_url=args.database_url,
        replace=not args.append,
    )
    for table, count in counts.items():
        print(f"{table}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
