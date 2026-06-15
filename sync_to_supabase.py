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
    "admin_audit_log",
]


def build_insert_sql(table: str, columns: list[str]) -> str:
    column_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"


def build_truncate_sql(tables: list[str]) -> str:
    return "TRUNCATE TABLE " + ", ".join(tables) + " RESTART IDENTITY CASCADE"


def _count_rows(connection, table: str) -> int | None:
    """Row count for a table, or None if the table is absent on this connection."""
    try:
        row = connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
    except Exception:
        return None
    if row is None:
        return None
    return int(row["n"])


def build_sync_plan(sqlite_connection, postgres_connection, tables: list[str]) -> list[dict]:
    """Per-table comparison of local (source) vs hosted (target) row counts.

    ``would_lose`` is True when the hosted table has more rows than local — i.e.
    the truncate+reload would delete rows that exist only on hosted. Treating an
    absent local table as 0 rows means it also flags a table that exists hosted
    but not locally.
    """
    plan: list[dict] = []
    for table in tables:
        local = _count_rows(sqlite_connection, table)
        hosted = _count_rows(postgres_connection, table)
        delta = (local - hosted) if (local is not None and hosted is not None) else None
        plan.append(
            {
                "table": table,
                "local": local,
                "hosted": hosted,
                "delta": delta,
                "would_lose": hosted is not None and (local or 0) < hosted,
            }
        )
    return plan


def format_plan(plan: list[dict]) -> str:
    header = f"{'table':<26}{'local':>8}{'hosted':>8}{'delta':>8}  status"
    lines = [header, "-" * len(header)]
    for entry in plan:
        local = "-" if entry["local"] is None else str(entry["local"])
        hosted = "-" if entry["hosted"] is None else str(entry["hosted"])
        delta = "" if entry["delta"] is None else f"{entry['delta']:+d}"
        if entry["would_lose"]:
            status = "** WOULD LOSE ROWS **"
        elif entry["hosted"] is None:
            status = "new (create on hosted)"
        else:
            status = "ok"
        lines.append(f"{entry['table']:<26}{local:>8}{hosted:>8}{delta:>8}  {status}")
    return "\n".join(lines)


def sync_sqlite_to_postgres(
    *,
    sqlite_path: Path,
    database_url: str,
    tables: list[str] | None = None,
    replace: bool = True,
    force: bool = False,
    dry_run: bool = False,
    log=print,
) -> dict[str, int]:
    sqlite_connection = connect_db(sqlite_path)
    postgres_connection = connect_postgres_db(database_url, autocommit=False)
    sync_tables = list(tables) if tables else list(SYNC_TABLES)
    counts: dict[str, int] = {}
    try:
        initialize_database(sqlite_connection)

        plan = build_sync_plan(sqlite_connection, postgres_connection, sync_tables)
        log(format_plan(plan))
        losers = [entry for entry in plan if entry["would_lose"]]

        if dry_run:
            log("\nDry run — no changes written.")
            return counts

        if losers and not force:
            detail = "; ".join(
                f"{entry['table']} (hosted {entry['hosted']} > local {entry['local'] or 0})"
                for entry in losers
            )
            raise SystemExit(
                "Refusing to sync — these tables would LOSE rows that exist on hosted but not "
                f"local:\n  {detail}\n"
                "Reconcile local first, exclude them with --tables, or override with --force."
            )

        if replace:
            # Full sync truncates everything together (CASCADE handles the FK web).
            # A --tables subset truncates only the named tables WITHOUT cascade, so a
            # foreign-key reference from an un-synced table fails safe instead of
            # silently wiping dependent rows.
            truncate_sql = (
                build_truncate_sql(sync_tables)
                if tables is None
                else "TRUNCATE TABLE " + ", ".join(sync_tables) + " RESTART IDENTITY"
            )
            postgres_connection.execute(truncate_sql)

        for table in sync_tables:
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
        "--dry-run",
        action="store_true",
        help="Report per-table local/hosted row counts and flag any table that would lose hosted rows. Writes nothing.",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated subset of tables to sync (default: all). Only the named tables are truncated.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Proceed even if some tables would lose hosted rows. Use only after reviewing a --dry-run.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append rows instead of replacing the hosted tables. Replacement is safer for full weekly syncs.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required. Pass --database-url or set the DATABASE_URL environment variable.")

    tables = [name.strip() for name in args.tables.split(",") if name.strip()] or None
    if tables:
        unknown = [name for name in tables if name not in SYNC_TABLES]
        if unknown:
            raise SystemExit(
                f"Unknown table(s): {', '.join(unknown)}.\nKnown tables: {', '.join(SYNC_TABLES)}"
            )

    counts = sync_sqlite_to_postgres(
        sqlite_path=Path(args.sqlite_path),
        database_url=args.database_url,
        tables=tables,
        replace=not args.append,
        force=args.force,
        dry_run=args.dry_run,
    )
    if not args.dry_run:
        print("\nRows written:")
        for table, count in counts.items():
            print(f"  {table}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
