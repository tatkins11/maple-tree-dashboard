from pathlib import Path

import src.utils.db as db_utils
import sync_to_supabase
from sync_to_supabase import build_insert_sql, build_truncate_sql


def test_build_insert_sql_uses_sqlite_style_placeholders_for_adapter_translation() -> None:
    assert (
        build_insert_sql("writeups", ["season", "week_label", "phase"])
        == "INSERT INTO writeups (season, week_label, phase) VALUES (?, ?, ?)"
    )


def test_build_truncate_sql_replaces_dashboard_tables_in_dependency_order() -> None:
    sql = build_truncate_sql(["players", "writeups"])

    assert sql == "TRUNCATE TABLE players, writeups RESTART IDENTITY CASCADE"


def test_sync_sqlite_to_postgres_uses_transactional_postgres_connection(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class FakeSqliteConnection:
        def close(self) -> None:
            calls.append(("sqlite_close", None))

    class FakePostgresConnection:
        def commit(self) -> None:
            calls.append(("postgres_commit", None))

        def rollback(self) -> None:
            calls.append(("postgres_rollback", None))

        def close(self) -> None:
            calls.append(("postgres_close", None))

    monkeypatch.setattr(sync_to_supabase, "SYNC_TABLES", [])
    monkeypatch.setattr(sync_to_supabase, "connect_db", lambda path: FakeSqliteConnection())
    monkeypatch.setattr(sync_to_supabase, "initialize_database", lambda connection: None)

    def fake_connect_postgres_db(database_url: str, *, autocommit: bool = True):
        calls.append(("postgres_autocommit", autocommit))
        return FakePostgresConnection()

    monkeypatch.setattr(sync_to_supabase, "connect_postgres_db", fake_connect_postgres_db)

    counts = sync_to_supabase.sync_sqlite_to_postgres(
        sqlite_path=Path("ignored.sqlite"),
        database_url="postgresql://example",
        replace=False,
    )

    assert counts == {}
    assert ("postgres_autocommit", False) in calls


def test_connect_app_db_triggers_hosted_source_sync_before_connect(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, object, object]] = []
    fake_connection = object()

    monkeypatch.setattr(db_utils, "should_use_hosted_database", lambda: True)
    monkeypatch.setattr(db_utils, "get_database_url", lambda: "postgresql://example")

    def fake_sync(database_url: str) -> None:
        calls.append(("sync", database_url, None))

    def fake_connect_postgres_db(database_url: str, *, autocommit: bool = True):
        calls.append(("connect", database_url, autocommit))
        return fake_connection

    monkeypatch.setattr(db_utils, "_sync_hosted_database_from_repo_sources", fake_sync)
    monkeypatch.setattr(db_utils, "connect_postgres_db", fake_connect_postgres_db)

    result = db_utils.connect_app_db(tmp_path / "dashboard.sqlite")

    assert result is fake_connection
    assert calls == [
        ("sync", "postgresql://example", None),
        ("connect", "postgresql://example", True),
    ]


def test_repo_source_sync_paths_include_writeup_sources() -> None:
    paths = [path.as_posix() for path in db_utils._iter_repo_source_sync_paths()]

    assert any(path.endswith("/data/processed/writeups_manifest.csv") for path in paths)
    assert any(path.endswith("/data/writeups/maple-tree-spring-2026/week-2-postgame-recap.md") for path in paths)
