from pathlib import Path

import pytest

import src.utils.db as db_utils
import sync_to_supabase
from sync_to_supabase import build_insert_sql, build_sync_plan, build_truncate_sql


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


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Minimal connection double that answers COUNT/PRAGMA/SELECT by table name."""

    def __init__(self, *, counts=None, columns=None, table_rows=None):
        self.counts = counts or {}
        self.columns = columns or {}
        self.table_rows = table_rows or {}
        self.executed: list[str] = []
        self.executemany_calls: list[tuple] = []
        self.committed = False
        self.rolled_back = False

    def execute(self, sql, params=None):
        self.executed.append(sql)
        text = sql.strip()
        upper = text.upper()
        if upper.startswith("PRAGMA TABLE_INFO"):
            table = text[text.index("(") + 1 : text.rindex(")")]
            return _FakeCursor([{"name": c} for c in self.columns.get(table, [])])
        if upper.startswith("SELECT COUNT(*)"):
            table = text.rsplit("FROM", 1)[1].strip()
            n = self.counts.get(table)
            return _FakeCursor([] if n is None else [{"n": n}])
        if upper.startswith("SELECT"):
            table = text.rsplit("FROM", 1)[1].strip()
            return _FakeCursor(self.table_rows.get(table, []))
        return _FakeCursor([])  # TRUNCATE, etc.

    def executemany(self, sql, seq):
        self.executemany_calls.append((sql, list(seq)))
        return _FakeCursor([])

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _patch_sync(monkeypatch, local, hosted, sync_tables):
    monkeypatch.setattr(sync_to_supabase, "SYNC_TABLES", sync_tables)
    monkeypatch.setattr(sync_to_supabase, "connect_db", lambda path: local)
    monkeypatch.setattr(sync_to_supabase, "connect_postgres_db", lambda url, *, autocommit=True: hosted)
    monkeypatch.setattr(sync_to_supabase, "initialize_database", lambda connection: None)


def test_build_sync_plan_flags_tables_that_would_lose_rows() -> None:
    local = _FakeConn(counts={"a": 5, "b": 3})
    hosted = _FakeConn(counts={"a": 5, "b": 9, "c": 2})  # b ahead on hosted; c missing locally
    plan = {entry["table"]: entry for entry in build_sync_plan(local, hosted, ["a", "b", "c"])}

    assert plan["a"]["would_lose"] is False  # equal counts
    assert plan["a"]["delta"] == 0
    assert plan["b"]["would_lose"] is True  # hosted 9 > local 3
    assert plan["b"]["delta"] == -6
    assert plan["c"]["would_lose"] is True  # absent locally (0) < hosted 2


def test_sync_refuses_when_a_table_would_lose_hosted_rows(monkeypatch) -> None:
    local = _FakeConn(counts={"a": 5}, columns={"a": ["x"]}, table_rows={"a": [{"x": 1}]})
    hosted = _FakeConn(counts={"a": 9})  # hosted ahead -> would lose 4 rows
    _patch_sync(monkeypatch, local, hosted, ["a"])

    with pytest.raises(SystemExit):
        sync_to_supabase.sync_sqlite_to_postgres(
            sqlite_path=Path("ignored"), database_url="postgresql://example", log=lambda *_: None
        )

    assert not any("TRUNCATE" in sql.upper() for sql in hosted.executed)
    assert hosted.executemany_calls == []
    assert hosted.committed is False


def test_dry_run_reports_plan_but_writes_nothing(monkeypatch) -> None:
    local = _FakeConn(counts={"a": 5}, columns={"a": ["x"]}, table_rows={"a": [{"x": 1}]})
    hosted = _FakeConn(counts={"a": 9})
    _patch_sync(monkeypatch, local, hosted, ["a"])

    messages: list[str] = []
    counts = sync_to_supabase.sync_sqlite_to_postgres(
        sqlite_path=Path("ignored"),
        database_url="postgresql://example",
        dry_run=True,
        log=messages.append,
    )

    assert counts == {}
    assert not any("TRUNCATE" in sql.upper() for sql in hosted.executed)
    assert hosted.executemany_calls == []
    assert any("WOULD LOSE" in message for message in messages)


def test_force_overrides_the_lose_guard_and_writes(monkeypatch) -> None:
    local = _FakeConn(counts={"a": 5}, columns={"a": ["x"]}, table_rows={"a": [{"x": 1}, {"x": 2}]})
    hosted = _FakeConn(counts={"a": 9})
    _patch_sync(monkeypatch, local, hosted, ["a"])

    counts = sync_to_supabase.sync_sqlite_to_postgres(
        sqlite_path=Path("ignored"),
        database_url="postgresql://example",
        force=True,
        log=lambda *_: None,
    )

    assert counts == {"a": 2}
    assert any("TRUNCATE" in sql.upper() for sql in hosted.executed)
    assert len(hosted.executemany_calls) == 1
    assert hosted.committed is True


def test_subset_sync_truncates_only_named_tables_without_cascade(monkeypatch) -> None:
    local = _FakeConn(counts={"a": 5, "b": 5}, columns={"a": ["x"]}, table_rows={"a": [{"x": 1}]})
    hosted = _FakeConn(counts={"a": 5, "b": 5})
    _patch_sync(monkeypatch, local, hosted, ["a", "b"])

    sync_to_supabase.sync_sqlite_to_postgres(
        sqlite_path=Path("ignored"),
        database_url="postgresql://example",
        tables=["a"],  # subset
        log=lambda *_: None,
    )

    truncates = [sql for sql in hosted.executed if "TRUNCATE" in sql.upper()]
    assert len(truncates) == 1
    assert "CASCADE" not in truncates[0].upper()  # subset must not cascade
    assert " b" not in truncates[0]  # only table 'a' truncated
