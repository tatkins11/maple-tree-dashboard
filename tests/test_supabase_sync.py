from sync_to_supabase import build_insert_sql, build_truncate_sql


def test_build_insert_sql_uses_sqlite_style_placeholders_for_adapter_translation() -> None:
    assert (
        build_insert_sql("writeups", ["season", "week_label", "phase"])
        == "INSERT INTO writeups (season, week_label, phase) VALUES (?, ?, ?)"
    )


def test_build_truncate_sql_replaces_dashboard_tables_in_dependency_order() -> None:
    sql = build_truncate_sql(["players", "writeups"])

    assert sql == "TRUNCATE TABLE players, writeups RESTART IDENTITY CASCADE"

