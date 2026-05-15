"""Tests for admin audit logging, undo flow, and schedule CRUD helpers."""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.models.audit import (
    ACTION_ALIAS_REASSIGN,
    ACTION_GAME_RESULT_UPDATE,
    ACTION_SCHEDULE_GAME_CREATE,
    ACTION_SCHEDULE_GAME_UPDATE,
    AuditError,
    fetch_recent_audit_log,
    is_latest_active_entry,
    undo_audit_entry,
)
from src.models.schedule import (
    SCHEDULE_CSV_FIELDNAMES,
    create_schedule_game,
    ensure_schedule_csv,
    fetch_schedule_game_row,
    import_schedule_csv,
    record_game_result,
    update_schedule_game_fields,
    write_schedule_csv_from_db,
)
from src.utils.db import connect_db, initialize_database
from src.utils.player_identity import reassign_alias


SEASON = "Maple Tree Spring 2026"
TEAM = "Maple Tree"


def _write_seed_csv(tmp_path: Path) -> Path:
    csv_path = tmp_path / "team_schedule.csv"
    rows = [
        {
            "game_id": "g1",
            "season": SEASON,
            "league_name": "Wednesday Men's",
            "division_name": "Blue Division",
            "week_label": "Week 1",
            "game_date": "2026-04-22",
            "game_time": "6:30 PM",
            "team_name": TEAM,
            "opponent_name": "Soft Ballz",
            "home_away": "home",
            "location_or_field": "Boncosky Blue",
            "status": "scheduled",
            "completed_flag": "0",
            "is_bye": "0",
            "result": "",
            "runs_for": "",
            "runs_against": "",
            "notes": "",
            "source": "seed.csv",
        },
        {
            "game_id": "g2",
            "season": SEASON,
            "league_name": "Wednesday Men's",
            "division_name": "Blue Division",
            "week_label": "Week 2",
            "game_date": "2026-04-29",
            "game_time": "7:30 PM",
            "team_name": TEAM,
            "opponent_name": "Wasted Talent",
            "home_away": "away",
            "location_or_field": "Boncosky Red",
            "status": "scheduled",
            "completed_flag": "0",
            "is_bye": "0",
            "result": "",
            "runs_for": "",
            "runs_against": "",
            "notes": "",
            "source": "seed.csv",
        },
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCHEDULE_CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return csv_path


def _make_db(tmp_path: Path):
    db_path = tmp_path / "admin.sqlite"
    connection = connect_db(db_path)
    initialize_database(connection)
    return connection


def test_record_game_result_logs_audit_and_writes_csv(tmp_path: Path) -> None:
    csv_path = _write_seed_csv(tmp_path)
    connection = _make_db(tmp_path)
    try:
        import_schedule_csv(connection, csv_path)

        record_game_result(
            connection,
            game_id="g1",
            runs_for=12,
            runs_against=8,
            csv_path=csv_path,
        )

        row = fetch_schedule_game_row(connection, "g1")
        assert row is not None
        assert row["runs_for"] == 12
        assert row["runs_against"] == 8
        assert row["result"] == "W"
        assert int(row["completed_flag"]) == 1

        log = fetch_recent_audit_log(connection)
        assert len(log) == 1
        assert log.iloc[0]["action_type"] == ACTION_GAME_RESULT_UPDATE
        assert log.iloc[0]["entity_id"] == "g1"

        with csv_path.open("r", encoding="utf-8") as handle:
            csv_rows = list(csv.DictReader(handle))
        g1_row = next(row for row in csv_rows if row["game_id"] == "g1")
        assert g1_row["runs_for"] == "12"
        assert g1_row["result"] == "W"
        assert g1_row["completed_flag"] == "1"
    finally:
        connection.close()


def test_undo_game_result_restores_prior_state(tmp_path: Path) -> None:
    csv_path = _write_seed_csv(tmp_path)
    connection = _make_db(tmp_path)
    try:
        import_schedule_csv(connection, csv_path)
        record_game_result(
            connection,
            game_id="g1",
            runs_for=10,
            runs_against=5,
            csv_path=csv_path,
        )
        log = fetch_recent_audit_log(connection)
        audit_id = int(log.iloc[0]["audit_id"])

        message = undo_audit_entry(connection, audit_id, schedule_csv_path=csv_path)
        assert "Restored" in message

        row = fetch_schedule_game_row(connection, "g1")
        assert row is not None
        assert row["runs_for"] is None
        assert row["runs_against"] is None
        assert row["result"] in (None, "")
        assert int(row["completed_flag"]) == 0

        log = fetch_recent_audit_log(connection)
        assert int(log.iloc[0]["undone_flag"]) == 1

        with csv_path.open("r", encoding="utf-8") as handle:
            csv_rows = list(csv.DictReader(handle))
        g1_row = next(row for row in csv_rows if row["game_id"] == "g1")
        assert g1_row["runs_for"] == ""
        assert g1_row["completed_flag"] == "0"
    finally:
        connection.close()


def test_undo_rejects_stale_entries(tmp_path: Path) -> None:
    csv_path = _write_seed_csv(tmp_path)
    connection = _make_db(tmp_path)
    try:
        import_schedule_csv(connection, csv_path)
        record_game_result(connection, game_id="g1", runs_for=1, runs_against=0, csv_path=csv_path)
        first_audit_id = int(fetch_recent_audit_log(connection).iloc[0]["audit_id"])
        # second edit on the same game
        record_game_result(connection, game_id="g1", runs_for=5, runs_against=3, csv_path=csv_path)

        assert is_latest_active_entry(connection, audit_id=first_audit_id) is False
        with pytest.raises(AuditError):
            undo_audit_entry(connection, first_audit_id, schedule_csv_path=csv_path)
    finally:
        connection.close()


def test_create_schedule_game_logs_audit_and_can_be_undone(tmp_path: Path) -> None:
    csv_path = _write_seed_csv(tmp_path)
    connection = _make_db(tmp_path)
    try:
        import_schedule_csv(connection, csv_path)
        new_id = create_schedule_game(
            connection,
            season=SEASON,
            game_date="2026-05-06",
            team_name=TEAM,
            opponent_name="Eagles",
            game_time="6:30 PM",
            home_away="home",
            location_or_field="Boncosky Blue",
            week_label="Week 3",
            csv_path=csv_path,
        )
        assert fetch_schedule_game_row(connection, new_id) is not None

        log = fetch_recent_audit_log(connection)
        assert log.iloc[0]["action_type"] == ACTION_SCHEDULE_GAME_CREATE
        audit_id = int(log.iloc[0]["audit_id"])

        undo_audit_entry(connection, audit_id, schedule_csv_path=csv_path)
        assert fetch_schedule_game_row(connection, new_id) is None

        with csv_path.open("r", encoding="utf-8") as handle:
            game_ids = {row["game_id"] for row in csv.DictReader(handle)}
        assert new_id not in game_ids
    finally:
        connection.close()


def test_update_schedule_game_fields_logs_and_undoes(tmp_path: Path) -> None:
    csv_path = _write_seed_csv(tmp_path)
    connection = _make_db(tmp_path)
    try:
        import_schedule_csv(connection, csv_path)
        update_schedule_game_fields(
            connection,
            game_id="g1",
            updates={"opponent_name": "Wasted Talent", "notes": "Rescheduled"},
            csv_path=csv_path,
        )
        row = fetch_schedule_game_row(connection, "g1")
        assert row["opponent_name"] == "Wasted Talent"
        assert row["notes"] == "Rescheduled"

        log = fetch_recent_audit_log(connection)
        assert log.iloc[0]["action_type"] == ACTION_SCHEDULE_GAME_UPDATE
        audit_id = int(log.iloc[0]["audit_id"])

        undo_audit_entry(connection, audit_id, schedule_csv_path=csv_path)
        row = fetch_schedule_game_row(connection, "g1")
        assert row["opponent_name"] == "Soft Ballz"
        assert row["notes"] in (None, "")
    finally:
        connection.close()


def test_reassign_alias_logs_and_undoes(tmp_path: Path) -> None:
    connection = _make_db(tmp_path)
    try:
        # Seed two identities + an alias attached to player 1
        for player_id, name in [(1, "Tristan"), (2, "Joey")]:
            connection.execute(
                "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
                (player_id, name, name.lower()),
            )
            connection.execute(
                "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
                (player_id, name, name.lower()),
            )
        connection.execute(
            """
            INSERT INTO player_aliases (
                alias_id, player_id, source_name, normalized_source_name,
                source_type, source_file, match_method, approved_flag
            ) VALUES (10, 1, 'Tristen', 'tristen', 'season_csv', 'seed.csv', 'safe_normalized_match', 0)
            """
        )
        connection.commit()

        outcome = reassign_alias(connection, alias_id=10, new_player_id=2, approve=True)
        assert outcome["changed"] is True

        row = connection.execute(
            "SELECT player_id, approved_flag FROM player_aliases WHERE alias_id = 10"
        ).fetchone()
        assert int(row["player_id"]) == 2
        assert int(row["approved_flag"]) == 1

        log = fetch_recent_audit_log(connection)
        assert log.iloc[0]["action_type"] == ACTION_ALIAS_REASSIGN
        audit_id = int(log.iloc[0]["audit_id"])

        undo_audit_entry(connection, audit_id)
        row = connection.execute(
            "SELECT player_id FROM player_aliases WHERE alias_id = 10"
        ).fetchone()
        assert int(row["player_id"]) == 1
    finally:
        connection.close()


def test_write_schedule_csv_from_db_preserves_columns(tmp_path: Path) -> None:
    csv_path = _write_seed_csv(tmp_path)
    connection = _make_db(tmp_path)
    try:
        import_schedule_csv(connection, csv_path)
        target = tmp_path / "exported.csv"
        ensure_schedule_csv(target)
        count = write_schedule_csv_from_db(connection, target)
        assert count == 2
        with target.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == SCHEDULE_CSV_FIELDNAMES
            rows = list(reader)
        assert {row["game_id"] for row in rows} == {"g1", "g2"}
    finally:
        connection.close()
