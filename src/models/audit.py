"""Admin audit log: write, fetch, and undo administrative mutations.

The audit log captures schedule game edits, alias reassignments, and other
admin-only mutations so they can be reviewed and rolled back from the UI.
Each entry records a JSON before/after snapshot so undo can restore prior
state without recomputing it.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Mapping

import pandas as pd


ACTION_GAME_RESULT_UPDATE = "game_result_update"
ACTION_SCHEDULE_GAME_UPDATE = "schedule_game_update"
ACTION_SCHEDULE_GAME_CREATE = "schedule_game_create"
ACTION_ALIAS_REASSIGN = "alias_reassign"

ENTITY_SCHEDULE_GAME = "schedule_game"
ENTITY_PLAYER_ALIAS = "player_alias"

UNDOABLE_ACTIONS: frozenset[str] = frozenset(
    {
        ACTION_GAME_RESULT_UPDATE,
        ACTION_SCHEDULE_GAME_UPDATE,
        ACTION_SCHEDULE_GAME_CREATE,
        ACTION_ALIAS_REASSIGN,
    }
)


class AuditError(RuntimeError):
    pass


def log_audit_entry(
    connection: sqlite3.Connection,
    *,
    action_type: str,
    entity_type: str,
    entity_id: str,
    summary: str,
    before_state: Mapping[str, Any] | None = None,
    after_state: Mapping[str, Any] | None = None,
    actor: str = "admin",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO admin_audit_log (
            action_type,
            entity_type,
            entity_id,
            summary,
            before_state,
            after_state,
            actor
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            action_type,
            entity_type,
            str(entity_id),
            summary,
            _serialize_state(before_state),
            _serialize_state(after_state),
            actor,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def fetch_recent_audit_log(
    connection: sqlite3.Connection,
    *,
    limit: int = 50,
    include_undone: bool = True,
) -> pd.DataFrame:
    where_clause = "" if include_undone else "WHERE undone_flag = 0"
    return pd.read_sql_query(
        f"""
        SELECT
            audit_id,
            created_at,
            action_type,
            entity_type,
            entity_id,
            summary,
            actor,
            undone_flag,
            undone_at
        FROM admin_audit_log
        {where_clause}
        ORDER BY audit_id DESC
        LIMIT ?
        """,
        connection,
        params=(int(limit),),
    )


def is_latest_active_entry(
    connection: sqlite3.Connection,
    *,
    audit_id: int,
) -> bool:
    row = connection.execute(
        "SELECT entity_type, entity_id, undone_flag FROM admin_audit_log WHERE audit_id = ?",
        (int(audit_id),),
    ).fetchone()
    if row is None:
        return False
    if int(row["undone_flag"]) == 1:
        return False
    later = connection.execute(
        """
        SELECT 1 FROM admin_audit_log
        WHERE entity_type = ?
          AND entity_id = ?
          AND undone_flag = 0
          AND audit_id > ?
        LIMIT 1
        """,
        (str(row["entity_type"]), str(row["entity_id"]), int(audit_id)),
    ).fetchone()
    return later is None


def undo_audit_entry(
    connection: sqlite3.Connection,
    audit_id: int,
    *,
    schedule_csv_path=None,
) -> str:
    row = connection.execute(
        "SELECT * FROM admin_audit_log WHERE audit_id = ?",
        (int(audit_id),),
    ).fetchone()
    if row is None:
        raise AuditError(f"Audit entry {audit_id} not found.")
    if int(row["undone_flag"]) == 1:
        raise AuditError(f"Audit entry {audit_id} has already been undone.")
    if not is_latest_active_entry(connection, audit_id=int(audit_id)):
        raise AuditError(
            "A more recent change for the same item must be undone first."
        )

    action_type = str(row["action_type"])
    entity_id = str(row["entity_id"])
    before_state = _deserialize_state(row["before_state"])
    after_state = _deserialize_state(row["after_state"])

    if action_type in {ACTION_GAME_RESULT_UPDATE, ACTION_SCHEDULE_GAME_UPDATE}:
        from src.models.schedule import _restore_schedule_game_row, write_schedule_csv_from_db

        if not before_state:
            raise AuditError("No prior state recorded for this change; cannot undo.")
        _restore_schedule_game_row(connection, before_state)
        if schedule_csv_path is not None:
            write_schedule_csv_from_db(connection, schedule_csv_path)
        summary = f"Restored schedule game {entity_id} to prior state."
    elif action_type == ACTION_SCHEDULE_GAME_CREATE:
        from src.models.schedule import write_schedule_csv_from_db

        connection.execute(
            "DELETE FROM schedule_games WHERE game_id = ?", (entity_id,)
        )
        if schedule_csv_path is not None:
            write_schedule_csv_from_db(connection, schedule_csv_path)
        summary = f"Removed created schedule game {entity_id}."
    elif action_type == ACTION_ALIAS_REASSIGN:
        if not before_state or "player_id" not in before_state:
            raise AuditError("No prior alias mapping recorded; cannot undo.")
        connection.execute(
            "UPDATE player_aliases SET player_id = ? WHERE alias_id = ?",
            (int(before_state["player_id"]), int(entity_id)),
        )
        summary = f"Restored alias {entity_id} to player_id {before_state['player_id']}."
    else:
        raise AuditError(f"Audit action '{action_type}' does not support undo.")

    connection.execute(
        """
        UPDATE admin_audit_log
        SET undone_flag = 1,
            undone_at = CURRENT_TIMESTAMP
        WHERE audit_id = ?
        """,
        (int(audit_id),),
    )
    connection.commit()
    return summary


def _serialize_state(state: Mapping[str, Any] | None) -> str | None:
    if state is None:
        return None
    return json.dumps(dict(state), sort_keys=True, default=str)


def _deserialize_state(value: Any) -> dict[str, Any] | None:
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        return dict(value)
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return None
