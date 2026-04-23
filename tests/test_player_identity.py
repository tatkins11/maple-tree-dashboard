import sqlite3
from pathlib import Path

from src.utils.db import connect_db, initialize_database
from src.utils.player_identity import (
    apply_manual_alias_overrides,
    resolve_player,
)


def test_manual_alias_override_wins_exact_alias_match(tmp_path: Path) -> None:
    db_path = tmp_path / "identity.sqlite"
    alias_csv = tmp_path / "player_alias_overrides.csv"
    alias_csv.write_text(
        "\n".join(
            [
                "source_name,player_name,canonical_name,notes",
                "T. Atkins,Tristan,tristan,approved alias",
            ]
        ),
        encoding="utf-8",
    )
    connection = connect_db(db_path)
    try:
        initialize_database(connection)
        apply_manual_alias_overrides(connection, alias_csv)
        resolution = resolve_player(connection, "T. Atkins", "test.csv")
    finally:
        connection.close()

    assert resolution.player_id is not None
    assert resolution.status == "exact_alias_match"


def test_exact_canonical_name_match_reuses_existing_identity(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "identity.sqlite")
    try:
        initialize_database(connection)
        connection.execute(
            "INSERT INTO players (player_name, canonical_name, active_flag) VALUES (?, ?, 1)",
            ("Jane Smith", "jane smith"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Jane Smith", "jane smith"),
        )
        resolution = resolve_player(connection, "Jane Smith", "test.csv")
    finally:
        connection.close()

    assert resolution.player_id == 1
    assert resolution.status == "exact_canonical_name_match"


def test_ambiguous_normalized_alias_requires_manual_review(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "identity.sqlite")
    try:
        initialize_database(connection)
        connection.execute(
            "INSERT INTO players (player_name, canonical_name, active_flag) VALUES (?, ?, 1)",
            ("Tristan Atkins", "tristan atkins"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan Atkins", "tristan atkins"),
        )
        connection.execute(
            "INSERT INTO players (player_name, canonical_name, active_flag) VALUES (?, ?, 1)",
            ("Tyler Atkins", "tyler atkins"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (2, ?, ?, 1)",
            ("Tyler Atkins", "tyler atkins"),
        )
        connection.execute(
            """
            INSERT INTO player_aliases (
                player_id, source_name, normalized_source_name, source_type, source_file, match_method, approved_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "T Atkins", "t atkins", "manual_override", "aliases.csv", "manual_override", 1),
        )
        connection.execute(
            """
            INSERT INTO player_aliases (
                player_id, source_name, normalized_source_name, source_type, source_file, match_method, approved_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (2, "T Atkins Alt", "t atkins", "manual_override", "aliases.csv", "manual_override", 1),
        )
        resolution = resolve_player(connection, "T. Atkins", "test.csv")
    finally:
        connection.close()

    assert resolution.player_id is None
    assert resolution.status == "manual_review"
