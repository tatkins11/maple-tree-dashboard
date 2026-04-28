from pathlib import Path

from src.models.season_roster import (
    fetch_active_roster_rows,
    import_season_roster,
    seed_availability_from_active_roster,
    sync_season_roster_additive,
)
from src.models.roster import load_available_player_names_with_active_roster_defaults
from src.utils.db import connect_db, get_or_create_player, initialize_database
from src.utils.player_identity import apply_manual_alias_overrides


def test_import_season_roster_matches_known_players_and_flags_unknowns(
    tmp_path: Path,
) -> None:
    connection = connect_db(tmp_path / "roster.sqlite")
    roster_csv = tmp_path / "roster.csv"
    roster_csv.write_text(
        "\n".join(
            [
                "season_name,player_name,active_flag,notes",
                "Current Spring,Tristan,yes,",
                "Current Spring,Corey,yes,",
                "Current Spring,Joey,yes,review me",
            ]
        ),
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        for player_id, player_name, canonical_name in [
            (1, "Tristan", "tristan"),
            (2, "Corey", "corey"),
        ]:
            connection.execute(
                "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
                (player_id, player_name, canonical_name),
            )
            connection.execute(
                "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
                (player_id, player_name, canonical_name),
            )
        initialize_database(connection)
        result = import_season_roster(connection, roster_csv, "Current Spring")
        rows = fetch_active_roster_rows(connection, "Current Spring")
    finally:
        connection.close()

    assert result.matched_count == 2
    assert any("Joey" in item for item in result.review_items)
    assert len(rows) == 2
    assert rows[0]["preferred_display_name"] == "Tristan"
    assert rows[0]["is_fixed_dhh"] == 1


def test_availability_defaults_to_active_roster_when_game_date_missing(
    tmp_path: Path,
) -> None:
    connection = connect_db(tmp_path / "roster.sqlite")
    availability_csv = tmp_path / "availability.csv"
    try:
        initialize_database(connection)
        connection.execute(
            "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan", "tristan"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (1, ?, ?, 1)",
            ("Tristan", "tristan"),
        )
        connection.execute(
            "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (2, ?, ?, 1)",
            ("Corey", "corey"),
        )
        connection.execute(
            "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (2, ?, ?, 1)",
            ("Corey", "corey"),
        )
        initialize_database(connection)
        connection.execute(
            """
            INSERT INTO season_rosters (season_name, player_id, source_name, active_flag, notes)
            VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
            """,
            (
                "Current Spring",
                1,
                "Tristan",
                1,
                None,
                "Current Spring",
                2,
                "Corey",
                1,
                None,
            ),
        )
        names = load_available_player_names_with_active_roster_defaults(
            connection=connection,
            csv_path=availability_csv,
            game_date="2026-04-20",
            season_name="Current Spring",
        )
    finally:
        connection.close()

    assert names == ["Tristan", "Corey"]


def test_import_season_roster_matches_approved_alias_and_uses_current_display_name(
    tmp_path: Path,
) -> None:
    connection = connect_db(tmp_path / "roster.sqlite")
    roster_csv = tmp_path / "roster.csv"
    alias_csv = tmp_path / "aliases.csv"
    roster_csv.write_text(
        "\n".join(
            [
                "season_name,player_name,active_flag,notes",
                "Current Spring,Joey,yes,approved alias",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    alias_csv.write_text(
        "\n".join(
            [
                "source_name,player_name,canonical_name,notes",
                "Joey,Snaxx,snaxx,approved alias",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        snaxx_player_id = get_or_create_player(connection, "Snaxx", "snaxx")
        apply_manual_alias_overrides(connection, alias_csv)
        initialize_database(connection)
        result = import_season_roster(connection, roster_csv, "Current Spring")
        rows = fetch_active_roster_rows(connection, "Current Spring")
    finally:
        connection.close()

    assert result.matched_count == 1
    assert result.review_items == []
    assert len(rows) == 1
    assert rows[0]["player_id"] == snaxx_player_id
    assert rows[0]["canonical_name"] == "snaxx"
    assert rows[0]["source_name"] == "Joey"
    assert rows[0]["preferred_display_name"] == "Joey"


def test_sync_season_roster_additive_adds_missing_player_without_deleting_existing_rows(
    tmp_path: Path,
) -> None:
    connection = connect_db(tmp_path / "roster.sqlite")
    roster_csv = tmp_path / "roster.csv"
    roster_csv.write_text(
        "\n".join(
            [
                "season_name,player_name,active_flag,notes",
                "Current Spring,Tristan,yes,",
                "Current Spring,Slomka,yes,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        for player_id, player_name, canonical_name in [
            (1, "Tristan", "tristan"),
            (2, "Corey", "corey"),
            (3, "Slomka", "slomka"),
        ]:
            connection.execute(
                "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
                (player_id, player_name, canonical_name),
            )
            connection.execute(
                "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
                (player_id, player_name, canonical_name),
            )
        initialize_database(connection)
        connection.execute(
            """
            INSERT INTO season_rosters (season_name, player_id, source_name, active_flag, notes)
            VALUES (?, ?, ?, 1, '')
            """,
            ("Current Spring", 2, "Corey"),
        )
        result = sync_season_roster_additive(connection, roster_csv, "Current Spring")
        rows = fetch_active_roster_rows(connection, "Current Spring")
    finally:
        connection.close()

    assert result.matched_count == 2
    assert result.review_items == []
    assert [row["preferred_display_name"] for row in rows] == ["Tristan", "Corey", "Slomka"]
