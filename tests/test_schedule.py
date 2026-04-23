from __future__ import annotations

from datetime import date
from pathlib import Path

from src.dashboard.data import (
    fetch_current_league_week,
    fetch_current_schedule_week,
    fetch_league_schedule_games,
    fetch_league_team_recent_results,
    fetch_league_team_summary,
    fetch_league_team_upcoming_games,
    fetch_league_team_week_opponents,
    fetch_week_scoreboard,
    fetch_next_game,
    fetch_schedule_games,
    fetch_schedule_opponents,
    fetch_schedule_season_summary,
    fetch_schedule_seasons,
    fetch_schedule_weeks,
    fetch_upcoming_schedule,
)
from src.models.schedule import import_schedule_bundle, update_game_result
from src.utils.db import connect_db, initialize_database


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _build_schedule_csv(tmp_path: Path) -> Path:
    return _write_text(
        tmp_path / "schedule.csv",
        "\n".join(
            [
                "game_id,season,league_name,division_name,week_label,game_date,game_time,team_name,opponent_name,home_away,location_or_field,status,completed_flag,is_bye,result,runs_for,runs_against,notes,source",
                "g1,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,6:30 PM,Maple Tree,Soft Ballz,home,Boncosky Blue,scheduled,0,0,,,,Opening,schedule.csv",
                "g2,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,7:30 PM,Maple Tree,Soft Ballz,away,Boncosky Blue,scheduled,0,0,,,,Opening,schedule.csv",
                "bye-week,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,,Maple Tree,,bye,,scheduled,0,1,,,,Bye week,schedule.csv",
                "g3,Spring 2026,Wednesday Men's,Blue Division,Week 3,2026-05-06,8:30 PM,Maple Tree,Wasted Talent,home,Boncosky Green,completed,1,0,W,19,14,Win,schedule.csv",
            ]
        ),
    )


def _build_standings_csv(tmp_path: Path) -> Path:
    return _write_text(
        tmp_path / "standings.csv",
        "\n".join(
            [
                "season,league_name,division_name,snapshot_date,team_name,wins,losses,ties,win_pct,games_back,notes,source",
                "Spring 2026,Wednesday Men's,Blue Division,2026-04-17,Maple Tree,0,0,0,0.000,0.0,Preseason,standings.csv",
                "Spring 2026,Wednesday Men's,Blue Division,2026-04-17,Soft Ballz,0,0,0,0.000,0.0,Preseason,standings.csv",
            ]
        ),
    )


def _build_league_schedule_csv(tmp_path: Path) -> Path:
    return _write_text(
        tmp_path / "league_schedule.csv",
        "\n".join(
            [
                "league_game_id,season,league_name,division_name,week_label,game_date,game_time,location_or_field,home_team,away_team,status,completed_flag,home_runs,away_runs,result_summary,notes,source",
                "lg1,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,6:30 PM,Boncosky Blue,Maple Tree,Soft Ballz,completed,1,22,18,,Opening,league_schedule.csv",
                "lg2,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,7:30 PM,Boncosky Red,No Dice,Wasted Talent,scheduled,0,,,,Opening,league_schedule.csv",
                "lg3,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,6:30 PM,Boncosky Blue,Bullseyes,Maple Tree,scheduled,0,,,,,league_schedule.csv",
                "lg4,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,7:30 PM,Boncosky Blue,Maple Tree,Bullseyes,scheduled,0,,,,,league_schedule.csv",
            ]
        ),
    )


def test_schedule_import_bundle_loads_rows(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        result = import_schedule_bundle(
            connection,
            _build_schedule_csv(tmp_path),
            _build_standings_csv(tmp_path),
        )
        assert result.games_imported == 4
        assert result.standings_rows_imported == 2

        seasons = fetch_schedule_seasons(connection)
        assert seasons == ["Spring 2026"]
        assert fetch_schedule_weeks(connection, "Spring 2026", "Maple Tree") == ["Week 1", "Week 2", "Week 3"]
        assert fetch_schedule_opponents(connection, "Spring 2026", "Maple Tree") == ["Soft Ballz", "Wasted Talent"]
    finally:
        connection.close()


def test_schedule_helpers_handle_byes_and_completed_filters(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None)

        upcoming = fetch_schedule_games(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            view_filter="Upcoming only",
            as_of=date(2026, 4, 23),
        )
        assert list(upcoming["week_label"]) == ["Week 2"]
        assert upcoming.iloc[0]["opponent_display"] == "BYE"

        completed = fetch_schedule_games(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            view_filter="Completed only",
            as_of=date(2026, 5, 7),
        )
        assert set(completed["week_label"]) == {"Week 1", "Week 3"}
        assert "W" in set(completed["result_display"])
    finally:
        connection.close()


def test_next_game_skips_bye_rows(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None)

        next_game = fetch_next_game(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            as_of=date(2026, 4, 23),
        )
        assert next_game is not None
        assert next_game["week_label"] == "Week 3"
        assert next_game["opponent_display"] == "Wasted Talent"
    finally:
        connection.close()


def test_current_week_helpers_default_to_next_relevant_week(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None, _build_league_schedule_csv(tmp_path))

        team_week = fetch_current_schedule_week(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            as_of=date(2026, 4, 23),
        )
        assert team_week == "Week 3"

        league_week = fetch_current_league_week(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
            as_of=date(2026, 4, 23),
        )
        assert league_week == "Week 2"
    finally:
        connection.close()


def test_upcoming_schedule_returns_sorted_games(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None)

        upcoming = fetch_upcoming_schedule(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            limit=10,
            as_of=date(2026, 4, 1),
        )
        assert list(upcoming["game_id"])[:3] == ["g1", "g2", "bye-week"]
    finally:
        connection.close()


def test_update_game_result_derives_result_and_summary(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None)

        updated = update_game_result(
            connection,
            game_id="g1",
            runs_for=22,
            runs_against=18,
            notes="Walk-off in the 7th",
        )
        assert updated == 1

        completed = fetch_schedule_games(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            view_filter="Completed only",
            as_of=date(2026, 4, 23),
        )
        row = completed.loc[completed["game_id"] == "g1"].iloc[0]
        assert row["result_display"] == "W"
        assert row["rf_ra_display"] == "22-18"
        assert row["status_display"] == "Final"

        summary = fetch_schedule_season_summary(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            as_of=date(2026, 5, 7),
        )
        assert summary["record"] == "2-0"
        assert summary["runs_for"] == 41
        assert summary["runs_against"] == 32
        assert summary["games_completed"] == 2
        assert summary["games_remaining"] == 1
    finally:
        connection.close()


def test_league_schedule_import_and_upsert(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        first_csv = _build_league_schedule_csv(tmp_path)
        result = import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None, first_csv)
        assert result.league_games_imported == 4

        rows = connection.execute("SELECT COUNT(*) FROM league_schedule_games").fetchone()[0]
        assert rows == 4

        updated_csv = _write_text(
            tmp_path / "league_schedule.csv",
            "\n".join(
                [
                    "league_game_id,season,league_name,division_name,week_label,game_date,game_time,location_or_field,home_team,away_team,status,completed_flag,home_runs,away_runs,result_summary,notes,source",
                    "lg1,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,6:30 PM,Boncosky Blue,Maple Tree,Soft Ballz,completed,1,23,18,,Updated,league_schedule.csv",
                    "lg2,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,7:30 PM,Boncosky Red,No Dice,Wasted Talent,completed,1,17,11,,Final,league_schedule.csv",
                    "lg3,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,6:30 PM,Boncosky Blue,Bullseyes,Maple Tree,scheduled,0,,,,,league_schedule.csv",
                    "lg4,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,7:30 PM,Boncosky Blue,Maple Tree,Bullseyes,scheduled,0,,,,,league_schedule.csv",
                ]
            ),
        )
        second = import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None, updated_csv)
        assert second.league_games_imported == 4
        rows = connection.execute("SELECT COUNT(*) FROM league_schedule_games").fetchone()[0]
        assert rows == 4
        updated_row = connection.execute(
            "SELECT home_runs, away_runs, notes FROM league_schedule_games WHERE league_game_id = 'lg1'"
        ).fetchone()
        assert updated_row["home_runs"] == 23
        assert updated_row["away_runs"] == 18
        assert updated_row["notes"] == "Updated"
    finally:
        connection.close()


def test_league_scouting_helpers_return_scoreboard_summary_and_team_views(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None, _build_league_schedule_csv(tmp_path))

        scoreboard = fetch_week_scoreboard(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
            week_label="Week 1",
            as_of=date(2026, 4, 23),
        )
        assert list(scoreboard["league_game_id"]) == ["lg1", "lg2"]
        assert scoreboard.iloc[0]["score_display"] == "18-22"
        assert scoreboard.iloc[0]["status_display"] == "Final"

        summary = fetch_league_team_summary(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            division_name="Blue Division",
            as_of=date(2026, 4, 23),
        )
        assert summary["record"] == "1-0"
        assert summary["runs_for"] == 22
        assert summary["runs_against"] == 18
        assert summary["games_completed"] == 1
        assert summary["games_remaining"] == 2

        recent = fetch_league_team_recent_results(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            division_name="Blue Division",
            limit=3,
            as_of=date(2026, 4, 23),
        )
        assert list(recent["league_game_id"]) == ["lg1"]

        upcoming = fetch_league_team_upcoming_games(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            division_name="Blue Division",
            limit=3,
            as_of=date(2026, 4, 23),
        )
        assert list(upcoming["league_game_id"]) == ["lg3", "lg4"]

        filtered = fetch_league_schedule_games(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
            team_name="Maple Tree",
            opponent="Bullseyes",
            view_filter="Upcoming only",
            as_of=date(2026, 4, 23),
        )
        assert set(filtered["league_game_id"]) == {"lg3", "lg4"}

        maple_tree_week_two = fetch_league_team_week_opponents(
            connection,
            season="Spring 2026",
            team_name="Maple Tree",
            week_label="Week 2",
            division_name="Blue Division",
            as_of=date(2026, 4, 23),
        )
        assert maple_tree_week_two == ["Bullseyes"]
    finally:
        connection.close()
