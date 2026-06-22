from __future__ import annotations

from datetime import date
from pathlib import Path

from src.dashboard.data import (
    build_schedule_filter_summary,
    fetch_current_league_week,
    fetch_current_schedule_week,
    fetch_enriched_standings_snapshot,
    fetch_previous_completed_league_week,
    fetch_league_schedule_games,
    fetch_league_standings_enrichment,
    fetch_league_team_recent_results,
    fetch_league_team_summary,
    fetch_league_team_upcoming_games,
    fetch_league_team_week_opponents,
    fetch_week_scoreboard,
    fetch_next_game,
    fetch_schedule_games,
    fetch_seed_race,
    fetch_schedule_opponents,
    fetch_schedule_season_summary,
    fetch_schedule_seasons,
    fetch_schedule_weeks,
    fetch_team_weekly_results,
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


def test_schedule_filter_summary_and_nan_cleanup_inputs_are_supported() -> None:
    summary = build_schedule_filter_summary(
        [
            ("Season", "Spring 2026"),
            ("Team", "Maple Tree"),
            ("Opponent", "All opponents"),
        ]
    )
    assert summary == "Season: Spring 2026 | Team: Maple Tree | Opponent: All opponents"


def test_current_week_helpers_include_bye_weeks_for_team_schedule(tmp_path: Path) -> None:
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
        assert team_week == "Week 2"

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
        assert list(scoreboard["league_game_id"]) == ["lg1"]
        assert scoreboard.iloc[0]["score_display"] == "18-22"
        assert scoreboard.iloc[0]["league_result_display"] == "Maple Tree def. Soft Ballz, 22-18"
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
        assert recent.iloc[0]["team_result_display"] == "W 22-18 vs Soft Ballz"

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
        assert set(filtered["team_result_display"]) == {""}

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


def test_previous_completed_league_week_returns_latest_finished_week(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None, _build_league_schedule_csv(tmp_path))

        previous_week = fetch_previous_completed_league_week(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
            as_of=date(2026, 4, 23),
        )
        assert previous_week == "Week 1"
    finally:
        connection.close()


def test_enriched_standings_include_runs_for_against_and_diff(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), _build_standings_csv(tmp_path), _build_league_schedule_csv(tmp_path))

        enrichment = fetch_league_standings_enrichment(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
            as_of=date(2026, 4, 23),
        )
        maple_tree = enrichment.loc[enrichment["team_name"] == "Maple Tree"].iloc[0]
        assert maple_tree["runs_for"] == 22
        assert maple_tree["runs_against"] == 18
        assert maple_tree["run_diff"] == 4

        standings = fetch_enriched_standings_snapshot(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
        )
        assert list(standings["team_name"]) == ["Maple Tree", "Soft Ballz"]
        soft_ballz = standings.loc[standings["team_name"] == "Soft Ballz"].iloc[0]
        assert soft_ballz["runs_for"] == 18
        assert soft_ballz["runs_against"] == 22
        assert soft_ballz["run_diff"] == -4
    finally:
        connection.close()


def test_enriched_standings_default_to_zero_when_no_completed_games_exist(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), _build_standings_csv(tmp_path), None)

        standings = fetch_enriched_standings_snapshot(
            connection,
            season="Spring 2026",
            division_name="Blue Division",
        )
        maple_tree = standings.loc[standings["team_name"] == "Maple Tree"].iloc[0]
        assert maple_tree["runs_for"] == 0
        assert maple_tree["runs_against"] == 0
        assert maple_tree["run_diff"] == 0
    finally:
        connection.close()


def _build_weekly_results_csv(tmp_path: Path) -> Path:
    return _write_text(
        tmp_path / "weekly_results.csv",
        "\n".join(
            [
                "game_id,season,league_name,division_name,week_label,game_date,game_time,team_name,opponent_name,home_away,location_or_field,status,completed_flag,is_bye,result,runs_for,runs_against,notes,source",
                # Week 1 doubleheader -> split (win game 1, lose game 2). Out-of-order times to prove sorting.
                "w1b,Spring 2026,Wednesday Men's,Blue,Week 1,2026-05-06,7:30 PM,Maple Tree,Soft Ballz,away,Field,completed,1,0,L,5,12,,results.csv",
                "w1a,Spring 2026,Wednesday Men's,Blue,Week 1,2026-05-06,6:30 PM,Maple Tree,Soft Ballz,home,Field,completed,1,0,W,19,14,,results.csv",
                # Week 2 doubleheader -> swept (both wins).
                "w2a,Spring 2026,Wednesday Men's,Blue,Week 2,2026-05-13,6:30 PM,Maple Tree,Wasted Talent,home,Field,completed,1,0,W,10,2,,results.csv",
                "w2b,Spring 2026,Wednesday Men's,Blue,Week 2,2026-05-13,7:30 PM,Maple Tree,Wasted Talent,away,Field,completed,1,0,W,8,7,,results.csv",
                # Week 3 single completed game -> lone loss.
                "w3a,Spring 2026,Wednesday Men's,Blue,Week 3,2026-05-20,6:30 PM,Maple Tree,No Dice,home,Field,completed,1,0,L,4,16,,results.csv",
                # Week 4 scheduled (no result) + a bye -> both excluded.
                "w4a,Spring 2026,Wednesday Men's,Blue,Week 4,2026-05-27,6:30 PM,Maple Tree,Bullseyes,home,Field,scheduled,0,0,,,,,results.csv",
                "w4bye,Spring 2026,Wednesday Men's,Blue,Week 5,2026-06-03,,Maple Tree,,bye,,scheduled,0,1,,,,,results.csv",
            ]
        ),
    )


def test_team_weekly_results_summarize_each_game_day(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_weekly_results_csv(tmp_path), None)

        results = fetch_team_weekly_results(connection)
        by_date = {row["game_date"]: row for _, row in results.iterrows()}

        # Only the three completed game days appear; the scheduled game and the bye are excluded.
        assert set(by_date) == {"2026-05-06", "2026-05-13", "2026-05-20"}

        split = by_date["2026-05-06"]
        assert split["games"] == 2 and split["wins"] == 1 and split["losses"] == 1
        assert split["runs_for"] == 24 and split["runs_against"] == 26
        # Scores are ordered by game time (6:30 before 7:30) despite the CSV row order.
        assert split["result_display"] == "Split 1-1 (19-14, 5-12)"

        swept = by_date["2026-05-13"]
        assert swept["wins"] == 2 and swept["losses"] == 0
        assert swept["result_display"] == "Swept 2-0 (10-2, 8-7)"

        single = by_date["2026-05-20"]
        assert single["games"] == 1
        assert single["result_display"] == "L 4-16"

        # Season filter narrows to the same set; an unknown season yields nothing.
        assert len(fetch_team_weekly_results(connection, season="Spring 2026")) == 3
        assert fetch_team_weekly_results(connection, season="Nonexistent 2099").empty
    finally:
        connection.close()


def _build_seed_race_league_csv(tmp_path: Path) -> Path:
    return _write_text(
        tmp_path / "seed_league.csv",
        "\n".join(
            [
                "league_game_id,season,league_name,division_name,week_label,game_date,game_time,location_or_field,home_team,away_team,status,completed_flag,home_runs,away_runs,result_summary,notes,source",
                # Week 1: Maple Tree and No Dice both win big.
                "sg1,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,6:30 PM,Field,Maple Tree,Soft Ballz,completed,1,20,10,,,seed.csv",
                "sg2,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,7:30 PM,Field,No Dice,Bullseyes,completed,1,15,5,,,seed.csv",
                # Week 2: Maple Tree beats No Dice (2-0); Soft Ballz beats Bullseyes.
                "sg3,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,6:30 PM,Field,Maple Tree,No Dice,completed,1,12,8,,,seed.csv",
                "sg4,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,7:30 PM,Field,Soft Ballz,Bullseyes,completed,1,14,7,,,seed.csv",
                # Week 3: still on the schedule (one game left for everyone).
                "sg5,Spring 2026,Wednesday Men's,Blue Division,Week 3,2026-05-06,6:30 PM,Field,Maple Tree,Bullseyes,scheduled,0,,,,,seed.csv",
                "sg6,Spring 2026,Wednesday Men's,Blue Division,Week 3,2026-05-06,7:30 PM,Field,No Dice,Soft Ballz,scheduled,0,,,,,seed.csv",
            ]
        ),
    )


def test_seed_race_orders_by_winpct_then_run_diff(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "schedule.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(
            connection,
            _build_schedule_csv(tmp_path),
            None,
            _build_seed_race_league_csv(tmp_path),
        )

        race = fetch_seed_race(connection, "Spring 2026", team_name="Maple Tree")
        standings = race["standings"]

        # Maple Tree (2-0) is the #1 seed; the headline speaks from our point of view.
        assert race["leader"] == "Maple Tree"
        assert race["team_seed"] == 1
        assert race["games_played_total"] == 8  # 4 completed games x 2 teams each
        assert "Maple Tree holds the #1 seed" in race["headline"]

        # No Dice and Soft Ballz are both 1-1; run differential breaks the tie (No Dice +6 > -3).
        order = list(standings["team_name"])
        assert order == ["Maple Tree", "No Dice", "Soft Ballz", "Bullseyes"]
        no_dice = standings[standings["team_name"] == "No Dice"].iloc[0]
        soft_ballz = standings[standings["team_name"] == "Soft Ballz"].iloc[0]
        assert no_dice["seed"] == 2 and soft_ballz["seed"] == 3
        assert no_dice["run_diff"] > soft_ballz["run_diff"]

        # Games back is measured from the top seed; everyone still has one game left.
        by_team = {row["team_name"]: row for _, row in standings.iterrows()}
        assert by_team["Maple Tree"]["games_back"] == 0
        assert by_team["No Dice"]["games_back"] == 1.0
        assert by_team["Bullseyes"]["games_back"] == 2.0
        assert set(standings["games_remaining"]) == {1}
        assert by_team["Maple Tree"]["max_wins"] == 3  # 2 wins + 1 game left

        # An unknown season produces an empty board, not an error.
        empty = fetch_seed_race(connection, "Nonexistent 2099")
        assert empty["standings"].empty
        assert empty["team_seed"] is None
    finally:
        connection.close()
