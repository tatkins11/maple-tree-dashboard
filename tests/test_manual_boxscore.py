from pathlib import Path

from src.dashboard.data import fetch_single_game_stats
from src.ingest.manual_boxscore import import_manual_boxscore_bundle
from src.utils.db import connect_db, initialize_database


def test_import_manual_boxscore_bundle_loads_game_batting_and_schedule(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "manual_boxscore.sqlite")
    games_csv = tmp_path / "game_boxscore_games.csv"
    batting_csv = tmp_path / "game_boxscore_batting.csv"
    alias_csv = tmp_path / "player_alias_overrides.csv"

    games_csv.write_text(
        "\n".join(
            [
                "game_key,season,team_name,game_date,game_time,opponent_name,team_score,opponent_score,notes,source",
                "soviet-sluggers-2021-05-26-1830-balls-deep,Soviet Sluggers Summer 2021,Soviet Sluggers,2021-05-26,6:30 PM,Balls Deep,6,12,Imported from screenshot,gamechanger_screenshot",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    batting_csv.write_text(
        "\n".join(
            [
                "game_key,lineup_spot,player_name,pa,ab,h,1b,2b,3b,hr,rbi,r,bb,so,sf,fc,gidp,outs,notes",
                "soviet-sluggers-2021-05-26-1830-balls-deep,1,Layshock,4,4,3,3,0,0,0,0,1,0,0,0,0,1,",
                "soviet-sluggers-2021-05-26-1830-balls-deep,2,Teo,4,2,1,0,0,0,1,4,1,1,0,1,0,0,1,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    alias_csv.write_text(
        "\n".join(
            [
                "source_name,player_name,canonical_name,notes",
                "Teo,Tristan,tristan,Approved alias: Teo and Tristan are the same player",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        initialize_database(connection)
        result = import_manual_boxscore_bundle(
            connection,
            games_csv_path=games_csv,
            batting_csv_path=batting_csv,
            alias_override_path=alias_csv,
        )
        game_row = connection.execute(
            """
            SELECT team_name, game_date, game_time, opponent_name, team_score, opponent_score, season, source_file
            FROM games
            """
        ).fetchone()
        schedule_row = connection.execute(
            """
            SELECT team_name, opponent_name, game_date, game_time, result, runs_for, runs_against, status, completed_flag
            FROM schedule_games
            """
        ).fetchone()
        stats = fetch_single_game_stats(connection, seasons=["Soviet Sluggers Summer 2021"], min_pa=0)
    finally:
        connection.close()

    assert result.games_imported == 1
    assert result.batting_rows_imported == 2
    assert result.schedule_rows_imported == 1
    assert game_row["team_name"] == "Soviet Sluggers"
    assert game_row["game_date"] == "2021-05-26"
    assert game_row["game_time"] == "6:30 PM"
    assert game_row["opponent_name"] == "Balls Deep"
    assert game_row["team_score"] == 6
    assert game_row["opponent_score"] == 12
    assert game_row["season"] == "Soviet Sluggers Summer 2021"
    assert game_row["source_file"] == "soviet-sluggers-2021-05-26-1830-balls-deep"
    assert schedule_row["result"] == "L"
    assert schedule_row["runs_for"] == 6
    assert schedule_row["runs_against"] == 12
    assert schedule_row["status"] == "final"
    assert schedule_row["completed_flag"] == 1
    assert {"game_date", "game_time", "team_name", "opponent", "player", "rbi", "ops"}.issubset(stats.columns)
    tristan_row = stats[stats["player"] == "Tristan"].iloc[0]
    assert tristan_row["game_time"] == "6:30 PM"
    assert tristan_row["opponent"] == "Balls Deep"
    assert tristan_row["rbi"] == 4
    assert tristan_row["hr"] == 1
