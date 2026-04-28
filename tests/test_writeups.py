from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.dashboard.data import (
    WRITEUP_BYE_WEEK_MESSAGE,
    WRITEUP_EMPTY_OPPONENT_SCOUTING,
    WRITEUP_INVALID_DOUBLEHEADER_MESSAGE,
    fetch_saved_writeup,
    fetch_saved_writeups,
    fetch_maple_tree_week_bundle,
    fetch_writeup_milestone_watch,
    fetch_writeup_opponent_scouting,
    save_weekly_writeup,
)
from src.dashboard.writeups import (
    annotate_pregame_lineup,
    build_postgame_markdown,
    build_pregame_key_lines,
    build_pregame_markdown,
    build_pregame_overview_insight_lines,
    resolve_postgame_games,
)
from src.models.roster import DEFAULT_ACTIVE_ROSTER_SEASON
from src.models.schedule import import_schedule_bundle
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
                "g1,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,6:30 PM,Maple Tree,Soft Ballz,home,Boncosky Blue,completed,1,0,W,22,18,Opening,schedule.csv",
                "g2,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,7:30 PM,Maple Tree,Soft Ballz,away,Boncosky Blue,scheduled,0,0,,,,schedule.csv",
                "bye-week,Spring 2026,Wednesday Men's,Blue Division,Week 2,2026-04-29,,Maple Tree,,bye,,scheduled,0,1,,,,Bye week,schedule.csv",
                "g3,Spring 2026,Wednesday Men's,Blue Division,Week 3,2026-05-06,8:30 PM,Maple Tree,Wasted Talent,home,Boncosky Green,scheduled,0,0,,,,schedule.csv",
            ]
        ),
    )


def _build_league_schedule_csv(tmp_path: Path, *, completed_soft_ballz: bool) -> Path:
    status = "completed" if completed_soft_ballz else "scheduled"
    completed_flag = "1" if completed_soft_ballz else "0"
    home_runs = "21" if completed_soft_ballz else ""
    away_runs = "17" if completed_soft_ballz else ""
    return _write_text(
        tmp_path / "league_schedule.csv",
        "\n".join(
            [
                "league_game_id,season,league_name,division_name,week_label,game_date,game_time,location_or_field,home_team,away_team,status,completed_flag,home_runs,away_runs,result_summary,notes,source",
                f"lg1,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,6:30 PM,Boncosky Blue,Maple Tree,Soft Ballz,{status},{completed_flag},{home_runs},{away_runs},,Opening,league_schedule.csv",
                "lg2,Spring 2026,Wednesday Men's,Blue Division,Week 1,2026-04-22,7:30 PM,Boncosky Red,No Dice,Wasted Talent,scheduled,0,,,,Opening,league_schedule.csv",
            ]
        ),
    )


def _build_standings_csv(tmp_path: Path) -> Path:
    return _write_text(
        tmp_path / "standings.csv",
        "\n".join(
            [
                "season,league_name,division_name,snapshot_date,team_name,wins,losses,ties,win_pct,games_back,notes,source",
                "Spring 2026,Wednesday Men's,Blue Division,2026-04-23,Soft Ballz,2,0,0,1.000,0.0,Week 1,standings.csv",
            ]
        ),
    )


def _insert_player(connection, player_id: int, player_name: str, canonical_name: str) -> None:
    connection.execute(
        "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )
    connection.execute(
        "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )
    connection.execute(
        """
        INSERT INTO player_metadata (
            player_id, preferred_display_name, is_fixed_dhh, baserunning_grade,
            consistency_grade, speed_flag, active_flag, notes
        ) VALUES (?, ?, 0, 'C', 'C', 0, 1, '')
        """,
        (player_id, player_name),
    )


def _insert_season_row(
    connection,
    *,
    season: str,
    player_id: int,
    games: int,
    pa: int,
    ab: int,
    hits: int,
    singles: int,
    doubles: int,
    triples: int,
    hr: int,
    walks: int,
    runs: int,
    rbi: int,
    tb: int,
    raw_source_file: str,
) -> None:
    avg = hits / ab if ab else 0
    obp = (hits + walks) / (ab + walks) if (ab + walks) else 0
    slg = tb / ab if ab else 0
    ops = obp + slg
    connection.execute(
        """
        INSERT INTO season_batting_stats (
            season, player_id, games, plate_appearances, at_bats, hits, singles, doubles, triples,
            home_runs, walks, strikeouts, hit_by_pitch, sacrifice_hits, sacrifice_flies,
            reached_on_error, fielder_choice, grounded_into_double_play, runs, rbi, total_bases,
            batting_average, on_base_percentage, slugging_percentage, ops,
            batting_average_risp, two_out_rbi, left_on_base, raw_source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?)
        """,
        (
            season,
            player_id,
            games,
            pa,
            ab,
            hits,
            singles,
            doubles,
            triples,
            hr,
            walks,
            runs,
            rbi,
            tb,
            avg,
            obp,
            slg,
            ops,
            raw_source_file,
        ),
    )


def test_fetch_maple_tree_week_bundle_returns_doubleheader_in_order(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(
            connection,
            _build_schedule_csv(tmp_path),
            None,
            _build_league_schedule_csv(tmp_path, completed_soft_ballz=False),
        )

        bundle = fetch_maple_tree_week_bundle(
            connection,
            season="Spring 2026",
            week_label="Week 1",
        )
    finally:
        connection.close()

    assert bundle["generation_enabled"] is True
    assert list(bundle["non_bye_games"]["game_id"]) == ["g1", "g2"]
    assert bundle["opponent_names"] == ["Soft Ballz"]


def test_fetch_writeup_opponent_scouting_uses_fixed_empty_state_when_no_completed_results(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(
            connection,
            _build_schedule_csv(tmp_path),
            None,
            _build_league_schedule_csv(tmp_path, completed_soft_ballz=False),
        )

        scouting_lines = fetch_writeup_opponent_scouting(
            connection,
            season="Spring 2026",
            opponent_names=["Soft Ballz"],
            division_name="Blue Division",
        )
    finally:
        connection.close()

    assert scouting_lines == [WRITEUP_EMPTY_OPPONENT_SCOUTING]


def test_fetch_writeup_opponent_scouting_uses_two_part_standings_record(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(
            connection,
            _build_schedule_csv(tmp_path),
            _build_standings_csv(tmp_path),
            _build_league_schedule_csv(tmp_path, completed_soft_ballz=True),
        )

        scouting_lines = fetch_writeup_opponent_scouting(
            connection,
            season="Spring 2026",
            opponent_names=["Soft Ballz"],
            division_name="Blue Division",
        )
    finally:
        connection.close()

    assert len(scouting_lines) == 1
    assert "standings 2-0" in scouting_lines[0]
    assert "standings 2-0-0" not in scouting_lines[0]
    assert "scores 17.0/game and allows 21.0/game" in scouting_lines[0]
    assert "Maple Tree scores 22.0/game and allows 18.0/game" in scouting_lines[0]


def test_fetch_writeup_milestone_watch_returns_clean_active_roster_lines(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        _insert_player(connection, 1, "Jj", "jj")
        _insert_player(connection, 2, "Bench", "bench")
        connection.execute(
            "INSERT INTO season_rosters (season_name, player_id, source_name, active_flag, notes) VALUES (?, ?, ?, 1, '')",
            (DEFAULT_ACTIVE_ROSTER_SEASON, 1, "Jj"),
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=1,
            games=12,
            pa=50,
            ab=46,
            hits=23,
            singles=17,
            doubles=4,
            triples=1,
            hr=3,
            walks=4,
            runs=18,
            rbi=16,
            tb=40,
            raw_source_file="jj.csv",
        )
        _insert_season_row(
            connection,
            season="Maple Tree Fall 2025",
            player_id=2,
            games=12,
            pa=52,
            ab=47,
            hits=24,
            singles=18,
            doubles=4,
            triples=1,
            hr=1,
            walks=5,
            runs=17,
            rbi=15,
            tb=33,
            raw_source_file="bench.csv",
        )
        connection.commit()

        lines = fetch_writeup_milestone_watch(connection, distance_threshold=5, limit=5)
    finally:
        connection.close()

    assert any("Jj is 2 away from 25 Hits" in line for line in lines)
    assert any(line.startswith("First into club watch:") for line in lines)
    assert all(line.strip() for line in lines)
    assert all(line.endswith(".") for line in lines)
    assert not any("Bench" in line for line in lines)


def test_postgame_helpers_build_one_combined_doubleheader_recap(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None)
        bundle = fetch_maple_tree_week_bundle(
            connection,
            season="Spring 2026",
            week_label="Week 1",
        )
        non_bye_games = bundle["non_bye_games"]

        resolved_games, errors = resolve_postgame_games(
            non_bye_games,
            {
                "g1": {
                    "team_score": "",
                    "opponent_score": "",
                    "headline": "Maple Tree jumped out early and never gave the opener back.",
                    "standout_1": "Jj kept the pressure on at the top.",
                    "standout_2": "Glove drove in traffic in the middle innings.",
                    "improvement": "Finish innings cleaner on defense.",
                },
                "g2": {
                    "team_score": "15",
                    "opponent_score": "19",
                    "headline": "The second game flipped after a late crooked number.",
                    "standout_1": "Tim stayed productive even while chasing the game.",
                    "standout_2": "",
                    "improvement": "Control the damage after free baserunners.",
                },
            },
        )
    finally:
        connection.close()

    assert errors == []
    assert resolved_games[0].team_score == 22
    assert resolved_games[0].opponent_score == 18
    assert resolved_games[1].team_score == 15
    assert resolved_games[1].opponent_score == 19

    markdown = build_postgame_markdown(
        season="Spring 2026",
        week_bundle=bundle,
        resolved_games=resolved_games,
        weekly_summary_note="The split felt competitive, but the late-game execution has to sharpen up.",
        week_mvp="Jj",
        context_lines=["Current milestone watch: Jj is 2 away from 25 Hits."],
    )

    assert markdown.count("# Week 1 Postgame Recap") == 1
    assert "## Weekly Result Summary" in markdown
    assert "## Game 1 Recap" in markdown
    assert "## Game 2 Recap" in markdown
    assert "Maple Tree went 1-1 in Week 1, scoring 37 runs and allowing 37 across the doubleheader." in markdown
    assert "## Week MVP" in markdown
    assert "## Milestone/Record Context" in markdown


def test_saved_postgame_writeups_upsert_and_list_from_database(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        first_id = save_weekly_writeup(
            connection,
            season="Maple Tree Spring 2026",
            week_label="Week 1",
            phase="postgame",
            markdown="# Week 1 Postgame Recap\n\nOriginal recap.\n",
            source="test",
        )
        second_id = save_weekly_writeup(
            connection,
            season="Maple Tree Spring 2026",
            week_label="Week 1",
            phase="postgame",
            title="Week 1 Saved Recap",
            markdown="# Week 1 Saved Recap\n\nUpdated recap.\n",
            source="test-update",
        )

        saved = fetch_saved_writeup(
            connection,
            season="Maple Tree Spring 2026",
            week_label="Week 1",
            phase="postgame",
        )
        archive = fetch_saved_writeups(
            connection,
            season="Maple Tree Spring 2026",
            phase="postgame",
        )
    finally:
        connection.close()

    assert first_id == second_id
    assert saved is not None
    assert saved["title"] == "Week 1 Saved Recap"
    assert "Updated recap." in str(saved["markdown"])
    assert len(archive) == 1
    assert archive.iloc[0]["source"] == "test-update"


def test_build_pregame_markdown_uses_cleaner_lineup_stats_and_manager_tone() -> None:
    lineup_rows = [
        {
            "spot": 1,
            "player": "Jj",
            "lineup_note": "",
            "proj_obp": 0.616,
            "proj_run_rate": 0.420,
            "proj_rbi_rate": 0.210,
            "proj_xbh_rate": 0.311,
            "season_pa": 8,
            "season_r": 5,
            "season_rbi": 6,
            "season_avg": 0.800,
            "season_obp": 0.750,
            "season_slg": 2.600,
            "season_ops": 3.350,
        },
        {
            "spot": 2,
            "player": "Glove",
            "lineup_note": "",
            "proj_obp": 0.682,
            "proj_run_rate": 0.530,
            "proj_rbi_rate": 0.460,
            "proj_xbh_rate": 0.444,
            "season_pa": 8,
            "season_r": 5,
            "season_rbi": 5,
            "season_avg": 0.625,
            "season_obp": 0.625,
            "season_slg": 1.000,
            "season_ops": 1.625,
        },
        {
            "spot": 3,
            "player": "Tristan",
            "lineup_note": "DHH",
            "proj_obp": 0.674,
            "proj_run_rate": 0.410,
            "proj_rbi_rate": 0.625,
            "proj_xbh_rate": 0.512,
            "season_pa": 8,
            "season_r": 5,
            "season_rbi": 6,
            "season_avg": 0.800,
            "season_obp": 0.750,
            "season_slg": 2.600,
            "season_ops": 3.350,
        },
        {
            "spot": 4,
            "player": "Tim",
            "lineup_note": "",
            "proj_obp": 0.706,
            "proj_run_rate": 0.470,
            "proj_rbi_rate": 0.509,
            "proj_xbh_rate": 0.488,
            "season_pa": 8,
            "season_r": 2,
            "season_rbi": 2,
            "season_avg": 0.500,
            "season_obp": 0.500,
            "season_slg": 0.750,
            "season_ops": 1.250,
        },
        {
            "spot": 5,
            "player": "Kives",
            "lineup_note": "",
            "proj_obp": 0.616,
            "proj_run_rate": 0.330,
            "proj_rbi_rate": 0.427,
            "proj_xbh_rate": 0.377,
            "season_pa": 7,
            "season_r": 2,
            "season_rbi": 5,
            "season_avg": 0.571,
            "season_obp": 0.571,
            "season_slg": 0.714,
            "season_ops": 1.286,
        },
    ]
    markdown = build_pregame_markdown(
        season="Spring 2026",
        week_bundle={
            "week_label": "Week 1",
            "opponent_names": ["Soft Ballz"],
            "non_bye_games": pd.DataFrame(
                [
                    {
                        "date_display": "Wed 04/22/26",
                        "time_display": "6:30 PM",
                        "location_or_field": "Boncosky Blue",
                    },
                    {
                        "date_display": "Wed 04/22/26",
                        "time_display": "7:30 PM",
                        "location_or_field": "Boncosky Blue",
                    },
                ]
            ),
        },
        season_summary={
            "record": "0-0",
            "runs_for": 0,
            "runs_against": 0,
            "games_completed": 0,
        },
        lineup_rows=lineup_rows,
        milestone_lines=["Glove is 1 away from 15 HR (1 in club)."],
        opponent_lines=[WRITEUP_EMPTY_OPPONENT_SCOUTING],
        key_lines=build_pregame_key_lines(
            lineup_rows=[
                {"player": "Jj"},
                {"player": "Glove"},
                {"player": "Tristan"},
                {"player": "Tim"},
                {"player": "Kives"},
                {"player": "Porter"},
            ],
            milestone_lines=["Glove is 1 away from 15 HR (1 in club)."],
            opponent_lines=[WRITEUP_EMPTY_OPPONENT_SCOUTING],
            week_bundle={"week_label": "Week 1"},
            season_summary={"wins": 0, "losses": 0, "games_completed": 0, "runs_for": 0, "runs_against": 0},
        ),
        overview_insight_lines=build_pregame_overview_insight_lines(
            lineup_rows,
            projected_runs_per_game=16.4,
            lineup_season_summary={
                "pa": 39,
                "runs": 19,
                "rbi": 24,
                "home_runs": 4,
                "avg": 0.659,
                "obp": 0.640,
                "slg": 1.590,
                "ops": 2.230,
            },
        ),
    )

    assert "Projection snapshot: the recommended order simulates to 16.4 runs per game." in markdown
    assert (
        "Current season lineup snapshot: tonight's available group is slashing 0.659/0.640/1.590 (2.230 OPS) with 19 runs, 4 homers, and 24 RBI across 39 PA."
    ) in markdown
    assert (
        "Biggest lineup edge: Glove, Tristan, Tim form the heaviest pressure pocket with average projected RBI rate 0.531 "
        "and XBH rate 0.481."
    ) in markdown
    assert "1. Jj - Run-pressure engine (Proj run rate 0.420) Current season: 0.800/0.750/2.600 (3.350 OPS) in 8 PA, 5 R, 6 RBI." in markdown
    assert "2. Glove - Basepath agitator (Proj run rate 0.530) Current season: 0.625/0.625/1.000 (1.625 OPS) in 8 PA, 5 R, 5 RBI." in markdown
    assert "3. Tristan (DHH) - Traffic finisher (Proj RBI rate 0.625) Current season: 0.800/0.750/2.600 (3.350 OPS) in 8 PA, 5 R, 6 RBI." in markdown
    assert "4. Tim - Extra-base menace (Proj XBH rate 0.488) Current season: 0.500/0.500/0.750 (1.250 OPS) in 8 PA, 2 R, 2 RBI." in markdown
    assert "5. Kives - Crooked-number broker (Proj RBI rate 0.427) Current season: 0.571/0.571/0.714 (1.286 OPS) in 7 PA, 2 R, 5 RBI." in markdown
    assert "BAT" not in markdown
    assert "TB rate" not in markdown
    assert "baseball" not in markdown
    assert "## Manager's Corner" in markdown
    assert "Week 1 starts the real scouting file" in markdown
    assert "Treat Game 1 like live reconnaissance" in markdown
    assert "administrative problem for the other dugout" in markdown
    assert "Opening night means the standings are clean" in markdown
    assert "milestone movement" not in markdown


def test_build_pregame_overview_insight_lines_surfaces_scoring_and_pressure_pocket() -> None:
    lines = build_pregame_overview_insight_lines(
        [
            {"spot": 1, "player": "Jj", "proj_rbi_rate": 0.210, "proj_xbh_rate": 0.311},
            {"spot": 2, "player": "Glove", "proj_rbi_rate": 0.460, "proj_xbh_rate": 0.444},
            {"spot": 3, "player": "Tristan", "proj_rbi_rate": 0.625, "proj_xbh_rate": 0.512},
            {"spot": 4, "player": "Tim", "proj_rbi_rate": 0.509, "proj_xbh_rate": 0.488},
            {"spot": 5, "player": "Kives", "proj_rbi_rate": 0.427, "proj_xbh_rate": 0.377},
        ],
        projected_runs_per_game=16.4,
        lineup_season_summary={
            "pa": 75,
            "runs": 24,
            "rbi": 22,
            "home_runs": 5,
            "avg": 0.515,
            "obp": 0.547,
            "slg": 0.809,
            "ops": 1.355,
        },
    )

    assert lines == [
        "Projection snapshot: the recommended order simulates to 16.4 runs per game.",
        "Current season lineup snapshot: tonight's available group is slashing 0.515/0.547/0.809 (1.355 OPS) with 24 runs, 5 homers, and 22 RBI across 75 PA.",
        "Biggest lineup edge: Glove, Tristan, Tim form the heaviest pressure pocket with average projected RBI rate 0.531 and XBH rate 0.481.",
    ]


def test_build_pregame_key_lines_avoids_milestone_language_and_uses_context() -> None:
    lines = build_pregame_key_lines(
        lineup_rows=[
            {"player": "Jj"},
            {"player": "Glove"},
            {"player": "Tristan"},
            {"player": "Tim"},
            {"player": "Kives"},
            {"player": "Porter"},
        ],
        milestone_lines=["Glove is 1 away from 20 Doubles (2 in club)."],
        opponent_lines=[
            "Bullseyes: record 2-0 | standings 2-0 | runs 40-18 | scores 20.0/game and allows 9.0/game | Maple Tree scores 12.0/game and allows 17.5/game | recent: W 12-22 vs Wasted Potential."
        ],
        week_bundle={"week_label": "Week 2"},
        season_summary={"wins": 0, "losses": 2, "games_completed": 2, "runs_for": 24, "runs_against": 35},
    )

    assert len(lines) == 3
    assert "milestone" not in " ".join(lines).lower()
    assert any("Bullseyes is averaging 20.0 runs a game" in line for line in lines)
    assert any("Week 2 needs a cleaner defensive tone" in line for line in lines)


def test_annotate_pregame_lineup_assigns_unique_archetypes_across_full_order() -> None:
    annotated = annotate_pregame_lineup(
        [
            {"spot": 1, "player": "Jj", "proj_obp": 0.616, "proj_run_rate": 0.362, "proj_rbi_rate": 0.271, "proj_xbh_rate": 0.168},
            {"spot": 2, "player": "Glove", "proj_obp": 0.682, "proj_run_rate": 0.511, "proj_rbi_rate": 0.508, "proj_xbh_rate": 0.243},
            {"spot": 3, "player": "Tristan", "proj_obp": 0.674, "proj_run_rate": 0.431, "proj_rbi_rate": 0.625, "proj_xbh_rate": 0.346},
            {"spot": 4, "player": "Tim", "proj_obp": 0.706, "proj_run_rate": 0.471, "proj_rbi_rate": 0.510, "proj_xbh_rate": 0.373},
            {"spot": 5, "player": "Kives", "proj_obp": 0.616, "proj_run_rate": 0.353, "proj_rbi_rate": 0.428, "proj_xbh_rate": 0.194},
            {"spot": 6, "player": "Porter", "proj_obp": 0.558, "proj_run_rate": 0.310, "proj_rbi_rate": 0.279, "proj_xbh_rate": 0.207},
            {"spot": 7, "player": "Walsh", "proj_obp": 0.431, "proj_run_rate": 0.106, "proj_rbi_rate": 0.301, "proj_xbh_rate": 0.154},
            {"spot": 8, "player": "Duff", "proj_obp": 0.542, "proj_run_rate": 0.240, "proj_rbi_rate": 0.225, "proj_xbh_rate": 0.127},
            {"spot": 9, "player": "Joey", "proj_obp": 0.492, "proj_run_rate": 0.181, "proj_rbi_rate": 0.140, "proj_xbh_rate": 0.014},
            {"spot": 10, "player": "Corey", "proj_obp": 0.471, "proj_run_rate": 0.272, "proj_rbi_rate": 0.206, "proj_xbh_rate": 0.066},
            {"spot": 11, "player": "Joel", "proj_obp": 0.482, "proj_run_rate": 0.287, "proj_rbi_rate": 0.152, "proj_xbh_rate": 0.049},
            {"spot": 12, "player": "Jason", "proj_obp": 0.457, "proj_run_rate": 0.262, "proj_rbi_rate": 0.125, "proj_xbh_rate": 0.015},
        ]
    )

    strength_labels = [str(row["strength_label"]) for row in annotated]
    assert len(strength_labels) == 12
    assert len(set(strength_labels)) == 12
    assert annotated[0]["strength_note"] == "Ignition switch (Proj OBP 0.616)"
    assert annotated[3]["strength_note"] == "Extra-base menace (Proj XBH rate 0.373)"
    assert annotated[6]["strength_note"] == "RBI collector (Proj RBI rate 0.301)"


def test_fetch_maple_tree_week_bundle_disables_bye_and_incomplete_weeks(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "writeups.sqlite")
    try:
        initialize_database(connection)
        import_schedule_bundle(connection, _build_schedule_csv(tmp_path), None)

        bye_bundle = fetch_maple_tree_week_bundle(
            connection,
            season="Spring 2026",
            week_label="Week 2",
        )
        incomplete_bundle = fetch_maple_tree_week_bundle(
            connection,
            season="Spring 2026",
            week_label="Week 3",
        )
    finally:
        connection.close()

    assert bye_bundle["generation_enabled"] is False
    assert bye_bundle["validation_message"] == WRITEUP_BYE_WEEK_MESSAGE
    assert incomplete_bundle["generation_enabled"] is False
    assert incomplete_bundle["validation_message"] == WRITEUP_INVALID_DOUBLEHEADER_MESSAGE
