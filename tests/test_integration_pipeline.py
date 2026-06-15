"""End-to-end smoke test for the weekly data pipeline.

Walks one GameChanger-shape CSV through `sync_sources`, builds projections,
and verifies that the dashboard's leaderboard query returns the ingested
hitter with the expected fields. Acts as a safety net so future schema or
query changes break loudly instead of silently producing empty leaderboards.
"""
from __future__ import annotations

from pathlib import Path

from src.dashboard.data import (
    fetch_career_stats,
    fetch_current_season_stats,
    fetch_pregame_hot_bats,
    fetch_seasons,
    fetch_single_season_stats,
    fetch_team_data_freshness,
    fetch_team_recent_form,
    fetch_team_vs_opponent,
    fetch_top_hitters,
)
from src.ingest.pipeline import sync_sources
from src.models.audit import fetch_recent_audit_log
from src.models.projections import (
    DEFAULT_PROJECTION_CONFIG,
    ProjectionConfig,
    build_hitter_projections,
)
from src.models.schedule import import_schedule_csv, record_game_result
from src.utils.db import (
    _APP_INDEX_STATEMENTS,
    connect_db,
    initialize_database,
    replace_hitter_projections,
)


GAMECHANGER_HEADER_ROW = (
    ",,,Batting,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,Pitching"
)
GAMECHANGER_COLUMN_ROW = (
    "Number,Last,First,GP,PA,AB,AVG,OBP,OPS,SLG,H,1B,2B,3B,HR,RBI,R,BB,SO,K-L,"
    "HBP,SAC,SF,ROE,FC,SB,SB%,CS,PIK,QAB,QAB%,PA/BB,BB/K,C%,HHB,LD%,FB%,GB%,"
    "BABIP,BA/RISP,LOB,2OUTRBI,XBH,TB,PS,PS/PA,2S+3,2S+3%,6+,6+%,AB/HR,GIDP,"
    "GITP,CI,IP"
)

# Two hitters, season "Maple Tree Spring 2026", parseable by import_season_stats_csv.
SEASON_NAME = "Maple Tree Spring 2026"
_SAMPLE_ROWS = [
    # Smith, Jane: 20 PA, 10 H, 1 HR — solid line
    ",Smith,Jane,10,20,18,.556,.600,1.544,.944,10,6,2,1,1,9,8,1,2,2,0,0,1,0,0,"
    "0,-,0,0,0,0.00,20.0,0.500,85.00,0,0.00,0.00,0.00,.000,.000,0,0,4,17,0,"
    "0.00,0,0.00,0,0.00,18.0,0,0,0,0.0",
    # Doe, John: 15 PA, 5 H, 0 HR — lighter line
    ",Doe,John,8,15,14,.357,.400,0.857,.500,5,4,1,0,0,2,3,1,1,1,0,0,0,0,0,"
    "0,-,0,0,0,0.00,15.0,1.000,80.00,0,0.00,0.00,0.00,.000,.000,0,0,1,7,0,"
    "0.00,0,0.00,0,0.00,14.0,0,0,0,0.0",
]


def _write_sample_csv(directory: Path) -> Path:
    csv_path = directory / f"{SEASON_NAME} Stats.csv"
    body = "\n".join(
        [
            GAMECHANGER_HEADER_ROW,
            GAMECHANGER_COLUMN_ROW,
            *_SAMPLE_ROWS,
            "Totals," + "," * 50,
            "Glossary," + "," * 50,
        ]
    )
    csv_path.write_text(body + "\n", encoding="utf-8")
    return csv_path


def test_csv_to_projections_to_leaderboard_pipeline(tmp_path: Path) -> None:
    season_csv_dir = tmp_path / "season_csv"
    season_csv_dir.mkdir()
    csv_path = _write_sample_csv(season_csv_dir)

    db_path = tmp_path / "pipeline.sqlite"
    audit_dir = tmp_path / "audits"
    alias_override_path = tmp_path / "player_alias_overrides.csv"

    sync_sources(
        db_path=db_path,
        audit_dir=audit_dir,
        season_csv_paths=[csv_path],
        alias_override_path=alias_override_path,
    )

    connection = connect_db(db_path)
    try:
        initialize_database(connection)

        projections = build_hitter_projections(connection, SEASON_NAME)
        inserted = replace_hitter_projections(connection, SEASON_NAME, projections)
        assert inserted == 2

        seasons = fetch_seasons(connection)
        assert SEASON_NAME in seasons

        season_stats = fetch_current_season_stats(connection, SEASON_NAME)
        top_hitters = fetch_top_hitters(connection, SEASON_NAME, min_pa=0, limit=5)
        career_stats = fetch_career_stats(connection, seasons=[SEASON_NAME])
        single_season_stats = fetch_single_season_stats(connection, seasons=[SEASON_NAME])
    finally:
        connection.close()

    assert not season_stats.empty
    assert set(season_stats["player"]) == {"Jane Smith", "John Doe"}

    jane = season_stats.loc[season_stats["player"] == "Jane Smith"].iloc[0]
    assert int(jane["pa"]) == 20
    assert int(jane["hits"]) == 10
    assert int(jane["hr"]) == 1
    assert int(jane["rbi"]) == 9
    # OBP/OPS are recomputed from components so they match the career/records pages,
    # not trusted from the CSV's stored figures (which credit reached-on-error).
    # Jane: OBP=(10+1)/(18+1+1)=.550, SLG=17/18=.944, OPS=1.494. The stored CSV
    # OPS of 1.544 is intentionally ignored.
    assert abs(float(jane["obp"]) - 0.550) < 1e-3
    assert abs(float(jane["ops"]) - 1.494) < 1e-3

    # A player's OBP/OPS must be identical on every batting-line view. Regression
    # guard for the bug where season pages showed stored rates and career/records
    # showed recomputed ones, so the same player had two different OPS.
    jane_career = career_stats.loc[career_stats["player"] == "Jane Smith"].iloc[0]
    jane_single = single_season_stats.loc[single_season_stats["player"] == "Jane Smith"].iloc[0]
    assert abs(float(jane_career["ops"]) - float(jane["ops"])) < 1e-6
    assert abs(float(jane_single["ops"]) - float(jane["ops"])) < 1e-6
    assert abs(float(jane_career["obp"]) - float(jane["obp"])) < 1e-6

    assert not top_hitters.empty
    assert top_hitters.iloc[0]["player"] == "Jane Smith"


def _write_schedule_csv(directory: Path) -> Path:
    csv_path = directory / "team_schedule.csv"
    csv_path.write_text(
        "\n".join(
            [
                "game_id,season,league_name,division_name,week_label,game_date,game_time,team_name,opponent_name,home_away,location_or_field,status,completed_flag,is_bye,result,runs_for,runs_against,notes,source",
                f"g1,{SEASON_NAME},Wed Men's,Blue,Week 1,2026-04-08,7:00 PM,Maple Tree,Eagles,home,Boncosky,scheduled,0,0,,,,,seed.csv",
            ]
        ),
        encoding="utf-8",
    )
    return csv_path


def test_full_pipeline_end_to_end(tmp_path: Path) -> None:
    """Walks the entire admin workflow: CSV ingest, projections with a custom config,
    schedule import, audit-logged result entry, and dashboard freshness/analytics fetches.
    """
    season_csv_dir = tmp_path / "season_csv"
    season_csv_dir.mkdir()
    csv_path = _write_sample_csv(season_csv_dir)
    schedule_csv_path = _write_schedule_csv(tmp_path)

    db_path = tmp_path / "pipeline.sqlite"
    audit_dir = tmp_path / "audits"
    alias_override_path = tmp_path / "player_alias_overrides.csv"

    sync_sources(
        db_path=db_path,
        audit_dir=audit_dir,
        season_csv_paths=[csv_path],
        alias_override_path=alias_override_path,
    )

    connection = connect_db(db_path)
    try:
        initialize_database(connection)
        import_schedule_csv(connection, schedule_csv_path)

        # Custom projection config — verifies the dataclass threads through the call chain.
        custom_config = ProjectionConfig(
            current_season_prior_pa=80.0,
            recency_weights=(1.0, 0.8),
            weight_ceiling=0.85,
        )
        projections = build_hitter_projections(
            connection,
            SEASON_NAME,
            config=custom_config,
        )
        assert len(projections) == 2
        # Custom ceiling is respected
        for projection in projections:
            assert projection.current_season_weight <= custom_config.weight_ceiling

        replace_hitter_projections(connection, SEASON_NAME, projections)

        # Audit-logged result entry on the first scheduled game.
        record_game_result(
            connection,
            game_id="g1",
            runs_for=14,
            runs_against=8,
            csv_path=schedule_csv_path,
        )

        # Audit log captured the result update.
        log = fetch_recent_audit_log(connection)
        assert not log.empty
        assert log.iloc[0]["action_type"] == "game_result_update"
        assert log.iloc[0]["entity_id"] == "g1"

        freshness = fetch_team_data_freshness(connection, season=SEASON_NAME)
        assert freshness is not None
        assert "Eagles" in str(freshness["summary"])
        assert "W" in str(freshness["result"])

        head_to_head = fetch_team_vs_opponent(connection, opponent="Eagles")
        assert head_to_head["games_played"] == 1
        assert head_to_head["wins"] == 1
        assert head_to_head["runs_for_total"] == 14

        recent_form = fetch_team_recent_form(connection, season=SEASON_NAME, window=5)
        assert recent_form["games_played"] == 1
        assert recent_form["wins"] == 1

        # Dashboard leaderboard still resolves with current data.
        top_hitters = fetch_top_hitters(connection, SEASON_NAME, min_pa=0, limit=5)
        assert not top_hitters.empty
        assert top_hitters.iloc[0]["player"] == "Jane Smith"

        # Pregame hot bats helper does not crash when no per-game data is loaded yet.
        hot_bats = fetch_pregame_hot_bats(connection, season=SEASON_NAME)
        assert isinstance(hot_bats.empty, bool)
    finally:
        connection.close()


def test_default_projection_config_matches_legacy_constants() -> None:
    """Backward-compat: legacy module constants should mirror the dataclass defaults."""
    from src.models import projections as projections_module

    assert projections_module.CURRENT_SEASON_PRIOR_PA == DEFAULT_PROJECTION_CONFIG.current_season_prior_pa
    assert projections_module.DEFAULT_INJURY_MULTIPLIER == DEFAULT_PROJECTION_CONFIG.default_injury_multiplier
    assert tuple(projections_module.RECENCY_WEIGHTS) == DEFAULT_PROJECTION_CONFIG.recency_weights
    assert projections_module.RECENCY_FLOOR == DEFAULT_PROJECTION_CONFIG.recency_floor


def test_initialize_database_creates_app_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "indexes.sqlite"
    connection = connect_db(db_path)
    try:
        initialize_database(connection)
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
    finally:
        connection.close()

    created_index_names = {str(row["name"]) for row in rows}
    expected_index_names = {
        statement.split(" ")[5]  # "CREATE INDEX IF NOT EXISTS <name> ON ..."
        for statement in _APP_INDEX_STATEMENTS
    }
    missing = expected_index_names - created_index_names
    assert not missing, f"Missing expected indexes: {sorted(missing)}"
