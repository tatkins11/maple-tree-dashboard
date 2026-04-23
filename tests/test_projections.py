from pathlib import Path

from src.models.projections import build_hitter_projections
from src.models.season_metadata import ensure_player_season_metadata_file, sync_player_season_metadata
from src.utils.db import connect_db, initialize_database, replace_hitter_projections


def test_build_hitter_projections_weights_recent_seasons_more_than_older_ones(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "projections.sqlite")
    try:
        initialize_database(connection)
        _insert_identity(connection, 1, "Jane Smith", "jane smith")
        _insert_season(connection, "2023", 1, pa=100, singles=10, doubles=2, triples=0, hr=1, walks=5, runs=8, rbi=9, tb=17)
        _insert_season(connection, "2024", 1, pa=100, singles=20, doubles=4, triples=0, hr=2, walks=8, runs=14, rbi=15, tb=32)
        _insert_season(connection, "2025", 1, pa=20, singles=5, doubles=1, triples=0, hr=2, walks=3, runs=6, rbi=7, tb=18)

        projections = build_hitter_projections(connection, "2025")
    finally:
        connection.close()

    projection = projections[0]
    expected_baseline_single_rate = ((20 * 1.0) + (10 * 0.7)) / ((100 * 1.0) + (100 * 0.7))
    assert projection.baseline_plate_appearances == 170
    assert abs(projection.weighted_prior_plate_appearances - 170.0) < 1e-9
    assert projection.season_count_used == 2
    assert projection.p_single > expected_baseline_single_rate


def test_injury_flagged_season_is_downweighted_in_baseline(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "projections.sqlite")
    metadata_path = tmp_path / "player_season_metadata.csv"
    try:
        initialize_database(connection)
        _insert_identity(connection, 1, "Tristan", "tristan")
        _insert_season(connection, "Soviet Sluggers Summer 2021", 1, pa=100, singles=10, doubles=2, triples=0, hr=1, walks=5, runs=10, rbi=12, tb=17)
        _insert_season(connection, "Smoking Bunts Summer 2022", 1, pa=100, singles=8, doubles=1, triples=0, hr=0, walks=3, runs=6, rbi=7, tb=9)
        _insert_season(connection, "Maple Tree Tappers Summer 2025", 1, pa=40, singles=5, doubles=1, triples=0, hr=0, walks=2, runs=4, rbi=5, tb=6)
        _insert_season(connection, "Maple Tree Fall 2025", 1, pa=50, singles=14, doubles=5, triples=1, hr=14, walks=2, runs=26, rbi=41, tb=77)
        metadata_path.write_text(
            "\n".join(
                [
                    "player_name,season,injury_flag,manual_weight_multiplier,notes",
                    "Tristan,Maple Tree Tappers Summer 2025,true,,Injury-affected season",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        sync_player_season_metadata(connection, metadata_path)

        projections = build_hitter_projections(connection, "Maple Tree Fall 2025")
    finally:
        connection.close()

    projection = projections[0]
    assert projection.weighted_prior_plate_appearances < projection.career_plate_appearances - projection.current_plate_appearances
    assert projection.season_count_used == 3
    assert projection.trend_score >= 0.0


def test_manual_weight_override_is_respected(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "projections.sqlite")
    metadata_path = tmp_path / "player_season_metadata.csv"
    try:
        initialize_database(connection)
        _insert_identity(connection, 1, "Tristan", "tristan")
        _insert_season(connection, "2023", 1, pa=100, singles=10, doubles=2, triples=0, hr=1, walks=5, runs=8, rbi=9, tb=17)
        _insert_season(connection, "2024", 1, pa=100, singles=10, doubles=2, triples=0, hr=1, walks=5, runs=8, rbi=9, tb=17)
        _insert_season(connection, "2025", 1, pa=20, singles=5, doubles=1, triples=0, hr=2, walks=3, runs=6, rbi=7, tb=18)
        metadata_path.write_text(
            "\n".join(
                [
                    "player_name,season,injury_flag,manual_weight_multiplier,notes",
                    "Tristan,2024,true,0.10,Manual override",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        sync_player_season_metadata(connection, metadata_path)
        projections = build_hitter_projections(connection, "2025")
    finally:
        connection.close()

    projection = projections[0]
    assert abs(projection.weighted_prior_plate_appearances - 80.0) < 1e-9


def test_consistent_player_scores_higher_consistency_and_lower_volatility(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "projections.sqlite")
    try:
        initialize_database(connection)
        _insert_identity(connection, 1, "Steady", "steady")
        _insert_identity(connection, 2, "Volatile", "volatile")
        for season, singles_steady, singles_volatile in [
            ("2023", 18, 5),
            ("2024", 19, 25),
            ("2025", 20, 20),
        ]:
            _insert_season(connection, season, 1, pa=100 if season != "2025" else 20, singles=singles_steady, doubles=2, triples=0, hr=1, walks=6, runs=10, rbi=10, tb=22)
            _insert_season(connection, season, 2, pa=100 if season != "2025" else 20, singles=singles_volatile, doubles=2, triples=0, hr=1, walks=6, runs=10, rbi=10, tb=22 + max(0, singles_volatile - 18))
        projections = {row.player_name: row for row in build_hitter_projections(connection, "2025")}
    finally:
        connection.close()

    assert projections["Steady"].consistency_score > projections["Volatile"].consistency_score
    assert projections["Steady"].volatility_score < projections["Volatile"].volatility_score


def test_replace_hitter_projections_persists_new_audit_fields(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "projections.sqlite")
    try:
        initialize_database(connection)
        _insert_identity(connection, 1, "Jane Smith", "jane smith")
        _insert_season(connection, "2024", 1, pa=100, singles=20, doubles=4, triples=0, hr=2, walks=8, runs=14, rbi=15, tb=32)
        _insert_season(connection, "2025", 1, pa=20, singles=5, doubles=1, triples=0, hr=2, walks=3, runs=6, rbi=7, tb=18)
        projections = build_hitter_projections(connection, "2025")
        inserted = replace_hitter_projections(connection, "2025", projections)
        stored = connection.execute(
            "SELECT weighted_prior_plate_appearances, season_count_used, consistency_score, trend_score FROM hitter_projections WHERE projection_season = ?",
            ("2025",),
        ).fetchone()
    finally:
        connection.close()

    assert inserted == 1
    assert stored is not None
    assert stored["weighted_prior_plate_appearances"] > 0
    assert stored["season_count_used"] >= 1


def test_ensure_player_season_metadata_file_seeds_tristan_injury_row(tmp_path: Path) -> None:
    csv_path = tmp_path / "player_season_metadata.csv"
    ensure_player_season_metadata_file(csv_path)
    text = csv_path.read_text(encoding="utf-8")
    assert "Tristan" in text
    assert "Maple Tree Tappers Summer 2025" in text


def _insert_identity(connection, player_id: int, player_name: str, canonical_name: str) -> None:
    connection.execute(
        "INSERT INTO players (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )
    connection.execute(
        "INSERT INTO player_identity (player_id, player_name, canonical_name, active_flag) VALUES (?, ?, ?, 1)",
        (player_id, player_name, canonical_name),
    )


def _insert_season(
    connection,
    season: str,
    player_id: int,
    *,
    pa: int,
    singles: int,
    doubles: int,
    triples: int,
    hr: int,
    walks: int,
    runs: int,
    rbi: int,
    tb: int,
) -> None:
    hits = singles + doubles + triples + hr
    ab = max(pa - walks, hits)
    avg = hits / ab if ab else 0.0
    obp = (hits + walks) / pa if pa else 0.0
    slg = tb / ab if ab else 0.0
    ops = obp + slg
    connection.execute(
        """
        INSERT INTO season_batting_stats (
            season, player_id, games, plate_appearances, at_bats, hits, singles, doubles, triples,
            home_runs, walks, strikeouts, hit_by_pitch, sacrifice_hits, sacrifice_flies,
            reached_on_error, fielder_choice, grounded_into_double_play, runs, rbi, total_bases,
            batting_average, on_base_percentage, slugging_percentage, ops,
            batting_average_risp, two_out_rbi, left_on_base, raw_source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            season,
            player_id,
            10,
            pa,
            ab,
            hits,
            singles,
            doubles,
            triples,
            hr,
            walks,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            runs,
            rbi,
            tb,
            avg,
            obp,
            slg,
            ops,
            0.0,
            0,
            0,
            f"{season}.csv",
        ),
    )
