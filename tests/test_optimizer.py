from pathlib import Path

from src.models.optimizer import (
    DEFAULT_CORE_TOP_POOL,
    DEFAULT_PREFERRED_LINEUP,
    _select_leadoff_candidates,
    _select_team_aware_top_group,
    _select_top_order_anchor_orders,
    _slot_fit_score,
    optimize_lineup,
)
from src.models.roster import select_game_day_projections
from src.models.records import LeagueRulesRecord
from src.utils.db import connect_db, initialize_database


def test_optimizer_returns_full_valid_lineup_with_fixed_dhh(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                "2026-04-20,Tristan,yes,",
                "2026-04-20,Corey,yes,",
                "2026-04-20,Duff,yes,",
                "2026-04-20,Glove,yes,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Tristan", "tristan", True, 0.22, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.68, 1.28, 0.40, 0.55)
        _insert_projection_player(connection, 2, "Glove", "glove", False, 0.36, 0.08, 0.02, 0.07, 0.05, 0.02, 0.30, 0.58, 0.95, 0.30, 0.33)
        _insert_projection_player(connection, 3, "Duff", "duff", False, 0.31, 0.06, 0.01, 0.03, 0.03, 0.02, 0.44, 0.44, 0.55, 0.18, 0.20)
        _insert_projection_player(connection, 4, "Corey", "corey", False, 0.28, 0.04, 0.0, 0.01, 0.05, 0.03, 0.49, 0.41, 0.37, 0.14, 0.12)

        result = optimize_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            simulations=250,
            seed=7,
            availability_path=availability_path,
        )
    finally:
        connection.close()

    best = result.best_lineup
    assert len(best.ordered_player_names) == 4
    assert set(best.ordered_player_names) == {"Tristan", "Glove", "Duff", "Corey"}
    assert best.dhh_slot in {2, 3, 4}
    assert best.ordered_player_names[best.dhh_slot - 1] == "Tristan"
    assert best.summary.average_runs > 0
    assert result.evaluated_lineups >= 1


def test_optimizer_prefers_middle_order_dhh_over_leadoff_in_toy_case(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                "2026-04-20,Tristan,yes,",
                "2026-04-20,Glove,yes,",
                "2026-04-20,Jj,yes,",
                "2026-04-20,Jason,yes,",
                "2026-04-20,Walsh,yes,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Tristan", "tristan", True, 0.18, 0.10, 0.02, 0.16, 0.06, 0.01, 0.36, 0.60, 1.02, 0.31, 0.44)
        _insert_projection_player(connection, 2, "Glove", "glove", False, 0.36, 0.07, 0.02, 0.06, 0.05, 0.01, 0.31, 0.57, 0.91, 0.27, 0.30)
        _insert_projection_player(connection, 3, "Jj", "jj", False, 0.32, 0.08, 0.01, 0.05, 0.08, 0.01, 0.35, 0.54, 0.82, 0.23, 0.25)
        _insert_projection_player(connection, 4, "Jason", "jason", False, 0.20, 0.02, 0.0, 0.0, 0.12, 0.02, 0.56, 0.34, 0.24, 0.12, 0.09)
        _insert_projection_player(connection, 5, "Walsh", "walsh", False, 0.19, 0.05, 0.01, 0.04, 0.04, 0.01, 0.56, 0.33, 0.47, 0.11, 0.13)

        result = optimize_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            simulations=300,
            seed=9,
            availability_path=availability_path,
        )
    finally:
        connection.close()

    assert result.best_lineup.dhh_slot in {2, 3, 4, 5}
    assert result.best_lineup.ordered_player_names[result.best_lineup.dhh_slot - 1] == "Tristan"


def test_slot_heuristic_penalizes_low_power_walk_heavy_bat_in_middle_order(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                "2026-04-20,Tristan,yes,",
                "2026-04-20,Glove,yes,",
                "2026-04-20,Tim,yes,",
                "2026-04-20,Kives,yes,",
                "2026-04-20,Jason,yes,",
                "2026-04-20,Duff,yes,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Tristan", "tristan", True, 0.19, 0.10, 0.02, 0.17, 0.06, 0.01, 0.34, 0.61, 1.05, 0.31, 0.42)
        _insert_projection_player(connection, 2, "Glove", "glove", False, 0.36, 0.08, 0.02, 0.07, 0.05, 0.01, 0.31, 0.58, 0.95, 0.28, 0.31)
        _insert_projection_player(connection, 3, "Tim", "tim", False, 0.20, 0.09, 0.05, 0.16, 0.04, 0.01, 0.31, 0.66, 1.30, 0.42, 0.47)
        _insert_projection_player(connection, 4, "Kives", "kives", False, 0.33, 0.07, 0.05, 0.05, 0.03, 0.01, 0.36, 0.61, 0.86, 0.33, 0.41)
        _insert_projection_player(connection, 5, "Jason", "jason", False, 0.23, 0.01, 0.0, 0.0, 0.15, 0.10, 0.45, 0.45, 0.25, 0.26, 0.12)
        _insert_projection_player(connection, 6, "Duff", "duff", False, 0.31, 0.06, 0.02, 0.03, 0.03, 0.02, 0.43, 0.52, 0.64, 0.24, 0.21)
        rows = select_game_day_projections(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            available_player_names=["Tristan", "Glove", "Tim", "Kives", "Jason", "Duff"],
        )
    finally:
        connection.close()

    by_name = {row.preferred_display_name: row for row in rows}
    assert _slot_fit_score(by_name["Jason"], 4) < _slot_fit_score(by_name["Tim"], 4)
    assert _slot_fit_score(by_name["Jason"], 4) < _slot_fit_score(by_name["Kives"], 4)
    assert _slot_fit_score(by_name["Jason"], 4) < _slot_fit_score(by_name["Duff"], 4)


def test_leadoff_candidate_selection_includes_strong_top_of_order_anchors(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                "2026-04-20,Glove,yes,",
                "2026-04-20,Jj,yes,",
                "2026-04-20,Kives,yes,",
                "2026-04-20,Joey,yes,",
                "2026-04-20,Tristan,yes,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Glove", "glove", False, 0.36, 0.08, 0.02, 0.07, 0.05, 0.20, 0.31, 0.68, 1.03, 0.28, 0.31)
        _insert_projection_player(connection, 2, "Jj", "jj", False, 0.33, 0.09, 0.02, 0.05, 0.10, 0.30, 0.31, 0.64, 0.80, 0.25, 0.27)
        _insert_projection_player(connection, 3, "Kives", "kives", False, 0.34, 0.07, 0.05, 0.05, 0.03, 0.40, 0.34, 0.63, 0.87, 0.33, 0.41)
        _insert_projection_player(connection, 4, "Joey", "snaxx", False, 0.44, 0.01, 0.00, 0.00, 0.02, 0.01, 0.50, 0.49, 0.46, 0.08, 0.14)
        _insert_projection_player(connection, 5, "Tristan", "tristan", True, 0.20, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.65, 1.26, 0.43, 0.60)
        rows = select_game_day_projections(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            available_player_names=["Glove", "Jj", "Kives", "Joey", "Tristan"],
        )
    finally:
        connection.close()

    candidates = _select_leadoff_candidates(rows)
    assert "Glove" in candidates
    assert "Jj" in candidates
    assert "Kives" in candidates


def test_top_order_anchor_orders_include_glove_jj_kives_combinations(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Glove", "glove", False, 0.36, 0.08, 0.02, 0.07, 0.05, 0.20, 0.31, 0.68, 1.03, 0.28, 0.31)
        _insert_projection_player(connection, 2, "Jj", "jj", False, 0.33, 0.09, 0.02, 0.05, 0.10, 0.30, 0.31, 0.64, 0.80, 0.25, 0.27)
        _insert_projection_player(connection, 3, "Kives", "kives", False, 0.34, 0.07, 0.05, 0.05, 0.03, 0.40, 0.34, 0.63, 0.87, 0.33, 0.41)
        _insert_projection_player(connection, 4, "Joey", "snaxx", False, 0.44, 0.01, 0.00, 0.00, 0.02, 0.01, 0.50, 0.49, 0.46, 0.08, 0.14)
        _insert_projection_player(connection, 5, "Tristan", "tristan", True, 0.20, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.65, 1.26, 0.43, 0.60)
        rows = select_game_day_projections(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            available_player_names=["Glove", "Jj", "Kives", "Joey", "Tristan"],
        )
    finally:
        connection.close()

    anchor_orders = _select_top_order_anchor_orders(rows, dhh_slot=3, max_orders=12)
    top_two_pairs = {(order.get(1), order.get(2)) for order in anchor_orders}
    assert ("Glove", "Jj") in top_two_pairs or ("Jj", "Glove") in top_two_pairs
    assert any("Kives" in pair for pair in top_two_pairs)


def test_strikeout_rate_does_not_change_leadoff_slot_score() -> None:
    class Row:
        def __init__(self, strikeout_rate: float) -> None:
            self.projected_on_base_rate = 0.60
            self.projected_total_base_rate = 0.80
            self.projected_extra_base_hit_rate = 0.18
            self.p_home_run = 0.05
            self.p_walk = 0.08
            self.p_out = 0.32
            self.projected_strikeout_rate = strikeout_rate

    low_k = Row(0.01)
    high_k = Row(0.15)

    assert _slot_fit_score(low_k, 1) == _slot_fit_score(high_k, 1)


def test_team_aware_mode_returns_exact_preferred_lineup_when_all_12_available(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                *[f"2026-04-20,{name},yes," for name in DEFAULT_PREFERRED_LINEUP],
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_preferred_projection_team(connection)
        result = optimize_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            simulations=200,
            seed=17,
            availability_path=availability_path,
            mode="team_aware",
        )
    finally:
        connection.close()

    assert result.best_lineup.ordered_player_names == list(DEFAULT_PREFERRED_LINEUP)
    assert result.best_lineup.lineup_type == "exact preferred lineup"
    assert "preferred full-order baseline" in result.best_lineup.reason
    assert result.best_lineup.dhh_slot == 3


def test_team_aware_mode_strictly_trims_bottom_half_absences(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    available_names = [name for name in DEFAULT_PREFERRED_LINEUP if name not in {"Walsh", "Joel"}]
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                *[f"2026-04-20,{name},yes," for name in available_names],
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_preferred_projection_team(connection)
        result = optimize_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            simulations=200,
            seed=19,
            availability_path=availability_path,
            mode="team_aware",
        )
    finally:
        connection.close()

    expected = [name for name in DEFAULT_PREFERRED_LINEUP if name in available_names]
    assert result.best_lineup.ordered_player_names == expected
    assert result.best_lineup.lineup_type == "preferred lineup with bottom-half trims"
    assert result.best_lineup.dhh_slot == 3


def test_team_aware_mode_limits_top_half_changes_when_core_bat_is_missing(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    available_names = [name for name in DEFAULT_PREFERRED_LINEUP if name != "Jj"]
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                *[f"2026-04-20,{name},yes," for name in available_names],
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_preferred_projection_team(connection)
        result = optimize_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            simulations=200,
            seed=23,
            availability_path=availability_path,
            mode="team_aware",
        )
    finally:
        connection.close()

    best = result.best_lineup.ordered_player_names
    assert best[5:] == ["Walsh", "Duff", "Joey", "Corey", "Joel", "Jason"]
    assert set(best[:5]) == {"Glove", "Tristan", "Tim", "Kives", "Porter"}
    assert best.index("Tristan") + 1 in {2, 3, 4, 5}
    assert result.best_lineup.lineup_type == "preferred lineup with limited top-half reshuffle"


def test_team_aware_mode_keeps_core_five_in_top_five_when_all_available(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    availability_path = tmp_path / "availability.csv"
    availability_path.write_text(
        "\n".join(
            [
                "game_date,player_name,available_flag,notes",
                "2026-04-20,Glove,yes,",
                "2026-04-20,Tim,yes,",
                "2026-04-20,Tristan,yes,",
                "2026-04-20,Kives,yes,",
                "2026-04-20,Jj,yes,",
                "2026-04-20,Joey,yes,",
                "2026-04-20,Corey,yes,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Glove", "glove", False, 0.36, 0.08, 0.02, 0.07, 0.05, 0.01, 0.31, 0.68, 1.03, 0.28, 0.31)
        _insert_projection_player(connection, 2, "Tim", "tim", False, 0.20, 0.09, 0.05, 0.16, 0.04, 0.01, 0.31, 0.69, 1.41, 0.49, 0.53)
        _insert_projection_player(connection, 3, "Tristan", "tristan", True, 0.20, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.65, 1.26, 0.43, 0.60)
        _insert_projection_player(connection, 4, "Kives", "kives", False, 0.34, 0.07, 0.05, 0.05, 0.03, 0.01, 0.34, 0.63, 0.87, 0.33, 0.41)
        _insert_projection_player(connection, 5, "Jj", "jj", False, 0.33, 0.09, 0.02, 0.05, 0.10, 0.01, 0.31, 0.64, 0.80, 0.25, 0.27)
        _insert_projection_player(connection, 6, "Joey", "snaxx", False, 0.44, 0.01, 0.00, 0.00, 0.02, 0.01, 0.50, 0.49, 0.46, 0.08, 0.14)
        _insert_projection_player(connection, 7, "Corey", "corey", False, 0.31, 0.05, 0.01, 0.01, 0.04, 0.01, 0.47, 0.48, 0.49, 0.27, 0.20)
        result = optimize_lineup(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            game_date="2026-04-20",
            league_rules=LeagueRulesRecord(),
            simulations=200,
            seed=5,
            availability_path=availability_path,
            mode="team_aware",
        )
    finally:
        connection.close()

    assert set(result.best_lineup.ordered_player_names[:5]) == {
        "Glove",
        "Tim",
        "Tristan",
        "Kives",
        "Jj",
    }
    assert result.best_lineup.ordered_player_names[0] in {"Glove", "Tim", "Kives", "Jj"}


def test_team_aware_top_group_fills_missing_core_spot_with_next_tier_bat(tmp_path: Path) -> None:
    connection = connect_db(tmp_path / "optimizer.sqlite")
    try:
        initialize_database(connection)
        _insert_projection_player(connection, 1, "Glove", "glove", False, 0.36, 0.08, 0.02, 0.07, 0.05, 0.01, 0.31, 0.68, 1.03, 0.28, 0.31)
        _insert_projection_player(connection, 2, "Tim", "tim", False, 0.20, 0.09, 0.05, 0.16, 0.04, 0.01, 0.31, 0.69, 1.41, 0.49, 0.53)
        _insert_projection_player(connection, 3, "Tristan", "tristan", True, 0.20, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.65, 1.26, 0.43, 0.60)
        _insert_projection_player(connection, 4, "Kives", "kives", False, 0.34, 0.07, 0.05, 0.05, 0.03, 0.01, 0.34, 0.63, 0.87, 0.33, 0.41)
        _insert_projection_player(connection, 5, "Joey", "snaxx", False, 0.44, 0.01, 0.00, 0.00, 0.02, 0.01, 0.50, 0.49, 0.46, 0.08, 0.14)
        _insert_projection_player(connection, 6, "Duff", "duff", False, 0.32, 0.06, 0.02, 0.03, 0.03, 0.01, 0.43, 0.52, 0.64, 0.24, 0.21)
        rows = select_game_day_projections(
            connection=connection,
            projection_season="Maple Tree Fall 2025",
            available_player_names=["Glove", "Tim", "Tristan", "Kives", "Joey", "Duff"],
        )
    finally:
        connection.close()

    top_group = _select_team_aware_top_group(rows, DEFAULT_CORE_TOP_POOL)
    assert top_group[:4] == ["Glove", "Tristan", "Tim", "Kives"]
    assert len(top_group) == 5
    assert top_group[4] in {"Duff", "Joey"}


def _insert_projection_player(
    connection,
    player_id: int,
    player_name: str,
    canonical_name: str,
    is_fixed_dhh: bool,
    p_single: float,
    p_double: float,
    p_triple: float,
    p_home_run: float,
    p_walk: float,
    projected_strikeout_rate: float,
    p_out: float,
    projected_on_base_rate: float,
    projected_total_base_rate: float,
    projected_run_rate: float,
    projected_rbi_rate: float,
) -> None:
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
        UPDATE player_metadata
        SET preferred_display_name = ?, is_fixed_dhh = ?
        WHERE player_id = ?
        """,
        (player_name, int(is_fixed_dhh), player_id),
    )
    connection.execute(
        """
        INSERT INTO hitter_projections (
            projection_season, player_id, projection_source, current_plate_appearances, career_plate_appearances,
            baseline_plate_appearances, current_season_weight, p_single, p_double, p_triple,
            p_home_run, p_walk, projected_strikeout_rate, p_hit_by_pitch, p_reached_on_error,
            p_fielder_choice, p_grounded_into_double_play, p_out, projected_on_base_rate,
            projected_total_base_rate, projected_run_rate, projected_rbi_rate,
            projected_extra_base_hit_rate, fixed_dhh_flag, baserunning_adjustment,
            secondary_batting_average_risp, secondary_two_out_rbi_rate, secondary_left_on_base_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "Maple Tree Fall 2025",
            player_id,
            "season_blended",
            50,
            150,
            100,
            0.3,
            p_single,
            p_double,
            p_triple,
            p_home_run,
            p_walk,
            projected_strikeout_rate,
            0.0,
            0.0,
            0.0,
            0.0,
            p_out,
            projected_on_base_rate,
            projected_total_base_rate,
            projected_run_rate,
            projected_rbi_rate,
            p_double + p_triple + p_home_run,
            int(is_fixed_dhh),
            0.0,
            0.0,
            0.0,
            0.0,
        ),
    )
    connection.commit()


def _insert_preferred_projection_team(connection) -> None:
    player_rows = [
        (1, "Jj", "jj", False, 0.30, 0.10, 0.02, 0.05, 0.08, 0.01, 0.35, 0.55, 0.86, 0.29, 0.28),
        (2, "Glove", "glove", False, 0.34, 0.08, 0.02, 0.06, 0.05, 0.01, 0.32, 0.58, 0.96, 0.31, 0.30),
        (3, "Tristan", "tristan", True, 0.20, 0.10, 0.02, 0.18, 0.06, 0.01, 0.34, 0.56, 1.26, 0.42, 0.58),
        (4, "Tim", "tim", False, 0.19, 0.10, 0.05, 0.15, 0.04, 0.01, 0.33, 0.53, 1.28, 0.41, 0.46),
        (5, "Kives", "kives", False, 0.31, 0.08, 0.04, 0.05, 0.04, 0.01, 0.36, 0.52, 0.88, 0.30, 0.38),
        (6, "Porter", "porter", False, 0.28, 0.07, 0.02, 0.05, 0.04, 0.01, 0.40, 0.46, 0.77, 0.24, 0.29),
        (7, "Walsh", "walsh", False, 0.26, 0.06, 0.01, 0.04, 0.04, 0.01, 0.43, 0.41, 0.63, 0.20, 0.24),
        (8, "Duff", "duff", False, 0.27, 0.05, 0.01, 0.03, 0.03, 0.01, 0.44, 0.39, 0.57, 0.18, 0.21),
        (9, "Joey", "joey", False, 0.25, 0.04, 0.01, 0.02, 0.03, 0.01, 0.46, 0.35, 0.48, 0.15, 0.18),
        (10, "Corey", "corey", False, 0.24, 0.04, 0.00, 0.01, 0.04, 0.01, 0.48, 0.33, 0.40, 0.14, 0.16),
        (11, "Joel", "joel", False, 0.23, 0.03, 0.00, 0.01, 0.04, 0.01, 0.49, 0.31, 0.36, 0.13, 0.15),
        (12, "Jason", "jason", False, 0.21, 0.02, 0.00, 0.00, 0.08, 0.01, 0.56, 0.31, 0.25, 0.12, 0.10),
    ]
    for row in player_rows:
        _insert_projection_player(connection, *row)
