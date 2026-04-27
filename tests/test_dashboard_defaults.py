from src.dashboard.data import (
    DEFAULT_DASHBOARD_SEASON,
    DEFAULT_STATS_SEASON,
    dashboard_default_season_index,
    with_dashboard_default_season,
)
from streamlit_app import get_home_selected_season

LEGACY_SCHEDULE_SEASON = "Spring 2026"


def test_dashboard_stats_default_prefers_populated_maple_tree_spring_season() -> None:
    seasons = [
        "Maple Tree Fall 2025",
        LEGACY_SCHEDULE_SEASON,
        DEFAULT_STATS_SEASON,
    ]

    ordered = with_dashboard_default_season(seasons)

    assert ordered[0] == DEFAULT_STATS_SEASON
    assert ordered.count(DEFAULT_DASHBOARD_SEASON) == 1
    assert dashboard_default_season_index(ordered) == 0


def test_dashboard_stats_default_does_not_add_empty_current_season() -> None:
    seasons = ["Maple Tree Fall 2025", "Maple Tree Tappers Spring 2025"]

    ordered = with_dashboard_default_season(seasons)

    assert DEFAULT_DASHBOARD_SEASON not in ordered
    assert DEFAULT_STATS_SEASON not in ordered
    assert dashboard_default_season_index(ordered) == 0


def test_home_page_uses_dashboard_default_current_season() -> None:
    seasons = [
        "Maple Tree Fall 2025",
        LEGACY_SCHEDULE_SEASON,
        DEFAULT_STATS_SEASON,
    ]

    assert get_home_selected_season(seasons) == DEFAULT_STATS_SEASON


def test_home_page_falls_back_to_first_available_season() -> None:
    seasons = ["Maple Tree Fall 2025", "Maple Tree Tappers Spring 2025"]

    assert get_home_selected_season(seasons) == "Maple Tree Fall 2025"
