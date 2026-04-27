import pandas as pd

from src.dashboard.ui import (
    RESPONSIVE_AUTO,
    RESPONSIVE_FULL,
    RESPONSIVE_MOBILE,
    build_mobile_standings_cards,
    resolve_responsive_layout_mode,
)


def test_responsive_auto_uses_mobile_on_narrow_screens() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_AUTO, detected_width=390) is True


def test_responsive_auto_uses_full_on_wider_screens() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_AUTO, detected_width=1024) is False


def test_responsive_auto_falls_back_to_full_when_width_unknown() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_AUTO, detected_width=None) is False


def test_responsive_mobile_override_always_enables_mobile_layout() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_MOBILE, detected_width=None) is True
    assert resolve_responsive_layout_mode(RESPONSIVE_MOBILE, detected_width=1440) is True


def test_responsive_full_override_always_disables_mobile_layout() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_FULL, detected_width=None) is False
    assert resolve_responsive_layout_mode(RESPONSIVE_FULL, detected_width=375) is False


def test_mobile_standings_cards_preserve_runs_and_selected_team_without_ties() -> None:
    standings = pd.DataFrame(
        [
            {
                "team_name": "Maple Tree",
                "wins": 3,
                "losses": 1,
                "games_back": 0.5,
                "runs_for": 44,
                "runs_against": 31,
                "run_diff": 13,
                "ties": 0,
            },
            {
                "team_name": "Bullseyes",
                "wins": 4,
                "losses": 0,
                "games_back": 0.0,
                "runs_for": 52,
                "runs_against": 28,
                "run_diff": 24,
                "ties": 0,
            },
        ]
    )

    cards = build_mobile_standings_cards(standings, selected_team="Maple Tree")

    assert len(cards) == 2
    assert cards[0].team_name == "Maple Tree"
    assert cards[0].is_selected is True
    assert cards[0].runs_for == 44
    assert cards[0].runs_against == 31
    assert cards[0].run_diff == 13
    assert not hasattr(cards[0], "ties")
