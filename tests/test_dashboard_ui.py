import pandas as pd
import src.dashboard.ui as dashboard_ui

from src.dashboard.ui import (
    RESPONSIVE_AUTO,
    RESPONSIVE_FULL,
    RESPONSIVE_MOBILE,
    PLAYER_CARD_URL_PATH,
    _format_link_cell,
    build_player_page_href,
    build_mobile_standings_cards,
    resolve_responsive_layout_mode,
    render_static_table,
    with_player_link_column,
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


def test_build_player_page_href_uses_canonical_name_and_display_anchor() -> None:
    assert build_player_page_href("tristan", "Tristan") == f"./{PLAYER_CARD_URL_PATH}?player=tristan#Tristan"


def test_with_player_link_column_creates_hidden_player_route_links() -> None:
    dataframe = pd.DataFrame(
        [
            {"player": "Tristan", "canonical_name": "tristan", "ops": 1.2},
            {"player": "Glove", "canonical_name": "glove", "ops": 1.0},
        ]
    )

    linked = with_player_link_column(dataframe, output_column="player")

    assert linked.loc[0, "player"] == f"./{PLAYER_CARD_URL_PATH}?player=tristan#Tristan"
    assert linked.loc[1, "player"] == f"./{PLAYER_CARD_URL_PATH}?player=glove#Glove"


def test_format_link_cell_renders_same_tab_anchor_with_player_name() -> None:
    html = _format_link_cell(f"./{PLAYER_CARD_URL_PATH}?player=tristan#Tristan")

    assert 'href="./player-card?player=tristan#Tristan"' in html
    assert 'target="_self"' in html
    assert ">Tristan</a>" in html


def test_render_static_table_keeps_html_table_layout_when_links_are_present(monkeypatch) -> None:
    calls: list[tuple[str, bool]] = []

    def fake_markdown(body: str, unsafe_allow_html: bool = False) -> None:
        calls.append((body, unsafe_allow_html))

    def fail_columns(*args, **kwargs):
        raise AssertionError("render_static_table should not fall back to st.columns for link tables")

    monkeypatch.setattr(dashboard_ui.st, "markdown", fake_markdown)
    monkeypatch.setattr(dashboard_ui.st, "columns", fail_columns)

    dataframe = pd.DataFrame(
        [
            {"player": "./player-card?player=tristan#Tristan", "ops": 1.355},
            {"player": "./player-card?player=jj#Jj", "ops": 2.250},
        ]
    )

    render_static_table(
        dataframe,
        column_labels={"player": "Player", "ops": "OPS"},
        formatters={"ops": "{:.3f}"},
        link_columns=["player"],
        css_class="test-static-table",
    )

    assert len(calls) == 2
    style_markup, style_unsafe = calls[0]
    table_markup, table_unsafe = calls[1]

    assert style_unsafe is True
    assert table_unsafe is True
    assert "table.test-static-table" in style_markup
    assert '<table class="dataframe test-static-table">' in table_markup
    assert 'href="./player-card?player=tristan#Tristan"' in table_markup
    assert ">Tristan</a>" in table_markup
    assert ">1.355<" in table_markup
