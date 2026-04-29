from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import pandas as pd
import streamlit as st

from src.dashboard.config import should_use_hosted_database


RESPONSIVE_AUTO = "Auto"
RESPONSIVE_MOBILE = "Mobile"
RESPONSIVE_FULL = "Full"
RESPONSIVE_LAYOUT_OPTIONS = [RESPONSIVE_AUTO, RESPONSIVE_MOBILE, RESPONSIVE_FULL]
RESPONSIVE_BREAKPOINT = 768
PLAYER_CARD_URL_PATH = "player-card"
PLAYER_LINK_DISPLAY_REGEX = r"#(.*)$"


@dataclass(frozen=True)
class ResponsiveLayoutContext:
    mode: str
    detected_width: int | None
    is_mobile_layout: bool


@dataclass(frozen=True)
class MobileStandingsCard:
    team_name: str
    is_selected: bool
    wins: int
    losses: int
    games_back: float
    runs_for: int
    runs_against: int
    run_diff: int


def resolve_responsive_layout_mode(
    mode: str,
    *,
    detected_width: int | None,
    breakpoint: int = RESPONSIVE_BREAKPOINT,
) -> bool:
    if mode == RESPONSIVE_MOBILE:
        return True
    if mode == RESPONSIVE_FULL:
        return False
    if detected_width is None:
        return False
    return int(detected_width) <= int(breakpoint)


def get_responsive_layout_context(*, key: str) -> ResponsiveLayoutContext:
    mode_key = f"{key}_responsive_mode"
    width_key = f"{key}_viewport_width"
    eval_key = f"{key}_viewport_eval"

    mode = st.sidebar.selectbox(
        "Layout",
        options=RESPONSIVE_LAYOUT_OPTIONS,
        index=0,
        key=mode_key,
        help="Auto switches to the mobile layout on narrow screens. Mobile and Full override it manually.",
    )

    detected_width = st.session_state.get(width_key)
    if mode == RESPONSIVE_AUTO:
        try:
            from streamlit_js_eval import streamlit_js_eval  # type: ignore

            latest_width = streamlit_js_eval(
                js_expressions="window.innerWidth",
                key=eval_key,
            )
            if isinstance(latest_width, (int, float)):
                detected_width = int(latest_width)
                st.session_state[width_key] = detected_width
        except Exception:
            detected_width = st.session_state.get(width_key)

    is_mobile_layout = resolve_responsive_layout_mode(
        str(mode),
        detected_width=detected_width if isinstance(detected_width, int) else None,
    )
    return ResponsiveLayoutContext(
        mode=str(mode),
        detected_width=detected_width if isinstance(detected_width, int) else None,
        is_mobile_layout=is_mobile_layout,
    )


def build_mobile_standings_cards(
    standings: pd.DataFrame,
    *,
    selected_team: str | None = None,
) -> list[MobileStandingsCard]:
    if standings.empty:
        return []

    cards: list[MobileStandingsCard] = []
    for _, row in standings.iterrows():
        team_name = str(row.get("team_name") or "").strip()
        cards.append(
            MobileStandingsCard(
                team_name=team_name,
                is_selected=bool(selected_team) and team_name == str(selected_team),
                wins=int(row.get("wins") or 0),
                losses=int(row.get("losses") or 0),
                games_back=float(row.get("games_back") or 0.0),
                runs_for=int(row.get("runs_for") or 0),
                runs_against=int(row.get("runs_against") or 0),
                run_diff=int(row.get("run_diff") or 0),
            )
        )
    return cards


def render_mobile_standings_cards(
    standings: pd.DataFrame,
    *,
    selected_team: str | None = None,
    css_class_prefix: str = "mobile-standings",
) -> None:
    cards = build_mobile_standings_cards(standings, selected_team=selected_team)
    if not cards:
        return

    st.markdown(
        f"""
        <style>
        .{css_class_prefix}-card {{
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.8rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }}
        .{css_class_prefix}-header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 0.8rem;
            margin-bottom: 0.22rem;
        }}
        .{css_class_prefix}-team {{
            font-size: 1rem;
            font-weight: 800;
            color: #111827;
        }}
        .{css_class_prefix}-record {{
            font-size: 0.92rem;
            font-weight: 700;
            color: #374151;
        }}
        .{css_class_prefix}-row {{
            font-size: 0.88rem;
            color: #4b5563;
            margin: 0.08rem 0;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    for card in cards:
        selected_marker = " *" if card.is_selected else ""
        st.markdown(
            f"""
            <div class="{css_class_prefix}-card">
              <div class="{css_class_prefix}-header">
                <div class="{css_class_prefix}-team">{escape(card.team_name)}{selected_marker}</div>
                <div class="{css_class_prefix}-record">{card.wins}-{card.losses}</div>
              </div>
              <div class="{css_class_prefix}-row"><strong>RD:</strong> {card.run_diff} &nbsp; <strong>GB:</strong> {card.games_back:.1f}</div>
              <div class="{css_class_prefix}-row"><strong>RF:</strong> {card.runs_for} &nbsp; <strong>RA:</strong> {card.runs_against}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def build_player_page_href(canonical_name: str, display_name: str | None = None) -> str:
    canonical = str(canonical_name).strip()
    label = str(display_name or canonical).strip()
    if not canonical:
        return "#"
    return f"./{PLAYER_CARD_URL_PATH}?player={quote(canonical, safe='')}#{label}"


def build_player_link_html(display_name: str, canonical_name: str) -> str:
    if not str(canonical_name).strip():
        return escape(display_name)
    href = build_player_page_href(canonical_name, display_name)
    return f'<a href="{escape(href, quote=True)}" target="_self">{escape(display_name)}</a>'


def with_player_link_column(
    dataframe: pd.DataFrame,
    *,
    player_column: str = "player",
    canonical_column: str = "canonical_name",
    output_column: str = "player_link",
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    working = dataframe.copy()
    if player_column not in working.columns or canonical_column not in working.columns:
        return working

    working.loc[:, output_column] = [
        build_player_page_href(
            str(canonical_name or ""),
            str(display_name or canonical_name or ""),
        )
        for display_name, canonical_name in zip(working[player_column], working[canonical_column])
    ]
    return working


def player_link_column_config(*, label: str = "Player", width: str = "medium") -> st.column_config.LinkColumn:
    return st.column_config.LinkColumn(label, width=width, display_text=PLAYER_LINK_DISPLAY_REGEX)


def _format_table_cell(value, formatter) -> str:
    if pd.isna(value):
        return ""
    if callable(formatter):
        return str(formatter(value))
    if isinstance(formatter, str):
        return formatter.format(value)
    return str(value)


def _format_link_cell(value) -> str:
    if pd.isna(value):
        return ""
    href = str(value).strip()
    if not href or href == "#":
        return ""
    parsed = urlsplit(href)
    label = unquote(parsed.fragment).strip() or href
    return f'<a href="{escape(href, quote=True)}" target="_self">{escape(label)}</a>'


def render_static_table(
    dataframe: pd.DataFrame,
    *,
    column_labels: dict[str, str] | None = None,
    formatters: dict[str, object] | None = None,
    link_columns: list[str] | None = None,
    css_class: str = "dashboard-static-table",
) -> None:
    if dataframe.empty:
        return

    display = dataframe.copy().astype(object)
    formatters = formatters or {}
    for column, formatter in formatters.items():
        if column in display.columns:
            display.loc[:, column] = display[column].map(lambda value: _format_table_cell(value, formatter))

    if column_labels:
        ordered_columns = [column for column in dataframe.columns if column in display.columns]
        display = display[ordered_columns].rename(columns=column_labels)

    if link_columns:
        resolved_link_columns = [
            column_labels.get(column, column) if column_labels else column
            for column in link_columns
        ]
        # Keep player links inside the HTML table so we preserve borders, striping, and alignment.
        for column in resolved_link_columns:
            if column in display.columns:
                display.loc[:, column] = display[column].map(_format_link_cell)

    st.markdown(
        f"""
        <style>
        .{css_class}-wrap {{
            overflow-x: auto;
            margin-bottom: 0.4rem;
        }}
        table.{css_class} {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
            background: white;
        }}
        table.{css_class} thead th {{
            text-align: left;
            font-weight: 700;
            color: #374151;
            padding: 0.55rem 0.65rem;
            border-bottom: 1px solid rgba(49, 51, 63, 0.12);
            background: #fafafa;
            white-space: nowrap;
        }}
        table.{css_class} tbody td {{
            padding: 0.5rem 0.65rem;
            border-bottom: 1px solid rgba(49, 51, 63, 0.08);
            color: #1f2937;
            white-space: nowrap;
            vertical-align: top;
        }}
        table.{css_class} tbody tr:nth-child(even) {{
            background: rgba(249, 250, 251, 0.7);
        }}
        table.{css_class} a {{
            color: #2563eb;
            text-decoration: none;
        }}
        table.{css_class} a:hover {{
            text-decoration: underline;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="{css_class}-wrap">{display.to_html(index=False, escape=False, classes=css_class, border=0)}</div>',
        unsafe_allow_html=True,
    )

def database_path_control(default_path: Path, *, key: str) -> str:
    if should_use_hosted_database():
        st.sidebar.caption("Database: hosted Supabase/Postgres")
        return str(default_path)
    return st.sidebar.text_input("Database path", value=str(default_path), key=key)


def render_mobile_install_help() -> None:
    st.markdown("### iPhone App Shortcut")
    st.info(
        "On iPhone, open this dashboard in Safari, tap Share, then choose Add to Home Screen. "
        "It will launch from your Home Screen like a team app."
    )
