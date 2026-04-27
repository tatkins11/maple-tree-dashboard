from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.config import should_use_hosted_database


RESPONSIVE_AUTO = "Auto"
RESPONSIVE_MOBILE = "Mobile"
RESPONSIVE_FULL = "Full"
RESPONSIVE_LAYOUT_OPTIONS = [RESPONSIVE_AUTO, RESPONSIVE_MOBILE, RESPONSIVE_FULL]
RESPONSIVE_BREAKPOINT = 768


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
