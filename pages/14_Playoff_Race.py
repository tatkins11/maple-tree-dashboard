from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DASHBOARD_SEASON,
    DEFAULT_DB_PATH,
    DEFAULT_SCHEDULE_TEAM_NAME,
    fetch_league_divisions,
    fetch_league_schedule_seasons,
    fetch_next_game,
    fetch_seed_race,
    format_player_season_label,
    get_connection,
)
from src.dashboard.ui import (
    database_path_control,
    get_responsive_layout_context,
    persistent_selectbox,
    render_page_header,
    render_static_table,
)


st.set_page_config(page_title="Playoff Race", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _fmt_pct(value: float) -> str:
    text = f"{float(value):.3f}"
    return text[1:] if text.startswith("0.") else text


def _fmt_diff(value: int) -> str:
    value = int(value)
    return f"+{value}" if value > 0 else str(value)


def _fmt_gb(value: float) -> str:
    value = float(value)
    return "—" if value <= 0 else f"{value:g}"


def _marker(is_team: bool, seed: int) -> str:
    if is_team:
        return "🍁"
    return "🥇" if seed == 1 else ""


def _team_label(name: str, is_team: bool) -> str:
    return f"<strong>{name}</strong>" if is_team else name


ensure_authenticated()
layout = get_responsive_layout_context(key="playoff_race")

render_page_header(
    "Race to the #1 Seed",
    kicker="Playoffs",
    subtitle="Every team makes the playoffs, so the regular season is really one race — for seeding, "
    "and the home bracket that comes with the top seed. Here's the live chase, computed from league results.",
)
st.markdown(
    """
    <style>
    .race-headline {
        background: linear-gradient(135deg, #14532d 0%, #166534 70%, #1d7a44 100%);
        color: #f0fdf4;
        border-radius: 0.85rem;
        padding: 0.85rem 1.1rem;
        font-size: 1.05rem;
        font-weight: 600;
        line-height: 1.4;
        margin: 0.2rem 0 1rem 0;
    }
    .race-note { color: #6b7280; font-size: 0.9rem; margin: -0.4rem 0 0.8rem 0; }
    </style>
    """,
    unsafe_allow_html=True,
)

db_path = database_path_control(DEFAULT_DB_PATH, key="playoff_race_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())

seasons = fetch_league_schedule_seasons(connection)
if not seasons:
    st.info("No league schedule is loaded yet — the seed race appears once a league schedule is imported.")
    st.stop()

default_season = DEFAULT_DASHBOARD_SEASON if DEFAULT_DASHBOARD_SEASON in seasons else seasons[0]
season = persistent_selectbox(
    "Season",
    options=seasons,
    query_key="race_season",
    default=default_season,
    format_func=format_player_season_label,
)

divisions = fetch_league_divisions(connection, season)
division = None
if len(divisions) > 1:
    division = persistent_selectbox(
        "Division", options=divisions, query_key="race_division", default=divisions[0])
elif divisions:
    division = divisions[0]

race = fetch_seed_race(connection, season, division_name=division, team_name=DEFAULT_SCHEDULE_TEAM_NAME)
standings = race["standings"]
if standings.empty:
    st.info("No teams are loaded for this season yet.")
    st.stop()

st.markdown(f"<div class='race-headline'>{race['headline']}</div>", unsafe_allow_html=True)

# ----- Preseason: no games played, seeds are not yet meaningful -----
if int(race["games_played_total"]) == 0:
    upcoming = fetch_next_game(connection, season=season, team_name=DEFAULT_SCHEDULE_TEAM_NAME)
    if upcoming:
        opponent = str(upcoming.get("opponent_display") or "").strip()
        when = str(upcoming.get("date_display") or "").strip()
        if opponent and when:
            st.markdown(f"<div class='race-note'>First pitch: {when} vs {opponent}.</div>",
                        unsafe_allow_html=True)
    st.caption("Seeds lock in as results come in. Every team in the field is listed below.")
    field = standings.sort_values("team_name").copy()
    field["marker"] = field["is_team"].map(lambda flag: "🍁" if flag else "")
    field["team_label"] = [
        _team_label(name, flag) for name, flag in zip(field["team_name"], field["is_team"])
    ]
    render_static_table(
        field[["marker", "team_label", "games_remaining"]],
        column_labels={"marker": "", "team_label": "Team", "games_remaining": "Games"},
        css_class="race-preseason",
    )
    st.stop()

# ----- Live seed board -----
board = standings.copy()
board["marker"] = [
    _marker(flag, seed) for flag, seed in zip(board["is_team"], board["seed"])
]
board["team_label"] = [
    _team_label(name, flag) for name, flag in zip(board["team_name"], board["is_team"])
]
board["record"] = [f"{int(w)}-{int(l)}" for w, l in zip(board["wins"], board["losses"])]

render_static_table(
    board[["seed", "marker", "team_label", "record", "win_pct", "games_back", "run_diff", "games_remaining"]],
    column_labels={
        "seed": "Seed",
        "marker": "",
        "team_label": "Team",
        "record": "Record",
        "win_pct": "Win%",
        "games_back": "GB",
        "run_diff": "Run Diff",
        "games_remaining": "Left",
    },
    formatters={"win_pct": _fmt_pct, "games_back": _fmt_gb, "run_diff": _fmt_diff},
    heat_columns=["win_pct", "run_diff"],
    css_class="race-board",
)
st.caption(
    "🥇 = current #1 seed · 🍁 = Maple Tree. Seeded by win %, then run differential. "
    "GB = games behind the top seed · Left = games remaining."
)
