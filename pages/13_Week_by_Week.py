from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_potw_history,
    fetch_potw_leaderboard,
    fetch_seasons,
    format_display_date,
    format_player_season_label,
    get_connection,
)
from src.dashboard.ui import (
    render_page_header,
    database_path_control,
    get_responsive_layout_context,
    persistent_selectbox,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="Week by Week", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _format_week_line(row) -> str:
    parts = [f"{int(row['hits'])}-for-{int(row['ab'])}"]
    if int(row["hr"]):
        parts.append(f"{int(row['hr'])} HR")
    if int(row["rbi"]):
        parts.append(f"{int(row['rbi'])} RBI")
    if int(row["r"]):
        parts.append(f"{int(row['r'])} R")
    line = ", ".join(parts)
    if int(row["games"]) > 1:
        line += f" ({int(row['games'])} games)"
    return line


ensure_authenticated()
layout = get_responsive_layout_context(key="week_by_week")

render_page_header(
    "Week by Week",
    kicker="History",
    subtitle="Player of the Week every game day across franchise history. Doubleheaders count as one "
    "week — the two games' Game Scores are added together to crown the winner.",
)
db_path = database_path_control(DEFAULT_DB_PATH, key="wbw_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())

history = fetch_potw_history(connection)
if history.empty:
    st.info("No game-by-game data on record yet — Player of the Week history will fill in as games are played.")
    st.stop()

leaderboard = fetch_potw_leaderboard(connection)

# ----- Player of the Week leaderboard (all-time) -----
st.markdown("### 🏅 Player of the Week — All-Time")
st.caption("How many times each player owned the week. Best Week is their highest combined Game Score.")
lb = with_player_link_column(leaderboard, output_column="player")
render_static_table(
    lb[["player", "potw", "best_week"]],
    column_labels={"player": "Player", "potw": "POTW Awards", "best_week": "Best Week (GS)"},
    formatters={"best_week": "{:.1f}"},
    link_columns=["player"],
    heat_columns=["potw"],
    css_class="wbw-leaderboard",
)

# ----- Biggest weeks on record -----
st.markdown("### 🔥 Biggest Weeks on Record")
st.caption("The most dominant single weeks any hitter has posted (by combined Game Score).")
big = history.sort_values("game_score", ascending=False).head(10).copy()
big["line"] = big.apply(_format_week_line, axis=1)
big["date_display"] = big["game_date"].map(format_display_date)
big["season_label"] = big["season"].map(format_player_season_label)
big_table = with_player_link_column(big, output_column="player")
render_static_table(
    big_table[["player", "date_display", "season_label", "opponents", "line", "game_score"]],
    column_labels={
        "player": "Player",
        "date_display": "Week",
        "season_label": "Season",
        "opponents": "Opponent",
        "line": "Combined Line",
        "game_score": "Game Score",
    },
    formatters={"game_score": "{:.1f}"},
    link_columns=["player"],
    heat_columns=["game_score"],
    css_class="wbw-biggest",
)

# ----- Weekly log -----
st.markdown("### 📅 Weekly Log")
seasons = fetch_seasons(connection)
season_choice = persistent_selectbox(
    "Season",
    options=["All seasons", *seasons],
    query_key="wbw_season",
    default="All seasons",
)
log = history if season_choice == "All seasons" else history[history["season"] == season_choice]
log = log.copy()
log["line"] = log.apply(_format_week_line, axis=1)
log["date_display"] = log["game_date"].map(format_display_date)
log["season_label"] = log["season"].map(format_player_season_label)
log_columns = ["date_display", "season_label", "opponents", "player", "line", "game_score"]
log_table = with_player_link_column(log, output_column="player")
render_static_table(
    log_table[log_columns],
    column_labels={
        "date_display": "Week",
        "season_label": "Season",
        "opponents": "Opponent",
        "player": "Player of the Week",
        "line": "Winning Line",
        "game_score": "GS",
    },
    formatters={"game_score": "{:.1f}"},
    link_columns=["player"],
    heat_columns=["game_score"],
    css_class="wbw-log",
)
st.caption(f"{len(log)} game days on record"
           + ("" if season_choice == "All seasons" else f" in {season_choice}") + ".")
