from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    MILESTONE_LADDERS,
    fetch_career_milestones,
    fetch_passed_milestones_summary,
    get_connection,
    select_first_to_milestones,
    select_in_play_milestones,
)
from src.dashboard.ui import (
    build_player_link_html,
    database_path_control,
    get_responsive_layout_context,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="Milestone Tracker", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_milestone_css() -> None:
    st.markdown(
        """
        <style>
        .milestone-controls {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.72rem 0.9rem 0.18rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.4rem;
        }
        .milestone-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.08rem;
            margin-bottom: 0.28rem;
        }
        .milestone-chip-row {
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            margin: 0.04rem 0 0.42rem 0;
        }
        .milestone-chip {
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 999px;
            padding: 0.22rem 0.6rem;
            font-size: 0.83rem;
            background: white;
        }
        .milestone-in-play-wrap {
            margin-bottom: 0.6rem;
        }
        .milestone-in-play-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.72rem;
            background: #fafafa;
            padding: 0.52rem 0.6rem;
            min-height: 5.1rem;
        }
        .milestone-in-play-player {
            font-size: 0.98rem;
            font-weight: 700;
            margin-bottom: 0.05rem;
        }
        .milestone-in-play-meta {
            font-size: 0.82rem;
            color: #6b7280;
            margin-bottom: 0.12rem;
        }
        .milestone-in-play-club {
            font-size: 0.78rem;
            color: #6b7280;
            margin-bottom: 0.18rem;
        }
        .milestone-watch-pill {
            display: inline-block;
            border-radius: 999px;
            padding: 0.08rem 0.42rem;
            font-size: 0.71rem;
            font-weight: 700;
        }
        .milestone-watch-one {
            background: rgba(239, 68, 68, 0.12);
            color: #b91c1c;
        }
        .milestone-watch-close {
            background: rgba(245, 158, 11, 0.14);
            color: #92400e;
        }
        .milestone-watch-watch {
            background: rgba(59, 130, 246, 0.10);
            color: #1d4ed8;
        }
        .milestone-club-first {
            display: inline-block;
            border-radius: 999px;
            padding: 0.08rem 0.42rem;
            font-size: 0.71rem;
            font-weight: 700;
            background: rgba(16, 185, 129, 0.12);
            color: #047857;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _format_category_label(categories: list[str]) -> str:
    if not categories:
        return "None"
    if len(categories) == len(MILESTONE_LADDERS):
        return "All categories"
    if len(categories) <= 2:
        return ", ".join(categories)
    return f"{len(categories)} categories"


def _max_remaining_label(value: int | None) -> str:
    return "Any distance" if value is None else f"{value} away or closer"


def _render_summary_bar(
    categories: list[str],
    max_remaining: int | None,
    min_current_total: int,
    sort_by: str,
    active_only: bool,
) -> None:
    chips = [
        f"<div class='milestone-chip'><strong>Categories:</strong> {_format_category_label(categories)}</div>",
        f"<div class='milestone-chip'><strong>Distance:</strong> {_max_remaining_label(max_remaining)}</div>",
        f"<div class='milestone-chip'><strong>Min total:</strong> {min_current_total}</div>",
        f"<div class='milestone-chip'><strong>Sort:</strong> {sort_by}</div>",
        f"<div class='milestone-chip'><strong>Roster:</strong> {'Active only' if active_only else 'All players'}</div>",
    ]
    st.markdown(
        "<div class='milestone-chip-row'>" + "".join(chips) + "</div>",
        unsafe_allow_html=True,
    )


def _prepare_display_table(dataframe: pd.DataFrame, include_active: bool = True) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    display = dataframe.copy()
    display["Progress"] = (display["progress_to_next"] * 100).round(0)
    display["Next"] = display["next_milestone_display"]
    display["Club"] = display["club_label"]
    display["Current"] = display["current_total"].astype(int)
    display["Remaining"] = display["remaining"]
    display["Watch"] = display["urgency"].replace("", "-")
    display["Active"] = display["active_roster"].map(lambda flag: "Yes" if flag else "")
    display = display.rename(columns={"player": "Player", "stat": "Stat"})

    ordered_columns = ["Player", "canonical_name", "Stat", "Current", "Next", "Club", "Remaining", "Watch", "Progress"]
    if include_active:
        ordered_columns.append("Active")
    return display[[column for column in ordered_columns if column in display.columns]]


def _prepare_category_table(dataframe: pd.DataFrame, include_active: bool = True) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    display = dataframe.copy()
    display["Progress"] = (display["progress_to_next"] * 100).round(0)
    display["Current"] = display["current_total"].astype(int)
    display["Next"] = display["next_milestone_display"]
    display["Club"] = display["club_label"]
    display["Highest Cleared"] = display["highest_cleared_milestone"].fillna("-")
    display["Remaining"] = display["remaining"].fillna("-")
    display["Watch"] = display["urgency"].replace("", "-")
    display["Active"] = display["active_roster"].map(lambda flag: "Yes" if flag else "")
    display = display.rename(columns={"player": "Player"})

    ordered_columns = ["Player", "canonical_name", "Current", "Next", "Club", "Remaining", "Watch", "Progress", "Highest Cleared"]
    if include_active:
        ordered_columns.append("Active")
    return display[ordered_columns]


def _prepare_passed_table(dataframe: pd.DataFrame, include_active: bool = True) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    display = dataframe.copy().rename(
        columns={
            "player": "Player",
            "stat": "Stat",
            "current_total": "Current",
            "highest_cleared_milestone": "Highest Cleared",
        }
    )
    display["Current"] = display["Current"].astype(int)
    display["Highest Cleared"] = display["Highest Cleared"].astype(int)
    display["Active"] = display["active_roster"].map(lambda flag: "Yes" if flag else "")

    columns = ["Player", "canonical_name", "Stat", "Current", "Highest Cleared"]
    if include_active:
        columns.append("Active")
    return display[columns]


def _milestone_column_config() -> dict[str, st.column_config.Column]:
    return {}


def _mobile_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    preferred = [column for column in ["Player", "Stat", "Current", "Next", "Remaining", "Watch"] if column in dataframe.columns]
    return dataframe[preferred].copy()


def _link_player_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    linked = with_player_link_column(
        dataframe,
        player_column="Player",
        canonical_column="canonical_name",
        output_column="Player",
    )
    return linked


def _style_milestone_rows(dataframe: pd.DataFrame):
    if dataframe.empty:
        return dataframe.style

    def row_style(row: pd.Series) -> list[str]:
        watch = str(row.get("Watch", ""))
        if watch == "1 away":
            color = "background-color: rgba(239, 68, 68, 0.08);"
        elif watch == "2-5 away":
            color = "background-color: rgba(245, 158, 11, 0.08);"
        elif watch == "6-10 away":
            color = "background-color: rgba(59, 130, 246, 0.06);"
        else:
            color = ""
        return [color] * len(row)

    return dataframe.style.apply(row_style, axis=1)


def _watch_badge_class(label: str) -> str:
    if label == "1 away":
        return "milestone-watch-pill milestone-watch-one"
    if label == "2-5 away":
        return "milestone-watch-pill milestone-watch-close"
    return "milestone-watch-pill milestone-watch-watch"


def _club_badge_markup(label: str, is_first_time: bool) -> str:
    if is_first_time:
        return f'<span class="milestone-club-first">{label}</span>'
    return f'<div class="milestone-in-play-club">{label}</div>'


def _render_in_play_section(dataframe: pd.DataFrame, *, is_mobile_layout: bool) -> None:
    st.subheader("In Play This Season")
    st.markdown(
        "<div class='milestone-note'>Active-roster milestones that are realistically in play right now based on the current distance threshold.</div>",
        unsafe_allow_html=True,
    )
    if dataframe.empty:
        st.info("No active-roster milestones are currently in play within the selected distance threshold.")
        return

    st.markdown("<div class='milestone-in-play-wrap'>", unsafe_allow_html=True)
    column_count = 2 if is_mobile_layout else 4
    columns = st.columns(column_count, gap="small")
    for index, (_, row) in enumerate(dataframe.iterrows()):
        watch = str(row["urgency"])
        player_markup = build_player_link_html(str(row["player"]), str(row.get("canonical_name") or ""))
        with columns[index % column_count]:
            club_label = str(row["club_label"])
            club_markup = _club_badge_markup(club_label, bool(row["is_first_time_milestone"]))
            st.markdown(
                f"""
                <div class="milestone-in-play-card">
                  <div class="milestone-in-play-player">{player_markup}</div>
                  <div class="milestone-in-play-meta">{row['stat']} - {int(row['current_total'])} now - next {row['next_milestone_display']}</div>
                  {club_markup}
                  <span class="{_watch_badge_class(watch)}">{watch}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_first_to_section(dataframe: pd.DataFrame) -> None:
    st.subheader("First To Milestones")
    st.markdown(
        "<div class='milestone-note'>Brand-new milestone clubs that no canonical player has reached yet, shown when a player is within 15% of the next untouched mark.</div>",
        unsafe_allow_html=True,
    )
    if dataframe.empty:
        st.info("No first-to milestones are currently close enough under the 15%-to-target rule.")
        return

    first_to_table = _prepare_display_table(dataframe, include_active=False)
    first_to_table = _link_player_table(first_to_table)
    first_to_display = _mobile_table(first_to_table) if layout.is_mobile_layout else first_to_table[[column for column in first_to_table.columns if column != "canonical_name"]]
    render_static_table(first_to_display, link_columns=["Player"], css_class="milestone-table")


_inject_milestone_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="milestones")

st.title("Milestone Tracker")
st.caption("Career batting milestones based on canonical player identities and verified career totals.")

db_path = database_path_control(DEFAULT_DB_PATH, key="milestones_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())

all_categories = list(MILESTONE_LADDERS.keys())
sort_options = ["nearest milestone", "player name", "stat category"]
max_remaining_options = {
    "Any distance": None,
    "1 away": 1,
    "3 away": 3,
    "5 away": 5,
    "10 away": 10,
    "15 away": 15,
    "20 away": 20,
}

st.markdown("<div class='milestone-controls'>", unsafe_allow_html=True)
if layout.is_mobile_layout:
    selected_categories = st.multiselect("Categories", options=all_categories, default=all_categories)
    max_remaining_label = st.selectbox("Max remaining", options=list(max_remaining_options.keys()), index=3)
    min_current_total = st.number_input("Minimum current total", min_value=0, max_value=500, value=0, step=5)
    active_only = st.toggle("Active roster only", value=True)
    sort_by = st.selectbox("Sort by", options=sort_options, index=0)
    category_focus = st.selectbox("Milestones by category", options=all_categories, index=all_categories.index("Hits"))
else:
    control_row_one = st.columns([1.8, 1, 1, 1], gap="small")
    with control_row_one[0]:
        selected_categories = st.multiselect("Categories", options=all_categories, default=all_categories)
    with control_row_one[1]:
        max_remaining_label = st.selectbox("Max remaining", options=list(max_remaining_options.keys()), index=3)
    with control_row_one[2]:
        min_current_total = st.number_input("Minimum current total", min_value=0, max_value=500, value=0, step=5)
    with control_row_one[3]:
        active_only = st.toggle("Active roster only", value=True)

    control_row_two = st.columns([1.2, 1, 2], gap="small")
    with control_row_two[0]:
        sort_by = st.selectbox("Sort by", options=sort_options, index=0)
    with control_row_two[1]:
        category_focus = st.selectbox("Milestones by category", options=all_categories, index=all_categories.index("Hits"))
    with control_row_two[2]:
        st.markdown("", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

max_remaining = max_remaining_options[max_remaining_label]
_render_summary_bar(selected_categories, max_remaining, min_current_total, sort_by, active_only)

milestones = fetch_career_milestones(
    connection,
    categories=selected_categories,
    active_only=active_only,
    max_remaining=max_remaining,
    min_current_total=min_current_total,
    sort_by=sort_by,
)
upcoming = milestones[milestones["remaining"].notna()].copy() if not milestones.empty else milestones

active_milestones = fetch_career_milestones(
    connection,
    categories=selected_categories,
    active_only=True,
    max_remaining=max_remaining,
    min_current_total=min_current_total,
    sort_by="nearest milestone",
)
active_upcoming = active_milestones[active_milestones["remaining"].notna()].copy() if not active_milestones.empty else active_milestones
in_play_threshold = max_remaining if max_remaining is not None else 5
_render_in_play_section(
    select_in_play_milestones(active_upcoming, distance_threshold=in_play_threshold, limit=8)
    ,
    is_mobile_layout=layout.is_mobile_layout
)
_render_first_to_section(
    select_first_to_milestones(active_upcoming, progress_threshold=0.85, max_remaining=10, limit=12)
)

st.subheader("Closest Overall Milestones")
st.markdown(
    "<div class='milestone-note'>Nearest upcoming milestones across the filtered player pool and stat categories.</div>",
    unsafe_allow_html=True,
)
overall_table = _prepare_display_table(upcoming.head(30), include_active=not active_only)
if overall_table.empty:
    st.info("No upcoming milestones match the current filters.")
else:
    overall_table = _link_player_table(overall_table)
    overall_display = _mobile_table(overall_table) if layout.is_mobile_layout else overall_table[[column for column in overall_table.columns if column != "canonical_name"]]
    render_static_table(overall_display, link_columns=["Player"], css_class="milestone-table")

st.subheader("Active Roster Milestones")
st.markdown(
    "<div class='milestone-note'>Current spring roster players who are closest to notable career marks.</div>",
    unsafe_allow_html=True,
)
active_table = _prepare_display_table(active_upcoming.head(20), include_active=False)
if active_table.empty:
    st.info("No active-roster milestones match the current filters.")
else:
    active_table = _link_player_table(active_table)
    active_display = _mobile_table(active_table) if layout.is_mobile_layout else active_table[[column for column in active_table.columns if column != "canonical_name"]]
    render_static_table(active_display, link_columns=["Player"], css_class="milestone-table")

st.subheader("Milestones by Category")
st.markdown(
    "<div class='milestone-note'>Full player progress table for the selected milestone category.</div>",
    unsafe_allow_html=True,
)
category_milestones = fetch_career_milestones(
    connection,
    categories=[category_focus],
    active_only=active_only,
    max_remaining=None,
    min_current_total=min_current_total,
    sort_by="nearest milestone",
)
category_table = _prepare_category_table(category_milestones, include_active=not active_only)
if category_table.empty:
    st.info("No player rows match the selected category and filters.")
else:
    category_table = _link_player_table(category_table)
    category_display = _mobile_table(category_table) if layout.is_mobile_layout else category_table[[column for column in category_table.columns if column != "canonical_name"]]
    render_static_table(category_display, link_columns=["Player"], css_class="milestone-table")

st.subheader("Passed Milestones Summary")
st.markdown(
    "<div class='milestone-note'>Highest listed milestone already cleared by top players in the selected categories.</div>",
    unsafe_allow_html=True,
)
passed_summary = fetch_passed_milestones_summary(
    connection,
    categories=selected_categories,
    active_only=active_only,
    min_current_total=min_current_total,
    limit=25,
)
passed_table = _prepare_passed_table(passed_summary, include_active=not active_only)
if passed_table.empty:
    st.info("No cleared milestone rows match the current filters.")
else:
    passed_table = _link_player_table(passed_table)
    passed_display = _mobile_table(passed_table) if layout.is_mobile_layout else passed_table[[column for column in passed_table.columns if column != "canonical_name"]]
    render_static_table(passed_display, link_columns=["Player"], css_class="milestone-table")
