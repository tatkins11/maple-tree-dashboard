from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_advanced_analytics_view,
    fetch_all_time_leaders,
    fetch_career_leader_snapshot,
    fetch_career_stats,
    fetch_career_summary,
    fetch_seasons,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import database_path_control


st.set_page_config(page_title="All-Time / Career Stats", page_icon=":bar_chart:", layout="wide")

STANDARD_CAREER_COLUMNS = [
    "player",
    "seasons_played",
    "games",
    "pa",
    "ab",
    "hits",
    "1b",
    "2b",
    "3b",
    "hr",
    "bb",
    "r",
    "rbi",
    "avg",
    "obp",
    "slg",
    "ops",
]
ADVANCED_CAREER_COLUMNS = [
    "player",
    "pa",
    "iso",
    "xbh_rate",
    "hr_rate",
    "tb_per_pa",
    "team_relative_ops",
    "rar",
    "owar",
    "archetype",
]


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_career_stats_css() -> None:
    st.markdown(
        """
        <style>
        .career-stats-leaders {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.85rem 0.95rem;
            background: #fafafa;
            margin: 0.15rem 0 0.9rem 0;
        }
        .career-stats-leaders-title {
            font-size: 0.92rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }
        .career-stats-leader-row {
            font-size: 0.88rem;
            color: #374151;
            margin: 0.12rem 0;
        }
        .career-stats-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.45rem;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_leader_snapshot(leaders: dict[str, str]) -> None:
    rows = [
        ("OPS leader", leaders.get("ops_leader", "")),
        ("HR leader", leaders.get("hr_leader", "")),
        ("RBI leader", leaders.get("rbi_leader", "")),
        ("AVG leader", leaders.get("avg_leader", "")),
        ("Most seasons", leaders.get("most_seasons", "")),
    ]
    html = "".join(
        f"<div class='career-stats-leader-row'><strong>{label}:</strong> {value}</div>"
        for label, value in rows
        if str(value).strip()
    )
    if not html:
        return
    st.markdown(
        f"""
        <div class="career-stats-leaders">
          <div class="career-stats-leaders-title">Leader Snapshot</div>
          {html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _standard_stats_column_config() -> dict[str, st.column_config.Column]:
    return {
        "player": st.column_config.TextColumn("Player", width="medium"),
        "seasons_played": st.column_config.NumberColumn("Seasons", format="%d", width="small"),
        "games": st.column_config.NumberColumn("G", format="%d", width="small"),
        "pa": st.column_config.NumberColumn("PA", format="%d", width="small"),
        "ab": st.column_config.NumberColumn("AB", format="%d", width="small"),
        "hits": st.column_config.NumberColumn("H", format="%d", width="small"),
        "1b": st.column_config.NumberColumn("1B", format="%d", width="small"),
        "2b": st.column_config.NumberColumn("2B", format="%d", width="small"),
        "3b": st.column_config.NumberColumn("3B", format="%d", width="small"),
        "hr": st.column_config.NumberColumn("HR", format="%d", width="small"),
        "bb": st.column_config.NumberColumn("BB", format="%d", width="small"),
        "r": st.column_config.NumberColumn("Runs", format="%d", width="small"),
        "rbi": st.column_config.NumberColumn("RBI", format="%d", width="small"),
        "avg": st.column_config.NumberColumn("AVG", format="%.3f", width="small"),
        "obp": st.column_config.NumberColumn("OBP", format="%.3f", width="small"),
        "slg": st.column_config.NumberColumn("SLG", format="%.3f", width="small"),
        "ops": st.column_config.NumberColumn("OPS", format="%.3f", width="small"),
    }


def _advanced_stats_column_config() -> dict[str, st.column_config.Column]:
    return {
        "player": st.column_config.TextColumn("Player", width="medium"),
        "pa": st.column_config.NumberColumn("PA", format="%d", width="small"),
        "iso": st.column_config.NumberColumn("ISO", format="%.3f", width="small"),
        "xbh_rate": st.column_config.NumberColumn("XBH Rate", format="%.3f", width="small"),
        "hr_rate": st.column_config.NumberColumn("HR Rate", format="%.3f", width="small"),
        "tb_per_pa": st.column_config.NumberColumn("TB / PA", format="%.3f", width="small"),
        "team_relative_ops": st.column_config.NumberColumn("Team OPS+", format="%.0f", width="small"),
        "rar": st.column_config.NumberColumn("RAR", format="%.2f", width="small"),
        "owar": st.column_config.NumberColumn("oWAR", format="%.2f", width="small"),
        "archetype": st.column_config.TextColumn("Archetype", width="medium"),
    }


_inject_career_stats_css()
ensure_authenticated()

st.title("All-Time / Career Stats")
db_path = database_path_control(DEFAULT_DB_PATH, key="career_stats_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
seasons = with_dashboard_default_season(fetch_seasons(connection))

if not seasons:
    st.info("No career stats found.")
else:
    selected_seasons = st.multiselect("Season filter", options=seasons, default=seasons)
    min_pa = st.slider("Minimum PA", min_value=0, max_value=100, value=20, step=5)

    career_summary = fetch_career_summary(connection, seasons=selected_seasons, min_pa=min_pa)
    leader_snapshot = fetch_career_leader_snapshot(connection, seasons=selected_seasons, min_pa=min_pa)
    career_stats = fetch_career_stats(connection, seasons=selected_seasons, min_pa=min_pa)
    advanced_stats, _ = fetch_advanced_analytics_view(
        connection,
        view_mode="Career",
        selected_seasons=selected_seasons,
        min_pa=min_pa,
        active_only=False,
    )
    leaders = fetch_all_time_leaders(connection, seasons=selected_seasons, min_pa=min_pa)

    summary_cols = st.columns(9)
    summary_cols[0].metric("Players", int(career_summary["players"]))
    summary_cols[1].metric("Seasons", int(career_summary["seasons"]))
    summary_cols[2].metric("PA", int(career_summary["pa"]))
    summary_cols[3].metric("Runs", int(career_summary["runs"]))
    summary_cols[4].metric("HR", int(career_summary["home_runs"]))
    summary_cols[5].metric("AVG", f"{career_summary['avg']:.3f}")
    summary_cols[6].metric("OBP", f"{career_summary['obp']:.3f}")
    summary_cols[7].metric("SLG", f"{career_summary['slg']:.3f}")
    summary_cols[8].metric("OPS", f"{career_summary['ops']:.3f}")

    _render_leader_snapshot(leader_snapshot)

    if career_stats.empty:
        st.info("No hitters match the selected season filter and minimum PA.")
    else:
        standard_tab, advanced_tab = st.tabs(["Career Totals", "Advanced Stats"])

        with standard_tab:
            st.markdown(
                "<div class='career-stats-note'>Career batting totals across the selected seasons with the highest-signal counting and rate stats.</div>",
                unsafe_allow_html=True,
            )
            st.dataframe(
                career_stats[[column for column in STANDARD_CAREER_COLUMNS if column in career_stats.columns]],
                use_container_width=True,
                hide_index=True,
                column_config=_standard_stats_column_config(),
            )

        with advanced_tab:
            st.markdown(
                "<div class='career-stats-note'>Curated advanced metrics across the selected seasons. The deeper methodology and broader comparisons remain on Advanced Analytics.</div>",
                unsafe_allow_html=True,
            )
            if advanced_stats.empty:
                st.info("No advanced career metrics are available for the current filter.")
            else:
                st.dataframe(
                    advanced_stats[[column for column in ADVANCED_CAREER_COLUMNS if column in advanced_stats.columns]],
                    use_container_width=True,
                    hide_index=True,
                    column_config=_advanced_stats_column_config(),
                )

    if leaders:
        st.subheader("All-Time Leaders")
        leader_cols = st.columns(len(leaders))
        for column, (label, dataframe) in zip(leader_cols, leaders.items()):
            column.markdown(f"**{label}**")
            column.dataframe(dataframe, hide_index=True, use_container_width=True)
