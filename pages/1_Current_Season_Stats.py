from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    dashboard_default_season_index,
    fetch_advanced_analytics_view,
    fetch_current_season_leader_snapshot,
    fetch_current_season_stats,
    fetch_seasons,
    fetch_team_summary,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import database_path_control


st.set_page_config(page_title="Current Season Stats", page_icon=":bar_chart:", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_current_stats_css() -> None:
    st.markdown(
        """
        <style>
        .current-stats-leaders {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.85rem 0.95rem;
            background: #fafafa;
            margin: 0.15rem 0 0.9rem 0;
        }
        .current-stats-leaders-title {
            font-size: 0.92rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }
        .current-stats-leader-row {
            font-size: 0.88rem;
            color: #374151;
            margin: 0.12rem 0;
        }
        .current-stats-note {
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
    ]
    html = "".join(
        f"<div class='current-stats-leader-row'><strong>{label}:</strong> {value}</div>"
        for label, value in rows
        if str(value).strip()
    )
    if not html:
        return
    st.markdown(
        f"""
        <div class="current-stats-leaders">
          <div class="current-stats-leaders-title">Leader Snapshot</div>
          {html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _standard_stats_column_config() -> dict[str, st.column_config.Column]:
    return {
        "player": st.column_config.TextColumn("Player", width="medium"),
        "games": st.column_config.NumberColumn("G", format="%d", width="small"),
        "pa": st.column_config.NumberColumn("PA", format="%d", width="small"),
        "ab": st.column_config.NumberColumn("AB", format="%d", width="small"),
        "hits": st.column_config.NumberColumn("H", format="%d", width="small"),
        "1b": st.column_config.NumberColumn("1B", format="%d", width="small"),
        "2b": st.column_config.NumberColumn("2B", format="%d", width="small"),
        "3b": st.column_config.NumberColumn("3B", format="%d", width="small"),
        "hr": st.column_config.NumberColumn("HR", format="%d", width="small"),
        "bb": st.column_config.NumberColumn("BB", format="%d", width="small"),
        "r": st.column_config.NumberColumn("R", format="%d", width="small"),
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


_inject_current_stats_css()
ensure_authenticated()

st.title("Current Season Stats")
db_path = database_path_control(DEFAULT_DB_PATH, key="current_stats_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
seasons = with_dashboard_default_season(fetch_seasons(connection))

if not seasons:
    st.info("No current season data found.")
else:
    selected_season = st.selectbox("Season", options=seasons, index=dashboard_default_season_index(seasons))

    team_summary = fetch_team_summary(connection, selected_season)
    leader_snapshot = fetch_current_season_leader_snapshot(connection, selected_season)
    standard_stats = fetch_current_season_stats(connection, selected_season)
    advanced_stats, _ = fetch_advanced_analytics_view(
        connection,
        view_mode="Season",
        selected_season=selected_season,
        min_pa=0,
        active_only=False,
    )

    summary_cols = st.columns(6)
    summary_cols[0].metric("Runs", int(team_summary["runs"]))
    summary_cols[1].metric("HR", int(team_summary["home_runs"]))
    summary_cols[2].metric("AVG", f"{team_summary['avg']:.3f}")
    summary_cols[3].metric("OBP", f"{team_summary['obp']:.3f}")
    summary_cols[4].metric("SLG", f"{team_summary['slg']:.3f}")
    summary_cols[5].metric("OPS", f"{team_summary['ops']:.3f}")

    _render_leader_snapshot(leader_snapshot)

    standard_tab, advanced_tab = st.tabs(["Standard Stats", "Advanced Stats"])

    with standard_tab:
        st.markdown(
            "<div class='current-stats-note'>Core season batting line with the highest-signal counting and rate stats.</div>",
            unsafe_allow_html=True,
        )
        standard_columns = ["player", "games", "pa", "ab", "hits", "1b", "2b", "3b", "hr", "bb", "r", "rbi", "avg", "obp", "slg", "ops"]
        st.dataframe(
            standard_stats[[column for column in standard_columns if column in standard_stats.columns]],
            use_container_width=True,
            hide_index=True,
            column_config=_standard_stats_column_config(),
        )

    with advanced_tab:
        st.markdown(
            "<div class='current-stats-note'>Curated advanced metrics for quick team-facing evaluation. The deeper methodology and expanded views remain on Advanced Analytics.</div>",
            unsafe_allow_html=True,
        )
        advanced_columns = ["player", "pa", "iso", "xbh_rate", "hr_rate", "tb_per_pa", "team_relative_ops", "rar", "owar", "archetype"]
        st.dataframe(
            advanced_stats[[column for column in advanced_columns if column in advanced_stats.columns]],
            use_container_width=True,
            hide_index=True,
            column_config=_advanced_stats_column_config(),
        )
