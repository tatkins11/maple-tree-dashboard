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
from src.dashboard.ui import (
    build_player_link_html,
    database_path_control,
    get_responsive_layout_context,
    render_static_table,
    with_player_link_column,
)


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
        .current-stats-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.8rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .current-stats-card-title {
            font-size: 1.08rem;
            font-weight: 800;
            margin-bottom: 0.3rem;
        }
        .current-stats-card-row {
            font-size: 0.9rem;
            color: #374151;
            margin: 0.12rem 0;
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


def _render_metric_grid(metrics: list[tuple[str, str]], *, per_row: int) -> None:
    for start in range(0, len(metrics), per_row):
        columns = st.columns(per_row, gap="small")
        for column, (label, value) in zip(columns, metrics[start:start + per_row]):
            column.metric(label, value)


def _render_mobile_standard_cards(dataframe) -> None:
    for _, row in dataframe.iterrows():
        player_markup = build_player_link_html(str(row["player"]), str(row.get("canonical_name") or ""))
        st.markdown(
            f"""
            <div class="current-stats-card">
              <div class="current-stats-card-title">{player_markup}</div>
              <div class="current-stats-card-row"><strong>G:</strong> {int(row['games'])} &nbsp; <strong>PA:</strong> {int(row['pa'])} &nbsp; <strong>AB:</strong> {int(row['ab'])} &nbsp; <strong>H:</strong> {int(row['hits'])}</div>
              <div class="current-stats-card-row"><strong>1B:</strong> {int(row['1b'])} &nbsp; <strong>2B:</strong> {int(row['2b'])} &nbsp; <strong>3B:</strong> {int(row['3b'])} &nbsp; <strong>HR:</strong> {int(row['hr'])}</div>
              <div class="current-stats-card-row"><strong>RBI:</strong> {int(row['rbi'])} &nbsp; <strong>R:</strong> {int(row['r'])} &nbsp; <strong>BB:</strong> {int(row['bb'])} &nbsp; <strong>TB:</strong> {int(row['tb'])}</div>
              <div class="current-stats-card-row"><strong>AVG:</strong> {row['avg']:.3f} &nbsp; <strong>OBP:</strong> {row['obp']:.3f} &nbsp; <strong>SLG:</strong> {row['slg']:.3f} &nbsp; <strong>OPS:</strong> {row['ops']:.3f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_mobile_advanced_cards(dataframe) -> None:
    for _, row in dataframe.iterrows():
        player_markup = build_player_link_html(str(row["player"]), str(row.get("canonical_name") or ""))
        st.markdown(
            f"""
            <div class="current-stats-card">
              <div class="current-stats-card-title">{player_markup}</div>
              <div class="current-stats-card-row"><strong>PA:</strong> {int(row['pa'])} &nbsp; <strong>ISO:</strong> {row['iso']:.3f} &nbsp; <strong>XBH:</strong> {row['xbh_rate']:.3f}</div>
              <div class="current-stats-card-row"><strong>HR Rate:</strong> {row['hr_rate']:.3f} &nbsp; <strong>TB / PA:</strong> {row['tb_per_pa']:.3f} &nbsp; <strong>Team OPS+:</strong> {row['team_relative_ops']:.0f}</div>
              <div class="current-stats-card-row"><strong>RAR:</strong> {row['rar']:.2f} &nbsp; <strong>oWAR:</strong> {row['owar']:.2f}</div>
              <div class="current-stats-card-row"><strong>Archetype:</strong> {row['archetype']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


_inject_current_stats_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="current_stats")

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

    _render_metric_grid(
        [
            ("Runs", str(int(team_summary["runs"]))),
            ("HR", str(int(team_summary["home_runs"]))),
            ("AVG", f"{team_summary['avg']:.3f}"),
            ("OBP", f"{team_summary['obp']:.3f}"),
            ("SLG", f"{team_summary['slg']:.3f}"),
            ("OPS", f"{team_summary['ops']:.3f}"),
        ],
        per_row=2 if layout.is_mobile_layout else 6,
    )

    _render_leader_snapshot(leader_snapshot)

    standard_tab, advanced_tab = st.tabs(["Standard Stats", "Advanced Stats"])

    with standard_tab:
        st.markdown(
            "<div class='current-stats-note'>Core season batting line with the highest-signal counting and rate stats.</div>",
            unsafe_allow_html=True,
        )
        standard_columns = ["player", "canonical_name", "games", "pa", "ab", "hits", "1b", "2b", "3b", "hr", "bb", "r", "rbi", "tb", "avg", "obp", "slg", "ops"]
        standard_display = standard_stats[[column for column in standard_columns if column in standard_stats.columns]]
        if layout.is_mobile_layout:
            _render_mobile_standard_cards(standard_display)
        else:
            standard_table = with_player_link_column(standard_display, output_column="player")
            render_static_table(
                standard_table[[column for column in standard_columns if column in standard_table.columns and column != "canonical_name"]],
                column_labels={
                    "player": "Player",
                    "games": "G",
                    "pa": "PA",
                    "ab": "AB",
                    "hits": "H",
                    "1b": "1B",
                    "2b": "2B",
                    "3b": "3B",
                    "hr": "HR",
                    "bb": "BB",
                    "r": "R",
                    "rbi": "RBI",
                    "tb": "TB",
                    "avg": "AVG",
                    "obp": "OBP",
                    "slg": "SLG",
                    "ops": "OPS",
                },
                formatters={
                    "avg": "{:.3f}",
                    "obp": "{:.3f}",
                    "slg": "{:.3f}",
                    "ops": "{:.3f}",
                },
                css_class="current-stats-standard-table",
            )

    with advanced_tab:
        st.markdown(
            "<div class='current-stats-note'>Curated advanced metrics for quick team-facing evaluation. The deeper methodology and expanded views remain on Advanced Analytics.</div>",
            unsafe_allow_html=True,
        )
        advanced_columns = ["player", "canonical_name", "pa", "iso", "xbh_rate", "hr_rate", "tb_per_pa", "team_relative_ops", "rar", "owar", "archetype"]
        advanced_display = advanced_stats[[column for column in advanced_columns if column in advanced_stats.columns]]
        if layout.is_mobile_layout:
            _render_mobile_advanced_cards(advanced_display)
        else:
            advanced_table = with_player_link_column(advanced_display, output_column="player")
            render_static_table(
                advanced_table[[column for column in advanced_columns if column in advanced_table.columns and column != "canonical_name"]],
                column_labels={
                    "player": "Player",
                    "pa": "PA",
                    "iso": "ISO",
                    "xbh_rate": "XBH Rate",
                    "hr_rate": "HR Rate",
                    "tb_per_pa": "TB / PA",
                    "team_relative_ops": "Team OPS+",
                    "rar": "RAR",
                    "owar": "oWAR",
                    "archetype": "Archetype",
                },
                formatters={
                    "iso": "{:.3f}",
                    "xbh_rate": "{:.3f}",
                    "hr_rate": "{:.3f}",
                    "tb_per_pa": "{:.3f}",
                    "team_relative_ops": "{:.0f}",
                    "rar": "{:.2f}",
                    "owar": "{:.2f}",
                },
                css_class="current-stats-advanced-table",
            )
