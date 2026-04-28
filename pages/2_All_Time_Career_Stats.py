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
from src.dashboard.ui import (
    build_player_link_html,
    database_path_control,
    get_responsive_layout_context,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="All-Time / Career Stats", page_icon=":bar_chart:", layout="wide")

STANDARD_CAREER_COLUMNS = [
    "player",
    "canonical_name",
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
    "tb",
    "avg",
    "obp",
    "slg",
    "ops",
]
ADVANCED_CAREER_COLUMNS = [
    "player",
    "canonical_name",
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
        .career-stats-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.8rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .career-stats-card-title {
            font-size: 1.08rem;
            font-weight: 800;
            margin-bottom: 0.3rem;
        }
        .career-stats-card-row {
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


def _render_metric_grid(metrics: list[tuple[str, str]], *, per_row: int) -> None:
    for start in range(0, len(metrics), per_row):
        columns = st.columns(per_row, gap="small")
        for column, (label, value) in zip(columns, metrics[start:start + per_row]):
            column.metric(label, value)


def _render_mobile_career_cards(dataframe) -> None:
    for _, row in dataframe.iterrows():
        player_markup = build_player_link_html(str(row["player"]), str(row.get("canonical_name") or ""))
        st.markdown(
            f"""
            <div class="career-stats-card">
              <div class="career-stats-card-title">{player_markup}</div>
              <div class="career-stats-card-row"><strong>Seasons:</strong> {int(row['seasons_played'])} &nbsp; <strong>G:</strong> {int(row['games'])} &nbsp; <strong>PA:</strong> {int(row['pa'])} &nbsp; <strong>AB:</strong> {int(row['ab'])}</div>
              <div class="career-stats-card-row"><strong>H:</strong> {int(row['hits'])} &nbsp; <strong>1B:</strong> {int(row['1b'])} &nbsp; <strong>2B:</strong> {int(row['2b'])} &nbsp; <strong>3B:</strong> {int(row['3b'])} &nbsp; <strong>HR:</strong> {int(row['hr'])}</div>
              <div class="career-stats-card-row"><strong>RBI:</strong> {int(row['rbi'])} &nbsp; <strong>R:</strong> {int(row['r'])} &nbsp; <strong>BB:</strong> {int(row['bb'])} &nbsp; <strong>TB:</strong> {int(row['tb'])}</div>
              <div class="career-stats-card-row"><strong>AVG:</strong> {row['avg']:.3f} &nbsp; <strong>OBP:</strong> {row['obp']:.3f} &nbsp; <strong>SLG:</strong> {row['slg']:.3f} &nbsp; <strong>OPS:</strong> {row['ops']:.3f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_mobile_advanced_cards(dataframe) -> None:
    for _, row in dataframe.iterrows():
        player_markup = build_player_link_html(str(row["player"]), str(row.get("canonical_name") or ""))
        st.markdown(
            f"""
            <div class="career-stats-card">
              <div class="career-stats-card-title">{player_markup}</div>
              <div class="career-stats-card-row"><strong>PA:</strong> {int(row['pa'])} &nbsp; <strong>ISO:</strong> {row['iso']:.3f} &nbsp; <strong>XBH:</strong> {row['xbh_rate']:.3f}</div>
              <div class="career-stats-card-row"><strong>HR Rate:</strong> {row['hr_rate']:.3f} &nbsp; <strong>TB / PA:</strong> {row['tb_per_pa']:.3f} &nbsp; <strong>Team OPS+:</strong> {row['team_relative_ops']:.0f}</div>
              <div class="career-stats-card-row"><strong>RAR:</strong> {row['rar']:.2f} &nbsp; <strong>oWAR:</strong> {row['owar']:.2f}</div>
              <div class="career-stats-card-row"><strong>Archetype:</strong> {row['archetype']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


_inject_career_stats_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="career_stats")

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

    _render_metric_grid(
        [
            ("Players", str(int(career_summary["players"]))),
            ("Seasons", str(int(career_summary["seasons"]))),
            ("PA", str(int(career_summary["pa"]))),
            ("Runs", str(int(career_summary["runs"]))),
            ("HR", str(int(career_summary["home_runs"]))),
            ("AVG", f"{career_summary['avg']:.3f}"),
            ("OBP", f"{career_summary['obp']:.3f}"),
            ("SLG", f"{career_summary['slg']:.3f}"),
            ("OPS", f"{career_summary['ops']:.3f}"),
        ],
        per_row=2 if layout.is_mobile_layout else 3,
    )

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
            career_display = career_stats[[column for column in STANDARD_CAREER_COLUMNS if column in career_stats.columns]]
            if layout.is_mobile_layout:
                _render_mobile_career_cards(career_display)
            else:
                career_table = with_player_link_column(career_display, output_column="player")
                render_static_table(
                    career_table[[column for column in STANDARD_CAREER_COLUMNS if column in career_table.columns and column != "canonical_name"]],
                    column_labels={
                        "player": "Player",
                        "seasons_played": "Seasons",
                        "games": "G",
                        "pa": "PA",
                        "ab": "AB",
                        "hits": "H",
                        "1b": "1B",
                        "2b": "2B",
                        "3b": "3B",
                        "hr": "HR",
                        "bb": "BB",
                        "r": "Runs",
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
                    css_class="career-stats-standard-table",
                )

        with advanced_tab:
            st.markdown(
                "<div class='career-stats-note'>Curated advanced metrics across the selected seasons. The deeper methodology and broader comparisons remain on Advanced Analytics.</div>",
                unsafe_allow_html=True,
            )
            if advanced_stats.empty:
                st.info("No advanced career metrics are available for the current filter.")
            else:
                advanced_display = advanced_stats[[column for column in ADVANCED_CAREER_COLUMNS if column in advanced_stats.columns]]
                if layout.is_mobile_layout:
                    _render_mobile_advanced_cards(advanced_display)
                else:
                    advanced_table = with_player_link_column(advanced_display, output_column="player")
                    render_static_table(
                        advanced_table[[column for column in ADVANCED_CAREER_COLUMNS if column in advanced_table.columns and column != "canonical_name"]],
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
                        css_class="career-stats-advanced-table",
                    )

    if leaders:
        st.subheader("All-Time Leaders")
        if layout.is_mobile_layout:
            for label, dataframe in leaders.items():
                st.markdown(f"**{label}**")
                st.dataframe(dataframe, hide_index=True, use_container_width=True)
        else:
            leader_cols = st.columns(len(leaders))
            for column, (label, dataframe) in zip(leader_cols, leaders.items()):
                column.markdown(f"**{label}**")
                column.dataframe(dataframe, hide_index=True, use_container_width=True)
