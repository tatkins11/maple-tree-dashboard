from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any

import streamlit as st

from src.dashboard.auth import ROLE_ADMIN, ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    DEFAULT_DASHBOARD_SEASON,
    dashboard_default_season_index,
    fetch_next_game,
    fetch_saved_writeups,
    fetch_seasons,
    fetch_schedule_season_summary,
    fetch_schedule_seasons,
    fetch_team_summary,
    fetch_top_hitters,
    fetch_writeup_milestone_watch,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import database_path_control, render_mobile_install_help
from src.models.schedule import DEFAULT_SCHEDULE_TEAM_NAME


st.set_page_config(
    page_title="Maple Tree Home",
    page_icon="🏠",
    layout="wide",
)


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def get_navigation_page_specs(role: str) -> list[dict[str, Any]]:
    viewer_pages = [
        {"page": render_home_page, "title": "Home", "icon": "🏠", "default": True},
        {"page": "pages/1_Current_Season_Stats.py", "title": "Current Season Stats"},
        {"page": "pages/2_All_Time_Career_Stats.py", "title": "All-Time / Career Stats"},
        {"page": "pages/5_Records.py", "title": "Records"},
        {"page": "pages/6_Milestones.py", "title": "Milestones"},
        {"page": "pages/7_Advanced_Analytics.py", "title": "Advanced Analytics"},
        {"page": "pages/8_Schedule.py", "title": "Schedule"},
        {"page": "pages/9_Write_Ups.py", "title": "Write-Ups"},
    ]
    if role == ROLE_ADMIN:
        viewer_pages.extend(
            [
                {"page": "pages/3_Lineup_Optimizer.py", "title": "Lineup Optimizer"},
                {"page": "pages/4_Admin_Data.py", "title": "Admin / Data"},
            ]
        )
    return viewer_pages


def _build_navigation(role: str) -> list[Any]:
    specs = get_navigation_page_specs(role)
    return [
        st.Page(
            spec["page"],
            title=str(spec["title"]),
            icon=str(spec["icon"]) if spec.get("icon") else None,
            default=bool(spec.get("default", False)),
        )
        for spec in specs
    ]


def _inject_home_css() -> None:
    st.markdown(
        """
        <style>
        .home-top-note {
            font-size: 0.92rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.75rem;
        }
        .home-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.95rem;
            padding: 0.95rem 1rem;
            background: #fafafa;
            min-height: 10.5rem;
            margin-bottom: 0.8rem;
        }
        .home-card-title {
            font-size: 1.45rem;
            font-weight: 800;
            line-height: 1.15;
            margin-bottom: 0.2rem;
            color: #1f2937;
        }
        .home-card-kicker {
            font-size: 0.82rem;
            color: #6b7280;
            margin-bottom: 0.28rem;
        }
        .home-card-meta {
            font-size: 0.93rem;
            color: #374151;
            margin: 0.1rem 0;
        }
        .home-card-body {
            font-size: 0.92rem;
            color: #374151;
            line-height: 1.45;
            margin-top: 0.35rem;
        }
        .home-card-list {
            margin: 0.15rem 0 0 1.05rem;
            padding: 0;
            color: #374151;
        }
        .home-card-list li {
            margin-bottom: 0.45rem;
        }
        .home-section-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.12rem;
            margin-bottom: 0.45rem;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _format_postgame_excerpt(markdown: str, *, line_limit: int = 5) -> str:
    lines = [line.strip() for line in str(markdown).splitlines() if line.strip()]
    if not lines:
        return "No recap excerpt available yet."
    excerpt = " ".join(lines[:line_limit])
    return excerpt[:280] + ("..." if len(excerpt) > 280 else "")


def _render_next_game_card(next_game: dict[str, object] | None) -> None:
    st.markdown("### Next Doubleheader")
    if not next_game:
        st.info("No upcoming Maple Tree game is loaded yet.")
        return

    week_label = str(next_game.get("week_label") or "")
    opponent = str(next_game.get("opponent_display") or "Opponent")
    date_display = str(next_game.get("date_display") or "TBD")
    time_display = str(next_game.get("time_display") or "TBD")
    field = str(next_game.get("location_or_field") or "TBD")
    home_away = str(next_game.get("home_away_display") or "")

    st.markdown(
        f"""
        <div class="home-card">
          <div class="home-card-kicker">{escape(week_label)}</div>
          <div class="home-card-title">vs {escape(opponent)}</div>
          <div class="home-card-meta">{escape(date_display)} at {escape(time_display)}</div>
          <div class="home-card-meta">{escape(home_away)} • {escape(field)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_milestone_card(lines: list[str]) -> None:
    st.markdown("### Milestone Watch")
    if not lines:
        st.caption("No immediate milestone watch items.")
        return

    items = "".join(f"<li>{escape(line)}</li>" for line in lines[:4])
    st.markdown(
        f"""
        <div class="home-card">
          <ul class="home-card-list">{items}</ul>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.page_link("pages/6_Milestones.py", label="Open milestones")


def _render_postgame_card(saved_postgames) -> None:
    st.markdown("### Latest Postgame")
    if saved_postgames.empty:
        st.info("No saved postgame recaps yet.")
        return

    latest = saved_postgames.iloc[0]
    title = str(latest["title"])
    excerpt = _format_postgame_excerpt(str(latest["markdown"]))
    st.markdown(
        f"""
        <div class="home-card">
          <div class="home-card-title" style="font-size: 1.15rem;">{escape(title)}</div>
          <div class="home-card-body">{escape(excerpt)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.page_link("pages/9_Write_Ups.py", label="Open saved write-ups")


def render_home_page() -> None:
    role = ensure_authenticated()
    _inject_home_css()

    st.title("Maple Tree Home")
    st.caption("Maple Tree team dashboard for stats, schedule, records, analytics, and write-ups.")

    db_path = database_path_control(DEFAULT_DB_PATH, key="home_db_path")
    connection = get_db_connection(db_path, get_connection_cache_key())
    seasons = with_dashboard_default_season(fetch_seasons(connection))
    if not seasons:
        st.info("No season batting stats found yet.")
        return

    season_col, _ = st.columns([1.15, 2], gap="small")
    with season_col:
        selected_season = st.selectbox(
            "Overview season",
            options=seasons,
            index=dashboard_default_season_index(seasons),
        )

    summary = fetch_team_summary(connection, selected_season)
    schedule_seasons = fetch_schedule_seasons(connection)
    schedule_season = (
        selected_season
        if selected_season in schedule_seasons
        else DEFAULT_DASHBOARD_SEASON
        if DEFAULT_DASHBOARD_SEASON in schedule_seasons
        else schedule_seasons[0]
        if schedule_seasons
        else selected_season
    )
    schedule_summary = fetch_schedule_season_summary(
        connection,
        season=schedule_season,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
    )
    next_game = fetch_next_game(
        connection,
        season=schedule_season,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
    )
    saved_postgames = fetch_saved_writeups(connection, season=selected_season, phase="postgame")
    milestone_lines = fetch_writeup_milestone_watch(connection, limit=4)

    st.markdown("### Team Snapshot")
    metric_cols = st.columns(5)
    metric_cols[0].metric("Record", str(schedule_summary["record"]))
    metric_cols[1].metric("Games", int(summary["team_games"]))
    metric_cols[2].metric("Runs", int(summary["runs"]))
    metric_cols[3].metric("HR", int(summary["home_runs"]))
    metric_cols[4].metric("Hitters", int(summary["hitters"]))

    rate_cols = st.columns(4)
    rate_cols[0].metric("AVG", f"{summary['avg']:.3f}")
    rate_cols[1].metric("OBP", f"{summary['obp']:.3f}")
    rate_cols[2].metric("SLG", f"{summary['slg']:.3f}")
    rate_cols[3].metric("OPS", f"{summary['ops']:.3f}")

    detail_cols = st.columns([1.1, 1], gap="small")
    with detail_cols[0]:
        _render_next_game_card(next_game)
    with detail_cols[1]:
        _render_milestone_card(milestone_lines)

    _render_postgame_card(saved_postgames)

    st.markdown("### Quick Links")
    link_rows = [st.columns(3), st.columns(3), st.columns(3)]
    link_rows[0][0].page_link("pages/1_Current_Season_Stats.py", label="Current Season Stats")
    link_rows[0][1].page_link("pages/2_All_Time_Career_Stats.py", label="All-Time / Career Stats")
    link_rows[0][2].page_link("pages/7_Advanced_Analytics.py", label="Advanced Analytics")
    link_rows[1][0].page_link("pages/5_Records.py", label="Records")
    link_rows[1][1].page_link("pages/6_Milestones.py", label="Milestones")
    link_rows[1][2].page_link("pages/8_Schedule.py", label="Schedule")
    link_rows[2][0].page_link("pages/9_Write_Ups.py", label="Write-Ups")
    if role == ROLE_ADMIN:
        link_rows[2][1].page_link("pages/3_Lineup_Optimizer.py", label="Lineup Optimizer")
        link_rows[2][2].page_link("pages/4_Admin_Data.py", label="Admin / Data")

    st.markdown("### Top Hitters")
    st.markdown(
        "<div class='home-section-note'>Quick look at the hottest bats for the selected overview season.</div>",
        unsafe_allow_html=True,
    )
    top_hitters = fetch_top_hitters(connection, selected_season, min_pa=0, limit=8)
    st.dataframe(
        top_hitters,
        use_container_width=True,
        hide_index=True,
        column_config={
            "avg": st.column_config.NumberColumn("AVG", format="%.3f"),
            "obp": st.column_config.NumberColumn("OBP", format="%.3f"),
            "slg": st.column_config.NumberColumn("SLG", format="%.3f"),
            "ops": st.column_config.NumberColumn("OPS", format="%.3f"),
        },
    )

    st.markdown("### Quick Season Totals")
    st.write(
        f"{selected_season}: {int(summary['runs'])} runs, {int(summary['home_runs'])} home runs, "
        f"{int(summary['plate_appearances'])} plate appearances."
    )
    render_mobile_install_help()


def main() -> None:
    role = ensure_authenticated(render_session_controls=False)
    navigation = st.navigation(_build_navigation(role), position="sidebar")
    navigation.run()


if __name__ == "__main__":
    main()
