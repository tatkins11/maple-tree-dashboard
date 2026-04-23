from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.config import get_connection_cache_key
from src.dashboard.auth import ROLE_ADMIN, ensure_authenticated
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
    sort_seasons,
    with_dashboard_default_season,
)
from src.dashboard.ui import database_path_control, render_mobile_install_help
from src.models.schedule import DEFAULT_SCHEDULE_TEAM_NAME


st.set_page_config(
    page_title="Slowpitch Optimizer",
    page_icon="🥎",
    layout="wide",
)


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def main() -> None:
    role = ensure_authenticated()
    st.title("Slowpitch Optimizer")
    st.caption("Maple Tree team dashboard for stats, schedule, records, analytics, and write-ups.")

    db_path = database_path_control(DEFAULT_DB_PATH, key="home_db_path")
    connection = get_db_connection(db_path, get_connection_cache_key())
    seasons = with_dashboard_default_season(fetch_seasons(connection))
    if not seasons:
        st.subheader("Overview")
        st.info("No season batting stats found yet.")
        return
    selected_season = st.sidebar.selectbox(
        "Overview season",
        options=seasons,
        index=dashboard_default_season_index(seasons),
    )

    st.subheader("Overview")

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

    mobile_cols = st.columns([1.1, 1], gap="small")
    with mobile_cols[0]:
        st.markdown("### Next Doubleheader")
        if next_game:
            st.info(
                f"{next_game.get('week_label', '')}: vs {next_game.get('opponent_display', '')} "
                f"on {next_game.get('date_display', '')} at {next_game.get('time_display', '')} "
                f"({next_game.get('location_or_field', 'TBD')})."
            )
        else:
            st.info("No upcoming Maple Tree game is loaded yet.")
    with mobile_cols[1]:
        st.markdown("### Milestone Watch")
        if milestone_lines:
            for line in milestone_lines:
                st.write(f"- {line}")
        else:
            st.caption("No immediate milestone watch items.")

    st.markdown("### Latest Postgame")
    if not saved_postgames.empty:
        latest = saved_postgames.iloc[0]
        st.markdown(f"**{latest['title']}**")
        excerpt = "\n".join(str(latest["markdown"]).splitlines()[:8])
        st.text_area("Latest saved recap", value=excerpt, height=180, disabled=True)
        st.page_link("pages/9_Write_Ups.py", label="Open saved write-ups")
    else:
        st.info("No saved postgame recaps yet.")

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


if __name__ == "__main__":
    main()
