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
    fetch_enriched_standings_snapshot,
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
from src.dashboard.ui import (
    PLAYER_CARD_URL_PATH,
    database_path_control,
    get_responsive_layout_context,
    player_link_column_config,
    render_mobile_install_help,
    render_mobile_standings_cards,
    with_player_link_column,
)
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
        {
            "page": "pages/10_Player_Card.py",
            "title": "Player Card",
            "url_path": PLAYER_CARD_URL_PATH,
            "visibility": "hidden",
        },
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


def get_home_selected_season(seasons: list[str]) -> str:
    ordered = with_dashboard_default_season(seasons)
    return ordered[0] if ordered else ""


def _build_navigation(role: str) -> list[Any]:
    specs = get_navigation_page_specs(role)
    return [
        st.Page(
            spec["page"],
            title=str(spec["title"]),
            icon=str(spec["icon"]) if spec.get("icon") else None,
            default=bool(spec.get("default", False)),
            url_path=str(spec["url_path"]) if spec.get("url_path") else None,
            visibility=str(spec["visibility"]) if spec.get("visibility") else "visible",
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


def _home_standings_column_config() -> dict[str, st.column_config.Column]:
    return {
        "selected_team": st.column_config.TextColumn("", width="small"),
        "team_name": st.column_config.TextColumn("Team", width="medium"),
        "wins": st.column_config.NumberColumn("W", format="%d", width="small"),
        "losses": st.column_config.NumberColumn("L", format="%d", width="small"),
        "win_pct": st.column_config.NumberColumn("Pct", format="%.3f", width="small"),
        "games_back": st.column_config.NumberColumn("GB", format="%.1f", width="small"),
        "runs_for": st.column_config.NumberColumn("RF", format="%d", width="small"),
        "runs_against": st.column_config.NumberColumn("RA", format="%d", width="small"),
        "run_diff": st.column_config.NumberColumn("RD", format="%d", width="small"),
    }


def _render_metric_grid(metrics: list[tuple[str, str]], *, per_row: int) -> None:
    for start in range(0, len(metrics), per_row):
        columns = st.columns(per_row, gap="small")
        for column, (label, value) in zip(columns, metrics[start:start + per_row]):
            column.metric(label, value)


def _render_home_standings(standings, *, is_mobile_layout: bool) -> None:
    st.markdown("### League Standings")
    if standings.empty:
        st.info("No league standings snapshot is currently loaded for the current season.")
        return

    snapshot_date = str(standings["snapshot_date"].iloc[0]) if "snapshot_date" in standings.columns else ""
    if snapshot_date:
        st.markdown(
            f"<div class='home-section-note'>Latest local standings snapshot for the current season. As of {escape(snapshot_date)}.</div>",
            unsafe_allow_html=True,
        )

    display = standings.copy()
    display.insert(
        0,
        "selected_team",
        display["team_name"].map(lambda value: "•" if str(value) == DEFAULT_SCHEDULE_TEAM_NAME else ""),
    )
    if is_mobile_layout:
        render_mobile_standings_cards(
            display,
            selected_team=DEFAULT_SCHEDULE_TEAM_NAME,
            css_class_prefix="home-standings",
        )
        return
    display_columns = [
        "selected_team",
        "team_name",
        "wins",
        "losses",
        "win_pct",
        "games_back",
        "runs_for",
        "runs_against",
        "run_diff",
    ]
    st.dataframe(
        display[[column for column in display_columns if column in display.columns]],
        use_container_width=True,
        hide_index=True,
        column_config=_home_standings_column_config(),
    )


def render_home_page() -> None:
    role = ensure_authenticated()
    _inject_home_css()
    layout = get_responsive_layout_context(key="home")

    st.title("Maple Tree Home")
    st.caption("Maple Tree team dashboard for stats, schedule, records, analytics, and write-ups.")

    db_path = database_path_control(DEFAULT_DB_PATH, key="home_db_path")
    connection = get_db_connection(db_path, get_connection_cache_key())
    seasons = with_dashboard_default_season(fetch_seasons(connection))
    if not seasons:
        st.info("No season batting stats found yet.")
        return

    selected_season = get_home_selected_season(seasons)

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
    standings = fetch_enriched_standings_snapshot(connection, season=schedule_season)

    st.markdown("### Team Snapshot")
    _render_metric_grid(
        [
            ("Record", str(schedule_summary["record"])),
            ("Games", str(int(summary["team_games"]))),
            ("Runs", str(int(summary["runs"]))),
            ("HR", str(int(summary["home_runs"]))),
            ("Hitters", str(int(summary["hitters"]))),
            ("AVG", f"{summary['avg']:.3f}"),
            ("OBP", f"{summary['obp']:.3f}"),
            ("SLG", f"{summary['slg']:.3f}"),
            ("OPS", f"{summary['ops']:.3f}"),
        ],
        per_row=2 if layout.is_mobile_layout else 5,
    )

    _render_home_standings(standings, is_mobile_layout=layout.is_mobile_layout)

    detail_cols = st.columns(1 if layout.is_mobile_layout else 2, gap="small")
    with detail_cols[0]:
        _render_next_game_card(next_game)
    if layout.is_mobile_layout:
        _render_milestone_card(milestone_lines)
    else:
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
    top_hitters = with_player_link_column(top_hitters, output_column="player")
    st.dataframe(
        top_hitters[[column for column in top_hitters.columns if column != "canonical_name"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "player": player_link_column_config(),
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
