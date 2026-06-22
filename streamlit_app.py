from __future__ import annotations

import warnings
from html import escape
from pathlib import Path
from typing import Any

import streamlit as st

# Pandas warns when read_sql_query is handed our custom Postgres adapter instead
# of a SQLAlchemy engine. The adapter works fine; the warning just clutters logs.
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable.*",
    category=UserWarning,
)

from src.dashboard.auth import ROLE_ADMIN, ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    DEFAULT_DASHBOARD_SEASON,
    fetch_enriched_standings_snapshot,
    fetch_league_team_recent_results,
    fetch_league_team_summary,
    fetch_next_game,
    fetch_player_of_the_week,
    fetch_pregame_hot_bats,
    fetch_records_and_milestones_watch,
    fetch_saved_writeups,
    fetch_seasons,
    fetch_schedule_season_summary,
    fetch_schedule_seasons,
    fetch_team_data_freshness,
    fetch_team_recent_form,
    fetch_team_summary,
    fetch_team_vs_opponent,
    fetch_top_hitters,
    format_display_date,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import (
    PLAYER_CARD_URL_PATH,
    database_path_control,
    get_responsive_layout_context,
    render_data_freshness_caption,
    render_static_table,
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
        {"page": render_home_page, "title": "Home", "icon": "🏠", "default": True, "section": "Game Day"},
        {"page": "pages/8_Schedule.py", "title": "Schedule", "section": "Game Day"},
        {"page": "pages/14_Playoff_Race.py", "title": "Playoff Race", "section": "Game Day"},
        {"page": "pages/9_Write_Ups.py", "title": "Write-Ups", "section": "Game Day"},
        {
            "page": "pages/10_Player_Card.py",
            "title": "Player Card",
            "url_path": PLAYER_CARD_URL_PATH,
            "section": "Stats",
        },
        {"page": "pages/1_Current_Season_Stats.py", "title": "Current Season Stats", "section": "Stats"},
        {"page": "pages/2_All_Time_Career_Stats.py", "title": "All-Time / Career Stats", "section": "Stats"},
        {"page": "pages/7_Advanced_Analytics.py", "title": "Advanced Analytics", "section": "Stats"},
        {"page": "pages/5_Records.py", "title": "Records", "section": "History"},
        {"page": "pages/12_Single_Game_Hall_of_Fame.py", "title": "Single-Game Hall of Fame", "section": "History"},
        {"page": "pages/6_Milestones.py", "title": "Milestones", "section": "History"},
        {"page": "pages/11_Rivalry_Ledger.py", "title": "Rivalry Ledger", "section": "History"},
        {"page": "pages/13_Week_by_Week.py", "title": "Week by Week", "section": "History"},
    ]
    if role == ROLE_ADMIN:
        viewer_pages.extend(
            [
                {"page": "pages/3_Lineup_Optimizer.py", "title": "Lineup Optimizer", "section": "Manager"},
                {"page": "pages/4_Admin_Data.py", "title": "Admin / Data", "section": "Manager"},
            ]
        )
    return viewer_pages


def get_home_selected_season(seasons: list[str]) -> str:
    ordered = with_dashboard_default_season(seasons)
    return ordered[0] if ordered else ""


def _build_navigation(role: str) -> dict[str, list[Any]]:
    sections: dict[str, list[Any]] = {}
    for spec in get_navigation_page_specs(role):
        page = st.Page(
            spec["page"],
            title=str(spec["title"]),
            icon=str(spec["icon"]) if spec.get("icon") else None,
            default=bool(spec.get("default", False)),
            url_path=str(spec["url_path"]) if spec.get("url_path") else None,
            visibility=str(spec["visibility"]) if spec.get("visibility") else "visible",
        )
        sections.setdefault(str(spec.get("section") or "Pages"), []).append(page)
    return sections


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
        .home-hero {
            background: linear-gradient(135deg, #14532d 0%, #166534 60%, #1d7a44 100%);
            border-radius: 1rem;
            padding: 1.35rem 1.5rem 1.25rem 1.5rem;
            margin-bottom: 1rem;
            box-shadow: 0 2px 10px rgba(20, 83, 45, 0.22);
        }
        .home-hero-kicker {
            color: #bbf7d0;
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            margin-bottom: 0.18rem;
        }
        .home-hero-title {
            color: #ffffff;
            font-size: 2.1rem;
            font-weight: 900;
            line-height: 1.05;
            letter-spacing: 0.01em;
        }
        .home-hero-sub {
            color: rgba(255, 255, 255, 0.85);
            font-size: 0.95rem;
            margin-top: 0.3rem;
        }
        .home-hero-pills {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin-top: 0.75rem;
        }
        .home-hero-pill {
            display: inline-block;
            border: 1px solid rgba(255, 255, 255, 0.28);
            border-radius: 999px;
            padding: 0.2rem 0.65rem;
            font-size: 0.82rem;
            font-weight: 600;
            color: #ffffff;
            background: rgba(255, 255, 255, 0.12);
        }
        .stMarkdown h3 {
            border-bottom: 2px solid #e3eadf;
            padding-bottom: 0.25rem;
        }
        .home-card {
            border: 1px solid #e2e8de;
            border-top: 3px solid #15803d;
            border-radius: 0.95rem;
            padding: 0.95rem 1rem;
            background: #ffffff;
            min-height: 10.5rem;
            margin-bottom: 0.8rem;
            box-shadow: 0 1px 3px rgba(22, 101, 52, 0.07);
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


def _format_pregame_meeting(row) -> str:
    runs_for = row.get("runs_for")
    runs_against = row.get("runs_against")
    result = str(row.get("result") or "").strip().upper()
    score = ""
    if runs_for is not None and runs_against is not None and str(runs_for) != "" and str(runs_against) != "":
        score = f" {int(runs_for)}-{int(runs_against)}"
    return f"{format_display_date(row.get('game_date', ''))} — {result}{score}".strip(" —")


def _format_scouting_result(row, team_name: str) -> str:
    """Format a league game from a given team's perspective: 'MM/DD/YY: W 18-8 vs Foe'."""
    home, away = str(row.get("home_team") or ""), str(row.get("away_team") or "")
    home_runs, away_runs = row.get("home_runs"), row.get("away_runs")
    if home_runs is None or away_runs is None or str(home_runs) == "" or str(away_runs) == "":
        return ""
    home_runs, away_runs = int(home_runs), int(away_runs)
    if team_name == home:
        team_runs, opp_runs, foe, loc = home_runs, away_runs, away, "vs"
    else:
        team_runs, opp_runs, foe, loc = away_runs, home_runs, home, "@"
    result = "W" if team_runs > opp_runs else "L" if team_runs < opp_runs else "T"
    return f"{format_display_date(row.get('game_date', ''))}: {result} {team_runs}-{opp_runs} {loc} {foe}"


def _render_pregame_snapshot(
    connection,
    *,
    next_game: dict[str, object] | None,
    schedule_season: str,
    is_mobile_layout: bool,
) -> None:
    st.markdown("### Pregame Snapshot")

    if next_game is None:
        st.caption("Pregame snapshot will appear once the next Maple Tree game is loaded.")
        return

    opponent = str(next_game.get("opponent_display") or next_game.get("opponent_name") or "").strip()
    if not opponent or opponent.upper() == "BYE":
        st.caption("Bye week — no opponent for the pregame snapshot.")
        return

    head_to_head = fetch_team_vs_opponent(connection, opponent=opponent)
    team_form = fetch_team_recent_form(connection, season=schedule_season, window=5)
    hot_bats = fetch_pregame_hot_bats(connection, season=schedule_season, window=5, min_recent_pa=3, limit=5)

    columns = st.columns(1 if is_mobile_layout else 2, gap="small")

    with columns[0]:
        st.markdown(f"**Series vs {escape(opponent)}**")
        if head_to_head["games_played"] == 0:
            st.caption("First time facing this opponent on record.")
        else:
            line = (
                f"All-time: {head_to_head['wins']}-{head_to_head['losses']}"
                + (f"-{head_to_head['ties']}" if head_to_head["ties"] else "")
                + f" · {head_to_head['avg_runs_for']:.1f} RF / {head_to_head['avg_runs_against']:.1f} RA per game"
            )
            st.markdown(
                f"<div class='home-section-note'>{escape(line)}</div>",
                unsafe_allow_html=True,
            )
            recent_meetings = head_to_head["recent_meetings"]
            completed_meetings = recent_meetings[recent_meetings["completed_flag"] == 1].head(3)
            if not completed_meetings.empty:
                meeting_items = "".join(
                    f"<li>{escape(_format_pregame_meeting(row))}</li>"
                    for _, row in completed_meetings.iterrows()
                )
                st.markdown(
                    f"<ul class='home-card-list'>{meeting_items}</ul>",
                    unsafe_allow_html=True,
                )

        st.markdown(f"**Scouting report: {escape(opponent)}**")
        opp_summary = fetch_league_team_summary(connection, season=schedule_season, team_name=opponent)
        opp_games = int(opp_summary.get("games_completed") or 0)
        if opp_games == 0:
            st.caption("No league games on record yet this season — check back once they've played.")
        else:
            opp_ties = int(opp_summary.get("ties") or 0)
            runs_for = int(opp_summary["runs_for"])
            runs_against = int(opp_summary["runs_against"])
            run_diff = runs_for - runs_against
            scouting_line = (
                f"League {opp_summary['record']}" + (f"-{opp_ties}" if opp_ties else "")
                + f" · {runs_for / opp_games:.1f} scored / {runs_against / opp_games:.1f} allowed per game"
                + f" · {'+' if run_diff >= 0 else ''}{run_diff} run diff"
            )
            st.markdown(
                f"<div class='home-section-note'>{escape(scouting_line)}</div>",
                unsafe_allow_html=True,
            )
            opp_recent = fetch_league_team_recent_results(
                connection, season=schedule_season, team_name=opponent, limit=3
            )
            scouting_items = [_format_scouting_result(row, opponent) for _, row in opp_recent.iterrows()]
            scouting_items = [item for item in scouting_items if item]
            if scouting_items:
                items_html = "".join(f"<li>{escape(item)}</li>" for item in scouting_items)
                st.markdown(f"<ul class='home-card-list'>{items_html}</ul>", unsafe_allow_html=True)

        st.markdown(f"**Recent form ({schedule_season})**")
        if team_form["games_played"] == 0:
            st.caption("No completed games on record for the current season yet.")
        else:
            record = (
                f"{team_form['wins']}-{team_form['losses']}"
                + (f"-{team_form['ties']}" if team_form["ties"] else "")
            )
            line = (
                f"Last {team_form['games_played']} game{'s' if team_form['games_played'] != 1 else ''}: "
                f"{record} · {team_form['avg_runs_for']:.1f} RF / {team_form['avg_runs_against']:.1f} RA"
            )
            st.markdown(
                f"<div class='home-section-note'>{escape(line)}</div>",
                unsafe_allow_html=True,
            )

    with columns[-1]:
        st.markdown("**Hot bats heading in**")
        if hot_bats.empty:
            st.caption("Not enough recent game-log data to flag hot streaks yet.")
        else:
            for _, row in hot_bats.iterrows():
                delta_text = _format_pregame_delta(float(row["ops_delta"]))
                line = (
                    f"{row['player']} — last {int(row['recent_pa'])} PA: "
                    f"OPS {row['recent_ops']:.3f} ({delta_text} vs season)"
                )
                st.markdown(
                    f"<div class='home-section-note'>{escape(line)}</div>",
                    unsafe_allow_html=True,
                )


def _format_pregame_delta(value: float) -> str:
    if value > 0:
        return f"+{value:.3f}"
    return f"{value:.3f}"


def _render_potw_card(potw: dict | None) -> None:
    st.markdown("### Player of the Week")
    if not potw:
        st.caption("The first Player of the Week posts after the season's opening games.")
        return
    extras = [f"{value} {label}" for value, label in
              ((potw["hr"], "HR"), (potw["rbi"], "RBI"), (potw["r"], "R"), (potw["bb"], "BB")) if value]
    line = f"{potw['hits']}-for-{potw['ab']}" + ((", " + ", ".join(extras)) if extras else "")
    if int(potw.get("games", 1)) > 1:
        line += f"  ·  {int(potw['games'])}-game total"
    meta = (f"vs {potw['opponents']} · {format_display_date(potw['game_date'])} · "
            f"Game Score {potw['game_score']:.1f} (combined)")
    st.markdown(
        f"""
        <div class="home-card">
          <div style="font-size:1.1rem;font-weight:800;color:#14532d;">{escape(potw['player'])}</div>
          <div class="home-section-note">{escape(line)}</div>
          <div class="home-section-note" style="color:#475569;">{escape(meta)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_milestone_card(lines: list[str]) -> None:
    st.markdown("### Milestones &amp; Records in Reach")
    if not lines:
        st.caption("No records or milestones within reach right now.")
        return

    items = "".join(f"<li>{escape(line)}</li>" for line in lines[:6])
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
            f"<div class='home-section-note'>Latest local standings snapshot for the current season. As of {escape(format_display_date(snapshot_date))}.</div>",
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
        width="stretch",
        hide_index=True,
        column_config=_home_standings_column_config(),
    )


def _render_home_hero(season: str = "", record: str = "") -> None:
    pills: list[str] = []
    if season:
        season_pill = season.replace("Maple Tree ", "").strip() or season
        pills.append(f'<span class="home-hero-pill">{escape(season_pill)}</span>')
    if record:
        pills.append(f'<span class="home-hero-pill">Record {escape(record)}</span>')
    pills_html = f'<div class="home-hero-pills">{"".join(pills)}</div>' if pills else ""
    st.markdown(
        f"""
        <div class="home-hero">
          <div class="home-hero-kicker">Slow-Pitch Softball</div>
          <div class="home-hero-title">🥎 Maple Tree</div>
          <div class="home-hero-sub">Stats, schedule, records, analytics &amp; write-ups</div>
          {pills_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_home_page() -> None:
    role = ensure_authenticated()
    _inject_home_css()
    layout = get_responsive_layout_context(key="home")

    db_path = database_path_control(DEFAULT_DB_PATH, key="home_db_path")
    connection = get_db_connection(db_path, get_connection_cache_key())
    seasons = with_dashboard_default_season(fetch_seasons(connection))
    if not seasons:
        _render_home_hero()
        st.info("No season batting stats found yet.")
        return

    selected_season = get_home_selected_season(seasons)

    schedule_seasons = fetch_schedule_seasons(connection)
    # Orient the home to the current dashboard season (its schedule + next game), even when
    # it's a brand-new season with no stats yet. The stats sections below fall back to the
    # most recent season that actually has data (selected_season).
    schedule_season = (
        DEFAULT_DASHBOARD_SEASON
        if DEFAULT_DASHBOARD_SEASON in schedule_seasons
        else selected_season
        if selected_season in schedule_seasons
        else schedule_seasons[0]
        if schedule_seasons
        else selected_season
    )
    schedule_summary = fetch_schedule_season_summary(
        connection, season=schedule_season, team_name=DEFAULT_SCHEDULE_TEAM_NAME,
    )
    stats_summary = fetch_schedule_season_summary(
        connection, season=selected_season, team_name=DEFAULT_SCHEDULE_TEAM_NAME,
    )
    _render_home_hero(schedule_season, str(schedule_summary.get("record") or ""))

    summary = fetch_team_summary(connection, selected_season)
    next_game = fetch_next_game(
        connection,
        season=schedule_season,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
    )
    saved_postgames = fetch_saved_writeups(connection, season=selected_season, phase="postgame")
    milestone_lines = fetch_records_and_milestones_watch(connection, schedule_season)
    player_of_week = fetch_player_of_the_week(connection, schedule_season)
    standings = fetch_enriched_standings_snapshot(connection, season=schedule_season)
    data_freshness = fetch_team_data_freshness(
        connection,
        season=schedule_season,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
    )
    render_data_freshness_caption(data_freshness)

    snapshot_title = (
        "Team Snapshot"
        if selected_season == schedule_season
        else f"Last Season — {selected_season.replace('Maple Tree ', '').strip()}"
    )
    st.markdown(f"### {snapshot_title}")
    _render_metric_grid(
        [
            ("Record", str(stats_summary.get("record") or "—")),
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

    _render_potw_card(player_of_week)

    detail_cols = st.columns(1 if layout.is_mobile_layout else 2, gap="small")
    with detail_cols[0]:
        _render_next_game_card(next_game)
    if layout.is_mobile_layout:
        _render_milestone_card(milestone_lines)
    else:
        with detail_cols[1]:
            _render_milestone_card(milestone_lines)

    _render_pregame_snapshot(
        connection,
        next_game=next_game,
        schedule_season=schedule_season,
        is_mobile_layout=layout.is_mobile_layout,
    )

    _render_postgame_card(saved_postgames)

    st.markdown("### Quick Links")
    link_rows = [st.columns(3), st.columns(3), st.columns(3), st.columns(3)]
    link_rows[0][0].page_link("pages/1_Current_Season_Stats.py", label="Current Season Stats")
    link_rows[0][1].page_link("pages/2_All_Time_Career_Stats.py", label="All-Time / Career Stats")
    link_rows[0][2].page_link("pages/7_Advanced_Analytics.py", label="Advanced Analytics")
    link_rows[1][0].page_link("pages/5_Records.py", label="Records")
    link_rows[1][1].page_link("pages/12_Single_Game_Hall_of_Fame.py", label="Single-Game Hall of Fame")
    link_rows[1][2].page_link("pages/6_Milestones.py", label="Milestones")
    link_rows[2][0].page_link("pages/8_Schedule.py", label="Schedule")
    link_rows[2][1].page_link("pages/11_Rivalry_Ledger.py", label="Rivalry Ledger")
    link_rows[2][2].page_link("pages/9_Write_Ups.py", label="Write-Ups")
    if role == ROLE_ADMIN:
        link_rows[3][0].page_link("pages/3_Lineup_Optimizer.py", label="Lineup Optimizer")
        link_rows[3][1].page_link("pages/4_Admin_Data.py", label="Admin / Data")

    st.markdown("### Top Hitters")
    st.markdown(
        "<div class='home-section-note'>Quick look at the hottest bats for the selected overview season.</div>",
        unsafe_allow_html=True,
    )
    top_hitters = fetch_top_hitters(connection, selected_season, min_pa=0, limit=8)
    top_hitters = with_player_link_column(top_hitters, output_column="player")
    top_hitters_display = top_hitters[[column for column in top_hitters.columns if column != "canonical_name"]]
    render_static_table(
        top_hitters_display,
        column_labels={
            "player": "Player",
            "pa": "PA",
            "hr": "HR",
            "r": "R",
            "rbi": "RBI",
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
        link_columns=["player"],
        css_class="home-top-hitters-table",
    )

    st.markdown("### Quick Season Totals")
    st.write(
        f"{selected_season}: {int(summary['runs'])} runs, {int(summary['home_runs'])} home runs, "
        f"{int(summary['plate_appearances'])} plate appearances."
    )
    render_mobile_install_help()


def _start_navigation(role: str):
    """Build the section-grouped sidebar navigation.

    ``expanded=True`` (newer Streamlit) keeps every section visible instead of collapsing the
    overflow behind a "View X more" link once we pass ~12 pages. The deployed runtime can be an
    older Streamlit that doesn't accept ``expanded`` — there, passing it raises ``TypeError`` and
    Streamlit drops to its flat auto-pages sidebar. Fall back to the plain call so the
    section-grouped sidebar still renders everywhere.
    """
    pages = _build_navigation(role)
    try:
        return st.navigation(pages, position="sidebar", expanded=True)
    except TypeError:
        return st.navigation(pages, position="sidebar")


def main() -> None:
    role = ensure_authenticated(render_session_controls=False)
    navigation = _start_navigation(role)
    navigation.run()


if __name__ == "__main__":
    main()
