from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    DEFAULT_DASHBOARD_SEASON,
    fetch_current_league_week,
    fetch_current_schedule_week,
    fetch_latest_standings_snapshot,
    fetch_league_divisions,
    fetch_league_schedule_games,
    fetch_league_schedule_seasons,
    fetch_league_team_names,
    fetch_league_team_recent_results,
    fetch_league_team_summary,
    fetch_league_team_upcoming_games,
    fetch_league_team_week_opponents,
    fetch_league_weeks,
    fetch_next_game,
    fetch_schedule_games,
    fetch_schedule_opponents,
    fetch_schedule_season_summary,
    fetch_schedule_seasons,
    fetch_schedule_team_names,
    fetch_schedule_weeks,
    fetch_upcoming_schedule,
    fetch_week_scoreboard,
    get_connection,
    sort_seasons,
)
from src.dashboard.ui import database_path_control
from src.models.schedule import DEFAULT_SCHEDULE_TEAM_NAME


st.set_page_config(page_title="Schedule", page_icon="📅", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_schedule_css() -> None:
    st.markdown(
        """
        <style>
        .schedule-next-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.9rem 1rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .schedule-next-label {
            font-size: 0.82rem;
            color: #6b7280;
            margin-bottom: 0.12rem;
        }
        .schedule-next-opponent {
            font-size: 1.42rem;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 0.18rem;
        }
        .schedule-next-meta {
            font-size: 0.92rem;
            color: #374151;
            margin-bottom: 0.08rem;
        }
        .schedule-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.35rem;
        }
        .schedule-chip-row {
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            margin: 0.08rem 0 0.4rem 0;
        }
        .schedule-chip {
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 999px;
            padding: 0.2rem 0.58rem;
            font-size: 0.82rem;
            background: white;
        }
        .schedule-scout-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.9rem 1rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .schedule-scout-title {
            font-size: 1.2rem;
            font-weight: 800;
            margin-bottom: 0.2rem;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _team_schedule_column_config() -> dict[str, st.column_config.Column]:
    return {
        "week_label": st.column_config.TextColumn("Week", width="small"),
        "date_display": st.column_config.TextColumn("Date", width="small"),
        "time_display": st.column_config.TextColumn("Time", width="small"),
        "opponent_display": st.column_config.TextColumn("Opponent", width="medium"),
        "home_away_display": st.column_config.TextColumn("Side", width="small"),
        "location_or_field": st.column_config.TextColumn("Field", width="medium"),
        "status_display": st.column_config.TextColumn("Status", width="small"),
        "result_display": st.column_config.TextColumn("Result", width="small"),
        "rf_ra_display": st.column_config.TextColumn("Score", width="small"),
        "notes": st.column_config.TextColumn("Notes", width="large"),
    }


def _league_schedule_column_config() -> dict[str, st.column_config.Column]:
    return {
        "week_label": st.column_config.TextColumn("Week", width="small"),
        "date_display": st.column_config.TextColumn("Date", width="small"),
        "time_display": st.column_config.TextColumn("Time", width="small"),
        "matchup_display": st.column_config.TextColumn("Matchup", width="large"),
        "location_or_field": st.column_config.TextColumn("Field", width="medium"),
        "status_display": st.column_config.TextColumn("Status", width="small"),
        "team_result": st.column_config.TextColumn("Result", width="small"),
        "score_display": st.column_config.TextColumn("Score", width="small"),
        "notes": st.column_config.TextColumn("Notes", width="large"),
    }


def _team_schedule_display_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    columns = [
        "week_label",
        "date_display",
        "time_display",
        "opponent_display",
        "home_away_display",
        "location_or_field",
        "status_display",
        "result_display",
        "rf_ra_display",
        "notes",
    ]
    return dataframe[[column for column in columns if column in dataframe.columns]].copy()


def _league_schedule_display_table(dataframe: pd.DataFrame, include_result: bool = True) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    columns = [
        "week_label",
        "date_display",
        "time_display",
        "matchup_display",
        "location_or_field",
        "status_display",
    ]
    if include_result:
        columns.extend(["team_result", "score_display"])
    columns.append("notes")
    return dataframe[[column for column in columns if column in dataframe.columns]].copy()


def _render_summary_chips(chips: list[str]) -> None:
    html = "".join(f"<div class='schedule-chip'>{chip}</div>" for chip in chips)
    st.markdown("<div class='schedule-chip-row'>" + html + "</div>", unsafe_allow_html=True)


def _render_next_game(next_game: dict[str, object] | None) -> None:
    st.subheader("Next Game")
    if not next_game:
        st.info("No upcoming scheduled game is currently loaded for the selected filters.")
        return

    notes = str(next_game.get("notes") or "").strip()
    field = str(next_game.get("location_or_field") or "").strip()
    st.markdown(
        f"""
        <div class="schedule-next-card">
          <div class="schedule-next-label">{next_game.get('week_label', '')}</div>
          <div class="schedule-next-opponent">vs {next_game.get('opponent_display', '')}</div>
          <div class="schedule-next-meta">{next_game.get('date_display', '')} at {next_game.get('time_display', '')}</div>
          <div class="schedule-next-meta">{next_game.get('home_away_display', '')}{' • ' + field if field else ''}</div>
          <div class="schedule-next-meta">{next_game.get('status_display', '')}</div>
          {f"<div class='schedule-note'>{notes}</div>" if notes else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_maple_tree_week_callout(
    *,
    focus_week: str | None,
    opponent_names: list[str],
    games: pd.DataFrame,
) -> None:
    st.subheader("Maple Tree This Week")
    if not focus_week:
        st.info("No current league week is loaded yet for Maple Tree.")
        return

    if games.empty:
        st.info(f"No Maple Tree league game is loaded for {focus_week}.")
        return

    field_values = [str(value).strip() for value in games["location_or_field"].fillna("").tolist() if str(value).strip()]
    unique_fields = sorted(set(field_values))
    first_game = games.iloc[0]
    opponent_label = ", ".join(opponent_names) if opponent_names else "TBD"
    if len(opponent_names) == 1 and len(games) > 1:
        headline = f"Doubleheader vs {opponent_names[0]}"
    elif len(opponent_names) == 1:
        headline = f"vs {opponent_names[0]}"
    else:
        headline = f"{len(games)} games this week"

    date_label = str(first_game.get("date_display") or "")
    time_values = [str(value).strip() for value in games["time_display"].fillna("").tolist() if str(value).strip()]
    time_label = ", ".join(time_values[:3])
    if len(time_values) > 3:
        time_label += ", ..."
    field_label = ", ".join(unique_fields[:2])
    if len(unique_fields) > 2:
        field_label += ", ..."

    st.markdown(
        f"""
        <div class="schedule-scout-card">
          <div class="schedule-next-label">{focus_week}</div>
          <div class="schedule-scout-title">{headline}</div>
          <div class="schedule-next-meta"><strong>Opponent{'' if len(opponent_names) == 1 else 's'}:</strong> {opponent_label}</div>
          <div class="schedule-next-meta"><strong>Date:</strong> {date_label}{f" &nbsp; <strong>Times:</strong> {time_label}" if time_label else ""}</div>
          <div class="schedule-next-meta">{f"<strong>Field:</strong> {field_label}" if field_label else ""}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_team_schedule(connection, selected_season: str) -> None:
    control_row_one = st.columns([1.1, 1.1, 1, 1], gap="small")
    season_team_options = fetch_schedule_team_names(connection, selected_season)
    default_team = DEFAULT_SCHEDULE_TEAM_NAME if DEFAULT_SCHEDULE_TEAM_NAME in season_team_options else season_team_options[0]
    with control_row_one[0]:
        team_name = st.selectbox("Team", options=season_team_options, index=season_team_options.index(default_team))
    with control_row_one[1]:
        view_filter = st.selectbox("Games view", options=["Upcoming only", "Completed only", "All"], index=0)
    with control_row_one[2]:
        opponent_options = ["All opponents", *fetch_schedule_opponents(connection, selected_season, team_name)]
        opponent_filter = st.selectbox("Opponent", options=opponent_options, index=0)
    with control_row_one[3]:
        week_options = ["All weeks", *fetch_schedule_weeks(connection, selected_season, team_name)]
        current_week = fetch_current_schedule_week(connection, selected_season, team_name, as_of=date.today())
        default_week = current_week if current_week in week_options else "All weeks"
        week_filter = st.selectbox("Week", options=week_options, index=week_options.index(default_week))

    _render_summary_chips(
        [
            f"<strong>Season:</strong> {selected_season}",
            f"<strong>Team:</strong> {team_name}",
            f"<strong>View:</strong> {view_filter}",
            f"<strong>Opponent:</strong> {opponent_filter}",
            f"<strong>Week:</strong> {week_filter}",
        ]
    )

    all_games = fetch_schedule_games(
        connection,
        season=selected_season,
        team_name=team_name,
        view_filter=view_filter,
        opponent=opponent_filter,
        week_label=week_filter,
        as_of=date.today(),
    )
    next_game = fetch_next_game(connection, season=selected_season, team_name=team_name, as_of=date.today())
    upcoming_games = fetch_upcoming_schedule(connection, season=selected_season, team_name=team_name, limit=8, as_of=date.today())
    standings = fetch_latest_standings_snapshot(connection, season=selected_season)
    season_summary = fetch_schedule_season_summary(connection, season=selected_season, team_name=team_name, as_of=date.today())

    st.subheader("Season Summary")
    summary_cols = st.columns(5)
    summary_cols[0].metric("Record", str(season_summary["record"]))
    summary_cols[1].metric("Runs For", int(season_summary["runs_for"]))
    summary_cols[2].metric("Runs Against", int(season_summary["runs_against"]))
    summary_cols[3].metric("Completed", int(season_summary["games_completed"]))
    summary_cols[4].metric("Remaining", int(season_summary["games_remaining"]))

    _render_next_game(next_game)

    if not standings.empty:
        st.subheader("Standings")
        st.markdown(
            "<div class='schedule-note'>Latest local standings snapshot loaded for the selected season.</div>",
            unsafe_allow_html=True,
        )
        standings_display = standings.rename(
            columns={
                "team_name": "Team",
                "wins": "W",
                "losses": "L",
                "win_pct": "Pct",
                "games_back": "GB",
            }
        )[["Team", "W", "L", "Pct", "GB"]]
        st.dataframe(
            standings_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Pct": st.column_config.NumberColumn("Pct", format="%.3f"),
                "GB": st.column_config.NumberColumn("GB", format="%.1f"),
            },
        )

    st.subheader("Upcoming Games")
    st.markdown(
        "<div class='schedule-note'>Next few loaded games for the selected team. Byes are shown clearly when present.</div>",
        unsafe_allow_html=True,
    )
    upcoming_display = _team_schedule_display_table(upcoming_games)
    if upcoming_display.empty:
        st.info("No upcoming games are currently loaded for this team.")
    else:
        st.dataframe(
            upcoming_display,
            use_container_width=True,
            hide_index=True,
            column_config=_team_schedule_column_config(),
        )

    st.subheader("Full Schedule")
    st.markdown(
        "<div class='schedule-note'>Filtered schedule view with room for future results, scores, and recap links.</div>",
        unsafe_allow_html=True,
    )
    display_table = _team_schedule_display_table(all_games)
    if display_table.empty:
        st.info("No schedule rows match the current filters.")
    else:
        st.dataframe(
            display_table,
            use_container_width=True,
            hide_index=True,
            column_config=_team_schedule_column_config(),
        )


def _render_team_scout_card(summary: dict[str, object], recent_results: pd.DataFrame, upcoming_games: pd.DataFrame, team_name: str) -> None:
    last_results = ", ".join(
        f"{row['team_result']} {row['score_display']} vs {row['home_team'] if row['away_team'] == team_name else row['away_team']}"
        for _, row in recent_results.iterrows()
    ) if not recent_results.empty else "No completed league results loaded yet"
    next_games = ", ".join(
        f"{row['date_display']} vs {row['home_team'] if row['away_team'] == team_name else row['away_team']}"
        for _, row in upcoming_games.iterrows()
    ) if not upcoming_games.empty else "No upcoming league games loaded"

    st.markdown(
        f"""
        <div class="schedule-scout-card">
          <div class="schedule-scout-title">{team_name}</div>
          <div class="schedule-next-meta"><strong>Record:</strong> {summary['record']}</div>
          <div class="schedule-next-meta"><strong>Runs Scored:</strong> {summary['runs_for']} &nbsp; <strong>Runs Allowed:</strong> {summary['runs_against']}</div>
          <div class="schedule-next-meta"><strong>Last 3:</strong> {last_results}</div>
          <div class="schedule-next-meta"><strong>Next 3:</strong> {next_games}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_league_scouting(connection, selected_season: str) -> None:
    divisions = fetch_league_divisions(connection, selected_season)
    division_options = ["All divisions", *divisions] if divisions else ["All divisions"]
    control_row_one = st.columns([1.1, 1.1, 1, 1, 1], gap="small")
    with control_row_one[0]:
        division_name = st.selectbox("Division", options=division_options, index=0)
    normalized_division = None if division_name == "All divisions" else division_name
    current_week = fetch_current_league_week(connection, selected_season, normalized_division, as_of=date.today())
    focus_week = current_week or "All weeks"
    maple_tree_opponents = (
        fetch_league_team_week_opponents(
            connection,
            season=selected_season,
            team_name=DEFAULT_SCHEDULE_TEAM_NAME,
            week_label=focus_week,
            division_name=normalized_division,
            as_of=date.today(),
        )
        if current_week
        else []
    )
    with control_row_one[1]:
        team_options = ["All teams", *fetch_league_team_names(connection, selected_season, division_name)]
        default_team = "All teams"
        if len(maple_tree_opponents) == 1 and maple_tree_opponents[0] in team_options:
            default_team = maple_tree_opponents[0]
        elif DEFAULT_SCHEDULE_TEAM_NAME in team_options:
            default_team = DEFAULT_SCHEDULE_TEAM_NAME
        team_name = st.selectbox("Scout team", options=team_options, index=team_options.index(default_team))
    with control_row_one[2]:
        view_filter = st.selectbox("League view", options=["All", "Upcoming only", "Completed only"], index=0)
    with control_row_one[3]:
        week_options = ["All weeks", *fetch_league_weeks(connection, selected_season, division_name)]
        default_week = current_week if current_week in week_options else "All weeks"
        week_filter = st.selectbox("Week", options=week_options, index=week_options.index(default_week))
    with control_row_one[4]:
        opponent_options = ["All opponents"]
        if team_name != "All teams":
            other_teams = [name for name in fetch_league_team_names(connection, selected_season, division_name) if name != team_name]
            opponent_options.extend(other_teams)
        opponent_filter = st.selectbox("Opponent", options=opponent_options, index=0)

    _render_summary_chips(
        [
            f"<strong>Season:</strong> {selected_season}",
            f"<strong>Division:</strong> {division_name}",
            f"<strong>Team:</strong> {team_name}",
            f"<strong>View:</strong> {view_filter}",
            f"<strong>Week:</strong> {week_filter}",
        ]
    )

    callout_week = week_filter if week_filter != "All weeks" else current_week
    maple_tree_week_games = (
        fetch_league_schedule_games(
            connection,
            season=selected_season,
            division_name=normalized_division,
            week_label=callout_week,
            team_name=DEFAULT_SCHEDULE_TEAM_NAME,
            view_filter="All",
            as_of=date.today(),
        )
        if callout_week
        else pd.DataFrame()
    )
    callout_opponents = (
        fetch_league_team_week_opponents(
            connection,
            season=selected_season,
            team_name=DEFAULT_SCHEDULE_TEAM_NAME,
            week_label=callout_week,
            division_name=normalized_division,
            as_of=date.today(),
        )
        if callout_week
        else []
    )

    _render_maple_tree_week_callout(
        focus_week=callout_week,
        opponent_names=callout_opponents,
        games=maple_tree_week_games,
    )

    scoreboard = fetch_week_scoreboard(
        connection,
        season=selected_season,
        division_name=normalized_division,
        week_label=None if week_filter == "All weeks" else week_filter,
        as_of=date.today(),
    )

    if team_name != "All teams":
        scout_summary = fetch_league_team_summary(
            connection,
            season=selected_season,
            team_name=team_name,
            division_name=normalized_division,
            as_of=date.today(),
        )
        recent_results = fetch_league_team_recent_results(
            connection,
            season=selected_season,
            team_name=team_name,
            division_name=normalized_division,
            limit=3,
            as_of=date.today(),
        )
        upcoming_games = fetch_league_team_upcoming_games(
            connection,
            season=selected_season,
            team_name=team_name,
            division_name=normalized_division,
            limit=3,
            as_of=date.today(),
        )
    else:
        scout_summary = None
        recent_results = pd.DataFrame()
        upcoming_games = pd.DataFrame()

    team_schedule_week_filter = None if team_name != "All teams" else (None if week_filter == "All weeks" else week_filter)
    opponent_schedule = fetch_league_schedule_games(
        connection,
        season=selected_season,
        division_name=normalized_division,
        week_label=team_schedule_week_filter,
        team_name=None if team_name == "All teams" else team_name,
        opponent=None if opponent_filter == "All opponents" else opponent_filter,
        view_filter=view_filter,
        as_of=date.today(),
    )

    st.subheader("Week Scoreboard")
    st.markdown(
        "<div class='schedule-note'>Completed games show final scores. Scheduled games keep time and field for weekly scouting.</div>",
        unsafe_allow_html=True,
    )
    if scoreboard.empty:
        st.info("No league games are currently loaded for the selected filters.")
    else:
        st.dataframe(
            _league_schedule_display_table(scoreboard, include_result=False),
            use_container_width=True,
            hide_index=True,
            column_config=_league_schedule_column_config(),
        )

    st.subheader("Team Scout Card")
    if team_name == "All teams" or scout_summary is None:
        st.info("Choose a specific team to view a compact scouting summary.")
    else:
        _render_team_scout_card(scout_summary, recent_results, upcoming_games, team_name)

    st.subheader("Opponent Schedule / Results")
    note = (
        "Showing the selected team's full loaded schedule/results across all weeks for scouting."
        if team_name != "All teams"
        else "Use this to scout an opponent's recent results and upcoming games."
    )
    st.markdown(f"<div class='schedule-note'>{note}</div>", unsafe_allow_html=True)
    if opponent_schedule.empty:
        st.info("No league schedule rows match the current scouting filters.")
    else:
        st.dataframe(
            _league_schedule_display_table(opponent_schedule),
            use_container_width=True,
            hide_index=True,
            column_config=_league_schedule_column_config(),
        )

    st.subheader("League Schedule")
    league_schedule = fetch_league_schedule_games(
        connection,
        season=selected_season,
        division_name=normalized_division,
        week_label=None if week_filter == "All weeks" else week_filter,
        view_filter=view_filter,
        as_of=date.today(),
    )
    if league_schedule.empty:
        st.info("No league schedule is currently loaded for the selected filters.")
    else:
        st.dataframe(
            _league_schedule_display_table(league_schedule, include_result=False),
            use_container_width=True,
            hide_index=True,
            column_config=_league_schedule_column_config(),
        )


_inject_schedule_css()
ensure_authenticated()

st.title("Schedule")
st.caption("Local team schedule plus league-wide scouting powered by imported CSV data.")

db_path = database_path_control(DEFAULT_DB_PATH, key="schedule_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())

team_schedule_seasons = fetch_schedule_seasons(connection)
league_schedule_seasons = fetch_league_schedule_seasons(connection)
all_seasons = sort_seasons(sorted(set(team_schedule_seasons + league_schedule_seasons)))
if not all_seasons:
    st.warning("No schedule data is loaded yet. Import local schedule CSVs with `python manage_schedule.py --mode import`.")
    st.stop()

default_season = DEFAULT_DASHBOARD_SEASON if DEFAULT_DASHBOARD_SEASON in all_seasons else all_seasons[0]
top_controls = st.columns([1, 1.4], gap="small")
with top_controls[0]:
    selected_season = st.selectbox("Season", options=all_seasons, index=all_seasons.index(default_season))
with top_controls[1]:
    schedule_mode = st.segmented_control(
        "Schedule Mode",
        options=["Team Schedule", "League Scouting"],
        default="Team Schedule",
    )

if schedule_mode == "League Scouting":
    _render_league_scouting(connection, selected_season)
else:
    _render_team_schedule(connection, selected_season)
