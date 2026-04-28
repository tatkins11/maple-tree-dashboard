from __future__ import annotations

import re
from datetime import date
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import ROLE_ADMIN, ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DASHBOARD_SEASON,
    DEFAULT_DB_PATH,
    evaluate_manual_lineup,
    fetch_active_roster,
    fetch_available_projection_rows,
    fetch_current_schedule_week,
    fetch_lineup_current_season_context,
    fetch_league_schedule_seasons,
    fetch_maple_tree_week_bundle,
    fetch_projection_seasons,
    fetch_schedule_season_summary,
    fetch_schedule_seasons,
    fetch_schedule_weeks,
    fetch_saved_writeup,
    fetch_saved_writeups,
    fetch_writeup_milestone_watch,
    fetch_writeup_opponent_scouting,
    fetch_writeup_record_context,
    get_connection,
    run_optimizer,
    save_weekly_writeup,
    sort_seasons,
)
from src.dashboard.ui import database_path_control
from src.dashboard.writeups import (
    annotate_pregame_lineup,
    build_postgame_markdown,
    build_pregame_key_lines,
    build_pregame_markdown,
    build_pregame_overview_insight_lines,
    resolve_postgame_games,
    suggest_markdown_filename,
)
from src.models.optimizer import DEFAULT_PREFERRED_LINEUP
from src.models.schedule import DEFAULT_SCHEDULE_TEAM_NAME


st.set_page_config(page_title="Write-Ups", page_icon="📝", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_writeup_css() -> None:
    st.markdown(
        """
        <style>
        .writeups-top-note {
            font-size: 0.9rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.55rem;
        }
        .writeups-week-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.95rem;
            padding: 0.85rem 0.95rem;
            background: #fafafa;
            margin-bottom: 0.45rem;
            min-height: 8.2rem;
        }
        .writeups-week-label {
            font-size: 0.8rem;
            color: #6b7280;
            margin-bottom: 0.18rem;
        }
        .writeups-week-title {
            font-size: 1.2rem;
            font-weight: 800;
            margin-bottom: 0.18rem;
            line-height: 1.15;
        }
        .writeups-week-meta {
            font-size: 0.92rem;
            color: #374151;
            margin-bottom: 0.08rem;
        }
        .writeups-controls {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.95rem;
            padding: 0.8rem 0.9rem 0.3rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .writeups-output-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.12rem;
            margin-bottom: 0.35rem;
        }
        .writeups-upload-box {
            border: 1px dashed rgba(49, 51, 63, 0.18);
            border-radius: 0.8rem;
            padding: 0.55rem 0.7rem;
            background: #fcfcfc;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value)


def _state_key(*parts: str) -> str:
    return _safe_key("_".join(parts))


def _lineup_table(ordered_names: list[str], projection_rows: pd.DataFrame) -> pd.DataFrame:
    by_name = projection_rows.set_index("player").to_dict("index") if not projection_rows.empty else {}
    rows: list[dict[str, object]] = []
    for index, name in enumerate(ordered_names, start=1):
        record = by_name.get(name, {})
        rows.append(
            {
                "spot": index,
                "player": name,
                "lineup_note": "DHH" if bool(record.get("fixed_dhh")) else "",
                "projection_source": record.get("projection_source", ""),
                "proj_obp": float(record.get("proj_obp", 0.0)),
                "proj_run_rate": float(record.get("proj_run_rate", 0.0)),
                "proj_rbi_rate": float(record.get("proj_rbi_rate", 0.0)),
                "proj_xbh_rate": float(record.get("proj_xbh_rate", 0.0)),
            }
        )
    return pd.DataFrame(rows)


def _attach_lineup_current_season_context(
    lineup_df: pd.DataFrame,
    season_context: dict[str, object],
) -> pd.DataFrame:
    if lineup_df.empty:
        return lineup_df

    player_metrics = season_context.get("player_metrics", {}) if isinstance(season_context, dict) else {}
    enriched = lineup_df.copy()
    for column, metric_key in (
        ("season_pa", "pa"),
        ("season_r", "r"),
        ("season_rbi", "rbi"),
        ("season_hr", "hr"),
        ("season_avg", "avg"),
        ("season_obp", "obp"),
        ("season_slg", "slg"),
        ("season_ops", "ops"),
    ):
        enriched.loc[:, column] = enriched["player"].map(
            lambda player_name: (player_metrics.get(str(player_name), {}) or {}).get(metric_key, 0)
        )
    return enriched


def _suggest_manual_lineup_order(player_pool: list[str]) -> list[str]:
    preferred = [name for name in DEFAULT_PREFERRED_LINEUP if name in player_pool]
    extras = [name for name in player_pool if name not in preferred]
    return [*preferred, *extras]


def _seed_manual_lineup_state(
    player_pool: list[str],
    *,
    default_order: list[str],
    key_prefix: str,
) -> None:
    signature = "|".join(player_pool) + "||" + "|".join(default_order)
    signature_key = f"{key_prefix}_signature"
    if st.session_state.get(signature_key) == signature:
        return

    for spot, player_name in enumerate(default_order, start=1):
        st.session_state[f"{key_prefix}_spot_{spot}"] = player_name
    st.session_state[signature_key] = signature


def _build_manual_lineup_order(
    player_pool: list[str],
    *,
    key_prefix: str,
    default_order: list[str] | None = None,
) -> list[str]:
    if not player_pool:
        return []

    seeded_order = default_order or player_pool
    _seed_manual_lineup_state(
        player_pool,
        default_order=seeded_order,
        key_prefix=key_prefix,
    )

    ordered_names: list[str] = []
    remaining_names = list(player_pool)
    columns = st.columns(3, gap="small")

    for spot, _ in enumerate(player_pool, start=1):
        state_key = f"{key_prefix}_spot_{spot}"
        current_value = st.session_state.get(state_key)
        if current_value not in remaining_names:
            current_value = remaining_names[0]
            st.session_state[state_key] = current_value

        with columns[(spot - 1) % len(columns)]:
            selected_name = st.selectbox(
                f"Spot {spot}",
                options=remaining_names,
                index=remaining_names.index(current_value),
                key=state_key,
            )

        ordered_names.append(selected_name)
        remaining_names.remove(selected_name)

    return ordered_names


def _render_week_cards(games: pd.DataFrame, week_label: str) -> None:
    if games.empty:
        st.info("No Maple Tree games are loaded for the selected week.")
        return

    columns = st.columns(max(len(games), 1), gap="small")
    for column, (_, row) in zip(columns, games.iterrows()):
        opponent = str(row.get("opponent_display") or row.get("opponent_name") or "Opponent")
        notes = str(row.get("notes") or "").strip()
        column.markdown(
            f"""
            <div class="writeups-week-card">
              <div class="writeups-week-label">{escape(week_label)}</div>
              <div class="writeups-week-title">vs {escape(opponent)}</div>
              <div class="writeups-week-meta">{escape(str(row.get('date_display') or ''))}</div>
              <div class="writeups-week-meta">{escape(str(row.get('time_display') or 'TBD'))}</div>
              <div class="writeups-week-meta">{escape(str(row.get('home_away_display') or ''))}</div>
              <div class="writeups-week-meta">{escape(str(row.get('location_or_field') or 'TBD'))}</div>
              {f"<div class='writeups-week-meta'>{escape(notes)}</div>" if notes else ""}
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_markdown_output(*, markdown: str, filename: str, key_prefix: str) -> None:
    st.markdown(
        "<div class='writeups-output-note'>Copy from the box below or download the Markdown file for posting elsewhere.</div>",
        unsafe_allow_html=True,
    )
    st.text_area(
        "Generated Markdown",
        value=markdown,
        height=420,
        key=f"{key_prefix}_markdown_view",
    )
    st.download_button(
        "Download Markdown",
        data=markdown,
        file_name=filename,
        mime="text/markdown",
        key=f"{key_prefix}_download",
    )


def _render_saved_postgame_archive(connection, *, season: str, key_prefix: str) -> None:
    saved_writeups = fetch_saved_writeups(connection, season=season, phase="postgame")
    st.subheader("Saved Postgame Archive")
    if saved_writeups.empty:
        st.info("No postgame recaps are saved for this season yet.")
        return

    summary = saved_writeups[["week_label", "title", "updated_at", "source"]].rename(
        columns={
            "week_label": "Week",
            "title": "Title",
            "updated_at": "Last Saved",
            "source": "Source",
        }
    )
    st.dataframe(summary, use_container_width=True, hide_index=True)

    selected_id = st.selectbox(
        "Open saved recap",
        options=saved_writeups["writeup_id"].tolist(),
        format_func=lambda value: str(
            saved_writeups.loc[saved_writeups["writeup_id"] == value, "title"].iloc[0]
        ),
        key=f"{key_prefix}_saved_select",
    )
    selected = saved_writeups.loc[saved_writeups["writeup_id"] == selected_id].iloc[0].to_dict()
    _render_markdown_output(
        markdown=str(selected.get("markdown") or ""),
        filename=suggest_markdown_filename(
            season=str(selected.get("season") or season),
            week_label=str(selected.get("week_label") or "saved"),
            phase="postgame",
        ),
        key_prefix=f"{key_prefix}_saved_{selected_id}",
    )


def _prefill_text_input(key: str, value: str) -> None:
    if key not in st.session_state:
        st.session_state[key] = value


def main() -> None:
    role = ensure_authenticated()
    _inject_writeup_css()

    st.title("Write-Ups")
    st.caption("Weekly pregame and doubleheader recap generation built directly into the dashboard.")

    db_path = database_path_control(DEFAULT_DB_PATH, key="writeups_db_path")
    connection = get_db_connection(db_path, get_connection_cache_key())

    schedule_seasons = fetch_schedule_seasons(connection)
    league_schedule_seasons = fetch_league_schedule_seasons(connection)
    all_seasons = sort_seasons(sorted(set(schedule_seasons + league_schedule_seasons)))
    if not all_seasons:
        st.warning("No schedule data is loaded yet. Import local schedule CSVs before using the write-up workflow.")
        st.stop()

    default_season = DEFAULT_DASHBOARD_SEASON if DEFAULT_DASHBOARD_SEASON in all_seasons else all_seasons[0]
    season_index = all_seasons.index(default_season)
    control_row = st.columns([1.1, 1.3], gap="small")
    with control_row[0]:
        selected_season = st.selectbox("Season", options=all_seasons, index=season_index)

    week_options = fetch_schedule_weeks(connection, selected_season, DEFAULT_SCHEDULE_TEAM_NAME)
    if not week_options:
        st.warning("No Maple Tree team schedule weeks are loaded for the selected season.")
        st.stop()

    current_week = fetch_current_schedule_week(
        connection,
        season=selected_season,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
        as_of=date.today(),
    )
    default_week = current_week if current_week in week_options else week_options[0]
    with control_row[1]:
        selected_week = st.selectbox("Week", options=week_options, index=week_options.index(default_week))

    week_bundle = fetch_maple_tree_week_bundle(
        connection,
        season=selected_season,
        week_label=selected_week,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
        as_of=date.today(),
    )
    week_games = week_bundle["games"] if isinstance(week_bundle.get("games"), pd.DataFrame) else pd.DataFrame()
    non_bye_games = (
        week_bundle["non_bye_games"]
        if isinstance(week_bundle.get("non_bye_games"), pd.DataFrame)
        else pd.DataFrame()
    )

    st.markdown(
        "<div class='writeups-top-note'>Tuned for a two-game Maple Tree week. Postgame recaps are saved to the dashboard archive and can still be copied or downloaded as Markdown.</div>",
        unsafe_allow_html=True,
    )
    _render_week_cards(non_bye_games if not non_bye_games.empty else week_games, selected_week)

    validation_message = str(week_bundle.get("validation_message") or "").strip()
    generation_enabled = bool(week_bundle.get("generation_enabled"))
    if validation_message:
        st.warning(validation_message)

    if role != ROLE_ADMIN:
        _render_saved_postgame_archive(
            connection,
            season=selected_season,
            key_prefix=_state_key("viewer_saved_postgames", selected_season),
        )
        st.stop()

    tabs = st.tabs(["Pregame", "Postgame", "Saved"])

    with tabs[0]:
        active_roster_df = fetch_active_roster(connection)
        active_names = active_roster_df["preferred_display_name"].tolist() if not active_roster_df.empty else []
        projection_seasons = fetch_projection_seasons(connection)
        projection_default = (
            selected_season
            if selected_season in projection_seasons
            else (projection_seasons[0] if projection_seasons else "")
        )
        projection_index = projection_seasons.index(projection_default) if projection_default in projection_seasons else 0
        week_date = str(week_bundle.get("primary_game_date") or "")
        state_prefix = _state_key("pregame", selected_season, selected_week)

        st.markdown("<div class='writeups-controls'>", unsafe_allow_html=True)
        pregame_row_one = st.columns([1.05, 1.05, 1.25, 1], gap="small")
        with pregame_row_one[0]:
            projection_season = st.selectbox(
                "Projection season",
                options=projection_seasons,
                index=projection_index if projection_seasons else None,
                key=f"{state_prefix}_projection_season",
            ) if projection_seasons else ""
        with pregame_row_one[1]:
            lineup_source = st.radio(
                "Lineup source",
                options=["Optimizer", "Manual"],
                horizontal=True,
                index=0,
                key=f"{state_prefix}_lineup_source",
            )
        with pregame_row_one[2]:
            optimizer_mode = st.radio(
                "Optimizer mode",
                options=["team_aware", "unconstrained"],
                horizontal=True,
                index=0,
                key=f"{state_prefix}_optimizer_mode",
                disabled=lineup_source != "Optimizer",
            )
        with pregame_row_one[3]:
            simulations = st.slider(
                "Simulation count",
                min_value=200,
                max_value=5000,
                value=1000,
                step=200,
                key=f"{state_prefix}_simulations",
            )

        selected_players = st.multiselect(
            "Available players",
            options=active_names,
            default=active_names,
            help="Choose who is available, then either optimize the order or set it manually and simulate that exact lineup.",
            key=f"{state_prefix}_players",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        projection_rows = (
            fetch_available_projection_rows(connection, projection_season, selected_players)
            if projection_season and selected_players
            else pd.DataFrame()
        )
        projected_player_names = (
            projection_rows["player"].dropna().astype(str).tolist()
            if not projection_rows.empty and "player" in projection_rows.columns
            else []
        )
        missing_projection_players = [
            name for name in selected_players if str(name).strip() and name not in projected_player_names
        ]
        if projection_seasons and not projection_rows.empty:
            st.dataframe(
                projection_rows[
                    [
                        "player",
                        "projection_source",
                        "fixed_dhh",
                        "proj_obp",
                        "proj_run_rate",
                        "proj_rbi_rate",
                        "proj_xbh_rate",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "fixed_dhh": st.column_config.CheckboxColumn("Fixed DHH"),
                    "proj_obp": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
                    "proj_run_rate": st.column_config.NumberColumn("Proj Run Rate", format="%.3f"),
                    "proj_rbi_rate": st.column_config.NumberColumn("Proj RBI Rate", format="%.3f"),
                    "proj_xbh_rate": st.column_config.NumberColumn("XBH Rate", format="%.3f"),
                },
            )
            if missing_projection_players:
                st.warning(
                    "These available players do not currently have usable projection rows and will be left out of lineup simulation: "
                    + ", ".join(missing_projection_players)
                )
        elif projection_seasons and selected_players:
            st.info("No usable projection rows were found for the currently selected player pool.")
        else:
            st.info("Load hitter projections before generating a pregame write-up.")

        manual_lineup_order: list[str] = []
        if lineup_source == "Manual" and projected_player_names:
            st.caption("Manual mode simulates exactly the batting order you set below.")
            manual_default_order = _suggest_manual_lineup_order(projected_player_names)
            manual_lineup_order = _build_manual_lineup_order(
                projected_player_names,
                key_prefix=f"{state_prefix}_manual_lineup",
                default_order=manual_default_order,
            )

        generate_pregame = st.button(
            "Generate Pregame Write-Up",
            type="primary",
            disabled=(
                not generation_enabled
                or not projection_seasons
                or not week_date
                or not selected_players
                or projection_rows.empty
                or (lineup_source == "Manual" and len(manual_lineup_order) != len(projected_player_names))
            ),
            key=f"{state_prefix}_generate",
        )

        if generate_pregame:
            try:
                if lineup_source == "Manual":
                    ordered_names = manual_lineup_order
                    simulation_summary = evaluate_manual_lineup(
                        connection=connection,
                        projection_season=projection_season,
                        ordered_player_names=ordered_names,
                        available_player_names=projected_player_names,
                        simulations=simulations,
                        seed=42,
                    )
                    lineup_title = "Manual Lineup"
                else:
                    result = run_optimizer(
                        connection=connection,
                        projection_season=projection_season,
                        game_date=week_date,
                        available_player_names=projected_player_names,
                        mode=optimizer_mode,
                        simulations=simulations,
                        seed=42,
                    )
                    ordered_names = result.best_lineup.ordered_player_names
                    simulation_summary = result.best_lineup.summary
                    lineup_title = "Recommended Lineup"

                lineup_df = _lineup_table(ordered_names, projection_rows)
                lineup_season_context = fetch_lineup_current_season_context(
                    connection,
                    season=selected_season,
                    ordered_player_names=ordered_names,
                )
                lineup_df = _attach_lineup_current_season_context(lineup_df, lineup_season_context)
                annotated_lineup_rows = annotate_pregame_lineup(lineup_df.to_dict("records"))
                milestone_lines = fetch_writeup_milestone_watch(connection)
                opponent_lines = fetch_writeup_opponent_scouting(
                    connection,
                    season=selected_season,
                    opponent_names=list(week_bundle.get("opponent_names", [])),
                    division_name=str(week_bundle.get("division_name") or "") or None,
                    as_of=date.today(),
                )
                season_summary = fetch_schedule_season_summary(
                    connection,
                    season=selected_season,
                    team_name=DEFAULT_SCHEDULE_TEAM_NAME,
                    as_of=date.today(),
                )
                markdown = build_pregame_markdown(
                    season=selected_season,
                    week_bundle=week_bundle,
                    season_summary=season_summary,
                    lineup_rows=annotated_lineup_rows,
                    milestone_lines=milestone_lines,
                    opponent_lines=opponent_lines,
                    key_lines=build_pregame_key_lines(
                        lineup_rows=annotated_lineup_rows,
                        milestone_lines=milestone_lines,
                        opponent_lines=opponent_lines,
                        week_bundle=week_bundle,
                        season_summary=season_summary,
                    ),
                    overview_insight_lines=build_pregame_overview_insight_lines(
                        annotated_lineup_rows,
                        projected_runs_per_game=simulation_summary.expected_runs_per_game,
                        lineup_season_summary=(
                            lineup_season_context.get("summary")
                            if isinstance(lineup_season_context, dict)
                            else None
                        ),
                        lineup_descriptor=(
                            "manual order" if lineup_source == "Manual" else "recommended order"
                        ),
                    ),
                )
                st.session_state[f"{state_prefix}_lineup"] = annotated_lineup_rows
                st.session_state[f"{state_prefix}_lineup_title"] = lineup_title
                st.session_state[f"{state_prefix}_markdown"] = markdown
            except Exception as exc:
                st.error(f"Pregame generation failed: {exc}")

        saved_lineup_rows = st.session_state.get(f"{state_prefix}_lineup", [])
        saved_lineup_title = str(st.session_state.get(f"{state_prefix}_lineup_title", "Recommended Lineup"))
        saved_pregame_markdown = str(st.session_state.get(f"{state_prefix}_markdown", ""))
        if saved_lineup_rows:
            st.subheader(saved_lineup_title)
            st.dataframe(
                pd.DataFrame(saved_lineup_rows),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "lineup_note": st.column_config.TextColumn("Note"),
                    "strength_note": st.column_config.TextColumn("Strength Focus"),
                    "proj_obp": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
                    "proj_run_rate": st.column_config.NumberColumn("Proj Run Rate", format="%.3f"),
                    "proj_rbi_rate": st.column_config.NumberColumn("Proj RBI Rate", format="%.3f"),
                    "proj_xbh_rate": st.column_config.NumberColumn("XBH Rate", format="%.3f"),
                    "season_pa": st.column_config.NumberColumn("Season PA", format="%d"),
                    "season_avg": st.column_config.NumberColumn("Season AVG", format="%.3f"),
                    "season_obp": st.column_config.NumberColumn("Season OBP", format="%.3f"),
                    "season_slg": st.column_config.NumberColumn("Season SLG", format="%.3f"),
                    "season_ops": st.column_config.NumberColumn("Season OPS", format="%.3f"),
                },
            )
        if saved_pregame_markdown:
            _render_markdown_output(
                markdown=saved_pregame_markdown,
                filename=suggest_markdown_filename(
                    season=selected_season,
                    week_label=selected_week,
                    phase="pregame",
                ),
                key_prefix=f"{state_prefix}_output",
            )

    with tabs[1]:
        postgame_prefix = _state_key("postgame", selected_season, selected_week)
        existing_postgame = fetch_saved_writeup(
            connection,
            season=selected_season,
            week_label=selected_week,
            phase="postgame",
        )
        active_roster_df = fetch_active_roster(connection)
        mvp_options = ["", *active_roster_df["preferred_display_name"].tolist()] if not active_roster_df.empty else [""]

        st.markdown("<div class='writeups-controls'>", unsafe_allow_html=True)
        weekly_summary_note = st.text_area(
            "Weekly summary note",
            help="Short overview of how the doubleheader felt overall.",
            key=f"{postgame_prefix}_weekly_note",
        )
        week_mvp = st.selectbox(
            "Week MVP",
            options=mvp_options,
            format_func=lambda value: "Select MVP" if value == "" else value,
            key=f"{postgame_prefix}_week_mvp",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        if non_bye_games.empty:
            st.info("No playable games are loaded for the selected week.")
        else:
            for index, (_, row) in enumerate(non_bye_games.iterrows(), start=1):
                game_id = str(row["game_id"])
                game_prefix = _state_key(postgame_prefix, game_id)
                team_score_key = f"{game_prefix}_team_score"
                opp_score_key = f"{game_prefix}_opp_score"
                headline_key = f"{game_prefix}_headline"
                standout_one_key = f"{game_prefix}_standout_1"
                standout_two_key = f"{game_prefix}_standout_2"
                improvement_key = f"{game_prefix}_improvement"

                _prefill_text_input(
                    team_score_key,
                    "" if pd.isna(row.get("runs_for")) else str(int(row["runs_for"])),
                )
                _prefill_text_input(
                    opp_score_key,
                    "" if pd.isna(row.get("runs_against")) else str(int(row["runs_against"])),
                )
                _prefill_text_input(headline_key, "")
                _prefill_text_input(standout_one_key, "")
                _prefill_text_input(standout_two_key, "")
                _prefill_text_input(improvement_key, "")

                st.subheader(f"Game {index}")
                game_cols = st.columns([1.2, 1], gap="large")
                with game_cols[0]:
                    st.caption(
                        f"{row['date_display']} | {row['time_display']} | {row['home_away_display']} | "
                        f"{row['location_or_field']} | vs {row['opponent_display']}"
                    )
                    score_cols = st.columns(2, gap="small")
                    with score_cols[0]:
                        st.text_input("Maple Tree score", key=team_score_key)
                    with score_cols[1]:
                        st.text_input("Opponent score", key=opp_score_key)
                    st.text_input("Headline / turning point", key=headline_key)
                    st.text_input("Standout note 1", key=standout_one_key)
                    st.text_input("Standout note 2", key=standout_two_key)
                    st.text_input("Improvement note", key=improvement_key)
                with game_cols[1]:
                    st.markdown("<div class='writeups-upload-box'>", unsafe_allow_html=True)
                    upload = st.file_uploader(
                        f"Upload Game {index} box score screenshot",
                        type=["png", "jpg", "jpeg", "webp"],
                        key=f"{game_prefix}_upload",
                    )
                    if upload is not None:
                        st.image(upload, use_container_width=True, caption=f"Game {index} box score reference")
                    else:
                        st.caption("Upload a screenshot here for visual reference while you write the recap.")
                    st.markdown("</div>", unsafe_allow_html=True)

        generate_postgame = st.button(
            "Generate Postgame Recap",
            type="primary",
            disabled=(not generation_enabled or len(non_bye_games) != 2),
            key=f"{postgame_prefix}_generate",
        )

        if generate_postgame:
            manual_inputs: dict[str, dict[str, str]] = {}
            for _, row in non_bye_games.iterrows():
                game_id = str(row["game_id"])
                game_prefix = _state_key(postgame_prefix, game_id)
                manual_inputs[game_id] = {
                    "team_score": str(st.session_state.get(f"{game_prefix}_team_score", "")),
                    "opponent_score": str(st.session_state.get(f"{game_prefix}_opp_score", "")),
                    "headline": str(st.session_state.get(f"{game_prefix}_headline", "")),
                    "standout_1": str(st.session_state.get(f"{game_prefix}_standout_1", "")),
                    "standout_2": str(st.session_state.get(f"{game_prefix}_standout_2", "")),
                    "improvement": str(st.session_state.get(f"{game_prefix}_improvement", "")),
                }

            resolved_games, errors = resolve_postgame_games(non_bye_games, manual_inputs)
            if errors:
                for message in errors:
                    st.error(message)
            else:
                context_lines = fetch_writeup_record_context(connection)
                markdown = build_postgame_markdown(
                    season=selected_season,
                    week_bundle=week_bundle,
                    resolved_games=resolved_games,
                    weekly_summary_note=weekly_summary_note,
                    week_mvp=week_mvp,
                    context_lines=context_lines,
                )
                save_weekly_writeup(
                    connection,
                    season=selected_season,
                    week_label=selected_week,
                    phase="postgame",
                    title=f"{selected_week} Postgame Recap",
                    markdown=markdown,
                    source="dashboard",
                )
                st.session_state[f"{postgame_prefix}_markdown"] = markdown
                st.success("Saved this postgame recap to the dashboard archive.")

        saved_postgame_markdown = str(st.session_state.get(f"{postgame_prefix}_markdown", ""))
        if saved_postgame_markdown:
            st.subheader("Generated Postgame Recap")
            _render_markdown_output(
                markdown=saved_postgame_markdown,
                filename=suggest_markdown_filename(
                    season=selected_season,
                    week_label=selected_week,
                    phase="postgame",
                ),
                key_prefix=f"{postgame_prefix}_output",
            )
        elif existing_postgame:
            st.subheader("Saved Postgame Recap")
            st.caption(f"Last saved: {existing_postgame.get('updated_at', '')}")
            _render_markdown_output(
                markdown=str(existing_postgame.get("markdown") or ""),
                filename=suggest_markdown_filename(
                    season=selected_season,
                    week_label=selected_week,
                    phase="postgame",
                ),
                key_prefix=f"{postgame_prefix}_saved_output",
            )

    with tabs[2]:
        _render_saved_postgame_archive(
            connection,
            season=selected_season,
            key_prefix=_state_key("saved_postgames", selected_season),
        )
if __name__ == "__main__":
    main()
