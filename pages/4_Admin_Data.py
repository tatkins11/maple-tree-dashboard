from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import require_admin
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    clear_query_cache,
    fetch_active_roster,
    fetch_player_aliases,
    fetch_player_identities,
    fetch_player_metadata,
    fetch_projection_inventory,
    fetch_projection_seasons,
    fetch_projection_source_counts,
    fetch_schedule_seasons,
    fetch_seasons,
    get_connection,
)
from src.dashboard.ui import database_path_control
from src.dashboard.ui import render_page_header
from src.models.audit import (
    AuditError,
    fetch_recent_audit_log,
    undo_audit_entry,
)
from src.models.roster import DEFAULT_ACTIVE_ROSTER_SEASON
from src.models.schedule import (
    DEFAULT_SCHEDULE_PATH,
    DEFAULT_SCHEDULE_TEAM_NAME,
    create_schedule_game,
    record_game_result,
    update_schedule_game_fields,
)
from src.utils.player_identity import reassign_alias


st.set_page_config(page_title="Admin / Data", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _schedule_csv_path() -> Path:
    return Path(DEFAULT_SCHEDULE_PATH)


def _fetch_schedule_rows(
    connection,
    *,
    season: str | None = None,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
) -> pd.DataFrame:
    params: list[object] = [team_name]
    where = "WHERE team_name = ?"
    if season:
        where += " AND season = ?"
        params.append(season)
    return pd.read_sql_query(
        f"""
        SELECT
            game_id,
            season,
            week_label,
            game_date,
            game_time,
            opponent_name,
            home_away,
            location_or_field,
            status,
            completed_flag,
            is_bye,
            result,
            runs_for,
            runs_against,
            notes
        FROM schedule_games
        {where}
        ORDER BY game_date, COALESCE(game_time, ''), week_label, game_id
        """,
        connection,
        params=params,
    )


def _format_game_label(row: pd.Series) -> str:
    date_text = str(row.get("game_date") or "").strip() or "TBD"
    time_text = str(row.get("game_time") or "").strip()
    opponent = str(row.get("opponent_name") or "").strip() or ("BYE" if int(row.get("is_bye") or 0) else "TBD")
    score_text = ""
    runs_for = row.get("runs_for")
    runs_against = row.get("runs_against")
    if pd.notna(runs_for) and pd.notna(runs_against):
        score_text = f" — {int(runs_for)}-{int(runs_against)}"
    status_flag = " (completed)" if int(row.get("completed_flag") or 0) else ""
    suffix = f" @ {time_text}" if time_text else ""
    return f"{date_text}{suffix} vs {opponent}{score_text}{status_flag}"


def _render_schedule_manager(connection) -> None:
    csv_path = _schedule_csv_path()
    schedule_seasons = fetch_schedule_seasons(connection)
    if not schedule_seasons:
        st.info("No schedule rows are loaded yet. Add a new game below to seed the schedule.")
        seasons_for_select: list[str] = []
    else:
        seasons_for_select = schedule_seasons

    record_tab, edit_tab, create_tab = st.tabs(["Record result", "Edit game", "Add new game"])

    with record_tab:
        if not seasons_for_select:
            st.caption("Add at least one scheduled game first.")
        else:
            season = st.selectbox(
                "Season",
                options=seasons_for_select,
                key="admin_record_season",
            )
            games = _fetch_schedule_rows(connection, season=season)
            incomplete = games.loc[
                (games["completed_flag"].fillna(0).astype(int) == 0)
                & (games["is_bye"].fillna(0).astype(int) == 0)
            ].copy()
            if incomplete.empty:
                st.success("All non-bye games in this season already have results recorded.")
            else:
                incomplete["label"] = incomplete.apply(_format_game_label, axis=1)
                label_to_id = dict(zip(incomplete["label"], incomplete["game_id"]))
                selected_label = st.selectbox(
                    "Game to record",
                    options=list(label_to_id.keys()),
                    key="admin_record_game_label",
                )
                game_id = label_to_id[selected_label]
                with st.form("admin_record_result_form", clear_on_submit=False):
                    runs_for = st.number_input("Maple Tree runs", min_value=0, max_value=99, step=1, value=0)
                    runs_against = st.number_input("Opponent runs", min_value=0, max_value=99, step=1, value=0)
                    notes = st.text_input("Optional notes", value="")
                    submitted = st.form_submit_button("Record result", type="primary")
                if submitted:
                    try:
                        record_game_result(
                            connection,
                            game_id=str(game_id),
                            runs_for=int(runs_for),
                            runs_against=int(runs_against),
                            notes=notes.strip() or None,
                            csv_path=csv_path,
                        )
                    except Exception as exc:
                        st.error(f"Failed to record result: {exc}")
                    else:
                        st.success(f"Recorded {int(runs_for)}-{int(runs_against)} for {selected_label}.")
                        st.rerun()

    with edit_tab:
        if not seasons_for_select:
            st.caption("Add at least one scheduled game first.")
        else:
            season = st.selectbox(
                "Season",
                options=seasons_for_select,
                key="admin_edit_season",
            )
            games = _fetch_schedule_rows(connection, season=season)
            if games.empty:
                st.info("No games found for this season.")
            else:
                games["label"] = games.apply(_format_game_label, axis=1)
                label_to_id = dict(zip(games["label"], games["game_id"]))
                selected_label = st.selectbox(
                    "Game to edit",
                    options=list(label_to_id.keys()),
                    key="admin_edit_game_label",
                )
                game_id = label_to_id[selected_label]
                row = games.loc[games["game_id"] == game_id].iloc[0]
                with st.form("admin_edit_game_form", clear_on_submit=False):
                    game_date_value = _coerce_date(row.get("game_date"))
                    new_date = st.date_input("Date", value=game_date_value)
                    new_time = st.text_input("Time (e.g. 7:00 PM)", value=str(row.get("game_time") or ""))
                    new_opponent = st.text_input("Opponent", value=str(row.get("opponent_name") or ""))
                    new_home_away = st.selectbox(
                        "Home / Away",
                        options=["", "Home", "Away"],
                        index=(["", "Home", "Away"].index(str(row.get("home_away") or "").title()) if str(row.get("home_away") or "").title() in ["Home", "Away"] else 0),
                    )
                    new_location = st.text_input("Field / Location", value=str(row.get("location_or_field") or ""))
                    new_week = st.text_input("Week label", value=str(row.get("week_label") or ""))
                    new_is_bye = st.toggle("Bye week", value=bool(int(row.get("is_bye") or 0)))
                    new_notes = st.text_input("Notes", value=str(row.get("notes") or ""))
                    submitted = st.form_submit_button("Save changes", type="primary")
                if submitted:
                    updates = {
                        "game_date": new_date.isoformat() if new_date else "",
                        "game_time": new_time,
                        "opponent_name": "" if new_is_bye else new_opponent,
                        "home_away": new_home_away,
                        "location_or_field": new_location,
                        "week_label": new_week,
                        "is_bye": new_is_bye,
                        "notes": new_notes,
                    }
                    try:
                        update_schedule_game_fields(
                            connection,
                            game_id=str(game_id),
                            updates=updates,
                            csv_path=csv_path,
                        )
                    except Exception as exc:
                        st.error(f"Failed to save: {exc}")
                    else:
                        st.success(f"Updated game {game_id}.")
                        st.rerun()

    with create_tab:
        season_options = seasons_for_select or fetch_seasons(connection)
        with st.form("admin_create_game_form", clear_on_submit=True):
            season = st.selectbox(
                "Season",
                options=season_options or [""],
                key="admin_create_season",
            )
            new_date = st.date_input("Date", value=date.today())
            new_time = st.text_input("Time (e.g. 7:00 PM)", value="")
            new_opponent = st.text_input("Opponent", value="")
            new_home_away = st.selectbox("Home / Away", options=["", "Home", "Away"])
            new_location = st.text_input("Field / Location", value="")
            new_week = st.text_input("Week label", value="")
            new_is_bye = st.toggle("Bye week", value=False)
            new_notes = st.text_input("Notes", value="")
            submitted = st.form_submit_button("Add game", type="primary")
        if submitted:
            if not season.strip():
                st.error("Season is required.")
            else:
                try:
                    new_game_id = create_schedule_game(
                        connection,
                        season=season.strip(),
                        game_date=new_date.isoformat(),
                        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
                        opponent_name=new_opponent or None,
                        game_time=new_time or None,
                        home_away=new_home_away or None,
                        location_or_field=new_location or None,
                        week_label=new_week or None,
                        is_bye=new_is_bye,
                        notes=new_notes or None,
                        csv_path=csv_path,
                    )
                except Exception as exc:
                    st.error(f"Failed to add: {exc}")
                else:
                    st.success(f"Added game {new_game_id}.")
                    st.rerun()


def _coerce_date(value) -> date:
    if value in (None, ""):
        return date.today()
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return date.today()


def _render_aliases_tab(connection) -> None:
    aliases = fetch_player_aliases(connection)
    if aliases.empty:
        st.info("No aliases recorded yet.")
        return

    st.dataframe(aliases, width="stretch", hide_index=True)

    identities = fetch_player_identities(connection)
    if identities.empty:
        st.caption("Add a player identity before reassigning aliases.")
        return

    st.markdown("**Reassign an alias**")
    alias_options = aliases.to_dict(orient="records")
    alias_labels = {
        f"{row.get('source_name', '')} — currently {row.get('canonical_name', '')} (alias {row.get('alias_id')})": row
        for row in alias_options
        if row.get("alias_id") is not None
    }
    if not alias_labels:
        st.caption("No reassignable aliases — every row needs an alias_id.")
        return

    identity_labels = {
        f"{row['player_name']} ({row['canonical_name']})": int(row["player_id"])
        for _, row in identities.iterrows()
    }

    with st.form("admin_alias_reassign_form", clear_on_submit=False):
        chosen_alias_label = st.selectbox("Alias", options=list(alias_labels.keys()))
        chosen_target_label = st.selectbox("Reassign to player", options=list(identity_labels.keys()))
        approve = st.toggle("Mark as approved", value=True)
        submitted = st.form_submit_button("Reassign alias", type="primary")
    if submitted:
        alias_row = alias_labels[chosen_alias_label]
        target_player_id = identity_labels[chosen_target_label]
        try:
            outcome = reassign_alias(
                connection,
                alias_id=int(alias_row["alias_id"]),
                new_player_id=target_player_id,
                approve=approve,
            )
        except Exception as exc:
            st.error(f"Failed to reassign: {exc}")
        else:
            if outcome["changed"]:
                st.success("Alias reassigned.")
                st.rerun()
            else:
                st.info("No change applied — alias already mapped to that player.")


def _render_recent_changes(connection) -> None:
    csv_path = _schedule_csv_path()
    log = fetch_recent_audit_log(connection, limit=50, include_undone=True)
    if log.empty:
        st.info("No admin changes recorded yet. Schedule edits and alias reassignments will show up here.")
        return

    pending = log.loc[log["undone_flag"] == 0]
    st.caption(f"Showing the {len(log)} most recent changes ({len(pending)} still active).")

    for _, entry in log.iterrows():
        with st.container(border=True):
            header_cols = st.columns([4, 1])
            with header_cols[0]:
                st.markdown(f"**{entry['summary']}**")
                meta = f"{entry['created_at']} · {entry['action_type']} · {entry['entity_type']}:{entry['entity_id']}"
                if int(entry["undone_flag"]) == 1:
                    meta += f" · undone {entry['undone_at']}"
                st.caption(meta)
            with header_cols[1]:
                if int(entry["undone_flag"]) == 1:
                    st.caption("Undone")
                else:
                    button_key = f"undo_audit_{int(entry['audit_id'])}"
                    if st.button("Undo", key=button_key):
                        try:
                            undo_audit_entry(
                                connection,
                                int(entry["audit_id"]),
                                schedule_csv_path=csv_path,
                            )
                        except AuditError as exc:
                            st.warning(str(exc))
                        except Exception as exc:
                            st.error(f"Undo failed: {exc}")
                        else:
                            st.success("Change reverted.")
                            st.rerun()


require_admin()
# Admin edits data; drop cached query results on every run of this page so any
# write (game result, alias change, roster edit) is immediately visible.
clear_query_cache()

render_page_header("Admin / Data", kicker="Manager")
db_path = database_path_control(DEFAULT_DB_PATH, key="admin_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())

projection_seasons = fetch_projection_seasons(connection)
selected_projection_season = st.selectbox(
    "Projection season",
    options=["All"] + projection_seasons,
    index=0,
)
projection_filter = None if selected_projection_season == "All" else selected_projection_season

tabs = st.tabs(
    [
        "Identities",
        "Aliases",
        "Player Metadata",
        "Active Roster",
        "Projection Sources",
        "Schedule Manager",
        "Recent Changes",
    ]
)
identities_tab, aliases_tab, metadata_tab, roster_tab, projections_tab, schedule_tab, audit_tab = tabs

with identities_tab:
    st.dataframe(fetch_player_identities(connection), width="stretch", hide_index=True)

with aliases_tab:
    _render_aliases_tab(connection)

with metadata_tab:
    st.dataframe(fetch_player_metadata(connection), width="stretch", hide_index=True)

with roster_tab:
    st.dataframe(
        fetch_active_roster(connection, season_name=DEFAULT_ACTIVE_ROSTER_SEASON),
        width="stretch",
        hide_index=True,
    )

with projections_tab:
    source_counts = fetch_projection_source_counts(connection, projection_filter)
    projection_inventory = fetch_projection_inventory(connection, projection_filter)
    counts_col, inventory_col = st.columns([1, 3])
    counts_col.markdown("**Source counts**")
    counts_col.dataframe(source_counts, width="stretch", hide_index=True)
    inventory_col.markdown("**Projection inventory**")
    inventory_col.dataframe(
        projection_inventory,
        width="stretch",
        hide_index=True,
        column_config={
            "current_season_weight": st.column_config.NumberColumn("Current Wt", format="%.3f"),
            "weighted_prior_pa": st.column_config.NumberColumn("Weighted Prior PA", format="%.1f"),
            "projected_on_base_rate": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
            "projected_total_base_rate": st.column_config.NumberColumn("Proj TB Rate", format="%.3f"),
            "projected_extra_base_hit_rate": st.column_config.NumberColumn("Proj XBH Rate", format="%.3f"),
        },
    )

with schedule_tab:
    _render_schedule_manager(connection)

with audit_tab:
    _render_recent_changes(connection)
