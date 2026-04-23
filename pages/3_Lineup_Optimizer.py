from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import require_admin
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_active_roster,
    fetch_available_projection_rows,
    fetch_projection_seasons,
    get_connection,
    run_optimizer,
)
from src.dashboard.ui import database_path_control
from src.models.optimizer import DEFAULT_PREFERRED_LINEUP
from src.models.roster import DEFAULT_ACTIVE_ROSTER_SEASON


st.set_page_config(page_title="Lineup Optimizer", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str):
    return get_connection(Path(db_path))


def _lineup_table(ordered_names: list[str], projection_rows: pd.DataFrame) -> pd.DataFrame:
    by_name = projection_rows.set_index("player").to_dict("index")
    rows: list[dict[str, object]] = []
    for index, name in enumerate(ordered_names, start=1):
        record = by_name.get(name, {})
        rows.append(
            {
                "spot": index,
                "player": name,
                "role": "DHH" if bool(record.get("fixed_dhh")) else "BAT",
                "projection_source": record.get("projection_source", ""),
                "proj_obp": record.get("proj_obp", 0.0),
                "proj_tb_rate": record.get("proj_tb_rate", 0.0),
            }
        )
    return pd.DataFrame(rows)


def _preferred_baseline(selected_players: list[str]) -> list[str]:
    preferred = [name for name in DEFAULT_PREFERRED_LINEUP if name in selected_players]
    extras = [name for name in selected_players if name not in preferred]
    return preferred + extras


def _mode_label(value: str) -> str:
    if value == "team_aware":
        return "team_aware (preferred baseline)"
    return value


require_admin()

st.title("Lineup Optimizer")
db_path = database_path_control(DEFAULT_DB_PATH, key="optimizer_db_path")
connection = get_db_connection(db_path)
projection_seasons = fetch_projection_seasons(connection)
active_roster_df = fetch_active_roster(connection, season_name=DEFAULT_ACTIVE_ROSTER_SEASON)
active_names = active_roster_df["preferred_display_name"].tolist() if not active_roster_df.empty else []

game_date = st.text_input("Game date", value="2026-04-20")
if not projection_seasons:
    st.info("No projection seasons found yet. Build hitter projections first.")
    st.stop()
projection_season = st.selectbox(
    "Projection model anchor season",
    options=projection_seasons,
    index=0,
    help="These projections are season-blended; this selects which season is treated as the current-season anchor for the blend.",
)
mode = st.radio(
    "Optimizer mode",
    options=["unconstrained", "team_aware"],
    horizontal=True,
    format_func=_mode_label,
    index=1,
)
selected_players = st.multiselect(
    "Available players",
    options=active_names,
    default=active_names,
    help="Every selected player will bat; lineup length always equals the number of available players.",
)
simulations = st.slider("Simulation count", min_value=200, max_value=5000, value=1000, step=200)
seed = st.number_input("Seed", min_value=0, max_value=999999, value=42, step=1)

projection_rows = (
    fetch_available_projection_rows(connection, projection_season, selected_players)
    if projection_season and selected_players
    else pd.DataFrame()
)
preferred_baseline = _preferred_baseline(selected_players)
dhh_rows = projection_rows[projection_rows["fixed_dhh"] == True] if not projection_rows.empty else pd.DataFrame()
if not dhh_rows.empty:
    st.info(f"Fixed DHH: {dhh_rows.iloc[0]['player']}")

if mode == "team_aware":
    st.markdown("### Preferred Baseline")
    st.caption(
        "This mode starts from your preferred lineup, trims missing bottom-half bats in place, "
        "and only allows limited top-half reshuffling when a core top-five hitter is out."
    )
    st.dataframe(
        _lineup_table(preferred_baseline, projection_rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "proj_obp": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
            "proj_tb_rate": st.column_config.NumberColumn("Proj TB Rate", format="%.3f"),
        },
    )

st.subheader("Available Projection Pool")
st.dataframe(
    projection_rows,
    use_container_width=True,
    hide_index=True,
    column_config={
        "proj_obp": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
        "proj_tb_rate": st.column_config.NumberColumn("Proj TB Rate", format="%.3f"),
        "proj_xbh_rate": st.column_config.NumberColumn("Proj XBH Rate", format="%.3f"),
    },
)

if st.button("Run optimizer", type="primary", disabled=not selected_players):
    result = run_optimizer(
        connection=connection,
        projection_season=projection_season,
        game_date=game_date,
        available_player_names=selected_players,
        mode=mode,
        simulations=simulations,
        seed=int(seed),
    )
    st.success("Optimizer complete.")

    metric_cols = st.columns(4)
    metric_cols[0].metric("Expected runs", f"{result.best_lineup.summary.average_runs:.3f}")
    metric_cols[1].metric("Median runs", f"{result.best_lineup.summary.median_runs:.3f}")
    metric_cols[2].metric("Evaluated lineups", result.evaluated_lineups)
    metric_cols[3].metric("Near ties", len(result.near_tie_lineups))

    st.markdown("### Best Lineup")
    st.caption(result.best_lineup.lineup_type)
    st.dataframe(
        _lineup_table(result.best_lineup.ordered_player_names, projection_rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "proj_obp": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
            "proj_tb_rate": st.column_config.NumberColumn("Proj TB Rate", format="%.3f"),
        },
    )
    st.caption(result.best_lineup.reason)

    st.markdown("### Alternate Lineups")
    alternate_rows: list[dict[str, object]] = []
    for lineup in result.alternate_lineups:
        alternate_rows.append(
            {
                "lineup": " / ".join(lineup.ordered_player_names),
                "expected_runs": lineup.summary.average_runs,
                "median_runs": lineup.summary.median_runs,
                "dhh_slot": lineup.dhh_slot,
            }
        )
    st.dataframe(
        pd.DataFrame(alternate_rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "expected_runs": st.column_config.NumberColumn("Expected Runs", format="%.3f"),
            "median_runs": st.column_config.NumberColumn("Median Runs", format="%.3f"),
        },
    )
