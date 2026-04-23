from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    dashboard_default_season_index,
    fetch_current_season_stats,
    fetch_projection_seasons,
    fetch_seasons,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import database_path_control


st.set_page_config(page_title="Current Season Stats", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str):
    return get_connection(Path(db_path))


ensure_authenticated()

st.title("Current Season Stats")
db_path = database_path_control(DEFAULT_DB_PATH, key="current_stats_db_path")
connection = get_db_connection(db_path)
seasons = with_dashboard_default_season(fetch_seasons(connection))
projection_seasons = fetch_projection_seasons(connection)

if not seasons:
    st.info("No current season data found.")
else:
    selected_season = st.selectbox("Season", options=seasons, index=dashboard_default_season_index(seasons))
    include_projections = st.checkbox("Show projection columns", value=False)
    selected_projection_season = None
    if include_projections and projection_seasons:
        selected_projection_season = st.selectbox(
            "Projection season",
            options=projection_seasons,
            index=0,
        )
    dataframe = fetch_current_season_stats(
        connection,
        selected_season,
        include_projections=include_projections,
        projection_season=selected_projection_season,
    )
    st.dataframe(
        dataframe,
        use_container_width=True,
        hide_index=True,
        column_config={
            "avg": st.column_config.NumberColumn("AVG", format="%.3f"),
            "obp": st.column_config.NumberColumn("OBP", format="%.3f"),
            "slg": st.column_config.NumberColumn("SLG", format="%.3f"),
            "ops": st.column_config.NumberColumn("OPS", format="%.3f"),
            "proj_obp": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
            "proj_tb_rate": st.column_config.NumberColumn("Proj TB Rate", format="%.3f"),
            "proj_xbh_rate": st.column_config.NumberColumn("Proj XBH Rate", format="%.3f"),
            "current_season_weight": st.column_config.NumberColumn("Current Wt", format="%.3f"),
            "weighted_prior_plate_appearances": st.column_config.NumberColumn("Weighted Prior PA", format="%.1f"),
        },
    )
