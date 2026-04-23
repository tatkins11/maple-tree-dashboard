from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_all_time_leaders,
    fetch_career_stats,
    fetch_seasons,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import database_path_control


st.set_page_config(page_title="All-Time / Career Stats", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


ensure_authenticated()

st.title("All-Time / Career Stats")
db_path = database_path_control(DEFAULT_DB_PATH, key="career_stats_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
seasons = with_dashboard_default_season(fetch_seasons(connection))

selected_seasons = st.multiselect("Season filter", options=seasons, default=seasons)
min_pa = st.slider("Minimum PA", min_value=0, max_value=100, value=20, step=5)

career_df = fetch_career_stats(connection, seasons=selected_seasons, min_pa=min_pa)
leaders = fetch_all_time_leaders(connection, seasons=selected_seasons, min_pa=min_pa)

st.subheader("Career Totals")
st.dataframe(
    career_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "avg": st.column_config.NumberColumn("AVG", format="%.3f"),
        "obp": st.column_config.NumberColumn("OBP", format="%.3f"),
        "slg": st.column_config.NumberColumn("SLG", format="%.3f"),
        "ops": st.column_config.NumberColumn("OPS", format="%.3f"),
    },
)

if leaders:
    st.subheader("All-Time Leaders")
    leader_cols = st.columns(len(leaders))
    for column, (label, dataframe) in zip(leader_cols, leaders.items()):
        column.markdown(f"**{label}**")
        column.dataframe(dataframe, hide_index=True, use_container_width=True)
