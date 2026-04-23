from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.auth import require_admin
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_active_roster,
    fetch_player_aliases,
    fetch_player_identities,
    fetch_player_metadata,
    fetch_projection_inventory,
    fetch_projection_seasons,
    fetch_projection_source_counts,
    get_connection,
)
from src.dashboard.ui import database_path_control
from src.models.roster import DEFAULT_ACTIVE_ROSTER_SEASON


st.set_page_config(page_title="Admin / Data", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str):
    return get_connection(Path(db_path))


require_admin()

st.title("Admin / Data")
db_path = database_path_control(DEFAULT_DB_PATH, key="admin_db_path")
connection = get_db_connection(db_path)
projection_seasons = fetch_projection_seasons(connection)
selected_projection_season = st.selectbox(
    "Projection season",
    options=["All"] + projection_seasons,
    index=0,
)
projection_filter = None if selected_projection_season == "All" else selected_projection_season

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Identities", "Aliases", "Player Metadata", "Active Roster", "Projection Sources"]
)

with tab1:
    st.dataframe(fetch_player_identities(connection), use_container_width=True, hide_index=True)

with tab2:
    st.dataframe(fetch_player_aliases(connection), use_container_width=True, hide_index=True)

with tab3:
    st.dataframe(fetch_player_metadata(connection), use_container_width=True, hide_index=True)

with tab4:
    st.dataframe(
        fetch_active_roster(connection, season_name=DEFAULT_ACTIVE_ROSTER_SEASON),
        use_container_width=True,
        hide_index=True,
    )

with tab5:
    source_counts = fetch_projection_source_counts(connection, projection_filter)
    projection_inventory = fetch_projection_inventory(connection, projection_filter)
    counts_col, inventory_col = st.columns([1, 3])
    counts_col.markdown("**Source counts**")
    counts_col.dataframe(source_counts, use_container_width=True, hide_index=True)
    inventory_col.markdown("**Projection inventory**")
    inventory_col.dataframe(
        projection_inventory,
        use_container_width=True,
        hide_index=True,
        column_config={
            "current_season_weight": st.column_config.NumberColumn("Current Wt", format="%.3f"),
            "weighted_prior_pa": st.column_config.NumberColumn("Weighted Prior PA", format="%.1f"),
            "projected_on_base_rate": st.column_config.NumberColumn("Proj OBP", format="%.3f"),
            "projected_total_base_rate": st.column_config.NumberColumn("Proj TB Rate", format="%.3f"),
            "projected_extra_base_hit_rate": st.column_config.NumberColumn("Proj XBH Rate", format="%.3f"),
        },
    )
