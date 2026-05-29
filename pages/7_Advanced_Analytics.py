from __future__ import annotations

from math import fabs
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    DEFAULT_DASHBOARD_SEASON,
    dashboard_default_season_index,
    fetch_advanced_analytics_archetype_summary,
    fetch_advanced_analytics_leaderboards,
    fetch_advanced_analytics_view,
    fetch_advanced_archetype_order,
    fetch_active_roster,
    fetch_consistency_scores,
    fetch_advanced_methodology_summary,
    fetch_advanced_player_comparison,
    fetch_current_season_stats,
    fetch_seasons,
    fetch_team_data_freshness,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import (
    database_path_control,
    persistent_multiselect,
    persistent_segmented_control,
    persistent_selectbox,
    persistent_slider,
    persistent_toggle,
    render_data_freshness_caption,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="Advanced Analytics", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_analytics_css() -> None:
    st.markdown(
        """
        <style>
        .analytics-note {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.85rem;
            padding: 0.75rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.6rem;
        }
        .analytics-method {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.8rem 0.95rem;
            background: #ffffff;
            margin: 0.2rem 0 0.9rem 0;
        }
        .analytics-method-title {
            font-size: 0.93rem;
            font-weight: 600;
            margin-bottom: 0.35rem;
        }
        .analytics-method-row {
            font-size: 0.88rem;
            color: #374151;
            margin: 0.08rem 0;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.89rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _format_metric_name(metric: str) -> str:
    labels = {
        "pa": "PA",
        "obp": "OBP",
        "slg": "SLG",
        "ops": "OPS",
        "iso": "ISO",
        "xbh_rate": "XBH Rate",
        "hr_rate": "HR Rate",
        "tb_per_pa": "TB / PA",
        "non_out_rate": "Non-Out Rate",
        "walk_rate": "BB Rate",
        "rbi_per_pa": "RBI / PA",
        "runs_per_on_base_event": "R / On-Base Event",
        "team_relative_obp": "Team OBP+",
        "team_relative_slg": "Team SLG+",
        "team_relative_ops": "Team OPS+",
        "raa": "RAA",
        "rar": "RAR",
        "owar": "oWAR",
        "archetype": "Archetype",
    }
    return labels.get(metric, metric.replace("_", " ").title())


def _rename_comparison_index(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    renamed = dataframe.copy()
    renamed.index = [_format_metric_name(str(value)) for value in renamed.index]
    return renamed


def _format_player_comparison(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    formatted = dataframe.copy()
    integer_rows = {"PA", "2OUTRBI", "LOB", "Team OBP+", "Team SLG+", "Team OPS+"}
    one_decimal_rows = {"RAA", "RAR"}
    two_decimal_rows = {"oWAR"}
    three_decimal_rows = {
        "AVG",
        "OBP",
        "SLG",
        "OPS",
        "ISO",
        "XBH Rate",
        "HR Rate",
        "TB / PA",
        "Non-Out Rate",
        "BB Rate",
        "RBI / PA",
        "R / On-Base Event",
        "BA / RISP",
        "2OUTRBI Rate",
        "LOB / PA",
        "ROE Rate",
        "FC Rate",
        "HBP Rate",
    }

    for index in formatted.index:
        row_label = str(index)
        numeric_row = pd.to_numeric(formatted.loc[index], errors="coerce")
        if numeric_row.isna().all():
            continue
        if row_label in integer_rows:
            formatted.loc[index] = numeric_row.map(lambda value: "" if pd.isna(value) else f"{int(round(value))}")
        elif row_label in one_decimal_rows:
            formatted.loc[index] = numeric_row.map(lambda value: "" if pd.isna(value) else f"{value:.1f}")
        elif row_label in two_decimal_rows:
            formatted.loc[index] = numeric_row.map(lambda value: "" if pd.isna(value) else f"{value:.2f}")
        elif row_label in three_decimal_rows:
            formatted.loc[index] = numeric_row.map(lambda value: "" if pd.isna(value) else f"{value:.3f}")
        else:
            formatted.loc[index] = numeric_row.map(lambda value: "" if pd.isna(value) else f"{value:.2f}")
    return formatted


def _render_methodology_box(summary: dict[str, str]) -> None:
    rows = "".join(
        f'<div class="analytics-method-row"><strong>{label}:</strong> {value}</div>'
        for label, value in summary.items()
    )
    st.markdown(
        f"""
        <div class="analytics-method">
          <div class="analytics-method-title">Methodology Snapshot</div>
          {rows}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _default_min_pa_for_season(connection, selected_season: str | None) -> tuple[int, int]:
    if not selected_season:
        return 5, 0
    season_stats = fetch_current_season_stats(connection, selected_season)
    if season_stats.empty or "pa" not in season_stats.columns:
        return 5, 0
    max_pa = int(season_stats["pa"].max())
    if max_pa < 5:
        return 0, max_pa
    return 5, max_pa


def _build_scatter_label_positions(
    dataframe: pd.DataFrame,
    obp_padding: float = 0.0045,
    slg_step: float = 0.022,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    labeled = dataframe.sort_values(["obp", "slg", "player"], ascending=[True, False, True]).copy()
    slg_min = float(labeled["slg"].min())
    slg_max = float(labeled["slg"].max())

    clusters: list[list[tuple[int, object]]] = []
    current_cluster: list[tuple[int, object]] = []
    last_x: float | None = None

    for row in labeled.itertuples():
        row_x = float(row.obp)
        row_y = float(row.slg)
        if (
            not current_cluster
            or last_x is None
            or fabs(row_x - last_x) <= 0.02
            or any(fabs(row_y - float(existing.slg)) <= 0.08 for _, existing in current_cluster)
        ):
            current_cluster.append((len(current_cluster), row))
        else:
            clusters.append(current_cluster)
            current_cluster = [(0, row)]
        last_x = row_x

    if current_cluster:
        clusters.append(current_cluster)

    label_obp: list[float] = []
    label_slg: list[float] = []

    for cluster_number, cluster in enumerate(clusters):
        cluster_size = len(cluster)
        offsets = [index - ((cluster_size - 1) / 2.0) for index in range(cluster_size)]
        direction = 1 if cluster_number % 2 == 0 else -1
        for position, ((_, row), vertical_slot) in enumerate(zip(cluster, offsets)):
            side = direction if position % 2 == 0 else -direction
            x_offset = obp_padding + (0.0018 * (cluster_size - 1)) + (0.0012 * abs(vertical_slot))
            y_offset = vertical_slot * slg_step

            label_obp.append(float(row.obp) + (x_offset * side))
            label_slg.append(min(max(float(row.slg) + y_offset, slg_min - 0.02), slg_max + 0.02))

    labeled["label_obp"] = label_obp
    labeled["label_slg"] = label_slg
    labeled["label_distance"] = (
        ((labeled["label_obp"] - labeled["obp"]) ** 2 + (labeled["label_slg"] - labeled["slg"]) ** 2) ** 0.5
    )
    return labeled


_inject_analytics_css()
ensure_authenticated()

st.title("Advanced Analytics")
st.markdown(
    """
    <div class="analytics-note">
      <strong>Offense-only, team-specific analytics.</strong><br/>
      This page uses only trusted batting outcomes and derived batting-event rates from the verified stats database.
      <strong>wOBA</strong> and <strong>wRC+</strong> weight each event by its real run value (league-calibrated linear weights),
      and the value stats <strong>RAA</strong>, <strong>RAR</strong>, and <strong>oWAR</strong> build on that same run model — all
      internal, offense-only, and not full baseball WAR. The <strong>Game-to-Game Consistency</strong> section (Season view) shows
      how steady each hitter is start to start.
    </div>
    """,
    unsafe_allow_html=True,
)

db_path = database_path_control(DEFAULT_DB_PATH, key="advanced_analytics_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
seasons = with_dashboard_default_season(fetch_seasons(connection))

if not seasons:
    st.info("No season batting data found.")
    st.stop()

scope = persistent_segmented_control(
    "Analytics scope",
    options=["Season", "Career"],
    query_key="adv_scope",
    default="Season",
)

toolbar_top = st.columns([1.35, 1.0], gap="small")
with toolbar_top[0]:
    if scope == "Season":
        default_season = seasons[dashboard_default_season_index(seasons)] if seasons else None
        selected_season = persistent_selectbox(
            "Season",
            options=seasons,
            query_key="adv_season",
            default=default_season,
        )
        selected_seasons: list[str] = [selected_season]
    else:
        selected_season = None
        default_career_seasons = (
            [DEFAULT_DASHBOARD_SEASON] if DEFAULT_DASHBOARD_SEASON in seasons else seasons[:1]
        )
        selected_seasons = persistent_multiselect(
            "Season filter",
            options=seasons,
            query_key="adv_seasons",
            default=default_career_seasons,
        )

if scope == "Season" and selected_season:
    render_data_freshness_caption(fetch_team_data_freshness(connection, season=selected_season))
with toolbar_top[1]:
    default_min_pa, selected_season_max_pa = (
        _default_min_pa_for_season(connection, selected_season)
        if scope == "Season"
        else (5, 0)
    )
    min_pa = persistent_slider(
        "Minimum PA",
        query_key=f"adv_min_pa_{scope.lower()}",
        min_value=0,
        max_value=120,
        default=default_min_pa,
        step=5,
    )
    if scope == "Season" and selected_season_max_pa and selected_season_max_pa < 5:
        st.caption(
            f"Early-season sample: max PA is {selected_season_max_pa}, so the default is 0 until hitters clear 5 PA."
        )

toolbar_bottom = st.columns([0.8, 0.8, 1.4], gap="small")
with toolbar_bottom[0]:
    active_only = persistent_toggle(
        "Active roster only",
        query_key="adv_active_only",
        default=False,
    )
with toolbar_bottom[1]:
    show_context = persistent_toggle(
        "Show context columns",
        query_key="adv_show_context",
        default=False,
    )
with toolbar_bottom[2]:
    st.caption("Use the main filters to define the comparison group, then narrow to specific hitters below.")

analytics_df, metadata = fetch_advanced_analytics_view(
    connection,
    view_mode=scope,
    selected_season=selected_season,
    selected_seasons=selected_seasons,
    min_pa=min_pa,
    active_only=active_only,
)

if analytics_df.empty:
    if scope == "Season" and selected_season_max_pa and min_pa > selected_season_max_pa:
        st.info(
            f"No hitters have at least {min_pa} PA yet. {selected_season}'s current max is "
            f"{selected_season_max_pa} PA, so lower Minimum PA to see early-season analytics."
        )
    else:
        st.info("No hitters match the selected analytics filters.")
    st.stop()

_render_methodology_box(fetch_advanced_methodology_summary(metadata))

player_options = analytics_df["player"].tolist()
selected_players = st.multiselect("Player filter", options=player_options, default=[])
display_df = analytics_df if not selected_players else analytics_df[analytics_df["player"].isin(selected_players)].copy()
player_keys = analytics_df[["player", "canonical_name"]].drop_duplicates()

st.subheader("Advanced Analytics Table")
default_columns = [
    "player",
    "pa",
    "obp",
    "slg",
    "ops",
    "iso",
    "hr_rate",
    "tb_per_pa",
    "non_out_rate",
    "rbi_per_pa",
    "woba",
    "wrc_plus",
    "archetype",
    "rar",
    "owar",
]
context_columns = [
    "team_relative_ops",
    "team_relative_obp",
    "team_relative_slg",
    "xbh_rate",
    "walk_rate",
    "runs_per_on_base_event",
    "ba_risp",
    "two_out_rbi",
    "two_out_rbi_rate",
    "lob",
    "lob_per_pa",
    "roe_rate",
    "fc_rate",
    "hbp_rate",
]
table_columns = default_columns + (context_columns if show_context else [])
table_display = with_player_link_column(
    display_df[[column for column in ["canonical_name", *table_columns] if column in display_df.columns]],
    output_column="player",
)
render_static_table(
    table_display[[column for column in table_columns if column in table_display.columns]],
    column_labels={
        "player": "Player",
        "pa": "PA",
        "obp": "OBP",
        "slg": "SLG",
        "ops": "OPS",
        "iso": "ISO",
        "hr_rate": "HR Rate",
        "tb_per_pa": "TB / PA",
        "non_out_rate": "Non-Out Rate",
        "rbi_per_pa": "RBI / PA",
        "woba": "wOBA",
        "wrc_plus": "wRC+",
        "team_relative_ops": "Team OPS+",
        "archetype": "Archetype",
        "rar": "RAR",
        "owar": "oWAR",
        "xbh_rate": "XBH Rate",
        "walk_rate": "BB Rate",
        "runs_per_on_base_event": "R / OBE",
        "team_relative_obp": "Team OBP+",
        "team_relative_slg": "Team SLG+",
        "ba_risp": "BA / RISP",
        "two_out_rbi": "2OUTRBI",
        "two_out_rbi_rate": "2OUTRBI Rate",
        "lob": "LOB",
        "lob_per_pa": "LOB / PA",
        "roe_rate": "ROE Rate",
        "fc_rate": "FC Rate",
        "hbp_rate": "HBP Rate",
    },
    formatters={
        "obp": "{:.3f}",
        "slg": "{:.3f}",
        "ops": "{:.3f}",
        "iso": "{:.3f}",
        "hr_rate": "{:.3f}",
        "tb_per_pa": "{:.3f}",
        "non_out_rate": "{:.3f}",
        "rbi_per_pa": "{:.3f}",
        "woba": "{:.3f}",
        "wrc_plus": "{:.0f}",
        "team_relative_ops": "{:.0f}",
        "xbh_rate": "{:.3f}",
        "walk_rate": "{:.3f}",
        "runs_per_on_base_event": "{:.3f}",
        "team_relative_obp": "{:.0f}",
        "team_relative_slg": "{:.0f}",
        "rar": "{:.2f}",
        "owar": "{:.2f}",
        "ba_risp": "{:.3f}",
        "two_out_rbi_rate": "{:.3f}",
        "lob_per_pa": "{:.3f}",
        "roe_rate": "{:.3f}",
        "fc_rate": "{:.3f}",
        "hbp_rate": "{:.3f}",
    },
    link_columns=["player"],
    css_class="advanced-analytics-table",
)

st.subheader("Category Leaders")
leaderboards = fetch_advanced_analytics_leaderboards(analytics_df, limit=5)
leaderboard_columns = st.columns(3, gap="small")
for index, (label, board) in enumerate(leaderboards.items()):
    with leaderboard_columns[index % 3]:
        st.markdown(f"**{label}**")
        linked_board = with_player_link_column(
            board.merge(player_keys, on="player", how="left"),
            output_column="player",
        )
        render_static_table(
            linked_board[[column for column in linked_board.columns if column != "canonical_name"]],
            column_labels={
                "player": "Player",
                "obp": "OBP",
                "team_relative_obp": "Team OBP+",
                "iso": "ISO",
                "hr_rate": "HR Rate",
                "tb_per_pa": "TB / PA",
                "rbi_per_pa": "RBI / PA",
                "runs_per_on_base_event": "R / OBE",
                "team_relative_ops": "Team OPS+",
                "wrc_plus": "wRC+",
                "woba": "wOBA",
                "ops": "OPS",
                "rar": "RAR",
                "owar": "oWAR",
                "pa": "PA",
            },
            formatters={
                "obp": "{:.3f}",
                "team_relative_obp": "{:.0f}",
                "iso": "{:.3f}",
                "hr_rate": "{:.3f}",
                "tb_per_pa": "{:.3f}",
                "rbi_per_pa": "{:.3f}",
                "runs_per_on_base_event": "{:.3f}",
                "team_relative_ops": "{:.0f}",
                "wrc_plus": "{:.0f}",
                "woba": "{:.3f}",
                "ops": "{:.3f}",
                "rar": "{:.2f}",
                "owar": "{:.2f}",
            },
            link_columns=["player"],
            css_class="advanced-leaderboard-table",
        )

st.subheader("OBP vs SLG")
scatter_source = display_df.copy()
scatter_source["rar_display"] = scatter_source["rar"].round(2)

default_chart_labels = scatter_source.sort_values(["rar", "player"], ascending=[False, True]).head(5)["player"].tolist()
chart_label_options = scatter_source["player"].tolist()
active_roster_names = set(fetch_active_roster(connection)["preferred_display_name"].tolist())
active_chart_labels = [player for player in chart_label_options if player in active_roster_names]
archetype_label_options = sorted(scatter_source["archetype"].dropna().unique().tolist(), key=lambda value: str(value))
chart_label_state_key = "advanced_chart_label_players"
if chart_label_state_key not in st.session_state:
    st.session_state[chart_label_state_key] = default_chart_labels

chart_preset_cols = st.columns([0.9, 0.9, 0.7, 1.2, 1.1], gap="small")
with chart_preset_cols[0]:
    if st.button("Top RAR labels", use_container_width=True):
        st.session_state[chart_label_state_key] = default_chart_labels
with chart_preset_cols[1]:
    if st.button("Active roster labels", use_container_width=True):
        st.session_state[chart_label_state_key] = active_chart_labels
with chart_preset_cols[2]:
    if st.button("Clear labels", use_container_width=True):
        st.session_state[chart_label_state_key] = []
with chart_preset_cols[3]:
    selected_label_archetype = st.selectbox(
        "Label archetype",
        options=["Custom", *archetype_label_options],
        index=0,
        key="advanced_chart_label_archetype",
    )
with chart_preset_cols[4]:
    if st.button("Apply archetype labels", use_container_width=True):
        if selected_label_archetype == "Custom":
            st.session_state[chart_label_state_key] = default_chart_labels
        else:
            st.session_state[chart_label_state_key] = (
                scatter_source[scatter_source["archetype"] == selected_label_archetype]["player"].tolist()
            )

chart_label_players = st.multiselect(
    "Chart labels",
    options=chart_label_options,
    key=chart_label_state_key,
    help="Choose exactly which hitters should be labeled on the scatter plot, or use the quick presets above.",
)

label_source_base = scatter_source[scatter_source["player"].isin(chart_label_players)].copy()
label_source = _build_scatter_label_positions(label_source_base)
obp_min = max(0.0, float(scatter_source["obp"].min()) - 0.03)
obp_label_max = float(label_source["label_obp"].max()) if not label_source.empty else float(scatter_source["obp"].max())
obp_max = min(1.0, max(float(scatter_source["obp"].max()), obp_label_max) + 0.02)
slg_min = max(0.0, float(scatter_source["slg"].min()) - 0.08)
slg_label_max = float(label_source["label_slg"].max()) if not label_source.empty else float(scatter_source["slg"].max())
slg_max = min(3.0, max(float(scatter_source["slg"].max()), slg_label_max) + 0.05)
base_scatter = alt.Chart(scatter_source).encode(
    x=alt.X("obp:Q", title="OBP", scale=alt.Scale(domain=[obp_min, obp_max])),
    y=alt.Y("slg:Q", title="SLG", scale=alt.Scale(domain=[slg_min, slg_max])),
    tooltip=[
        alt.Tooltip("player:N", title="Player"),
        alt.Tooltip("archetype:N", title="Archetype"),
        alt.Tooltip("pa:Q", title="PA", format=".0f"),
        alt.Tooltip("obp:Q", title="OBP", format=".3f"),
        alt.Tooltip("slg:Q", title="SLG", format=".3f"),
        alt.Tooltip("team_relative_ops:Q", title="Team OPS+", format=".0f"),
        alt.Tooltip("rar_display:Q", title="RAR", format=".2f"),
    ],
)
scatter = (
    base_scatter
    .mark_circle(opacity=0.82)
    .encode(
        color=alt.Color("archetype:N", title="Archetype"),
        size=alt.Size("rar:Q", title="RAR", scale=alt.Scale(range=[60, 240])),
    )
)
label_rules = (
    alt.Chart(label_source)
    .mark_rule(color="#cbd5e1", opacity=0.38)
    .encode(
        x=alt.X("obp:Q", scale=alt.Scale(domain=[obp_min, obp_max])),
        y=alt.Y("slg:Q", scale=alt.Scale(domain=[slg_min, slg_max])),
        x2="label_obp:Q",
        y2="label_slg:Q",
    )
)
label_halo = (
    alt.Chart(label_source[label_source["label_obp"] >= label_source["obp"]])
    .mark_text(
        fontSize=10,
        color="#ffffff",
        align="left",
        dx=2,
        fontWeight="bold",
        stroke="#ffffff",
        strokeWidth=3,
    )
    .encode(
        x=alt.X("label_obp:Q", scale=alt.Scale(domain=[obp_min, obp_max])),
        y=alt.Y("label_slg:Q", scale=alt.Scale(domain=[slg_min, slg_max])),
        text="player:N",
    )
)
label_halo_left = (
    alt.Chart(label_source[label_source["label_obp"] < label_source["obp"]])
    .mark_text(
        fontSize=10,
        color="#ffffff",
        align="right",
        dx=-2,
        fontWeight="bold",
        stroke="#ffffff",
        strokeWidth=3,
    )
    .encode(
        x=alt.X("label_obp:Q", scale=alt.Scale(domain=[obp_min, obp_max])),
        y=alt.Y("label_slg:Q", scale=alt.Scale(domain=[slg_min, slg_max])),
        text="player:N",
    )
)
labels = (
    alt.Chart(label_source[label_source["label_obp"] >= label_source["obp"]])
    .mark_text(fontSize=10, color="#1f2937", align="left", dx=2, fontWeight="bold")
    .encode(
        x=alt.X("label_obp:Q", scale=alt.Scale(domain=[obp_min, obp_max])),
        y=alt.Y("label_slg:Q", scale=alt.Scale(domain=[slg_min, slg_max])),
        text="player:N",
    )
)
labels_left = (
    alt.Chart(label_source[label_source["label_obp"] < label_source["obp"]])
    .mark_text(fontSize=10, color="#1f2937", align="right", dx=-2, fontWeight="bold")
    .encode(
        x=alt.X("label_obp:Q", scale=alt.Scale(domain=[obp_min, obp_max])),
        y=alt.Y("label_slg:Q", scale=alt.Scale(domain=[slg_min, slg_max])),
        text="player:N",
    )
)
st.altair_chart(
    (scatter + label_rules + label_halo + label_halo_left + labels + labels_left).properties(height=400),
    use_container_width=True,
)
st.caption("Choose exactly which hitters are labeled. Hover any point to inspect the full hitter details.")

st.subheader("Archetype View")
archetype_summary = fetch_advanced_analytics_archetype_summary(analytics_df)
archetype_order = fetch_advanced_archetype_order()
archetype_columns = st.columns([0.8, 1.4], gap="small")
with archetype_columns[0]:
    st.markdown("**Archetype Summary**")
    st.dataframe(
        archetype_summary,
        hide_index=True,
        use_container_width=True,
        column_config={
            "archetype": st.column_config.TextColumn("Archetype", width="medium"),
            "hitters": st.column_config.NumberColumn("Hitters", format="%d", width="small"),
            "avg_obp": st.column_config.NumberColumn("Avg OBP", format="%.3f", width="small"),
            "avg_slg": st.column_config.NumberColumn("Avg SLG", format="%.3f", width="small"),
            "avg_owar": st.column_config.NumberColumn("Avg oWAR", format="%.2f", width="small"),
        },
    )
with archetype_columns[1]:
    st.markdown("**Players by Archetype**")
    player_archetypes = analytics_df[["player", "canonical_name", "pa", "archetype", "wrc_plus", "rar", "owar"]].copy()
    player_archetypes["archetype_rank"] = player_archetypes["archetype"].apply(
        lambda value: archetype_order.index(value) if value in archetype_order else len(archetype_order)
    )
    player_archetypes = player_archetypes.sort_values(["archetype_rank", "rar", "player"], ascending=[True, False, True])
    player_archetypes = with_player_link_column(player_archetypes, output_column="player")
    render_static_table(
        player_archetypes.drop(columns=["archetype_rank", "canonical_name"]),
        column_labels={
            "player": "Player",
            "pa": "PA",
            "archetype": "Archetype",
            "wrc_plus": "wRC+",
            "rar": "RAR",
            "owar": "oWAR",
        },
        formatters={
            "wrc_plus": "{:.0f}",
            "rar": "{:.2f}",
            "owar": "{:.2f}",
        },
        link_columns=["player"],
        css_class="advanced-archetype-table",
    )

st.subheader("Player Comparison")
comparison_choices = st.multiselect(
    "Compare 2-3 players",
    options=player_options,
    default=player_options[:2] if len(player_options) >= 2 else player_options,
)
if len(comparison_choices) < 2:
    st.info("Select at least two players to compare advanced metrics side by side.")
else:
    comparison_df = fetch_advanced_player_comparison(analytics_df, comparison_choices[:3])
    st.dataframe(
        _format_player_comparison(_rename_comparison_index(comparison_df)),
        use_container_width=True,
    )

if scope == "Season" and selected_season:
    st.subheader("Game-to-Game Consistency")
    st.markdown(
        "<div class='analytics-note'>Measures how steady each hitter is from game to game, using "
        "per-game linear-weight runs per plate appearance. <strong>Consistency Score</strong> runs 0-100 "
        "(higher = steadier); <strong>Boom%</strong> is the share of games well above the player's own "
        "average and <strong>Quiet%</strong> the share of near-silent games. Requires per-game box-score "
        "data, so it only appears in Season view.</div>",
        unsafe_allow_html=True,
    )
    consistency_df = fetch_consistency_scores(connection, season=selected_season)
    if consistency_df.empty:
        st.info("Not enough per-game box-score data to compute consistency for this season yet.")
    else:
        consistency_display = with_player_link_column(
            consistency_df[["player", "canonical_name", "games", "consistency_score", "mean_production", "boom_rate", "quiet_rate", "classification"]],
            output_column="player",
        )
        render_static_table(
            consistency_display[["player", "games", "consistency_score", "mean_production", "boom_rate", "quiet_rate", "classification"]],
            column_labels={
                "player": "Player",
                "games": "G",
                "consistency_score": "Consistency",
                "mean_production": "Avg Runs/PA",
                "boom_rate": "Boom %",
                "quiet_rate": "Quiet %",
                "classification": "Profile",
            },
            formatters={
                "consistency_score": "{:.1f}",
                "mean_production": "{:.3f}",
                "boom_rate": "{:.0%}",
                "quiet_rate": "{:.0%}",
            },
            link_columns=["player"],
            css_class="advanced-consistency-table",
        )
