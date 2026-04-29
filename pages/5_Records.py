from __future__ import annotations

from html import escape
from pathlib import Path

import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    COUNTING_RECORD_COLUMNS,
    DEFAULT_DB_PATH,
    RATE_RECORD_COLUMNS,
    SINGLE_GAME_RECORD_COLUMNS,
    fetch_record_headliners,
    fetch_record_leaderboards,
    fetch_seasons,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import (
    build_player_link_html,
    database_path_control,
    get_responsive_layout_context,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="Records", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_records_css() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.89rem;
        }
        div[data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
            min-height: 0 !important;
        }
        .records-controls {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.85rem 0.95rem 0.2rem 0.95rem;
            background: #fafafa;
            margin-bottom: 0.45rem;
        }
        .records-sticky-bar {
            position: sticky;
            top: 0.5rem;
            z-index: 10;
            background: white;
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 0.75rem;
            padding: 0.58rem 0.8rem;
            margin: 0.25rem 0 0.7rem 0;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
        }
        .records-sticky-meta {
            display: flex;
            gap: 0.8rem 1rem;
            flex-wrap: wrap;
            font-size: 0.92rem;
        }
        .records-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.75rem;
            padding: 0.72rem 0.82rem;
            min-height: 5.8rem;
            background: #fafafa;
        }
        .records-card-label {
            font-size: 0.8rem;
            color: #6b7280;
            margin-bottom: 0.22rem;
        }
        .records-card-number {
            font-size: 1.34rem;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 0.22rem;
        }
        .records-card-player {
            font-size: 0.98rem;
            font-weight: 600;
            margin-bottom: 0.08rem;
        }
        .records-card-context {
            font-size: 0.82rem;
            color: #6b7280;
        }
        .records-active-badge {
            display: inline-block;
            margin-left: 0.28rem;
            padding: 0.03rem 0.34rem;
            border-radius: 999px;
            background: rgba(239, 68, 68, 0.10);
            color: #b91c1c;
            font-size: 0.66rem;
            font-weight: 600;
            vertical-align: middle;
        }
        .records-section-title {
            margin-top: 0.05rem;
            margin-bottom: 0.12rem;
        }
        .records-tight-caption {
            margin-top: -0.15rem;
            margin-bottom: 0.35rem;
        }
        .records-subnav {
            margin: 0.1rem 0 0.2rem 0;
        }
        .records-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.45rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _season_summary(selected_seasons: list[str], all_seasons: list[str]) -> str:
    if not selected_seasons:
        return "No seasons selected"
    if len(selected_seasons) == len(all_seasons):
        return f"All {len(all_seasons)} seasons"
    if len(selected_seasons) <= 2:
        return ", ".join(selected_seasons)
    return f"{len(selected_seasons)} seasons selected"


def _render_context_bar(
    scope_label: str,
    stat_view: str,
    selected_seasons: list[str],
    all_seasons: list[str],
    rate_min_pa: int,
    top_n: int,
    active_only: bool,
) -> None:
    season_text = _season_summary(selected_seasons, all_seasons)
    roster_text = "Active only" if active_only else "All players"
    st.markdown(
        f"""
        <div class="records-sticky-bar">
          <div class="records-sticky-meta">
            <div><strong>Scope:</strong> {escape(scope_label)}</div>
            <div><strong>View:</strong> {escape(stat_view)}</div>
            <div><strong>Seasons:</strong> {escape(season_text)}</div>
            <div><strong>Min PA:</strong> {rate_min_pa}</div>
            <div><strong>Size:</strong> Top {top_n}</div>
            <div><strong>Roster:</strong> {escape(roster_text)}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_headliners(headliners: dict[str, dict[str, object]], *, is_mobile_layout: bool) -> None:
    st.markdown("### Record Holders")
    per_row = 2 if is_mobile_layout else 4
    items = list(headliners.items())
    for start in range(0, len(items), per_row):
        columns = st.columns(per_row, gap="small")
        for column, (label, payload) in zip(columns, items[start:start + per_row]):
            badge = (
                '<span class="records-active-badge">Active</span>'
                if payload.get("is_active")
                else ""
            )
            value_label = escape(str(payload.get("value_label", "")))
            value = escape(str(payload.get("formatted_value", "")))
            player = build_player_link_html(
                str(payload.get("player", "")),
                str(payload.get("canonical_name") or ""),
            )
            context = escape(str(payload.get("context", "")))
            column.markdown(
                f"""
                <div class="records-card">
                  <div class="records-card-label">{escape(label)}</div>
                  <div class="records-card-number">{value} {value_label}</div>
                  <div class="records-card-player">{player}{badge}</div>
                  <div class="records-card-context">{context}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _base_column_config() -> dict[str, st.column_config.Column]:
    return {}


def _display_columns_for_board(scope: str, stat_view: str, label: str, dataframe) -> list[str]:
    present = list(dataframe.columns)

    if scope == "Single-Season Records" and stat_view == "Counting Stats":
        preferred = ["#", label, "Player", "Season"]
    elif scope == "Single-Season Records" and stat_view == "Rate Stats":
        preferred = ["#", label, "Player", "PA", "Season"]
    elif scope == "Single-Game Records" and stat_view == "Counting Stats":
        preferred = ["#", label, "Player", "Date", "Time", "Opponent", "Season"]
    elif scope == "Single-Game Records" and stat_view == "Rate Stats":
        preferred = ["#", label, "Player", "PA", "Date", "Time", "Opponent", "Season"]
    elif scope == "Career Records" and stat_view == "Counting Stats":
        preferred = ["#", label, "Player"]
    else:
        preferred = ["#", label, "Player", "PA"]

    ordered = [column for column in preferred if column in present]
    remainder = [column for column in present if column not in ordered]
    return [*ordered, *remainder]


def _column_config_for_board(scope: str, stat_view: str, label: str) -> dict[str, st.column_config.Column]:
    return {}


def _grid_size(scope: str, stat_view: str, *, is_mobile_layout: bool) -> int:
    if is_mobile_layout:
        return 1
    if scope in {"Single-Season Records", "Single-Game Records"}:
        return 2
    if stat_view == "Rate Stats":
        return 2
    return 3


def _leaderboard_labels(scope: str, stat_view: str, leaderboards: dict[str, object]) -> list[str]:
    if stat_view == "Rate Stats":
        base_labels = list(RATE_RECORD_COLUMNS.keys())
    elif scope == "Single-Game Records":
        base_labels = list(SINGLE_GAME_RECORD_COLUMNS.keys())
    else:
        base_labels = list(COUNTING_RECORD_COLUMNS.keys())
    return [label for label in base_labels if label in leaderboards]


def _render_leaderboards(leaderboards: dict[str, object], scope: str, stat_view: str, rate_min_pa: int, *, is_mobile_layout: bool) -> None:
    if not leaderboards:
        st.info("No record rows match the current filters.")
        return

    section_title = "Counting Stat Leaderboards" if stat_view == "Counting Stats" else "Rate Stat Leaderboards"
    labels = _leaderboard_labels(scope, stat_view, leaderboards)
    if not labels:
        st.info("No record rows match the current filters.")
        return
    st.markdown(f"### {section_title}")
    if stat_view == "Rate Stats":
        st.markdown(
            f'<div class="records-tight-caption">Minimum PA: {rate_min_pa}{" (lower this for single-game rate boards)" if scope == "Single-Game Records" else ""}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="records-note">Counting leaderboards show the record stat directly; PA is only shown on rate leaderboards.</div>',
            unsafe_allow_html=True,
        )

    column_config = _base_column_config()
    per_row = _grid_size(scope, stat_view, is_mobile_layout=is_mobile_layout)
    for start in range(0, len(labels), per_row):
        columns = st.columns(per_row, gap="small")
        for column, label in zip(columns, labels[start:start + per_row]):
            column.markdown(f'<div class="records-section-title"><strong>{label}</strong></div>', unsafe_allow_html=True)
            board = leaderboards.get(label)
            if board is None:
                column.info("No data")
                continue
            board = with_player_link_column(
                board,
                player_column="Player",
                canonical_column="canonical_name",
                output_column="Player",
            )
            display_columns = _display_columns_for_board(scope, stat_view, label, board)
            render_static_table(
                board[[column_name for column_name in display_columns if column_name != "canonical_name"]],
                formatters={
                    "AVG": "{:.3f}",
                    "OBP": "{:.3f}",
                    "SLG": "{:.3f}",
                    "OPS": "{:.3f}",
                },
                link_columns=["Player"],
                css_class="records-leaderboard-table",
                container=column,
            )


_inject_records_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="records")

st.title("Records")
st.caption("Team hitter records across career totals, single seasons, and single-game box score lines.")

db_path = database_path_control(DEFAULT_DB_PATH, key="records_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
seasons = with_dashboard_default_season(fetch_seasons(connection))

st.markdown('<div class="records-controls">', unsafe_allow_html=True)
selected_seasons = st.multiselect("Season filter", options=seasons, default=seasons)

if layout.is_mobile_layout:
    rate_min_pa = st.slider("Minimum PA for rate stats", min_value=0, max_value=100, value=20, step=5)
    top_n = st.selectbox("Leaderboard size", options=[5, 10, 15], index=1)
    scope = st.segmented_control(
        "Record scope",
        options=["Career Records", "Single-Season Records", "Single-Game Records"],
        default="Career Records",
    )
    active_only = st.toggle("Show active roster only", value=False)
else:
    control_columns = st.columns([1.2, 1, 1.3, 0.9], gap="small")
    with control_columns[0]:
        rate_min_pa = st.slider("Minimum PA for rate stats", min_value=0, max_value=100, value=20, step=5)
    with control_columns[1]:
        top_n = st.selectbox("Leaderboard size", options=[5, 10, 15], index=1)
    with control_columns[2]:
        scope = st.segmented_control(
            "Record scope",
            options=["Career Records", "Single-Season Records", "Single-Game Records"],
            default="Career Records",
        )
    with control_columns[3]:
        active_only = st.toggle("Show active roster only", value=False)

st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="records-subnav">', unsafe_allow_html=True)
stat_view = st.segmented_control("Leaderboard view", options=["Counting Stats", "Rate Stats"], default="Counting Stats")
st.markdown('</div>', unsafe_allow_html=True)

_render_context_bar(scope, stat_view, selected_seasons, seasons, rate_min_pa, top_n, active_only)

scope_key = {
    "Career Records": "career",
    "Single-Season Records": "single_season",
    "Single-Game Records": "single_game",
}[scope]

headliners = fetch_record_headliners(
    connection,
    scope=scope_key,
    seasons=selected_seasons,
    min_pa=rate_min_pa,
    active_only=active_only,
)
_render_headliners(headliners, is_mobile_layout=layout.is_mobile_layout)

leaderboards = fetch_record_leaderboards(
    connection,
    scope=scope_key,
    seasons=selected_seasons,
    min_pa=rate_min_pa,
    limit=top_n,
    active_only=active_only,
)
_render_leaderboards(leaderboards, scope, stat_view, rate_min_pa, is_mobile_layout=layout.is_mobile_layout)
