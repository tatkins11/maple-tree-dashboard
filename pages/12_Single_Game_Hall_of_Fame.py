from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_record_headliners,
    fetch_record_leaderboards,
    fetch_seasons,
    fetch_single_game_feats,
    fetch_single_game_score_leaders,
    format_player_season_label,
    get_connection,
    with_dashboard_default_season,
)
from src.dashboard.ui import (
    build_player_link_html,
    database_path_control,
    get_responsive_layout_context,
    persistent_multiselect,
    persistent_segmented_control,
    persistent_selectbox,
    persistent_slider,
    persistent_toggle,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="Single-Game Hall of Fame", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_hof_css() -> None:
    st.markdown(
        """
        <style>
        .hof-controls {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.85rem 0.95rem 0.2rem 0.95rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .hof-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.75rem;
            padding: 0.72rem 0.82rem;
            min-height: 6rem;
            background: #fafafa;
        }
        .hof-card-label {
            font-size: 0.8rem;
            color: #6b7280;
            margin-bottom: 0.22rem;
        }
        .hof-card-number {
            font-size: 1.34rem;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 0.18rem;
        }
        .hof-card-player {
            font-size: 0.98rem;
            font-weight: 600;
            margin-bottom: 0.06rem;
        }
        .hof-card-context {
            font-size: 0.82rem;
            color: #6b7280;
        }
        .hof-active-badge {
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
        .hof-section-title {
            margin-top: 0.05rem;
            margin-bottom: 0.12rem;
        }
        .hof-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.45rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# Which leaderboard boards each view shows. Labels match SINGLE_GAME / RATE record columns.
_VIEW_BOARDS = {
    "Power": ["Total Bases", "HR", "RBI"],
    "Hits & On-Base": ["Hits", "Runs", "Walks"],
    "Rate Stats": ["AVG", "OBP", "SLG", "OPS"],
}
_RATE_LABELS = {"AVG", "OBP", "SLG", "OPS"}


def _format_line(row) -> str:
    parts = [f"{int(row['hits'])}H"]
    if int(row["hr"]):
        parts.append(f"{int(row['hr'])}HR")
    parts.append(f"{int(row['tb'])}TB")
    if int(row["bb"]):
        parts.append(f"{int(row['bb'])}BB")
    return " ".join(parts)


def _game_score_card(score_leaders, active_players: set[str]) -> dict[str, object] | None:
    if score_leaders.empty:
        return None
    top = score_leaders.iloc[0]
    return {
        "label": "Top Game Score",
        "formatted_value": f"{float(top['game_score']):.1f}",
        "value_label": "runs created",
        "player": str(top["player"]),
        "canonical_name": str(top["canonical_name"] or ""),
        "context": f"{_format_line(top)} · vs {top['opponent']} · {format_player_season_label(str(top['season']))}",
        "is_active": str(top["player"]) in active_players,
    }


def _render_headliners(headliners, score_leaders, *, is_mobile_layout: bool) -> None:
    active_players = {str(p.get("player")) for p in headliners.values() if p.get("is_active")}
    cards: list[dict[str, object]] = []
    lead = _game_score_card(score_leaders, active_players)
    if lead is not None:
        cards.append(lead)
    for label in ["Single-Game TB", "Single-Game Hits", "Single-Game HR"]:
        if label in headliners:
            payload = dict(headliners[label])
            payload["label"] = label.replace("Single-Game ", "Most ")
            cards.append(payload)
    if not cards:
        return
    st.markdown("### All-Time Single-Game Bests")
    per_row = 2 if is_mobile_layout else 4
    for start in range(0, len(cards), per_row):
        columns = st.columns(per_row, gap="small")
        for column, payload in zip(columns, cards[start:start + per_row]):
            badge = '<span class="hof-active-badge">Active</span>' if payload.get("is_active") else ""
            value = escape(str(payload.get("formatted_value", "")))
            value_label = escape(str(payload.get("value_label", "")))
            player = build_player_link_html(
                str(payload.get("player", "")),
                str(payload.get("canonical_name") or ""),
            )
            context = escape(str(payload.get("context", "")))
            column.markdown(
                f"""
                <div class="hof-card">
                  <div class="hof-card-label">{escape(str(payload.get("label", "")))}</div>
                  <div class="hof-card-number">{value} {value_label}</div>
                  <div class="hof-card-player">{player}{badge}</div>
                  <div class="hof-card-context">{context}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_game_score_board(score_leaders) -> None:
    st.markdown("### Game Score Leaderboard")
    st.markdown(
        '<div class="hof-note">One number for the loudest overall single game — runs created from the site\'s calibrated '
        "linear weights (1B .50 · 2B .90 · 3B 1.10 · HR 1.40 · BB .40, the same engine behind wOBA/wRC+), plus a light "
        "+.20 per run scored and driven in for game impact, minus .25 per out so a clean 4-for-4 edges a 4-for-5. "
        "Records are not final — playoff games count.</div>",
        unsafe_allow_html=True,
    )
    if score_leaders.empty:
        st.info("No single-game lines match the current filters.")
        return
    board = pd.DataFrame(
        {
            "#": range(1, len(score_leaders) + 1),
            "Player": score_leaders["player"],
            "canonical_name": score_leaders["canonical_name"],
            "GS": score_leaders["game_score"],
            "Line": [_format_line(row) for _, row in score_leaders.iterrows()],
            "Date": score_leaders["game_date"],
            "Opponent": score_leaders["opponent"],
            "Season": score_leaders["season"].map(format_player_season_label),
        }
    )
    board = with_player_link_column(
        board, player_column="Player", canonical_column="canonical_name", output_column="Player"
    )
    render_static_table(
        board[["#", "Player", "GS", "Line", "Date", "Opponent", "Season"]],
        formatters={"GS": "{:.1f}"},
        link_columns=["Player"],
        css_class="hof-gamescore-table",
    )


def _display_columns_for_board(label: str, dataframe) -> list[str]:
    present = list(dataframe.columns)
    if label in _RATE_LABELS:
        preferred = ["#", label, "Player", "PA", "Date", "Opponent", "Season"]
    else:
        preferred = ["#", label, "Player", "Date", "Opponent", "Season"]
    ordered = [column for column in preferred if column in present]
    remainder = [
        column for column in present if column not in ordered and column not in {"canonical_name", "Time"}
    ]
    return [*ordered, *remainder]


def _render_leaderboards(leaderboards, view: str, min_pa: int, *, is_mobile_layout: bool) -> None:
    labels = [label for label in _VIEW_BOARDS[view] if label in leaderboards]
    if not labels:
        st.info("No single-game lines match the current filters.")
        return
    st.markdown(f"### {view} Leaderboards")
    if view == "Rate Stats":
        st.markdown(
            f'<div class="hof-note">Rate boards require at least {min_pa} PA — raise the floor to filter out small-sample nights.</div>',
            unsafe_allow_html=True,
        )
    per_row = 1 if is_mobile_layout else (2 if view == "Rate Stats" else 3)
    for start in range(0, len(labels), per_row):
        columns = st.columns(per_row, gap="small")
        for column, label in zip(columns, labels[start:start + per_row]):
            column.markdown(f'<div class="hof-section-title"><strong>{label}</strong></div>', unsafe_allow_html=True)
            board = leaderboards.get(label)
            if board is None or board.empty:
                column.info("No data")
                continue
            board = with_player_link_column(
                board,
                player_column="Player",
                canonical_column="canonical_name",
                output_column="Player",
            )
            display_columns = _display_columns_for_board(label, board)
            render_static_table(
                board[[column_name for column_name in display_columns if column_name != "canonical_name"]],
                formatters={"AVG": "{:.3f}", "OBP": "{:.3f}", "SLG": "{:.3f}", "OPS": "{:.3f}"},
                link_columns=["Player"],
                css_class="hof-leaderboard-table",
                container=column,
            )


def _render_feats(connection, selected_seasons, *, is_mobile_layout: bool) -> None:
    feats = fetch_single_game_feats(connection, seasons=selected_seasons)
    st.markdown("### Feats of the Franchise")
    st.markdown(
        '<div class="hof-note">The rare-air club: the only games anyone has ever posted these lines. Records are not final — playoff games count.</div>',
        unsafe_allow_html=True,
    )
    display_order = ["Player", "Date", "Opponent", "Season", "PA", "Hits", "HR", "RBI", "TB"]
    for label, board in feats.items():
        st.markdown(f'<div class="hof-section-title"><strong>{escape(label)}</strong></div>', unsafe_allow_html=True)
        if board.empty:
            st.caption("None on record under the current season filter.")
            continue
        prepared = pd.DataFrame(
            {
                "Player": board["player"],
                "canonical_name": board["canonical_name"],
                "Date": board["game_date"],
                "Opponent": board["opponent"],
                "Season": board["season"].map(format_player_season_label),
                "PA": board["pa"],
                "Hits": board["hits"],
                "HR": board["hr"],
                "RBI": board["rbi"],
                "TB": board["tb"],
            }
        )
        prepared = with_player_link_column(
            prepared,
            player_column="Player",
            canonical_column="canonical_name",
            output_column="Player",
        )
        render_static_table(
            prepared[display_order],
            link_columns=["Player"],
            css_class="hof-feat-table",
        )


_inject_hof_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="hall_of_fame")

st.title("Single-Game Hall of Fame")
st.caption("The loudest individual box-score lines in franchise history, across every season and era.")

db_path = database_path_control(DEFAULT_DB_PATH, key="hof_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
seasons = with_dashboard_default_season(fetch_seasons(connection))

VIEW_OPTIONS = ["Game Score", "Power", "Hits & On-Base", "Rate Stats", "Feats"]
TOP_N_OPTIONS = [5, 10, 15]

st.markdown('<div class="hof-controls">', unsafe_allow_html=True)
selected_seasons = persistent_multiselect(
    "Season filter",
    options=seasons,
    query_key="hof_seasons",
    default=seasons,
)
if layout.is_mobile_layout:
    view = persistent_segmented_control(
        "View", options=VIEW_OPTIONS, query_key="hof_view", default="Game Score",
    )
    min_pa = persistent_slider(
        "Minimum PA for rate stats", query_key="hof_min_pa", min_value=1, max_value=8, default=3, step=1,
    )
    top_n = persistent_selectbox(
        "Leaderboard size", options=TOP_N_OPTIONS, query_key="hof_top_n", default=10,
    )
    active_only = persistent_toggle(
        "Show active roster only", query_key="hof_active_only", default=False,
    )
else:
    control_columns = st.columns([1.6, 1.2, 1, 0.9], gap="small")
    with control_columns[0]:
        view = persistent_segmented_control(
            "View", options=VIEW_OPTIONS, query_key="hof_view", default="Game Score",
        )
    with control_columns[1]:
        min_pa = persistent_slider(
            "Minimum PA for rate stats", query_key="hof_min_pa", min_value=1, max_value=8, default=3, step=1,
        )
    with control_columns[2]:
        top_n = persistent_selectbox(
            "Leaderboard size", options=TOP_N_OPTIONS, query_key="hof_top_n", default=10,
        )
    with control_columns[3]:
        active_only = persistent_toggle(
            "Show active roster only", query_key="hof_active_only", default=False,
        )
st.markdown('</div>', unsafe_allow_html=True)

if not selected_seasons:
    st.info("Select at least one season to see the Hall of Fame.")
    st.stop()

headliners = fetch_record_headliners(
    connection,
    scope="single_game",
    seasons=selected_seasons,
    min_pa=min_pa,
    active_only=active_only,
)
score_leaders = fetch_single_game_score_leaders(
    connection,
    seasons=selected_seasons,
    limit=top_n,
    active_only=active_only,
)
_render_headliners(headliners, score_leaders, is_mobile_layout=layout.is_mobile_layout)

if view == "Game Score":
    _render_game_score_board(score_leaders)
elif view == "Feats":
    _render_feats(connection, selected_seasons, is_mobile_layout=layout.is_mobile_layout)
else:
    leaderboards = fetch_record_leaderboards(
        connection,
        scope="single_game",
        seasons=selected_seasons,
        min_pa=min_pa,
        limit=top_n,
        active_only=active_only,
    )
    _render_leaderboards(leaderboards, view, min_pa, is_mobile_layout=layout.is_mobile_layout)
