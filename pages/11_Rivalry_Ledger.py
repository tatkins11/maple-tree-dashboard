from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_franchise_opponent_ledger,
    fetch_franchise_opponents,
    fetch_franchise_vs_opponent,
    fetch_single_game_stats,
    format_player_season_label,
    get_connection,
)
from src.dashboard.ui import (
    database_path_control,
    get_responsive_layout_context,
    persistent_segmented_control,
    persistent_selectbox,
    render_static_table,
    with_player_link_column,
)


st.set_page_config(page_title="Rivalry Ledger", page_icon="🥎", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_rivalry_css() -> None:
    st.markdown(
        """
        <style>
        .rivalry-controls {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.85rem 0.95rem 0.2rem 0.95rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .rivalry-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.75rem;
            padding: 0.72rem 0.82rem;
            min-height: 6rem;
            background: #fafafa;
        }
        .rivalry-card-label {
            font-size: 0.8rem;
            color: #6b7280;
            margin-bottom: 0.22rem;
        }
        .rivalry-card-number {
            font-size: 1.34rem;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 0.18rem;
        }
        .rivalry-card-name {
            font-size: 0.98rem;
            font-weight: 600;
            margin-bottom: 0.06rem;
        }
        .rivalry-card-context {
            font-size: 0.82rem;
            color: #6b7280;
        }
        .rivalry-summary {
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 0.8rem;
            padding: 0.85rem 1rem;
            background: white;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
            margin-bottom: 0.8rem;
        }
        .rivalry-summary-record {
            font-size: 1.7rem;
            font-weight: 800;
            line-height: 1.05;
        }
        .rivalry-summary-meta {
            font-size: 0.9rem;
            color: #4b5563;
            margin-top: 0.25rem;
        }
        .rivalry-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.45rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _signed(value: object) -> str:
    number = int(value)
    return f"+{number}" if number > 0 else str(number)


def _record_text(wins: int, losses: int, ties: int) -> str:
    return f"{int(wins)}-{int(losses)}" + (f"-{int(ties)}" if int(ties) else "")


_LEDGER_LABELS = {
    "opponent": "Opponent",
    "games": "G",
    "wins": "W",
    "losses": "L",
    "ties": "T",
    "win_pct": "Win%",
    "runs_for": "RF",
    "runs_against": "RA",
    "run_diff": "Diff",
    "first_played": "First",
    "last_played": "Last",
}

_SORT_OPTIONS = {
    "Most played": ("games", False),
    "Best record": ("win_pct", False),
    "Worst record": ("win_pct", True),
    "Best run margin": ("run_diff", False),
}


def _render_headliners(ledger, *, is_mobile_layout: bool) -> None:
    if ledger.empty:
        return
    most_played = ledger.sort_values(["games", "win_pct"], ascending=[False, False]).iloc[0]
    nemesis = ledger.sort_values(["losses", "run_diff"], ascending=[False, True]).iloc[0]
    qualified = ledger[ledger["games"] >= 2]
    best_pool = qualified if not qualified.empty else ledger
    best_record = best_pool.sort_values(["win_pct", "games"], ascending=[False, False]).iloc[0]
    best_margin = ledger.sort_values(["run_diff", "games"], ascending=[False, False]).iloc[0]

    cards = [
        (
            "Most-played rival",
            f"{int(most_played['games'])} games",
            str(most_played["opponent"]),
            f"All-time {_record_text(most_played['wins'], most_played['losses'], most_played['ties'])}",
        ),
        (
            "Biggest nemesis",
            f"{int(nemesis['losses'])} losses",
            str(nemesis["opponent"]),
            f"{_record_text(nemesis['wins'], nemesis['losses'], nemesis['ties'])} · {_signed(nemesis['run_diff'])} run diff",
        ),
        (
            "Best matchup",
            f"{best_record['win_pct']:.3f}",
            str(best_record["opponent"]),
            f"{_record_text(best_record['wins'], best_record['losses'], best_record['ties'])} in {int(best_record['games'])} games",
        ),
        (
            "Best run margin",
            _signed(best_margin["run_diff"]),
            str(best_margin["opponent"]),
            f"{int(best_margin['runs_for'])} scored · {int(best_margin['runs_against'])} allowed",
        ),
    ]

    st.markdown("### Rivalry Headliners")
    per_row = 2 if is_mobile_layout else 4
    for start in range(0, len(cards), per_row):
        columns = st.columns(per_row, gap="small")
        for column, (label, number, name, context) in zip(columns, cards[start:start + per_row]):
            column.markdown(
                f"""
                <div class="rivalry-card">
                  <div class="rivalry-card-label">{escape(label)}</div>
                  <div class="rivalry-card-number">{escape(number)}</div>
                  <div class="rivalry-card-name">{escape(name)}</div>
                  <div class="rivalry-card-context">{escape(context)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_ledger(ledger, sort_choice: str) -> None:
    if ledger.empty:
        st.info("No completed games on record yet.")
        return
    sort_column, ascending = _SORT_OPTIONS[sort_choice]
    ordered = ledger.sort_values(
        [sort_column, "games", "opponent"],
        ascending=[ascending, False, True],
    ).reset_index(drop=True)
    display = ordered[list(_LEDGER_LABELS.keys())].rename(columns=_LEDGER_LABELS)
    render_static_table(
        display,
        formatters={
            "Win%": "{:.3f}",
            "Diff": _signed,
        },
        css_class="rivalry-ledger-table",
    )
    st.markdown(
        '<div class="rivalry-note">All four franchise eras (Soviet Sluggers, Smoking Bunts, Maple Tree Tappers, Maple Tree) count as one franchise. Tap an opponent in Opponent Detail for the full game-by-game history.</div>',
        unsafe_allow_html=True,
    )


def _opponent_player_splits(stats, opponent: str):
    if stats.empty:
        return stats
    filtered = stats[stats["opponent"].astype(str).str.casefold() == opponent.casefold()].copy()
    if filtered.empty:
        return filtered
    grouped = (
        filtered.groupby(["player", "canonical_name"], dropna=False)
        .agg(
            # One row per player-game, so count rows — NOT unique dates, which
            # would undercount doubleheaders (two games share one date).
            G=("game_date", "count"),
            PA=("pa", "sum"),
            AB=("ab", "sum"),
            H=("hits", "sum"),
            HR=("hr", "sum"),
            RBI=("rbi", "sum"),
            TB=("tb", "sum"),
            BB=("bb", "sum"),
            SF=("sf", "sum"),
        )
        .reset_index()
    )
    ab = grouped["AB"].clip(lower=0)
    safe_ab = ab.replace(0, 1)
    obp_denom = grouped["AB"] + grouped["BB"] + grouped["SF"]
    safe_obp_denom = obp_denom.replace(0, 1)
    avg = (grouped["H"] / safe_ab).where(ab > 0, 0.0)
    slg = (grouped["TB"] / safe_ab).where(ab > 0, 0.0)
    obp = ((grouped["H"] + grouped["BB"]) / safe_obp_denom).where(obp_denom > 0, 0.0)
    grouped = grouped.assign(AVG=avg, OBP=obp, SLG=slg, OPS=obp + slg)
    return grouped.sort_values(["TB", "H", "PA"], ascending=[False, False, False]).reset_index(drop=True)


def _render_detail(connection, opponent: str, *, is_mobile_layout: bool) -> None:
    detail = fetch_franchise_vs_opponent(connection, opponent=opponent)
    if detail["games"] == 0:
        st.info(f"No completed games on record against {opponent}.")
        return

    eras = sorted({str(era) for era in detail["meetings"]["era"].tolist() if str(era).strip()})
    st.markdown(
        f"""
        <div class="rivalry-summary">
          <div class="rivalry-card-label">All-time vs {escape(str(detail['opponent']))}</div>
          <div class="rivalry-summary-record">{escape(_record_text(detail['wins'], detail['losses'], detail['ties']))}
            &nbsp;<span style="font-size:1rem;font-weight:600;color:#6b7280;">({detail['win_pct']:.3f})</span></div>
          <div class="rivalry-summary-meta">
            {int(detail['runs_for'])} scored · {int(detail['runs_against'])} allowed · {escape(_signed(detail['run_diff']))} run differential<br>
            {escape(detail['first_played'])} → {escape(detail['last_played'])} · {escape(', '.join(eras))}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    src = detail["meetings"]
    meetings_display = pd.DataFrame(
        {
            "Season": src["season"].map(format_player_season_label),
            "Era": src["era"],
            "Date": src["game_date"],
            "Result": src["result"],
            "Score": [
                f"{int(rf)}-{int(ra)}" for rf, ra in zip(src["team_score"], src["opponent_score"])
            ],
            "Diff": src["run_diff"],
        }
    )
    st.markdown("#### Game-by-game")
    render_static_table(
        meetings_display,
        formatters={"Diff": _signed},
        css_class="rivalry-meetings-table",
    )

    splits = _opponent_player_splits(fetch_single_game_stats(connection, min_pa=0), opponent)
    if not splits.empty:
        with st.expander(f"Who hits {detail['opponent']} (career vs this rival)", expanded=not is_mobile_layout):
            board = with_player_link_column(
                splits.rename(columns={"player": "Player"}),
                player_column="Player",
                canonical_column="canonical_name",
                output_column="Player",
            )
            columns = ["Player", "G", "PA", "AB", "H", "HR", "RBI", "TB", "AVG", "OBP", "SLG", "OPS"]
            render_static_table(
                board[columns],
                formatters={
                    "AVG": "{:.3f}",
                    "OBP": "{:.3f}",
                    "SLG": "{:.3f}",
                    "OPS": "{:.3f}",
                },
                link_columns=["Player"],
                css_class="rivalry-splits-table",
            )


_inject_rivalry_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="rivalry_ledger")

st.title("Rivalry Ledger")
st.caption("All-time head-to-head records versus every opponent the franchise has ever played — across all four team-name eras.")

db_path = database_path_control(DEFAULT_DB_PATH, key="rivalry_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())

ledger = fetch_franchise_opponent_ledger(connection)
opponents = fetch_franchise_opponents(connection)

if ledger.empty or not opponents:
    st.info("No completed games are loaded yet, so there is no rivalry history to show.")
    st.stop()

st.markdown('<div class="rivalry-controls">', unsafe_allow_html=True)
if layout.is_mobile_layout:
    view = persistent_segmented_control(
        "View",
        options=["League Ledger", "Opponent Detail"],
        query_key="rivalry_view",
        default="League Ledger",
    )
    opponent = persistent_selectbox(
        "Opponent",
        options=opponents,
        query_key="rivalry_opponent",
        default=opponents[0],
    )
    sort_choice = persistent_segmented_control(
        "Sort ledger by",
        options=list(_SORT_OPTIONS.keys()),
        query_key="rivalry_sort",
        default="Most played",
    )
else:
    control_columns = st.columns([1.3, 1.4, 1.6], gap="small")
    with control_columns[0]:
        view = persistent_segmented_control(
            "View",
            options=["League Ledger", "Opponent Detail"],
            query_key="rivalry_view",
            default="League Ledger",
        )
    with control_columns[1]:
        opponent = persistent_selectbox(
            "Opponent",
            options=opponents,
            query_key="rivalry_opponent",
            default=opponents[0],
        )
    with control_columns[2]:
        sort_choice = persistent_segmented_control(
            "Sort ledger by",
            options=list(_SORT_OPTIONS.keys()),
            query_key="rivalry_sort",
            default="Most played",
        )
st.markdown('</div>', unsafe_allow_html=True)

if view == "Opponent Detail":
    _render_detail(connection, opponent, is_mobile_layout=layout.is_mobile_layout)
else:
    _render_headliners(ledger, is_mobile_layout=layout.is_mobile_layout)
    st.markdown("### League Ledger")
    _render_ledger(ledger, sort_choice)
