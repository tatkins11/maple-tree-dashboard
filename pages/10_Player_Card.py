from __future__ import annotations

from html import escape
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.auth import ensure_authenticated
from src.dashboard.config import get_connection_cache_key
from src.dashboard.data import (
    DEFAULT_DB_PATH,
    fetch_player_advanced_history,
    fetch_player_identities,
    fetch_player_milestone_context,
    fetch_player_profile_summary,
    fetch_player_record_context,
    fetch_player_season_history,
    get_connection,
)
from src.dashboard.ui import database_path_control, get_responsive_layout_context


st.set_page_config(page_title="Player Card", page_icon="👤", layout="wide")


@st.cache_resource
def get_db_connection(db_path: str, cache_key: str):
    return get_connection(Path(db_path))


def _inject_player_card_css() -> None:
    st.markdown(
        """
        <style>
        .player-card-header {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 1rem;
            padding: 1rem 1.05rem;
            background: #fafafa;
            margin-bottom: 0.8rem;
        }
        .player-card-title {
            font-size: 2rem;
            font-weight: 800;
            line-height: 1.05;
            margin-bottom: 0.25rem;
            color: #111827;
        }
        .player-card-subtitle {
            font-size: 0.95rem;
            color: #4b5563;
            margin-bottom: 0.55rem;
        }
        .player-pill-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin: 0.15rem 0 0.35rem 0;
        }
        .player-pill {
            display: inline-block;
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 999px;
            padding: 0.16rem 0.52rem;
            font-size: 0.8rem;
            background: white;
            color: #374151;
        }
        .player-pill-strong {
            background: rgba(59, 130, 246, 0.08);
            color: #1d4ed8;
        }
        .player-section-note {
            font-size: 0.84rem;
            color: #6b7280;
            margin-top: -0.1rem;
            margin-bottom: 0.45rem;
        }
        .player-compact-card {
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.8rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }
        .player-compact-title {
            font-size: 1rem;
            font-weight: 800;
            margin-bottom: 0.28rem;
            color: #111827;
        }
        .player-compact-row {
            font-size: 0.9rem;
            color: #374151;
            margin: 0.12rem 0;
        }
        div[data-testid="stDataFrame"] div[role="table"] {
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _set_player_query_param(canonical_name: str) -> None:
    st.query_params["player"] = str(canonical_name)


def _metric_grid(metrics: list[tuple[str, str]], *, per_row: int) -> None:
    for start in range(0, len(metrics), per_row):
        columns = st.columns(per_row, gap="small")
        for column, (label, value) in zip(columns, metrics[start:start + per_row]):
            column.metric(label, value)


def _profile_pills(summary: dict[str, object]) -> str:
    pills: list[str] = []
    if summary.get("active_roster"):
        pills.append('<span class="player-pill player-pill-strong">Active roster</span>')
    if summary.get("is_fixed_dhh"):
        pills.append('<span class="player-pill">DHH</span>')
    if summary.get("speed_flag"):
        pills.append('<span class="player-pill">Speed flag</span>')
    baserunning = str(summary.get("baserunning_grade") or "").strip()
    consistency = str(summary.get("consistency_grade") or "").strip()
    if baserunning:
        pills.append(f'<span class="player-pill">Baserunning: {escape(baserunning)}</span>')
    if consistency:
        pills.append(f'<span class="player-pill">Consistency: {escape(consistency)}</span>')
    aliases = [str(alias).strip() for alias in summary.get("aliases", []) if str(alias).strip()]
    if aliases:
        pills.append(f'<span class="player-pill">Aliases: {escape(", ".join(aliases[:4]))}</span>')
    return "".join(pills)


def _render_header(summary: dict[str, object]) -> None:
    notes = str(summary.get("notes") or "").strip()
    note_markup = f"<div class='player-card-subtitle'>{escape(notes)}</div>" if notes else ""
    st.markdown(
        f"""
        <div class="player-card-header">
          <div class="player-card-title">{escape(str(summary.get("player") or "Player"))}</div>
          <div class="player-card-subtitle">Canonical key: {escape(str(summary.get("canonical_name") or ""))}</div>
          <div class="player-pill-row">{_profile_pills(summary)}</div>
          {note_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_standard_mobile_cards(dataframe: pd.DataFrame) -> None:
    for _, row in dataframe.iterrows():
        st.markdown(
            f"""
            <div class="player-compact-card">
              <div class="player-compact-title">{escape(str(row['season_label']))}</div>
              <div class="player-compact-row"><strong>G:</strong> {int(row['games'])} &nbsp; <strong>PA:</strong> {int(row['pa'])} &nbsp; <strong>AB:</strong> {int(row['ab'])} &nbsp; <strong>H:</strong> {int(row['hits'])}</div>
              <div class="player-compact-row"><strong>1B:</strong> {int(row['1b'])} &nbsp; <strong>2B:</strong> {int(row['2b'])} &nbsp; <strong>3B:</strong> {int(row['3b'])} &nbsp; <strong>HR:</strong> {int(row['hr'])}</div>
              <div class="player-compact-row"><strong>RBI:</strong> {int(row['rbi'])} &nbsp; <strong>R:</strong> {int(row['r'])} &nbsp; <strong>BB:</strong> {int(row['bb'])} &nbsp; <strong>TB:</strong> {int(row['tb'])}</div>
              <div class="player-compact-row"><strong>AVG:</strong> {row['avg']:.3f} &nbsp; <strong>OBP:</strong> {row['obp']:.3f} &nbsp; <strong>SLG:</strong> {row['slg']:.3f} &nbsp; <strong>OPS:</strong> {row['ops']:.3f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_advanced_mobile_cards(dataframe: pd.DataFrame) -> None:
    for _, row in dataframe.iterrows():
        st.markdown(
            f"""
            <div class="player-compact-card">
              <div class="player-compact-title">{escape(str(row['season_label']))}</div>
              <div class="player-compact-row"><strong>PA:</strong> {int(row['pa'])} &nbsp; <strong>ISO:</strong> {row['iso']:.3f} &nbsp; <strong>XBH Rate:</strong> {row['xbh_rate']:.3f}</div>
              <div class="player-compact-row"><strong>HR Rate:</strong> {row['hr_rate']:.3f} &nbsp; <strong>TB / PA:</strong> {row['tb_per_pa']:.3f} &nbsp; <strong>Team OPS+:</strong> {row['team_relative_ops']:.0f}</div>
              <div class="player-compact-row"><strong>RAA:</strong> {row['raa']:.2f} &nbsp; <strong>RAR:</strong> {row['rar']:.2f} &nbsp; <strong>oWAR:</strong> {row['owar']:.2f}</div>
              <div class="player-compact-row"><strong>Archetype:</strong> {escape(str(row['archetype']))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_trend_chart(history: pd.DataFrame) -> None:
    if history.empty:
        st.info("No season trend data is available for this player yet.")
        return

    chart_source = history.iloc[::-1][["season_label", "ops", "pa", "hr"]].reset_index(drop=True).copy()
    season_order = chart_source["season_label"].astype(str).tolist()
    chart_source["ops_display"] = chart_source["ops"].round(3)
    chart_source["pa_display"] = chart_source["pa"].astype(int)
    chart_source["hr_display"] = chart_source["hr"].astype(int)

    chart = (
        alt.Chart(chart_source)
        .mark_line(point=True)
        .encode(
            x=alt.X("season_label:N", title="Season", sort=season_order),
            y=alt.Y("ops:Q", title="OPS"),
            tooltip=[
                alt.Tooltip("season_label:N", title="Season"),
                alt.Tooltip("ops_display:Q", title="OPS", format=".3f"),
                alt.Tooltip("pa_display:Q", title="PA", format=".0f"),
                alt.Tooltip("hr_display:Q", title="HR", format=".0f"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def _standard_history_column_config() -> dict[str, st.column_config.Column]:
    return {
        "season_label": st.column_config.TextColumn("Season", width="small"),
        "games": st.column_config.NumberColumn("G", format="%d", width="small"),
        "pa": st.column_config.NumberColumn("PA", format="%d", width="small"),
        "ab": st.column_config.NumberColumn("AB", format="%d", width="small"),
        "hits": st.column_config.NumberColumn("H", format="%d", width="small"),
        "1b": st.column_config.NumberColumn("1B", format="%d", width="small"),
        "2b": st.column_config.NumberColumn("2B", format="%d", width="small"),
        "3b": st.column_config.NumberColumn("3B", format="%d", width="small"),
        "hr": st.column_config.NumberColumn("HR", format="%d", width="small"),
        "bb": st.column_config.NumberColumn("BB", format="%d", width="small"),
        "r": st.column_config.NumberColumn("R", format="%d", width="small"),
        "rbi": st.column_config.NumberColumn("RBI", format="%d", width="small"),
        "tb": st.column_config.NumberColumn("TB", format="%d", width="small"),
        "avg": st.column_config.NumberColumn("AVG", format="%.3f", width="small"),
        "obp": st.column_config.NumberColumn("OBP", format="%.3f", width="small"),
        "slg": st.column_config.NumberColumn("SLG", format="%.3f", width="small"),
        "ops": st.column_config.NumberColumn("OPS", format="%.3f", width="small"),
    }


def _advanced_history_column_config() -> dict[str, st.column_config.Column]:
    return {
        "season_label": st.column_config.TextColumn("Season", width="small"),
        "pa": st.column_config.NumberColumn("PA", format="%d", width="small"),
        "iso": st.column_config.NumberColumn("ISO", format="%.3f", width="small"),
        "xbh_rate": st.column_config.NumberColumn("XBH Rate", format="%.3f", width="small"),
        "hr_rate": st.column_config.NumberColumn("HR Rate", format="%.3f", width="small"),
        "tb_per_pa": st.column_config.NumberColumn("TB / PA", format="%.3f", width="small"),
        "team_relative_ops": st.column_config.NumberColumn("Team OPS+", format="%.0f", width="small"),
        "raa": st.column_config.NumberColumn("RAA", format="%.2f", width="small"),
        "rar": st.column_config.NumberColumn("RAR", format="%.2f", width="small"),
        "owar": st.column_config.NumberColumn("oWAR", format="%.2f", width="small"),
        "archetype": st.column_config.TextColumn("Archetype", width="medium"),
    }


def _milestone_table(dataframe: pd.DataFrame, *, include_next: bool) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    display = dataframe.copy()
    display["Current"] = display["current_total"].fillna(0).astype(int)
    display["Stat"] = display["stat"]
    if "next_milestone_display" in display.columns:
        display["Next"] = display["next_milestone_display"]
    if "remaining" in display.columns:
        display["Remaining"] = display["remaining"].fillna("-")
    if "club_label" in display.columns:
        display["Club"] = display["club_label"]
    if "highest_cleared_milestone" in display.columns:
        display["Highest Cleared"] = display["highest_cleared_milestone"].fillna("-")
    ordered = ["Stat", "Current"]
    if include_next:
        ordered.extend(["Next", "Remaining", "Club"])
    else:
        ordered.extend(["Highest Cleared", "Club"])
    return display[[column for column in ordered if column in display.columns]]


def _record_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe
    display = dataframe.copy()
    if "season_label" in display.columns:
        display["Season"] = display["season_label"].replace("", "-")
    elif "season" in display.columns:
        display["Season"] = display["season"].replace("", "-")
    display["Scope"] = display["scope"]
    display["Stat"] = display["stat"]
    display["Rank"] = display["rank"]
    display["Value"] = display["value_display"]
    return display[[column for column in ["Scope", "Stat", "Rank", "Value", "Season"] if column in display.columns]]


def _render_player_picker(identities: pd.DataFrame, selected_canonical: str | None) -> str:
    options = identities["canonical_name"].astype(str).tolist()
    labels = {
        str(row["canonical_name"]): str(row["preferred_display_name"])
        for _, row in identities.iterrows()
    }
    selected_value = selected_canonical if selected_canonical in labels else (options[0] if options else "")
    picked = st.selectbox(
        "Jump to player",
        options=options,
        index=options.index(selected_value) if selected_value in options else 0,
        format_func=lambda value: labels.get(str(value), str(value)),
        key="player_card_picker",
    )
    return str(picked)


_inject_player_card_css()
ensure_authenticated()
layout = get_responsive_layout_context(key="player_card")

st.title("Player Card")
st.caption("Full player hub with career snapshot, season-by-season stats, milestones, records, and trend context.")

db_path = database_path_control(DEFAULT_DB_PATH, key="player_card_db_path")
connection = get_db_connection(db_path, get_connection_cache_key())
identities = fetch_player_identities(connection)

if identities.empty:
    st.info("No player identities are available yet.")
    st.stop()

player_query = str(st.query_params.get("player", "")).strip()
available_players = set(identities["canonical_name"].astype(str).tolist())

utility_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.8], gap="small")
with utility_cols[0]:
    st.page_link("pages/1_Current_Season_Stats.py", label="Season Stats")
with utility_cols[1]:
    st.page_link("pages/2_All_Time_Career_Stats.py", label="Career Stats")
with utility_cols[2]:
    st.page_link("pages/7_Advanced_Analytics.py", label="Advanced Analytics")
with utility_cols[3]:
    st.page_link("pages/6_Milestones.py", label="Milestones")
with utility_cols[4]:
    picked_player = _render_player_picker(identities, player_query if player_query in available_players else None)

if player_query in available_players and picked_player and picked_player != player_query:
    _set_player_query_param(picked_player)
    st.rerun()

if not player_query:
    st.info("Choose a player to open their full player card.")
    if st.button("Open player card", key="open_player_card_empty"):
        _set_player_query_param(picked_player)
        st.rerun()
    st.stop()

if player_query not in available_players:
    st.warning("Player not found. Choose a player from the selector to continue.")
    if st.button("Open selected player", key="open_player_card_invalid"):
        _set_player_query_param(picked_player)
        st.rerun()
    st.stop()

summary = fetch_player_profile_summary(connection, player_query)
season_history = fetch_player_season_history(connection, player_query)
advanced_history = fetch_player_advanced_history(connection, player_query)
milestone_context = fetch_player_milestone_context(connection, player_query)
record_context = fetch_player_record_context(connection, player_query)

if summary is None:
    st.warning("Player not found. Choose a player from the selector to continue.")
    st.stop()

_render_header(summary)

rank_bits = [
    f"OPS rank: #{summary['ops_rank']}" if summary.get("ops_rank") else "",
    f"HR rank: #{summary['hr_rank']}" if summary.get("hr_rank") else "",
    f"RBI rank: #{summary['rbi_rank']}" if summary.get("rbi_rank") else "",
    f"Hits rank: #{summary['hits_rank']}" if summary.get("hits_rank") else "",
]
rank_bits = [bit for bit in rank_bits if bit]
if rank_bits:
    st.markdown(
        "<div class='player-section-note'>All-time rank callouts: " + escape(" • ".join(rank_bits)) + "</div>",
        unsafe_allow_html=True,
    )

_metric_grid(
    [
        ("Seasons", str(int(summary.get("seasons_played") or 0))),
        ("Games", str(int(summary.get("games") or 0))),
        ("PA", str(int(summary.get("pa") or 0))),
        ("Hits", str(int(summary.get("hits") or 0))),
        ("HR", str(int(summary.get("hr") or 0))),
        ("RBI", str(int(summary.get("rbi") or 0))),
        ("Runs", str(int(summary.get("runs") or 0))),
        ("AVG", f"{float(summary.get('avg') or 0.0):.3f}"),
        ("OBP", f"{float(summary.get('obp') or 0.0):.3f}"),
        ("SLG", f"{float(summary.get('slg') or 0.0):.3f}"),
        ("OPS", f"{float(summary.get('ops') or 0.0):.3f}"),
    ],
    per_row=2 if layout.is_mobile_layout else 4,
)

st.markdown("### OPS Trend")
st.markdown(
    "<div class='player-section-note'>Season labels use the short format you requested, with Spring = Sp, Summer = S, and Fall = F.</div>",
    unsafe_allow_html=True,
)
_render_trend_chart(season_history)

overview_tab, standard_tab, advanced_tab, context_tab = st.tabs(
    ["Overview", "Season-by-Season Stats", "Advanced History", "Milestones & Records"]
)

with overview_tab:
    overview_cols = st.columns(1 if layout.is_mobile_layout else 2, gap="small")
    with overview_cols[0]:
        st.markdown("#### Upcoming Milestone Watch")
        st.markdown(
            "<div class='player-section-note'>Closest upcoming career milestones for this player.</div>",
            unsafe_allow_html=True,
        )
        upcoming = milestone_context["upcoming"]
        if upcoming.empty:
            st.info("No upcoming milestone rows are available for this player.")
        else:
            st.dataframe(
                _milestone_table(upcoming, include_next=True),
                hide_index=True,
                use_container_width=True,
            )
    with overview_cols[-1]:
        st.markdown("#### Record Context")
        st.markdown(
            "<div class='player-section-note'>Career and single-season record placements already on the books.</div>",
            unsafe_allow_html=True,
        )
        placements = record_context["placements"]
        if placements.empty:
            st.info("No record placements were found for this player yet.")
        else:
            st.dataframe(
                _record_table(placements),
                hide_index=True,
                use_container_width=True,
            )

with standard_tab:
    st.markdown(
        "<div class='player-section-note'>Year-by-year standard batting line across every loaded season for this player.</div>",
        unsafe_allow_html=True,
    )
    if season_history.empty:
        st.info("No season-by-season batting history is available for this player.")
    else:
        standard_columns = [
            "season_label",
            "games",
            "pa",
            "ab",
            "hits",
            "1b",
            "2b",
            "3b",
            "hr",
            "bb",
            "r",
            "rbi",
            "tb",
            "avg",
            "obp",
            "slg",
            "ops",
        ]
        standard_display = season_history[[column for column in standard_columns if column in season_history.columns]]
        if layout.is_mobile_layout:
            _render_standard_mobile_cards(standard_display)
        else:
            st.dataframe(
                standard_display,
                hide_index=True,
                use_container_width=True,
                column_config=_standard_history_column_config(),
            )

with advanced_tab:
    st.markdown(
        "<div class='player-section-note'>Season-by-season advanced offense view using the same analytics formulas as the main Advanced Analytics page.</div>",
        unsafe_allow_html=True,
    )
    if advanced_history.empty:
        st.info("No advanced season history is available for this player yet.")
    else:
        advanced_columns = [
            "season_label",
            "pa",
            "iso",
            "xbh_rate",
            "hr_rate",
            "tb_per_pa",
            "team_relative_ops",
            "raa",
            "rar",
            "owar",
            "archetype",
        ]
        advanced_display = advanced_history[[column for column in advanced_columns if column in advanced_history.columns]]
        if layout.is_mobile_layout:
            _render_advanced_mobile_cards(advanced_display)
        else:
            st.dataframe(
                advanced_display,
                hide_index=True,
                use_container_width=True,
                column_config=_advanced_history_column_config(),
            )

with context_tab:
    context_cols = st.columns(1 if layout.is_mobile_layout else 2, gap="small")
    with context_cols[0]:
        st.markdown("#### Upcoming Milestones")
        upcoming = milestone_context["upcoming"]
        if upcoming.empty:
            st.info("No upcoming milestone rows are available for this player.")
        else:
            st.dataframe(
                _milestone_table(upcoming, include_next=True),
                hide_index=True,
                use_container_width=True,
            )

        st.markdown("#### Cleared Milestones")
        cleared = milestone_context["cleared"]
        if cleared.empty:
            st.info("No cleared milestone summary rows are available for this player.")
        else:
            st.dataframe(
                _milestone_table(cleared, include_next=False),
                hide_index=True,
                use_container_width=True,
            )

    with context_cols[-1]:
        st.markdown("#### Records Owned")
        owned_records = record_context["owned"]
        if owned_records.empty:
            st.info("This player does not currently own a tracked team record in the loaded data.")
        else:
            st.dataframe(
                _record_table(owned_records),
                hide_index=True,
                use_container_width=True,
            )

        st.markdown("#### Best Record Placements")
        placements = record_context["placements"]
        if placements.empty:
            st.info("No top-10 record placements were found for this player yet.")
        else:
            st.dataframe(
                _record_table(placements),
                hide_index=True,
                use_container_width=True,
            )
