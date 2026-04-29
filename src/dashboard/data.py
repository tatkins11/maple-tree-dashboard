from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.models.advanced_analytics import (
    ADVANCED_RUNS_PER_WIN,
    ARCHETYPE_DISPLAY_ORDER,
    REPLACEMENT_LEVEL_MIN_PA,
    REPLACEMENT_LEVEL_PERCENTILE,
    AdvancedAnalyticsMetadata,
    build_advanced_leaderboards,
    build_archetype_summary,
    build_player_comparison,
    calculate_advanced_analytics,
)
from src.models.lineup import build_simulation_lineup_from_order
from src.models.optimizer import OptimizationResult, optimize_lineup
from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_LEAGUE_RULES_PATH,
    load_league_rules,
    select_game_day_projections,
)
from src.models.schedule import DEFAULT_SCHEDULE_TEAM_NAME
from src.models.season_roster import fetch_active_roster_rows
from src.models.simulator import SimulationSummary, simulate_lineup
from src.utils.db import connect_app_db


DEFAULT_DB_PATH = Path("db/all_seasons_identity.sqlite")
DEFAULT_DASHBOARD_SEASON = "Maple Tree Spring 2026"
DEFAULT_STATS_SEASON = DEFAULT_DASHBOARD_SEASON
WRITEUP_EMPTY_OPPONENT_SCOUTING = "No completed opponent results are loaded yet, so this week starts the scouting baseline."
WRITEUP_INVALID_DOUBLEHEADER_MESSAGE = "Phase 1 write-up generation expects a two-game Maple Tree doubleheader for the selected week."
WRITEUP_BYE_WEEK_MESSAGE = "The selected week is a bye week, so phase 1 write-up generation is disabled."

COUNTING_RECORD_COLUMNS = {
    "Games": "games",
    "PA": "pa",
    "AB": "ab",
    "Hits": "hits",
    "Singles": "1b",
    "Doubles": "2b",
    "Triples": "3b",
    "HR": "hr",
    "RBI": "rbi",
    "Runs": "r",
    "Walks": "bb",
    "Total Bases": "tb",
}

RATE_RECORD_COLUMNS = {
    "AVG": "avg",
    "OBP": "obp",
    "SLG": "slg",
    "OPS": "ops",
}

SINGLE_GAME_RECORD_COLUMNS = {
    "PA": "pa",
    "AB": "ab",
    "Hits": "hits",
    "Singles": "1b",
    "Doubles": "2b",
    "Triples": "3b",
    "HR": "hr",
    "RBI": "rbi",
    "Runs": "r",
    "Walks": "bb",
    "Total Bases": "tb",
}

DISPLAY_COLUMN_LABELS = {
    "season": "Season",
    "game_date": "Date",
    "game_time": "Time",
    "team_name": "Team",
    "opponent": "Opponent",
    "player": "Player",
    "pa": "PA",
    "games": "Games",
    "ab": "AB",
    "hits": "Hits",
    "1b": "Singles",
    "2b": "Doubles",
    "3b": "Triples",
    "hr": "HR",
    "rbi": "RBI",
    "r": "Runs",
    "bb": "Walks",
    "so": "SO",
    "tb": "Total Bases",
    "lineup_spot": "Spot",
    "fc": "FC",
    "dp": "DP",
    "outs": "Outs",
    "avg": "AVG",
    "obp": "OBP",
    "slg": "SLG",
    "ops": "OPS",
}

MILESTONE_LADDERS = {
    "Games": (25, 50, 75, 100, 125, 150),
    "PA": (50, 100, 150, 200, 250, 300, 400, 500),
    "AB": (50, 100, 150, 200, 250, 300, 400),
    "Hits": (25, 50, 75, 100, 125, 150, 200),
    "Singles": (25, 50, 75, 100, 125),
    "Doubles": (10, 20, 30, 40, 50),
    "Triples": (5, 10, 15, 20),
    "HR": (5, 10, 15, 20, 25, 30, 40),
    "RBI": (25, 50, 75, 100, 125, 150),
    "Runs": (25, 50, 75, 100, 125, 150),
    "Walks": (10, 25, 50, 75, 100),
    "Total Bases": (50, 100, 150, 200, 250, 300),
}


def get_connection(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    return connect_app_db(Path(db_path))


def sort_seasons(seasons: list[str]) -> list[str]:
    season_rank = {"spring": 1, "summer": 2, "fall": 3, "winter": 4}

    def key(value: str) -> tuple[int, int, str]:
        year_match = re.search(r"(20\d{2})", value)
        year = int(year_match.group(1)) if year_match else 0
        lowered = value.lower()
        phase = 0
        for label, rank in season_rank.items():
            if label in lowered:
                phase = rank
                break
        return (year, phase, value.lower())

    return sorted(seasons, key=key, reverse=True)


def with_dashboard_default_season(seasons: list[str]) -> list[str]:
    ordered = sort_seasons(list(seasons))
    for preferred_season in (DEFAULT_STATS_SEASON, DEFAULT_DASHBOARD_SEASON):
        if preferred_season in ordered:
            return [
                preferred_season,
                *[season for season in ordered if season != preferred_season],
            ]
    return ordered


def dashboard_default_season_index(seasons: list[str]) -> int:
    for preferred_season in (DEFAULT_STATS_SEASON, DEFAULT_DASHBOARD_SEASON):
        if preferred_season in seasons:
            return seasons.index(preferred_season)
    return 0


def _compact_season_label(value: str) -> str:
    year_match = re.search(r"(20\d{2})", value)
    year = year_match.group(1) if year_match else ""
    lowered = value.lower()
    if "maple tree fall" in lowered:
        return f"Fall {year}".strip()
    if "maple tree tappers" in lowered:
        return f"Tappers {year}".strip()
    if "smoking bunts" in lowered:
        return f"Bunts {year}".strip()
    if "soviet sluggers" in lowered:
        return f"Sluggers {year}".strip()
    return value


def format_player_season_label(season: str) -> str:
    value = str(season or "").strip()
    if not value:
        return ""
    year_match = re.search(r"(20\d{2})", value)
    year = year_match.group(1) if year_match else ""
    lowered = value.lower()
    if "spring" in lowered:
        return f"{year} Sp".strip()
    if "summer" in lowered or "sluggers" in lowered or "bunts" in lowered or "tappers" in lowered:
        return f"{year} S".strip()
    if "fall" in lowered:
        return f"{year} F".strip()
    return value


def build_player_rank_highlights(
    summary: dict[str, object] | None,
    *,
    limit: int = 4,
) -> list[dict[str, object]]:
    if not summary:
        return []

    rank_fields = [
        ("hits_rank", "Hits"),
        ("hr_rank", "HR"),
        ("rbi_rank", "RBI"),
        ("ops_rank", "OPS"),
    ]
    highlights: list[dict[str, object]] = []
    for field_name, label in rank_fields:
        rank = summary.get(field_name)
        if rank in (None, "", 0):
            continue
        highlights.append({"stat": label, "rank": int(rank)})

    highlights.sort(key=lambda item: (int(item["rank"]), str(item["stat"])))
    return highlights[:limit]


def build_player_trend_history(
    season_history: pd.DataFrame,
    advanced_history: pd.DataFrame,
) -> pd.DataFrame:
    if season_history.empty and advanced_history.empty:
        return pd.DataFrame(
            columns=[
                "season",
                "season_label",
                "pa",
                "hr",
                "ops",
                "team_relative_ops",
                "owar",
                "rar",
                "iso",
            ]
        )

    standard_columns = ["season", "season_label", "pa", "hr", "ops"]
    advanced_columns = ["season", "season_label", "team_relative_ops", "owar", "rar", "iso"]

    standard_source = season_history[[column for column in standard_columns if column in season_history.columns]].copy()
    advanced_source = advanced_history[[column for column in advanced_columns if column in advanced_history.columns]].copy()

    if standard_source.empty:
        merged = advanced_source.copy()
    elif advanced_source.empty:
        merged = standard_source.copy()
    else:
        merged = standard_source.merge(
            advanced_source,
            on=["season", "season_label"],
            how="outer",
        )

    if "season_label" not in merged.columns and "season" in merged.columns:
        merged.loc[:, "season_label"] = merged["season"].map(lambda value: format_player_season_label(str(value)))

    if "season" in merged.columns:
        chronological = list(reversed(sort_seasons(merged["season"].astype(str).dropna().tolist())))
        order_lookup = {season: index for index, season in enumerate(chronological)}
        merged = merged.assign(
            _season_order=merged["season"].astype(str).map(lambda value: order_lookup.get(value, 10**9))
        ).sort_values(["_season_order", "season_label"], ascending=[True, True]).drop(columns="_season_order")

    return merged.reset_index(drop=True)


def fetch_seasons(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT DISTINCT season FROM season_batting_stats WHERE season <> ''"
    ).fetchall()
    return sort_seasons([str(row["season"]) for row in rows])


def fetch_projection_seasons(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT DISTINCT projection_season FROM hitter_projections WHERE projection_season <> ''"
    ).fetchall()
    return sort_seasons([str(row["projection_season"]) for row in rows])


def fetch_team_summary(connection: sqlite3.Connection, season: str) -> dict[str, float | int]:
    row = connection.execute(
        """
        SELECT
            COALESCE(MAX(games), 0) AS team_games,
            COUNT(DISTINCT player_id) AS hitters,
            COALESCE(SUM(plate_appearances), 0) AS plate_appearances,
            COALESCE(SUM(at_bats), 0) AS at_bats,
            COALESCE(SUM(hits), 0) AS hits,
            COALESCE(SUM(home_runs), 0) AS home_runs,
            COALESCE(SUM(runs), 0) AS runs,
            COALESCE(SUM(rbi), 0) AS rbi,
            COALESCE(SUM(walks), 0) AS walks,
            COALESCE(SUM(sacrifice_flies), 0) AS sacrifice_flies,
            COALESCE(SUM(total_bases), 0) AS total_bases
        FROM season_batting_stats
        WHERE season = ?
        """,
        (season,),
    ).fetchone()
    at_bats = int(row["at_bats"])
    hits = int(row["hits"])
    walks = int(row["walks"])
    sf = int(row["sacrifice_flies"])
    total_bases = int(row["total_bases"])
    obp_denom = at_bats + walks + sf
    return {
        "team_games": int(row["team_games"]),
        "hitters": int(row["hitters"]),
        "plate_appearances": int(row["plate_appearances"]),
        "runs": int(row["runs"]),
        "home_runs": int(row["home_runs"]),
        "rbi": int(row["rbi"]),
        "avg": _safe_divide(hits, at_bats),
        "obp": _safe_divide(hits + walks, obp_denom),
        "slg": _safe_divide(total_bases, at_bats),
        "ops": _safe_divide(hits + walks, obp_denom) + _safe_divide(total_bases, at_bats),
    }


def fetch_current_season_stats(
    connection: sqlite3.Connection,
    season: str,
    include_projections: bool = False,
    projection_season: str | None = None,
) -> pd.DataFrame:
    dataframe = pd.read_sql_query(
        """
        SELECT
            pm.preferred_display_name AS player,
            pi.canonical_name,
            s.games,
            s.plate_appearances AS pa,
            s.at_bats AS ab,
            s.hits,
            s.singles AS "1b",
            s.doubles AS "2b",
            s.triples AS "3b",
            s.home_runs AS hr,
            s.walks AS bb,
            s.runs AS r,
            s.rbi,
            s.total_bases AS tb,
            s.reached_on_error AS roe,
            s.fielder_choice AS fc,
            s.grounded_into_double_play AS gidp,
            s.batting_average AS avg,
            s.on_base_percentage AS obp,
            s.slugging_percentage AS slg,
            s.ops
        FROM season_batting_stats s
        JOIN player_identity pi ON pi.player_id = s.player_id
        JOIN player_metadata pm ON pm.player_id = s.player_id
        WHERE s.season = ?
        ORDER BY s.ops DESC, LOWER(pm.preferred_display_name)
        """,
        connection,
        params=(season,),
    )
    if include_projections and projection_season:
        projection_df = pd.read_sql_query(
            """
            SELECT
                pm.preferred_display_name AS player,
                hp.projection_source,
                hp.projected_on_base_rate AS proj_obp,
                hp.projected_total_base_rate AS proj_tb_rate,
                hp.projected_extra_base_hit_rate AS proj_xbh_rate,
                hp.p_home_run AS proj_hr_rate,
                hp.current_season_weight,
                hp.weighted_prior_plate_appearances
            FROM hitter_projections hp
            JOIN player_metadata pm ON pm.player_id = hp.player_id
            WHERE hp.projection_season = ?
            """,
            connection,
            params=(projection_season,),
        )
        dataframe = dataframe.merge(projection_df, on="player", how="left")
    return dataframe


def fetch_lineup_current_season_context(
    connection: sqlite3.Connection,
    *,
    season: str,
    ordered_player_names: list[str],
) -> dict[str, object]:
    player_names = [str(name).strip() for name in ordered_player_names if str(name).strip()]
    empty_summary = {
        "pa": 0,
        "runs": 0,
        "rbi": 0,
        "home_runs": 0,
        "avg": 0.0,
        "obp": 0.0,
        "slg": 0.0,
        "ops": 0.0,
    }
    if not player_names:
        return {"player_metrics": {}, "summary": empty_summary}

    season_stats = fetch_current_season_stats(connection, season)
    if season_stats.empty:
        return {"player_metrics": {}, "summary": empty_summary}

    tracked_columns = ["player", "pa", "ab", "hits", "bb", "tb", "r", "rbi", "hr", "avg", "obp", "slg", "ops"]
    filtered = season_stats[[column for column in tracked_columns if column in season_stats.columns]].copy()
    filtered = filtered.drop_duplicates(subset=["player"], keep="first").set_index("player")
    ordered = filtered.reindex(player_names)

    counting_columns = [column for column in ("pa", "ab", "hits", "bb", "tb", "r", "rbi", "hr") if column in ordered.columns]
    for column in counting_columns:
        ordered.loc[:, column] = pd.to_numeric(ordered[column], errors="coerce").fillna(0)

    rate_columns = [column for column in ("avg", "obp", "slg", "ops") if column in ordered.columns]
    for column in rate_columns:
        ordered.loc[:, column] = pd.to_numeric(ordered[column], errors="coerce").fillna(0.0)

    player_metrics = {
        str(player_name): {
            "pa": int(row.get("pa", 0) or 0),
            "r": int(row.get("r", 0) or 0),
            "rbi": int(row.get("rbi", 0) or 0),
            "hr": int(row.get("hr", 0) or 0),
            "avg": float(row.get("avg", 0.0) or 0.0),
            "obp": float(row.get("obp", 0.0) or 0.0),
            "slg": float(row.get("slg", 0.0) or 0.0),
            "ops": float(row.get("ops", 0.0) or 0.0),
        }
        for player_name, row in ordered.iterrows()
    }

    total_pa = float(ordered["pa"].sum()) if "pa" in ordered.columns else 0.0
    total_ab = float(ordered["ab"].sum()) if "ab" in ordered.columns else 0.0
    total_hits = float(ordered["hits"].sum()) if "hits" in ordered.columns else 0.0
    total_walks = float(ordered["bb"].sum()) if "bb" in ordered.columns else 0.0
    total_bases = float(ordered["tb"].sum()) if "tb" in ordered.columns else 0.0

    summary = {
        "pa": int(total_pa),
        "runs": int(ordered["r"].sum()) if "r" in ordered.columns else 0,
        "rbi": int(ordered["rbi"].sum()) if "rbi" in ordered.columns else 0,
        "home_runs": int(ordered["hr"].sum()) if "hr" in ordered.columns else 0,
        "avg": _safe_divide(total_hits, total_ab),
        "obp": _safe_divide(total_hits + total_walks, total_ab + total_walks),
        "slg": _safe_divide(total_bases, total_ab),
    }
    summary["ops"] = float(summary["obp"]) + float(summary["slg"])
    return {"player_metrics": player_metrics, "summary": summary}


def fetch_top_hitters(
    connection: sqlite3.Connection,
    season: str,
    min_pa: int = 20,
    limit: int = 6,
) -> pd.DataFrame:
    dataframe = fetch_current_season_stats(connection, season)
    filtered = dataframe[dataframe["pa"] >= min_pa].copy()
    filtered = filtered.sort_values(["ops", "obp", "slg"], ascending=False)
    return filtered.head(limit)[["player", "canonical_name", "pa", "hr", "r", "rbi", "avg", "obp", "slg", "ops"]]


def fetch_current_season_leader_snapshot(
    connection: sqlite3.Connection,
    season: str,
) -> dict[str, str]:
    dataframe = fetch_current_season_stats(connection, season)
    if dataframe.empty:
        return {
            "ops_leader": "",
            "hr_leader": "",
            "rbi_leader": "",
            "avg_leader": "",
        }

    ordered = dataframe.copy()
    return {
        "ops_leader": _format_leader_label(ordered, sort_columns=["ops", "obp", "slg"], value_column="ops", label="OPS", value_format=".3f"),
        "hr_leader": _format_leader_label(ordered, sort_columns=["hr", "rbi", "ops"], value_column="hr", label="HR", value_format=".0f"),
        "rbi_leader": _format_leader_label(ordered, sort_columns=["rbi", "hr", "ops"], value_column="rbi", label="RBI", value_format=".0f"),
        "avg_leader": _format_leader_label(ordered, sort_columns=["avg", "ops", "obp"], value_column="avg", label="AVG", value_format=".3f"),
    }


def fetch_career_stats(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
    min_pa: int = 0,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if seasons:
        placeholders = ",".join("?" for _ in seasons)
        where_clause = f"WHERE s.season IN ({placeholders})"
        params.extend(seasons)

    dataframe = pd.read_sql_query(
        f"""
        SELECT
            pm.preferred_display_name AS player,
            pi.canonical_name,
            COUNT(DISTINCT s.season) AS seasons_played,
            SUM(s.games) AS games,
            SUM(s.plate_appearances) AS pa,
            SUM(s.at_bats) AS ab,
            SUM(s.hits) AS hits,
            SUM(s.singles) AS "1b",
            SUM(s.doubles) AS "2b",
            SUM(s.triples) AS "3b",
            SUM(s.home_runs) AS hr,
            SUM(s.walks) AS bb,
            SUM(s.runs) AS r,
            SUM(s.rbi) AS rbi,
            SUM(s.total_bases) AS tb,
            SUM(s.sacrifice_flies) AS sf
        FROM season_batting_stats s
        JOIN player_identity pi ON pi.player_id = s.player_id
        JOIN player_metadata pm ON pm.player_id = s.player_id
        {where_clause}
        GROUP BY pm.preferred_display_name, pi.canonical_name
        """,
        connection,
        params=params,
    )
    if dataframe.empty:
        return dataframe

    dataframe = dataframe.assign(
        avg=dataframe.apply(lambda row: _safe_divide(row["hits"], row["ab"]), axis=1),
        obp=dataframe.apply(
            lambda row: _safe_divide(
                row["hits"] + row["bb"],
                row["ab"] + row["bb"] + row["sf"],
            ),
            axis=1,
        ),
        slg=dataframe.apply(lambda row: _safe_divide(row["tb"], row["ab"]), axis=1),
    )
    dataframe = dataframe.assign(ops=dataframe["obp"] + dataframe["slg"])
    filtered = dataframe[dataframe["pa"] >= min_pa].copy()
    filtered = filtered.sort_values(["ops", "pa"], ascending=[False, False])
    return filtered


def fetch_career_summary(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
    min_pa: int = 0,
) -> dict[str, float | int]:
    career = fetch_career_stats(connection, seasons=seasons, min_pa=min_pa)
    selected_season_count = len(seasons) if seasons else len(fetch_seasons(connection))
    if career.empty:
        return {
            "players": 0,
            "seasons": selected_season_count,
            "pa": 0,
            "runs": 0,
            "home_runs": 0,
            "avg": 0.0,
            "obp": 0.0,
            "slg": 0.0,
            "ops": 0.0,
        }

    hits = float(career["hits"].sum())
    at_bats = float(career["ab"].sum())
    walks = float(career["bb"].sum())
    sacrifice_flies = float(career["sf"].sum()) if "sf" in career.columns else 0.0
    total_bases = float(career["tb"].sum())

    avg = _safe_divide(hits, at_bats)
    obp = _safe_divide(hits + walks, at_bats + walks + sacrifice_flies)
    slg = _safe_divide(total_bases, at_bats)

    return {
        "players": int(len(career)),
        "seasons": selected_season_count,
        "pa": int(career["pa"].sum()),
        "runs": int(career["r"].sum()),
        "home_runs": int(career["hr"].sum()),
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": obp + slg,
    }


def fetch_career_leader_snapshot(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
    min_pa: int = 0,
) -> dict[str, str]:
    career = fetch_career_stats(connection, seasons=seasons, min_pa=min_pa)
    if career.empty:
        return {
            "ops_leader": "",
            "hr_leader": "",
            "rbi_leader": "",
            "avg_leader": "",
            "most_seasons": "",
        }

    ordered = career.copy()
    most_seasons_df = ordered.sort_values(["seasons_played", "pa", "ops", "player"], ascending=[False, False, False, True]).reset_index(drop=True)
    most_seasons_row = most_seasons_df.iloc[0]

    return {
        "ops_leader": _format_leader_label(ordered, sort_columns=["ops", "obp", "slg"], value_column="ops", label="OPS", value_format=".3f"),
        "hr_leader": _format_leader_label(ordered, sort_columns=["hr", "rbi", "ops"], value_column="hr", label="HR", value_format=".0f"),
        "rbi_leader": _format_leader_label(ordered, sort_columns=["rbi", "hr", "ops"], value_column="rbi", label="RBI", value_format=".0f"),
        "avg_leader": _format_leader_label(ordered, sort_columns=["avg", "ops", "obp"], value_column="avg", label="AVG", value_format=".3f"),
        "most_seasons": f"{most_seasons_row['player']} (Seasons {int(most_seasons_row['seasons_played'])})",
    }


def fetch_all_time_leaders(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
    min_pa: int = 0,
) -> dict[str, pd.DataFrame]:
    career = fetch_career_stats(connection, seasons=seasons, min_pa=min_pa)
    if career.empty:
        return {}
    return {
        "OPS": career.sort_values(["ops", "pa"], ascending=[False, False]).head(10)[["player", "pa", "ops"]],
        "AVG": career.sort_values(["avg", "pa"], ascending=[False, False]).head(10)[["player", "pa", "avg"]],
        "OBP": career.sort_values(["obp", "pa"], ascending=[False, False]).head(10)[["player", "pa", "obp"]],
        "HR": career.sort_values(["hr", "pa"], ascending=[False, False]).head(10)[["player", "hr", "pa"]],
        "RBI": career.sort_values(["rbi", "pa"], ascending=[False, False]).head(10)[["player", "rbi", "pa"]],
    }


def fetch_single_season_stats(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
    min_pa: int = 0,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if seasons:
        placeholders = ",".join("?" for _ in seasons)
        where_clause = f"WHERE s.season IN ({placeholders})"
        params.extend(seasons)

    dataframe = pd.read_sql_query(
        f"""
        SELECT
            s.season,
            pm.preferred_display_name AS player,
            pi.canonical_name,
            s.games,
            s.plate_appearances AS pa,
            s.at_bats AS ab,
            s.hits,
            s.singles AS "1b",
            s.doubles AS "2b",
            s.triples AS "3b",
            s.home_runs AS hr,
            s.walks AS bb,
            s.runs AS r,
            s.rbi,
            s.total_bases AS tb,
            s.sacrifice_flies AS sf,
            s.batting_average AS avg,
            s.on_base_percentage AS obp,
            s.slugging_percentage AS slg,
            s.ops
        FROM season_batting_stats s
        JOIN player_identity pi ON pi.player_id = s.player_id
        JOIN player_metadata pm ON pm.player_id = s.player_id
        {where_clause}
        ORDER BY s.season DESC, s.ops DESC, LOWER(pm.preferred_display_name)
        """,
        connection,
        params=params,
    )
    if dataframe.empty:
        return dataframe
    return dataframe[dataframe["pa"] >= min_pa].copy()


def fetch_single_game_stats(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
    min_pa: int = 0,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if seasons:
        placeholders = ",".join("?" for _ in seasons)
        where_clause = f"WHERE g.season IN ({placeholders})"
        params.extend(seasons)

    dataframe = pd.read_sql_query(
        f"""
        SELECT
            g.game_date,
            g.game_time,
            g.team_name,
            g.opponent_name AS opponent,
            g.season,
            pm.preferred_display_name AS player,
            pi.canonical_name,
            pg.lineup_spot,
            pg.plate_appearances AS pa,
            pg.at_bats AS ab,
            (pg.singles + pg.doubles + pg.triples + pg.home_runs) AS hits,
            pg.singles AS "1b",
            pg.doubles AS "2b",
            pg.triples AS "3b",
            pg.home_runs AS hr,
            pg.walks AS bb,
            pg.strikeouts AS so,
            pg.runs AS r,
            pg.rbi,
            (pg.singles + (2 * pg.doubles) + (3 * pg.triples) + (4 * pg.home_runs)) AS tb,
            pg.sacrifice_flies AS sf,
            pg.fielder_choice AS fc,
            pg.double_plays AS dp,
            pg.outs,
            pg.raw_scorebook_file
        FROM player_game_batting pg
        JOIN games g ON g.game_id = pg.game_id
        JOIN player_identity pi ON pi.player_id = pg.player_id
        JOIN player_metadata pm ON pm.player_id = pg.player_id
        {where_clause}
        ORDER BY g.game_date DESC, COALESCE(g.game_time, '') DESC, LOWER(g.opponent_name), pg.lineup_spot, LOWER(pm.preferred_display_name)
        """,
        connection,
        params=params,
    )
    if dataframe.empty:
        return dataframe

    for column_name in ["game_date", "game_time", "team_name", "opponent", "season", "player", "canonical_name", "raw_scorebook_file"]:
        if column_name in dataframe.columns:
            dataframe.loc[:, column_name] = dataframe[column_name].map(
                lambda value: "" if value is None or pd.isna(value) else str(value)
            )

    dataframe = dataframe.assign(
        avg=dataframe.apply(lambda row: _safe_divide(row["hits"], row["ab"]), axis=1),
        obp=dataframe.apply(
            lambda row: _safe_divide(
                row["hits"] + row["bb"],
                row["ab"] + row["bb"] + row["sf"],
            ),
            axis=1,
        ),
        slg=dataframe.apply(lambda row: _safe_divide(row["tb"], row["ab"]), axis=1),
    )
    dataframe = dataframe.assign(
        ops=dataframe["obp"] + dataframe["slg"],
        season_label=dataframe["season"].map(lambda value: format_player_season_label(str(value))),
    )
    return dataframe[dataframe["pa"] >= min_pa].copy()


def fetch_advanced_analytics_view(
    connection: sqlite3.Connection,
    *,
    view_mode: str,
    selected_season: str | None = None,
    selected_seasons: list[str] | None = None,
    min_pa: int = 0,
    active_only: bool = False,
) -> tuple[pd.DataFrame, AdvancedAnalyticsMetadata]:
    if view_mode == "Season":
        if not selected_season:
            raise ValueError("selected_season is required for Season mode")
        source = _fetch_advanced_season_source(connection, selected_season)
        comparison_label = selected_season
    elif view_mode == "Career":
        source = _fetch_advanced_career_source(connection, selected_seasons)
        comparison_label = "All selected seasons"
    else:
        raise ValueError(f"Unsupported analytics view mode: {view_mode}")

    if source.empty:
        empty_metadata = AdvancedAnalyticsMetadata(
            mode=view_mode,
            comparison_group_label=comparison_label,
            baseline_player_count=0,
            average_offensive_run_rate=0.0,
            replacement_offensive_run_rate=0.0,
            replacement_percentile=REPLACEMENT_LEVEL_PERCENTILE,
            replacement_min_pa=max(REPLACEMENT_LEVEL_MIN_PA, min_pa),
            runs_per_win=ADVANCED_RUNS_PER_WIN,
        )
        return source, empty_metadata

    if active_only:
        active_names = set(_fetch_active_roster_names(connection))
        source = source[source["player"].isin(active_names)].copy()
        if source.empty:
            empty_metadata = AdvancedAnalyticsMetadata(
                mode=view_mode,
                comparison_group_label=comparison_label,
                baseline_player_count=0,
                average_offensive_run_rate=0.0,
                replacement_offensive_run_rate=0.0,
                replacement_percentile=REPLACEMENT_LEVEL_PERCENTILE,
                replacement_min_pa=max(REPLACEMENT_LEVEL_MIN_PA, min_pa),
                runs_per_win=ADVANCED_RUNS_PER_WIN,
            )
            return source, empty_metadata

    comparison_source = source[source["pa"] >= max(min_pa, 1)].copy()
    if comparison_source.empty:
        comparison_source = source.copy()

    analytics, metadata = calculate_advanced_analytics(
        source,
        comparison_dataframe=comparison_source,
        mode=view_mode,
        comparison_group_label=comparison_label,
        replacement_percentile=REPLACEMENT_LEVEL_PERCENTILE,
        replacement_min_pa=max(REPLACEMENT_LEVEL_MIN_PA, min_pa),
        runs_per_win=ADVANCED_RUNS_PER_WIN,
    )
    filtered = analytics[analytics["pa"] >= min_pa].copy()
    filtered = filtered.sort_values(["rar", "team_relative_ops", "pa"], ascending=[False, False, False]).reset_index(drop=True)
    return filtered, metadata


def fetch_advanced_analytics_leaderboards(dataframe: pd.DataFrame, limit: int = 5) -> dict[str, pd.DataFrame]:
    return build_advanced_leaderboards(dataframe, limit=limit)


def fetch_advanced_analytics_archetype_summary(dataframe: pd.DataFrame) -> pd.DataFrame:
    return build_archetype_summary(dataframe)


def fetch_advanced_player_comparison(dataframe: pd.DataFrame, players: list[str]) -> pd.DataFrame:
    return build_player_comparison(dataframe, players)


def fetch_advanced_methodology_summary(metadata: AdvancedAnalyticsMetadata) -> dict[str, str]:
    return {
        "Comparison group": metadata.comparison_group_label,
        "Baseline hitters": str(metadata.baseline_player_count),
        "Replacement level": f"{int(metadata.replacement_percentile * 100)}th percentile offense rate (min {metadata.replacement_min_pa} PA)",
        "Runs per win": f"{metadata.runs_per_win:.1f}",
        "Model scope": "Offense-only, team-specific RAA / RAR / oWAR",
    }


def fetch_advanced_archetype_order() -> list[str]:
    return ARCHETYPE_DISPLAY_ORDER.copy()


def _sort_player_history_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "season" not in dataframe.columns:
        return dataframe
    order = {season: index for index, season in enumerate(sort_seasons(dataframe["season"].astype(str).unique().tolist()))}
    return dataframe.assign(_season_sort=dataframe["season"].map(order)).sort_values(
        ["_season_sort", "pa", "ops"],
        ascending=[True, False, False],
    ).drop(columns=["_season_sort"]).reset_index(drop=True)


def _sort_player_game_log_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "game_date" not in dataframe.columns:
        return dataframe

    ordered = dataframe.assign(_game_date_sort=pd.to_datetime(dataframe["game_date"], errors="coerce"))
    sort_columns = ["_game_date_sort"]
    ascending = [False]
    for column_name, is_ascending in (("game_time", False), ("season", False), ("opponent", True), ("lineup_spot", True)):
        if column_name in ordered.columns:
            sort_columns.append(column_name)
            ascending.append(is_ascending)
    return ordered.sort_values(sort_columns, ascending=ascending).drop(columns=["_game_date_sort"]).reset_index(drop=True)


def fetch_player_profile_summary(
    connection: sqlite3.Connection,
    canonical_name: str,
) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT
            pi.player_id,
            pm.preferred_display_name AS player,
            pi.canonical_name,
            pm.is_fixed_dhh,
            pm.baserunning_grade,
            pm.consistency_grade,
            pm.speed_flag,
            pm.active_flag,
            COALESCE(pm.notes, '') AS notes
        FROM player_identity pi
        JOIN player_metadata pm ON pm.player_id = pi.player_id
        WHERE pi.canonical_name = ?
        """,
        (canonical_name,),
    ).fetchone()
    if row is None:
        return None

    career = fetch_career_stats(connection, min_pa=0)
    player_career = career[career["canonical_name"] == canonical_name].copy()
    active_roster = fetch_active_roster(connection)
    active_roster_names = set(active_roster["canonical_name"].astype(str).tolist()) if not active_roster.empty else set()
    aliases = fetch_player_aliases(connection)
    alias_rows = aliases[aliases["canonical_name"] == canonical_name].copy() if not aliases.empty else pd.DataFrame()

    summary: dict[str, object] = {
        "player_id": int(row["player_id"]),
        "player": str(row["player"]),
        "canonical_name": str(row["canonical_name"]),
        "is_fixed_dhh": bool(row["is_fixed_dhh"]),
        "baserunning_grade": str(row["baserunning_grade"] or ""),
        "consistency_grade": str(row["consistency_grade"] or ""),
        "speed_flag": bool(row["speed_flag"]),
        "active_flag": bool(row["active_flag"]),
        "active_roster": str(row["canonical_name"]) in active_roster_names,
        "notes": str(row["notes"] or "").strip(),
        "aliases": sorted(alias_rows["source_name"].astype(str).unique().tolist()) if not alias_rows.empty else [],
    }

    if player_career.empty:
        summary.update(
            {
                "seasons_played": 0,
                "games": 0,
                "pa": 0,
                "hits": 0,
                "hr": 0,
                "rbi": 0,
                "runs": 0,
                "avg": 0.0,
                "obp": 0.0,
                "slg": 0.0,
                "ops": 0.0,
                "ops_rank": None,
                "hr_rank": None,
                "rbi_rank": None,
                "hits_rank": None,
            }
        )
        return summary

    player_row = player_career.iloc[0]

    def _rank_for(sort_columns: list[str]) -> int | None:
        ordered = player_career if sort_columns[0] == "player" else career.sort_values(sort_columns + ["player"], ascending=[False] * len(sort_columns) + [True]).reset_index(drop=True)
        matches = ordered.index[ordered["canonical_name"] == canonical_name].tolist()
        return int(matches[0] + 1) if matches else None

    summary.update(
        {
            "seasons_played": int(player_row["seasons_played"]),
            "games": int(player_row["games"]),
            "pa": int(player_row["pa"]),
            "hits": int(player_row["hits"]),
            "hr": int(player_row["hr"]),
            "rbi": int(player_row["rbi"]),
            "runs": int(player_row["r"]),
            "avg": float(player_row["avg"]),
            "obp": float(player_row["obp"]),
            "slg": float(player_row["slg"]),
            "ops": float(player_row["ops"]),
            "ops_rank": _rank_for(["ops", "pa"]),
            "hr_rank": _rank_for(["hr", "pa"]),
            "rbi_rank": _rank_for(["rbi", "pa"]),
            "hits_rank": _rank_for(["hits", "pa"]),
        }
    )
    return summary


def fetch_player_season_history(
    connection: sqlite3.Connection,
    canonical_name: str,
) -> pd.DataFrame:
    history = fetch_single_season_stats(connection, min_pa=0)
    if history.empty:
        return history
    filtered = history[history["canonical_name"] == canonical_name].copy()
    if filtered.empty:
        return filtered
    filtered.loc[:, "season_label"] = filtered["season"].map(lambda value: format_player_season_label(str(value)))
    return _sort_player_history_rows(filtered)


def fetch_player_advanced_history(
    connection: sqlite3.Connection,
    canonical_name: str,
) -> pd.DataFrame:
    season_history = fetch_player_season_history(connection, canonical_name)
    if season_history.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for season in season_history["season"].astype(str).tolist():
        analytics, _ = fetch_advanced_analytics_view(
            connection,
            view_mode="Season",
            selected_season=season,
            min_pa=0,
            active_only=False,
        )
        if analytics.empty:
            continue
        player_rows = analytics[analytics["canonical_name"] == canonical_name].copy()
        if player_rows.empty:
            continue
        row = player_rows.iloc[0].to_dict()
        row["season"] = season
        row["season_label"] = format_player_season_label(season)
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    return _sort_player_history_rows(pd.DataFrame(rows))


def fetch_player_game_log(
    connection: sqlite3.Connection,
    canonical_name: str,
) -> pd.DataFrame:
    game_log = fetch_single_game_stats(connection, min_pa=0)
    if game_log.empty:
        return game_log
    filtered = game_log[game_log["canonical_name"] == canonical_name].copy()
    if filtered.empty:
        return filtered
    return _sort_player_game_log_rows(filtered)


def fetch_player_milestone_context(
    connection: sqlite3.Connection,
    canonical_name: str,
) -> dict[str, pd.DataFrame]:
    upcoming = fetch_career_milestones(
        connection,
        active_only=False,
        min_current_total=0,
        sort_by="nearest milestone",
    )
    cleared = fetch_passed_milestones_summary(
        connection,
        active_only=False,
        min_current_total=0,
        limit=100,
    )
    upcoming_rows = upcoming[upcoming["canonical_name"] == canonical_name].copy() if not upcoming.empty else pd.DataFrame()
    cleared_rows = cleared[cleared["canonical_name"] == canonical_name].copy() if not cleared.empty else pd.DataFrame()
    if not upcoming_rows.empty:
        upcoming_rows = upcoming_rows.sort_values(
            ["remaining", "club_size", "next_milestone_sort", "stat"],
            ascending=[True, True, False, True],
        ).reset_index(drop=True)
    if not cleared_rows.empty:
        cleared_rows = cleared_rows.sort_values(
            ["stat", "highest_cleared_milestone", "current_total"],
            ascending=[True, False, False],
        ).reset_index(drop=True)
    return {
        "upcoming": upcoming_rows.head(8),
        "cleared": cleared_rows.head(12),
    }


def fetch_player_record_context(
    connection: sqlite3.Connection,
    canonical_name: str,
) -> dict[str, pd.DataFrame]:
    placement_rows: list[dict[str, object]] = []
    owned_rows: list[dict[str, object]] = []
    for scope_label, scope_key in (
        ("Career", "career"),
        ("Single Season", "single_season"),
        ("Single Game", "single_game"),
    ):
        leaderboards = fetch_record_leaderboards(
            connection,
            scope=scope_key,
            min_pa=0,
            limit=10,
            active_only=False,
        )
        for stat_label, board in leaderboards.items():
            if board.empty or "canonical_name" not in board.columns:
                continue
            matches = board[board["canonical_name"] == canonical_name].copy()
            if matches.empty:
                continue
            row = matches.iloc[0]
            value = row.get(stat_label)
            game_date = str(row.get("Date") or "")
            game_time = str(row.get("Time") or "")
            opponent = str(row.get("Opponent") or "")
            result_row = {
                "scope": scope_label,
                "stat": stat_label,
                "rank": int(row["#"]),
                "value": value,
                "value_display": _format_record_value(stat_label, value),
                "season": str(row.get("Season") or ""),
                "game_date": game_date,
                "game_time": game_time,
                "opponent": opponent,
                "game": _format_game_context(game_date, game_time, opponent),
            }
            placement_rows.append(result_row)
            if int(row["#"]) == 1:
                owned_rows.append(result_row)

    placements = pd.DataFrame(placement_rows)
    if not placements.empty:
        placements.loc[:, "season_label"] = placements["season"].map(lambda value: format_player_season_label(str(value)) if str(value).strip() else "")
        placements = placements.sort_values(["rank", "scope", "stat", "game_date", "season"], ascending=[True, True, True, False, False]).reset_index(drop=True)
    owned = pd.DataFrame(owned_rows)
    if not owned.empty:
        owned.loc[:, "season_label"] = owned["season"].map(lambda value: format_player_season_label(str(value)) if str(value).strip() else "")
        owned = owned.sort_values(["scope", "stat", "game_date", "season"], ascending=[True, True, False, False]).reset_index(drop=True)
    return {
        "owned": owned,
        "placements": placements.head(12),
    }


def fetch_schedule_seasons(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT DISTINCT season FROM schedule_games WHERE COALESCE(season, '') <> ''"
    ).fetchall()
    return sort_seasons([str(row["season"]) for row in rows])


def fetch_schedule_team_names(
    connection: sqlite3.Connection,
    season: str | None = None,
) -> list[str]:
    params: list[object] = []
    where_clause = ""
    if season:
        where_clause = "WHERE season = ?"
        params.append(season)
    rows = connection.execute(
        f"""
        SELECT DISTINCT team_name
        FROM schedule_games
        {where_clause}
        ORDER BY team_name
        """,
        params,
    ).fetchall()
    return [str(row["team_name"]) for row in rows]


def fetch_schedule_weeks(
    connection: sqlite3.Connection,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT week_label, MIN(game_date) AS first_game_date, MIN(COALESCE(game_time, '')) AS first_game_time
        FROM schedule_games
        WHERE season = ? AND team_name = ? AND COALESCE(week_label, '') <> ''
        GROUP BY week_label
        ORDER BY first_game_date, first_game_time, week_label
        """,
        (season, team_name),
    ).fetchall()
    return [str(row["week_label"]) for row in rows]


def fetch_current_schedule_week(
    connection: sqlite3.Connection,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
    as_of: date | datetime | str | None = None,
) -> str | None:
    next_game = fetch_next_game(connection, season=season, team_name=team_name, as_of=as_of)
    if next_game and next_game.get("week_label"):
        return str(next_game["week_label"])

    weeks = fetch_schedule_weeks(connection, season, team_name)
    return weeks[0] if weeks else None


def fetch_schedule_opponents(
    connection: sqlite3.Connection,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT DISTINCT opponent_name
        FROM schedule_games
        WHERE season = ?
          AND team_name = ?
          AND is_bye = 0
          AND COALESCE(opponent_name, '') <> ''
        ORDER BY opponent_name
        """,
        (season, team_name),
    ).fetchall()
    return [str(row["opponent_name"]) for row in rows]


def fetch_schedule_games(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
    view_filter: str = "Upcoming only",
    opponent: str | None = None,
    week_label: str | None = None,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    dataframe = pd.read_sql_query(
        """
        SELECT
            game_id,
            season,
            league_name,
            division_name,
            week_label,
            game_date,
            game_time,
            team_name,
            opponent_name,
            home_away,
            location_or_field,
            status,
            completed_flag,
            is_bye,
            result,
            runs_for,
            runs_against,
            notes,
            source
        FROM schedule_games
        WHERE season = ? AND team_name = ?
        ORDER BY game_date, COALESCE(game_time, ''), week_label
        """,
        connection,
        params=(season, team_name),
    )
    if dataframe.empty:
        return dataframe

    filtered = dataframe.copy()
    filtered.loc[:, "game_datetime"] = filtered.apply(_combine_schedule_datetime, axis=1)
    filtered.loc[:, "schedule_date"] = pd.to_datetime(filtered["game_date"], errors="coerce")
    reference_dt = _coerce_schedule_reference_datetime(as_of)

    filtered = filtered.astype({"completed_flag": int, "is_bye": bool})
    completed_mask = filtered.apply(_schedule_completed_mask, axis=1)
    if view_filter == "Upcoming only":
        filtered = filtered.loc[~completed_mask & (filtered["game_datetime"] >= reference_dt)].copy()
    elif view_filter == "Completed only":
        filtered = filtered.loc[(completed_mask | ((filtered["game_datetime"] < reference_dt) & ~filtered["is_bye"]))].copy()

    if opponent and opponent != "All opponents":
        filtered = filtered[filtered["opponent_name"].fillna("") == opponent].copy()
    if week_label and week_label != "All weeks":
        filtered = filtered[filtered["week_label"].fillna("") == week_label].copy()

    filtered.loc[:, "date_display"] = filtered["schedule_date"].dt.strftime("%a %m/%d/%y").fillna(filtered["game_date"])
    filtered.loc[:, "time_display"] = filtered["game_time"].fillna("")
    filtered.loc[:, "opponent_display"] = filtered.apply(
        lambda row: "BYE" if bool(row["is_bye"]) else str(row["opponent_name"] or ""),
        axis=1,
    )
    filtered.loc[:, "home_away_display"] = filtered.apply(
        lambda row: "Bye"
        if bool(row["is_bye"])
        else ("Home" if str(row.get("home_away") or "").strip().lower() == "home" else "Away"),
        axis=1,
    )
    filtered.loc[:, "status_display"] = filtered.apply(_schedule_status_display, axis=1)
    filtered.loc[:, "result_display"] = filtered.apply(_schedule_result_display, axis=1)
    filtered.loc[:, "rf_ra_display"] = filtered.apply(
        lambda row: ""
        if pd.isna(row["runs_for"]) and pd.isna(row["runs_against"])
        else f"{int(row['runs_for'])}-{int(row['runs_against'])}",
        axis=1,
    )

    return filtered.sort_values(["game_datetime", "week_label", "game_id"]).reset_index(drop=True)


def fetch_schedule_season_summary(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
    as_of: date | datetime | str | None = None,
) -> dict[str, int | float | str]:
    games = fetch_schedule_games(
        connection,
        season=season,
        team_name=team_name,
        view_filter="All",
        as_of=as_of,
    )
    if games.empty:
        return {
            "record": "0-0",
            "wins": 0,
            "losses": 0,
            "ties": 0,
            "runs_for": 0,
            "runs_against": 0,
            "games_completed": 0,
            "games_remaining": 0,
            "non_bye_games": 0,
        }

    non_bye_games = games.loc[~games["is_bye"]].copy()
    completed_games = non_bye_games.loc[non_bye_games.apply(_schedule_completed_mask, axis=1)].copy()

    wins = int((completed_games["result_display"] == "W").sum()) if not completed_games.empty else 0
    losses = int((completed_games["result_display"] == "L").sum()) if not completed_games.empty else 0
    ties = int((completed_games["result_display"] == "T").sum()) if not completed_games.empty else 0
    runs_for = int(completed_games["runs_for"].fillna(0).sum()) if not completed_games.empty else 0
    runs_against = int(completed_games["runs_against"].fillna(0).sum()) if not completed_games.empty else 0
    games_completed = int(len(completed_games))
    games_remaining = int(len(non_bye_games) - games_completed)

    return {
        "record": _format_team_record(wins, losses),
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "runs_for": runs_for,
        "runs_against": runs_against,
        "games_completed": games_completed,
        "games_remaining": games_remaining,
        "non_bye_games": int(len(non_bye_games)),
    }


def fetch_next_game(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
    as_of: date | datetime | str | None = None,
) -> dict[str, object] | None:
    all_games = fetch_schedule_games(
        connection,
        season=season,
        team_name=team_name,
        view_filter="All",
        as_of=as_of,
    )
    if all_games.empty:
        return None
    reference_dt = _coerce_schedule_reference_datetime(as_of)
    upcoming_games = all_games.loc[
        (~all_games["is_bye"].astype(bool)) & (all_games["game_datetime"] >= reference_dt)
    ].copy()
    if upcoming_games.empty:
        return None
    upcoming_games = upcoming_games.sort_values(["game_datetime", "week_label", "game_id"]).reset_index(drop=True)
    return upcoming_games.iloc[0].to_dict()


def fetch_upcoming_schedule(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
    limit: int = 6,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    upcoming = fetch_schedule_games(
        connection,
        season=season,
        team_name=team_name,
        view_filter="Upcoming only",
        as_of=as_of,
    )
    return upcoming.head(limit).reset_index(drop=True)


def fetch_maple_tree_week_bundle(
    connection: sqlite3.Connection,
    *,
    season: str,
    week_label: str | None = None,
    team_name: str = DEFAULT_SCHEDULE_TEAM_NAME,
    as_of: date | datetime | str | None = None,
) -> dict[str, object]:
    resolved_week = week_label or fetch_current_schedule_week(
        connection,
        season=season,
        team_name=team_name,
        as_of=as_of,
    )
    if not resolved_week:
        return {
            "season": season,
            "team_name": team_name,
            "week_label": "",
            "games": pd.DataFrame(),
            "non_bye_games": pd.DataFrame(),
            "opponent_names": [],
            "league_name": "",
            "division_name": "",
            "primary_game_date": "",
            "generation_enabled": False,
            "validation_message": "No Maple Tree schedule week is currently loaded.",
        }

    games = fetch_schedule_games(
        connection,
        season=season,
        team_name=team_name,
        view_filter="All",
        week_label=resolved_week,
        as_of=as_of,
    )
    if games.empty:
        return {
            "season": season,
            "team_name": team_name,
            "week_label": resolved_week,
            "games": games,
            "non_bye_games": games,
            "opponent_names": [],
            "league_name": "",
            "division_name": "",
            "primary_game_date": "",
            "generation_enabled": False,
            "validation_message": "No Maple Tree games are loaded for the selected week.",
        }

    ordered_games = games.sort_values(["game_datetime", "week_label", "game_id"]).reset_index(drop=True)
    non_bye_games = ordered_games.loc[~ordered_games["is_bye"].astype(bool)].copy().reset_index(drop=True)
    opponent_names = [
        str(name)
        for name in non_bye_games["opponent_name"].fillna("").tolist()
        if str(name).strip()
    ]
    unique_opponents = sorted(set(opponent_names))

    validation_message = ""
    generation_enabled = True
    if not ordered_games.empty and ordered_games["is_bye"].astype(bool).all():
        validation_message = WRITEUP_BYE_WEEK_MESSAGE
        generation_enabled = False
    elif len(ordered_games) != 2 or len(non_bye_games) != 2:
        validation_message = WRITEUP_INVALID_DOUBLEHEADER_MESSAGE
        generation_enabled = False

    source_row = non_bye_games.iloc[0] if not non_bye_games.empty else ordered_games.iloc[0]
    return {
        "season": season,
        "team_name": team_name,
        "week_label": resolved_week,
        "games": ordered_games,
        "non_bye_games": non_bye_games,
        "opponent_names": unique_opponents,
        "league_name": str(source_row.get("league_name") or ""),
        "division_name": str(source_row.get("division_name") or ""),
        "primary_game_date": str(source_row.get("game_date") or ""),
        "generation_enabled": generation_enabled,
        "validation_message": validation_message,
    }


def fetch_writeup_milestone_watch(
    connection: sqlite3.Connection,
    *,
    distance_threshold: int = 10,
    limit: int = 5,
) -> list[str]:
    active_milestones = fetch_career_milestones(
        connection,
        active_only=True,
        max_remaining=distance_threshold,
        sort_by="nearest milestone",
    )
    in_play = select_in_play_milestones(
        active_milestones,
        distance_threshold=distance_threshold,
        limit=limit,
    )
    if in_play.empty:
        lines: list[str] = []
    else:
        lines = []
        for _, row in in_play.iterrows():
            remaining = int(row["remaining"])
            next_milestone = int(row["next_milestone"])
            player = str(row["player"])
            stat = str(row["stat"])
            club_label = str(row.get("club_label") or "").strip()
            line = f"{player} is {_writeup_remaining_phrase(remaining)} from {next_milestone} {stat}"
            extras = [value for value in (club_label,) if value]
            if extras:
                line += f" ({'; '.join(extras)})"
            lines.append(line + ".")

    first_to_watch = select_first_to_milestones(
        active_milestones,
        progress_threshold=0.85,
        max_remaining=min(distance_threshold, 5),
        limit=limit,
    )
    seen_pairs = {
        (str(row["player"]), str(row["stat"]), int(row["next_milestone"]))
        for _, row in in_play.iterrows()
    } if not in_play.empty else set()
    for _, row in first_to_watch.iterrows():
        identifier = (str(row["player"]), str(row["stat"]), int(row["next_milestone"]))
        if identifier in seen_pairs:
            continue
        remaining = int(row["remaining"])
        next_milestone = int(row["next_milestone"])
        player = str(row["player"])
        stat = str(row["stat"])
        lines.append(
            f"First into club watch: {player} is {_writeup_remaining_phrase(remaining)} from becoming the first Maple Tree hitter to reach {next_milestone} {stat}."
        )
    return [line for line in lines if line.strip()]


def fetch_writeup_opponent_scouting(
    connection: sqlite3.Connection,
    *,
    season: str,
    opponent_names: list[str],
    division_name: str | None = None,
    as_of: date | datetime | str | None = None,
) -> list[str]:
    unique_opponents = [name for name in sorted(set(opponent_names)) if str(name).strip()]
    if not unique_opponents:
        return [WRITEUP_EMPTY_OPPONENT_SCOUTING]

    standings = fetch_latest_standings_snapshot(
        connection,
        season=season,
        division_name=division_name,
    )
    standings_lookup = (
        standings.set_index("team_name").to_dict("index")
        if not standings.empty and "team_name" in standings.columns
        else {}
    )
    maple_tree_summary = fetch_schedule_season_summary(
        connection,
        season=season,
        team_name=DEFAULT_SCHEDULE_TEAM_NAME,
        as_of=as_of,
    )
    maple_tree_games_completed = int(maple_tree_summary.get("games_completed", 0) or 0)
    maple_tree_scored_per_game = _safe_divide(
        float(maple_tree_summary.get("runs_for", 0) or 0),
        float(maple_tree_games_completed),
    )
    maple_tree_allowed_per_game = _safe_divide(
        float(maple_tree_summary.get("runs_against", 0) or 0),
        float(maple_tree_games_completed),
    )

    lines: list[str] = []
    any_completed_data = False
    for opponent_name in unique_opponents:
        summary = fetch_league_team_summary(
            connection,
            season=season,
            team_name=opponent_name,
            division_name=division_name,
            as_of=as_of,
        )
        recent = fetch_league_team_recent_results(
            connection,
            season=season,
            team_name=opponent_name,
            division_name=division_name,
            limit=3,
            as_of=as_of,
        )
        if recent.empty or int(summary["games_completed"]) == 0:
            if len(unique_opponents) == 1:
                return [WRITEUP_EMPTY_OPPONENT_SCOUTING]
            lines.append(f"{opponent_name}: {WRITEUP_EMPTY_OPPONENT_SCOUTING}")
            continue

        any_completed_data = True
        standing_row = standings_lookup.get(opponent_name, {})
        opponent_games_completed = int(summary.get("games_completed", 0) or 0)
        opponent_scored_per_game = _safe_divide(
            float(summary.get("runs_for", 0) or 0),
            float(opponent_games_completed),
        )
        opponent_allowed_per_game = _safe_divide(
            float(summary.get("runs_against", 0) or 0),
            float(opponent_games_completed),
        )
        parts = [f"{opponent_name}: record {summary['record']}"]
        if standing_row:
            parts.append(
                f"standings {int(standing_row.get('wins', 0))}-{int(standing_row.get('losses', 0))}"
            )
        parts.append(f"runs {int(summary['runs_for'])}-{int(summary['runs_against'])}")
        parts.append(
            f"scores {opponent_scored_per_game:.1f}/game and allows {opponent_allowed_per_game:.1f}/game"
        )
        if maple_tree_games_completed > 0:
            parts.append(
                f"Maple Tree scores {maple_tree_scored_per_game:.1f}/game and allows {maple_tree_allowed_per_game:.1f}/game"
            )
        recent_lines = [
            f"{row['team_result']} {row['score_display']} vs {row['home_team'] if row['away_team'] == opponent_name else row['away_team']}"
            for _, row in recent.iterrows()
        ]
        if recent_lines:
            parts.append("recent: " + "; ".join(recent_lines))
        lines.append(" | ".join(parts) + ".")

    if not any_completed_data:
        return [WRITEUP_EMPTY_OPPONENT_SCOUTING]
    return [line for line in lines if line.strip()]


def fetch_writeup_record_context(
    connection: sqlite3.Connection,
    *,
    milestone_limit: int = 2,
) -> list[str]:
    lines: list[str] = []
    milestone_lines = fetch_writeup_milestone_watch(
        connection,
        distance_threshold=10,
        limit=milestone_limit,
    )
    for line in milestone_lines:
        lines.append(f"Current milestone watch: {line}")

    headliners = fetch_record_headliners(connection, scope="career", active_only=False)
    for label in ("Career HR Leader", "Career OPS Leader"):
        payload = headliners.get(label, {})
        player = str(payload.get("player", "")).strip()
        formatted_value = str(payload.get("formatted_value", "")).strip()
        value_label = str(payload.get("value_label", "")).strip()
        context = str(payload.get("context", "")).strip()
        if not player or player == "No data" or not formatted_value or not value_label:
            continue
        line = f"Current {label}: {player} ({formatted_value} {value_label}"
        if context:
            line += f", {context}"
        line += ")."
        lines.append(line)
    return lines[:4]


def save_weekly_writeup(
    connection: sqlite3.Connection,
    *,
    season: str,
    week_label: str,
    phase: str,
    markdown: str,
    title: str | None = None,
    source: str = "dashboard",
) -> int:
    normalized_phase = phase.strip().lower()
    normalized_title = (title or _title_from_markdown(markdown) or f"{week_label} {normalized_phase.title()}").strip()
    connection.execute(
        """
        INSERT INTO writeups (
            season,
            week_label,
            phase,
            title,
            markdown,
            source
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(season, week_label, phase) DO UPDATE SET
            title = excluded.title,
            markdown = excluded.markdown,
            source = excluded.source,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            season.strip(),
            week_label.strip(),
            normalized_phase,
            normalized_title,
            markdown,
            source.strip() or "dashboard",
        ),
    )
    connection.commit()
    row = connection.execute(
        """
        SELECT writeup_id
        FROM writeups
        WHERE season = ? AND week_label = ? AND phase = ?
        """,
        (season.strip(), week_label.strip(), normalized_phase),
    ).fetchone()
    return int(row["writeup_id"]) if row else 0


def fetch_saved_writeup(
    connection: sqlite3.Connection,
    *,
    season: str,
    week_label: str,
    phase: str,
) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT
            writeup_id,
            season,
            week_label,
            phase,
            title,
            markdown,
            source,
            created_at,
            updated_at
        FROM writeups
        WHERE season = ? AND week_label = ? AND phase = ?
        """,
        (season.strip(), week_label.strip(), phase.strip().lower()),
    ).fetchone()
    return dict(row) if row else None


def fetch_saved_writeups(
    connection: sqlite3.Connection,
    *,
    season: str | None = None,
    phase: str | None = None,
) -> pd.DataFrame:
    params: list[object] = []
    where_parts: list[str] = []
    if season:
        where_parts.append("season = ?")
        params.append(season.strip())
    if phase:
        where_parts.append("phase = ?")
        params.append(phase.strip().lower())

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return pd.read_sql_query(
        f"""
        SELECT
            writeup_id,
            season,
            week_label,
            phase,
            title,
            markdown,
            source,
            created_at,
            updated_at
        FROM writeups
        {where_clause}
        ORDER BY season DESC, week_label DESC, updated_at DESC, writeup_id DESC
        """,
        connection,
        params=params,
    )


def fetch_latest_standings_snapshot(
    connection: sqlite3.Connection,
    *,
    season: str,
    league_name: str | None = None,
    division_name: str | None = None,
) -> pd.DataFrame:
    params: list[object] = [season]
    where_parts = ["season = ?"]
    if league_name:
        where_parts.append("league_name = ?")
        params.append(league_name)
    if division_name:
        where_parts.append("division_name = ?")
        params.append(division_name)

    where_clause = " AND ".join(where_parts)
    latest_date_row = connection.execute(
        f"SELECT MAX(snapshot_date) AS snapshot_date FROM standings_snapshot WHERE {where_clause}",
        params,
    ).fetchone()
    snapshot_date = latest_date_row["snapshot_date"] if latest_date_row else None
    if not snapshot_date:
        return pd.DataFrame()

    params_with_date = [*params, snapshot_date]
    dataframe = pd.read_sql_query(
        f"""
        SELECT
            team_name,
            wins,
            losses,
            ties,
            win_pct,
            games_back,
            snapshot_date,
            league_name,
            division_name
        FROM standings_snapshot
        WHERE {where_clause} AND snapshot_date = ?
        ORDER BY win_pct DESC, wins DESC, LOWER(team_name)
        """,
        connection,
        params=params_with_date,
    )
    if dataframe.empty:
        return dataframe
    dataframe = dataframe.drop_duplicates(subset=["team_name"], keep="last").reset_index(drop=True)
    return dataframe


def fetch_enriched_standings_snapshot(
    connection: sqlite3.Connection,
    *,
    season: str,
    league_name: str | None = None,
    division_name: str | None = None,
) -> pd.DataFrame:
    standings = fetch_latest_standings_snapshot(
        connection,
        season=season,
        league_name=league_name,
        division_name=division_name,
    )
    if standings.empty:
        return standings

    enrichment = fetch_league_standings_enrichment(
        connection,
        season=season,
        division_name=division_name,
    )
    if enrichment.empty:
        standings = standings.copy()
        standings.loc[:, "runs_for"] = 0
        standings.loc[:, "runs_against"] = 0
        standings.loc[:, "run_diff"] = 0
        return standings

    merged = standings.merge(enrichment, on="team_name", how="left")
    for column in ("runs_for", "runs_against", "run_diff"):
        merged.loc[:, column] = merged[column].fillna(0).astype(int)
    return merged


def fetch_league_schedule_seasons(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT DISTINCT season FROM league_schedule_games WHERE COALESCE(season, '') <> ''"
    ).fetchall()
    return sort_seasons([str(row["season"]) for row in rows])


def fetch_league_divisions(
    connection: sqlite3.Connection,
    season: str,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT DISTINCT division_name
        FROM league_schedule_games
        WHERE season = ? AND COALESCE(division_name, '') <> ''
        ORDER BY division_name
        """,
        (season,),
    ).fetchall()
    return [str(row["division_name"]) for row in rows]


def fetch_league_team_names(
    connection: sqlite3.Connection,
    season: str,
    division_name: str | None = None,
) -> list[str]:
    params: list[object] = [season]
    where_parts = ["season = ?"]
    if division_name and division_name != "All divisions":
        where_parts.append("division_name = ?")
        params.append(division_name)
    where_clause = " AND ".join(where_parts)
    rows = connection.execute(
        f"""
        SELECT team_name
        FROM (
            SELECT home_team AS team_name FROM league_schedule_games WHERE {where_clause}
            UNION
            SELECT away_team AS team_name FROM league_schedule_games WHERE {where_clause}
        )
        ORDER BY team_name
        """,
        [*params, *params],
    ).fetchall()
    return [str(row["team_name"]) for row in rows]


def fetch_league_weeks(
    connection: sqlite3.Connection,
    season: str,
    division_name: str | None = None,
) -> list[str]:
    params: list[object] = [season]
    where_parts = ["season = ?"]
    if division_name and division_name != "All divisions":
        where_parts.append("division_name = ?")
        params.append(division_name)
    where_clause = " AND ".join(where_parts)
    rows = connection.execute(
        f"""
        SELECT week_label, MIN(game_date) AS first_game_date, MIN(COALESCE(game_time, '')) AS first_game_time
        FROM league_schedule_games
        WHERE {where_clause} AND COALESCE(week_label, '') <> ''
        GROUP BY week_label
        ORDER BY first_game_date, first_game_time, week_label
        """,
        params,
    ).fetchall()
    return [str(row["week_label"]) for row in rows]


def fetch_current_league_week(
    connection: sqlite3.Connection,
    season: str,
    division_name: str | None = None,
    as_of: date | datetime | str | None = None,
) -> str | None:
    upcoming = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        view_filter="Upcoming only",
        as_of=as_of,
    )
    if not upcoming.empty:
        return str(upcoming.iloc[0]["week_label"])

    weeks = fetch_league_weeks(connection, season, division_name)
    return weeks[0] if weeks else None


def fetch_previous_completed_league_week(
    connection: sqlite3.Connection,
    season: str,
    division_name: str | None = None,
    as_of: date | datetime | str | None = None,
) -> str | None:
    completed = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        view_filter="Completed only",
        as_of=as_of,
    )
    if completed.empty or "week_label" not in completed.columns:
        return None

    completed = completed.loc[completed["week_label"].fillna("") != ""].copy()
    if completed.empty:
        return None

    completed = completed.sort_values(["game_datetime", "league_game_id"], ascending=[False, False]).reset_index(drop=True)
    return str(completed.iloc[0]["week_label"])


def fetch_league_schedule_games(
    connection: sqlite3.Connection,
    *,
    season: str,
    division_name: str | None = None,
    week_label: str | None = None,
    team_name: str | None = None,
    opponent: str | None = None,
    view_filter: str = "All",
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    params: list[object] = [season]
    where_parts = ["season = ?"]
    if division_name and division_name != "All divisions":
        where_parts.append("division_name = ?")
        params.append(division_name)
    where_clause = " AND ".join(where_parts)
    dataframe = pd.read_sql_query(
        f"""
        SELECT
            league_game_id,
            season,
            league_name,
            division_name,
            week_label,
            game_date,
            game_time,
            location_or_field,
            home_team,
            away_team,
            status,
            completed_flag,
            home_runs,
            away_runs,
            result_summary,
            notes,
            source
        FROM league_schedule_games
        WHERE {where_clause}
        ORDER BY game_date, COALESCE(game_time, ''), COALESCE(location_or_field, '')
        """,
        connection,
        params=params,
    )
    if dataframe.empty:
        return dataframe

    filtered = dataframe.copy()
    filtered.loc[:, "game_datetime"] = filtered.apply(_combine_schedule_datetime, axis=1)
    filtered.loc[:, "schedule_date"] = pd.to_datetime(filtered["game_date"], errors="coerce")
    reference_dt = _coerce_schedule_reference_datetime(as_of)
    filtered = filtered.astype({"completed_flag": int})
    completed_mask = filtered.apply(_league_game_completed_mask, axis=1)

    if view_filter == "Upcoming only":
        filtered = filtered.loc[~completed_mask & (filtered["game_datetime"] >= reference_dt)].copy()
    elif view_filter == "Completed only":
        filtered = filtered.loc[completed_mask | (filtered["game_datetime"] < reference_dt)].copy()

    if week_label and week_label != "All weeks":
        filtered = filtered[filtered["week_label"].fillna("") == week_label].copy()

    if team_name and team_name != "All teams":
        filtered = filtered[
            (filtered["home_team"].fillna("") == team_name)
            | (filtered["away_team"].fillna("") == team_name)
        ].copy()

    if opponent and opponent != "All opponents" and team_name and team_name != "All teams":
        filtered = filtered[
            (
                (filtered["home_team"].fillna("") == team_name)
                & (filtered["away_team"].fillna("") == opponent)
            )
            | (
                (filtered["away_team"].fillna("") == team_name)
                & (filtered["home_team"].fillna("") == opponent)
            )
        ].copy()

    if filtered.empty:
        return filtered.reset_index(drop=True)

    filtered.loc[:, "date_display"] = filtered["schedule_date"].dt.strftime("%a %m/%d/%y").fillna(filtered["game_date"])
    filtered.loc[:, "time_display"] = filtered["game_time"].fillna("")
    filtered.loc[:, "matchup_display"] = filtered["away_team"].fillna("") + " @ " + filtered["home_team"].fillna("")
    filtered.loc[:, "status_display"] = filtered.apply(_league_status_display, axis=1)
    filtered.loc[:, "score_display"] = filtered.apply(_league_score_display, axis=1)
    filtered.loc[:, "league_result_display"] = filtered.apply(_league_result_display, axis=1)
    filtered.loc[:, "team_result"] = filtered.apply(
        lambda row: _league_team_result_for_row(row, team_name) if team_name and team_name != "All teams" else "",
        axis=1,
    )
    filtered.loc[:, "team_result_display"] = filtered.apply(
        lambda row: _league_team_result_display(row, team_name) if team_name and team_name != "All teams" else "",
        axis=1,
    )
    return filtered.sort_values(["game_datetime", "week_label", "league_game_id"]).reset_index(drop=True)


def fetch_league_team_week_opponents(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str,
    week_label: str,
    division_name: str | None = None,
    as_of: date | datetime | str | None = None,
) -> list[str]:
    games = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        week_label=week_label,
        team_name=team_name,
        view_filter="All",
        as_of=as_of,
    )
    if games.empty:
        return []

    opponents = []
    for _, row in games.iterrows():
        if str(row["home_team"]) == team_name:
            opponents.append(str(row["away_team"]))
        else:
            opponents.append(str(row["home_team"]))
    return sorted(set(opponents))


def fetch_week_scoreboard(
    connection: sqlite3.Connection,
    *,
    season: str,
    division_name: str | None = None,
    week_label: str | None = None,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    scoreboard = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        week_label=week_label,
        view_filter="Completed only",
        as_of=as_of,
    )
    if scoreboard.empty:
        return scoreboard
    scoreboard = scoreboard.loc[scoreboard.apply(_league_game_completed_mask, axis=1)].copy()
    return scoreboard.reset_index(drop=True)


def fetch_league_standings_enrichment(
    connection: sqlite3.Connection,
    *,
    season: str,
    division_name: str | None = None,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    games = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        view_filter="All",
        as_of=as_of,
    )
    if games.empty:
        return pd.DataFrame(columns=["team_name", "runs_for", "runs_against", "run_diff"])

    completed = games.loc[games.apply(_league_game_completed_mask, axis=1)].copy()
    if completed.empty:
        team_names = fetch_league_team_names(connection, season, division_name)
        if not team_names:
            return pd.DataFrame(columns=["team_name", "runs_for", "runs_against", "run_diff"])
        return pd.DataFrame(
            {
                "team_name": team_names,
                "runs_for": [0] * len(team_names),
                "runs_against": [0] * len(team_names),
                "run_diff": [0] * len(team_names),
            }
        )

    rows: list[dict[str, int | str]] = []
    for _, row in completed.iterrows():
        away_runs = None if pd.isna(row.get("away_runs")) else int(row["away_runs"])
        home_runs = None if pd.isna(row.get("home_runs")) else int(row["home_runs"])
        if away_runs is None or home_runs is None:
            continue
        rows.append(
            {
                "team_name": str(row["away_team"]),
                "runs_for": away_runs,
                "runs_against": home_runs,
            }
        )
        rows.append(
            {
                "team_name": str(row["home_team"]),
                "runs_for": home_runs,
                "runs_against": away_runs,
            }
        )

    team_totals = pd.DataFrame(rows)
    if team_totals.empty:
        return pd.DataFrame(columns=["team_name", "runs_for", "runs_against", "run_diff"])

    grouped = (
        team_totals.groupby("team_name", as_index=False)[["runs_for", "runs_against"]]
        .sum()
        .sort_values("team_name")
        .reset_index(drop=True)
    )
    grouped.loc[:, "run_diff"] = grouped["runs_for"] - grouped["runs_against"]

    all_team_names = fetch_league_team_names(connection, season, division_name)
    if all_team_names:
        all_teams = pd.DataFrame({"team_name": all_team_names})
        grouped = all_teams.merge(grouped, on="team_name", how="left")
        for column in ("runs_for", "runs_against", "run_diff"):
            grouped.loc[:, column] = grouped[column].fillna(0).astype(int)
    return grouped


def fetch_league_team_summary(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str,
    division_name: str | None = None,
    as_of: date | datetime | str | None = None,
) -> dict[str, object]:
    games = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        team_name=team_name,
        view_filter="All",
        as_of=as_of,
    )
    if games.empty:
        return {
            "record": "0-0",
            "wins": 0,
            "losses": 0,
            "ties": 0,
            "runs_for": 0,
            "runs_against": 0,
            "games_completed": 0,
            "games_remaining": 0,
        }

    completed = games.loc[games.apply(_league_game_completed_mask, axis=1)].copy()
    wins = losses = ties = runs_for = runs_against = 0
    for _, row in completed.iterrows():
        team_runs, opp_runs = _team_runs_for_league_row(row, team_name)
        if team_runs is None or opp_runs is None:
            continue
        runs_for += team_runs
        runs_against += opp_runs
        if team_runs > opp_runs:
            wins += 1
        elif team_runs < opp_runs:
            losses += 1
        else:
            ties += 1
    games_completed = int(len(completed))
    games_remaining = int(len(games) - games_completed)
    return {
        "record": _format_team_record(wins, losses),
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "runs_for": runs_for,
        "runs_against": runs_against,
        "games_completed": games_completed,
        "games_remaining": games_remaining,
    }


def fetch_league_team_recent_results(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str,
    division_name: str | None = None,
    limit: int = 3,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    games = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        team_name=team_name,
        view_filter="Completed only",
        as_of=as_of,
    )
    if games.empty:
        return games
    return games.sort_values(["game_datetime", "league_game_id"], ascending=[False, False]).head(limit).reset_index(drop=True)


def fetch_league_team_upcoming_games(
    connection: sqlite3.Connection,
    *,
    season: str,
    team_name: str,
    division_name: str | None = None,
    limit: int = 3,
    as_of: date | datetime | str | None = None,
) -> pd.DataFrame:
    games = fetch_league_schedule_games(
        connection,
        season=season,
        division_name=division_name,
        team_name=team_name,
        view_filter="Upcoming only",
        as_of=as_of,
    )
    if games.empty:
        return games
    return games.head(limit).reset_index(drop=True)


def build_schedule_filter_summary(parts: list[tuple[str, str]]) -> str:
    visible_parts = [f"{label}: {value}" for label, value in parts if str(value).strip()]
    return " | ".join(visible_parts)


def _coerce_schedule_reference_datetime(
    as_of: date | datetime | str | None,
) -> datetime:
    if as_of is None:
        return datetime.now()
    if isinstance(as_of, datetime):
        return as_of
    if isinstance(as_of, date):
        return datetime.combine(as_of, datetime.min.time())

    parsed = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(parsed):
        return datetime.now()
    return parsed.to_pydatetime()


def _combine_schedule_datetime(row: pd.Series) -> datetime:
    date_value = pd.to_datetime(row.get("game_date"), errors="coerce")
    if pd.isna(date_value):
        return datetime.max

    time_text = str(row.get("game_time") or "").strip()
    if not time_text:
        return datetime.combine(date_value.date(), datetime.min.time())

    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            parsed_time = datetime.strptime(time_text, fmt).time()
            return datetime.combine(date_value.date(), parsed_time)
        except ValueError:
            continue
    return datetime.combine(date_value.date(), datetime.min.time())


def _league_game_completed_mask(row: pd.Series) -> bool:
    if int(row.get("completed_flag", 0) or 0) == 1:
        return True
    status = str(row.get("status") or "").strip().lower()
    return status in {"completed", "final"}


def _league_status_display(row: pd.Series) -> str:
    if _league_game_completed_mask(row):
        return "Final"
    status = str(row.get("status") or "").strip()
    return status.title() if status else "Scheduled"


def _league_score_display(row: pd.Series) -> str:
    if pd.isna(row.get("away_runs")) or pd.isna(row.get("home_runs")):
        return ""
    return f"{int(row['away_runs'])}-{int(row['home_runs'])}"


def _league_result_display(row: pd.Series) -> str:
    if not _league_game_completed_mask(row):
        return ""
    away_runs = None if pd.isna(row.get("away_runs")) else int(row["away_runs"])
    home_runs = None if pd.isna(row.get("home_runs")) else int(row["home_runs"])
    if away_runs is None or home_runs is None:
        return ""
    away_team = str(row.get("away_team") or "")
    home_team = str(row.get("home_team") or "")
    if away_runs > home_runs:
        return f"{away_team} def. {home_team}, {away_runs}-{home_runs}"
    if home_runs > away_runs:
        return f"{home_team} def. {away_team}, {home_runs}-{away_runs}"
    return f"{away_team} tied {home_team}, {away_runs}-{home_runs}"


def _team_runs_for_league_row(row: pd.Series, team_name: str) -> tuple[int | None, int | None]:
    if str(row.get("home_team") or "") == team_name:
        return (
            None if pd.isna(row.get("home_runs")) else int(row["home_runs"]),
            None if pd.isna(row.get("away_runs")) else int(row["away_runs"]),
        )
    if str(row.get("away_team") or "") == team_name:
        return (
            None if pd.isna(row.get("away_runs")) else int(row["away_runs"]),
            None if pd.isna(row.get("home_runs")) else int(row["home_runs"]),
        )
    return (None, None)


def _league_team_result_for_row(row: pd.Series, team_name: str) -> str:
    team_runs, opp_runs = _team_runs_for_league_row(row, team_name)
    if team_runs is None or opp_runs is None:
        return ""
    if team_runs > opp_runs:
        return "W"
    if team_runs < opp_runs:
        return "L"
    return "T"


def _league_team_result_display(row: pd.Series, team_name: str) -> str:
    team_runs, opp_runs = _team_runs_for_league_row(row, team_name)
    if team_runs is None or opp_runs is None:
        return ""

    opponent_name = ""
    if str(row.get("home_team") or "") == team_name:
        opponent_name = str(row.get("away_team") or "")
    elif str(row.get("away_team") or "") == team_name:
        opponent_name = str(row.get("home_team") or "")

    result = _league_team_result_for_row(row, team_name)
    score = f"{team_runs}-{opp_runs}"
    if result == "T":
        return f"T {score} vs {opponent_name}"
    return f"{result} {score} vs {opponent_name}"


def _schedule_completed_mask(row: pd.Series) -> bool:
    if bool(row.get("completed_flag")):
        return True
    status = str(row.get("status") or "").strip().lower()
    if status in {"completed", "final"}:
        return True
    result_value = row.get("result")
    if result_value is not None and not pd.isna(result_value) and str(result_value).strip():
        return True
    runs_for = row.get("runs_for")
    runs_against = row.get("runs_against")
    return not pd.isna(runs_for) and not pd.isna(runs_against)


def _schedule_status_display(row: pd.Series) -> str:
    if bool(row.get("is_bye")):
        return "Bye"
    if _schedule_completed_mask(row):
        return "Final"
    status = str(row.get("status") or "").strip()
    return status.title() if status else "Scheduled"


def _schedule_result_display(row: pd.Series) -> str:
    if bool(row.get("is_bye")):
        return ""
    explicit_value = row.get("result")
    explicit = (
        str(explicit_value).strip().upper()
        if explicit_value is not None and not pd.isna(explicit_value)
        else ""
    )
    if explicit and explicit != "NAN":
        return explicit
    runs_for = row.get("runs_for")
    runs_against = row.get("runs_against")
    if pd.isna(runs_for) or pd.isna(runs_against):
        return ""
    if int(runs_for) > int(runs_against):
        return "W"
    if int(runs_for) < int(runs_against):
        return "L"
    return "T"


def _format_leader_label(
    dataframe: pd.DataFrame,
    *,
    sort_columns: list[str],
    value_column: str,
    label: str,
    value_format: str,
) -> str:
    if dataframe.empty:
        return ""
    sorted_df = dataframe.sort_values(sort_columns + ["player"], ascending=[False] * len(sort_columns) + [True]).reset_index(drop=True)
    row = sorted_df.iloc[0]
    value = row.get(value_column)
    if value is None or pd.isna(value):
        return str(row["player"])
    return f"{row['player']} ({label} {format(float(value), value_format)})"


def _fetch_advanced_season_source(connection: sqlite3.Connection, season: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            s.season,
            pm.preferred_display_name AS player,
            pi.canonical_name,
            s.games,
            s.plate_appearances AS pa,
            s.at_bats AS ab,
            s.hits,
            s.singles AS "1b",
            s.doubles AS "2b",
            s.triples AS "3b",
            s.home_runs AS hr,
            s.walks AS bb,
            s.strikeouts AS so,
            s.hit_by_pitch AS hbp,
            s.sacrifice_hits AS sac,
            s.sacrifice_flies AS sf,
            s.reached_on_error AS roe,
            s.fielder_choice AS fc,
            s.grounded_into_double_play AS gidp,
            s.runs AS r,
            s.rbi,
            s.total_bases AS tb,
            s.batting_average_risp AS ba_risp,
            s.two_out_rbi,
            s.left_on_base AS lob
        FROM season_batting_stats s
        JOIN player_identity pi ON pi.player_id = s.player_id
        JOIN player_metadata pm ON pm.player_id = s.player_id
        WHERE s.season = ?
        ORDER BY LOWER(pm.preferred_display_name)
        """,
        connection,
        params=(season,),
    )


def _fetch_advanced_career_source(
    connection: sqlite3.Connection,
    seasons: list[str] | None = None,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if seasons:
        placeholders = ",".join("?" for _ in seasons)
        where_clause = f"WHERE s.season IN ({placeholders})"
        params.extend(seasons)

    return pd.read_sql_query(
        f"""
        SELECT
            pm.preferred_display_name AS player,
            pi.canonical_name,
            COUNT(DISTINCT s.season) AS seasons_played,
            SUM(s.games) AS games,
            SUM(s.plate_appearances) AS pa,
            SUM(s.at_bats) AS ab,
            SUM(s.hits) AS hits,
            SUM(s.singles) AS "1b",
            SUM(s.doubles) AS "2b",
            SUM(s.triples) AS "3b",
            SUM(s.home_runs) AS hr,
            SUM(s.walks) AS bb,
            SUM(s.strikeouts) AS so,
            SUM(s.hit_by_pitch) AS hbp,
            SUM(s.sacrifice_hits) AS sac,
            SUM(s.sacrifice_flies) AS sf,
            SUM(s.reached_on_error) AS roe,
            SUM(s.fielder_choice) AS fc,
            SUM(s.grounded_into_double_play) AS gidp,
            SUM(s.runs) AS r,
            SUM(s.rbi) AS rbi,
            SUM(s.total_bases) AS tb,
            SUM(s.two_out_rbi) AS two_out_rbi,
            SUM(s.left_on_base) AS lob,
            CASE
                WHEN SUM(s.plate_appearances) = 0 THEN 0
                ELSE SUM(s.batting_average_risp * s.plate_appearances) / SUM(s.plate_appearances)
            END AS ba_risp
        FROM season_batting_stats s
        JOIN player_identity pi ON pi.player_id = s.player_id
        JOIN player_metadata pm ON pm.player_id = s.player_id
        {where_clause}
        GROUP BY pm.preferred_display_name, pi.canonical_name
        ORDER BY LOWER(pm.preferred_display_name)
        """,
        connection,
        params=params,
    )


def fetch_record_leaderboards(
    connection: sqlite3.Connection,
    scope: str,
    seasons: list[str] | None = None,
    min_pa: int = 0,
    limit: int = 10,
    active_only: bool = False,
) -> dict[str, pd.DataFrame]:
    active_names = set(_fetch_active_roster_names(connection))

    def _unique_columns(columns: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for column in columns:
            if column in seen:
                continue
            seen.add(column)
            ordered.append(column)
        return ordered

    if scope == "career":
        source = fetch_career_stats(connection, seasons=seasons, min_pa=0)
        group_columns = ["player", "canonical_name"]
        counting_columns = COUNTING_RECORD_COLUMNS
    elif scope == "single_season":
        source = fetch_single_season_stats(connection, seasons=seasons, min_pa=0)
        group_columns = ["season", "player", "canonical_name"]
        counting_columns = COUNTING_RECORD_COLUMNS
    elif scope == "single_game":
        source = fetch_single_game_stats(connection, seasons=seasons, min_pa=0)
        group_columns = ["game_date", "game_time", "opponent", "season", "player", "canonical_name"]
        counting_columns = SINGLE_GAME_RECORD_COLUMNS
    else:
        raise ValueError(f"Unsupported record scope: {scope}")

    if source.empty:
        return {}

    if active_only:
        source = source[source["player"].isin(active_names)].copy()
        if source.empty:
            return {}

    leaderboards: dict[str, pd.DataFrame] = {}
    for label, column_name in counting_columns.items():
        leaderboard = source.sort_values([column_name, *group_columns], ascending=[False, *([True] * len(group_columns))]).head(limit)
        columns = _unique_columns(group_columns + [column_name])
        leaderboards[label] = _finalize_record_leaderboard(
            leaderboard[columns].copy(),
            value_column=column_name,
        )

    eligible_for_rates = source[source["pa"] >= min_pa].copy()
    for label, column_name in RATE_RECORD_COLUMNS.items():
        if eligible_for_rates.empty:
            empty_columns = _unique_columns(group_columns + [column_name])
            leaderboards[label] = _finalize_record_leaderboard(
                pd.DataFrame(columns=empty_columns),
                value_column=column_name,
            )
            continue
        leaderboard = eligible_for_rates.sort_values(
            [column_name, "pa", *group_columns],
            ascending=[False, False, *([True] * len(group_columns))],
        ).head(limit)
        columns = _unique_columns(group_columns + ["pa", column_name])
        leaderboards[label] = _finalize_record_leaderboard(
            leaderboard[columns].copy(),
            value_column=column_name,
        )
    return leaderboards


def fetch_record_headliners(
    connection: sqlite3.Connection,
    scope: str,
    seasons: list[str] | None = None,
    min_pa: int = 0,
    active_only: bool = False,
) -> dict[str, dict[str, object]]:
    active_names = set(_fetch_active_roster_names(connection))
    leaderboards = fetch_record_leaderboards(
        connection,
        scope=scope,
        seasons=seasons,
        min_pa=min_pa,
        limit=1,
        active_only=active_only,
    )
    if scope == "career":
        return {
            "Career OPS Leader": _headliner_from_board(leaderboards.get("OPS"), active_names),
            "Career HR Leader": _headliner_from_board(leaderboards.get("HR"), active_names),
            "Career RBI Leader": _headliner_from_board(leaderboards.get("RBI"), active_names),
            "Career Hits Leader": _headliner_from_board(leaderboards.get("Hits"), active_names),
        }
    if scope == "single_season":
        return {
            "Single-Season OPS": _headliner_from_board(leaderboards.get("OPS"), active_names),
            "Single-Season HR": _headliner_from_board(leaderboards.get("HR"), active_names),
            "Single-Season RBI": _headliner_from_board(leaderboards.get("RBI"), active_names),
            "Single-Season Hits": _headliner_from_board(leaderboards.get("Hits"), active_names),
        }
    if scope == "single_game":
        return {
            "Single-Game Hits": _headliner_from_board(leaderboards.get("Hits"), active_names),
            "Single-Game HR": _headliner_from_board(leaderboards.get("HR"), active_names),
            "Single-Game RBI": _headliner_from_board(leaderboards.get("RBI"), active_names),
            "Single-Game TB": _headliner_from_board(leaderboards.get("Total Bases"), active_names),
        }
    raise ValueError(f"Unsupported record headliner scope: {scope}")


def calculate_next_milestone_state(total: int | float, ladder: tuple[int, ...] | list[int]) -> dict[str, object]:
    numeric_total = int(round(float(total)))
    ordered_ladder = tuple(sorted(int(value) for value in ladder))
    highest_cleared = max((value for value in ordered_ladder if numeric_total >= value), default=None)
    next_milestone = next((value for value in ordered_ladder if numeric_total < value), None)

    if next_milestone is None:
        return {
            "next_milestone": None,
            "remaining": None,
            "highest_cleared_milestone": highest_cleared,
            "progress_to_next": 1.0,
            "status": "All listed milestones cleared",
        }

    progress_to_next = max(0.0, min(1.0, numeric_total / max(next_milestone, 1)))
    return {
        "next_milestone": next_milestone,
        "remaining": next_milestone - numeric_total,
        "highest_cleared_milestone": highest_cleared,
        "progress_to_next": progress_to_next,
        "status": f"{next_milestone - numeric_total} away",
    }


def fetch_career_milestones(
    connection: sqlite3.Connection,
    categories: list[str] | None = None,
    active_only: bool = False,
    max_remaining: int | None = None,
    min_current_total: int = 0,
    sort_by: str = "nearest milestone",
) -> pd.DataFrame:
    career = fetch_career_stats(connection, min_pa=0)
    if career.empty:
        return pd.DataFrame(
            columns=[
                "player",
                "stat",
                "current_total",
                "next_milestone",
                "next_milestone_display",
                "remaining",
                "progress_to_next",
                "highest_cleared_milestone",
                "status",
                "active_roster",
                "urgency",
            ]
        )

    selected_categories = categories or list(MILESTONE_LADDERS.keys())
    active_names = set(_fetch_active_roster_names(connection))
    club_reference_totals = {
        category: career[COUNTING_RECORD_COLUMNS[category]].fillna(0).astype(int)
        for category in selected_categories
        if category in COUNTING_RECORD_COLUMNS
    }
    rows: list[dict[str, object]] = []

    for _, row in career.iterrows():
        player_name = str(row["player"])
        is_active = player_name in active_names
        if active_only and not is_active:
            continue

        for category in selected_categories:
            column_name = COUNTING_RECORD_COLUMNS.get(category)
            ladder = MILESTONE_LADDERS.get(category)
            if column_name is None or ladder is None:
                continue

            current_total = int(round(float(row.get(column_name, 0) or 0)))
            if current_total < min_current_total:
                continue

            milestone_state = calculate_next_milestone_state(current_total, ladder)
            remaining = milestone_state["remaining"]
            if max_remaining is not None and (remaining is None or int(remaining) > max_remaining):
                continue

            rows.append(
                {
                    "player": player_name,
                    "canonical_name": str(row.get("canonical_name") or ""),
                    "stat": category,
                    "current_total": current_total,
                    "next_milestone": milestone_state["next_milestone"],
                    "next_milestone_display": (
                        milestone_state["next_milestone"]
                        if milestone_state["next_milestone"] is not None
                        else "All listed milestones cleared"
                    ),
                    "remaining": remaining,
                    "progress_to_next": milestone_state["progress_to_next"],
                    "highest_cleared_milestone": milestone_state["highest_cleared_milestone"],
                    "status": milestone_state["status"],
                    "active_roster": is_active,
                    "urgency": _milestone_urgency_label(remaining),
                }
            )

    dataframe = pd.DataFrame(rows)
    if dataframe.empty:
        return dataframe

    dataframe = _add_milestone_club_context(dataframe, club_reference_totals)

    if sort_by == "player name":
        return dataframe.sort_values(["player", "stat"], ascending=[True, True]).reset_index(drop=True)
    if sort_by == "stat category":
        sortable = dataframe.assign(
            _remaining_sort=dataframe["remaining"].fillna(10**9),
        )
        return sortable.sort_values(
            ["stat", "_remaining_sort", "club_size", "next_milestone_sort", "player"],
            ascending=[True, True, True, False, True],
        ).drop(
            columns="_remaining_sort"
        ).reset_index(drop=True)

    sortable = dataframe.assign(
        _remaining_sort=dataframe["remaining"].fillna(10**9),
        _active_sort=~dataframe["active_roster"],
    )
    return sortable.sort_values(
        ["_remaining_sort", "_active_sort", "club_size", "next_milestone_sort", "player", "stat"],
        ascending=[True, True, True, False, True, True],
    ).drop(columns=["_remaining_sort", "_active_sort"]).reset_index(drop=True)


def fetch_passed_milestones_summary(
    connection: sqlite3.Connection,
    categories: list[str] | None = None,
    active_only: bool = False,
    min_current_total: int = 0,
    limit: int = 20,
) -> pd.DataFrame:
    milestone_df = fetch_career_milestones(
        connection,
        categories=categories,
        active_only=active_only,
        min_current_total=min_current_total,
        sort_by="stat category",
    )
    if milestone_df.empty:
        return milestone_df

    passed = milestone_df[milestone_df["highest_cleared_milestone"].notna()].copy()
    if passed.empty:
        return passed

    club_reference_totals = {
        str(stat): stat_rows["current_total"].fillna(0).astype(int)
        for stat, stat_rows in milestone_df.groupby("stat")
    }
    passed = _add_milestone_club_context(
        passed,
        club_reference_totals,
        threshold_column="highest_cleared_milestone",
    )
    passed.loc[:, "highest_cleared_milestone"] = passed["highest_cleared_milestone"].astype(int)
    passed = passed.sort_values(
        ["stat", "highest_cleared_milestone", "current_total", "player"],
        ascending=[True, False, False, True],
    )
    return passed.head(limit).reset_index(drop=True)


def select_in_play_milestones(
    dataframe: pd.DataFrame,
    distance_threshold: int = 5,
    limit: int = 10,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    in_play = dataframe[
        dataframe["remaining"].notna()
        & (dataframe["remaining"] <= distance_threshold)
    ].copy()
    if in_play.empty:
        return in_play

    urgency_rank = {"1 away": 0, "2-5 away": 1, "6-10 away": 2, "": 3}
    in_play = in_play.assign(
        _urgency_rank=in_play["urgency"].map(lambda value: urgency_rank.get(str(value), 3)),
        _next_milestone_sort=in_play["next_milestone"].fillna(-1),
    )
    in_play = in_play.sort_values(
        ["_urgency_rank", "remaining", "club_size", "_next_milestone_sort", "player", "stat"],
        ascending=[True, True, True, False, True, True],
    )
    in_play = in_play.drop_duplicates(subset=["player"], keep="first")
    return in_play.drop(columns=["_urgency_rank", "_next_milestone_sort"]).head(limit).reset_index(drop=True)


def select_first_to_milestones(
    dataframe: pd.DataFrame,
    progress_threshold: float = 0.85,
    max_remaining: int | None = 10,
    limit: int = 12,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    first_to = dataframe[
        dataframe["remaining"].notna()
        & (dataframe["club_size"] == 0)
        & (dataframe["progress_to_next"] >= progress_threshold)
    ].copy()
    if max_remaining is not None:
        first_to = first_to[first_to["remaining"] <= max_remaining].copy()
    if first_to.empty:
        return first_to

    urgency_rank = {"1 away": 0, "2-5 away": 1, "6-10 away": 2, "": 3}
    first_to = first_to.assign(
        _urgency_rank=first_to["urgency"].map(lambda value: urgency_rank.get(str(value), 3)),
        _next_milestone_sort=first_to["next_milestone"].fillna(-1),
    )
    first_to = first_to.sort_values(
        ["_urgency_rank", "remaining", "_next_milestone_sort", "player", "stat"],
        ascending=[True, True, False, True, True],
    )
    return first_to.drop(columns=["_urgency_rank", "_next_milestone_sort"]).head(limit).reset_index(drop=True)


def fetch_player_identities(connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            pi.player_id,
            pm.preferred_display_name,
            pi.player_name AS canonical_player_name,
            pi.canonical_name,
            pm.is_fixed_dhh,
            pm.active_flag
        FROM player_identity pi
        JOIN player_metadata pm ON pm.player_id = pi.player_id
        ORDER BY LOWER(pm.preferred_display_name)
        """,
        connection,
    )


def fetch_player_aliases(connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            pm.preferred_display_name,
            pi.canonical_name,
            pa.source_name,
            pa.source_type,
            pa.match_method,
            pa.approved_flag
        FROM player_aliases pa
        JOIN player_identity pi ON pi.player_id = pa.player_id
        JOIN player_metadata pm ON pm.player_id = pa.player_id
        ORDER BY LOWER(pm.preferred_display_name), LOWER(pa.source_name)
        """,
        connection,
    )


def fetch_player_metadata(connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            pm.player_id,
            pm.preferred_display_name,
            pm.is_fixed_dhh,
            pm.baserunning_grade,
            pm.consistency_grade,
            pm.speed_flag,
            pm.active_flag,
            COALESCE(pm.notes, '') AS notes
        FROM player_metadata pm
        ORDER BY LOWER(pm.preferred_display_name)
        """,
        connection,
    )


def fetch_active_roster(
    connection: sqlite3.Connection,
    season_name: str = DEFAULT_ACTIVE_ROSTER_SEASON,
) -> pd.DataFrame:
    rows = fetch_active_roster_rows(
        connection=connection,
        season_name=season_name,
    )
    return pd.DataFrame(
        [
            {
                "season_name": row["season_name"],
                "preferred_display_name": row["preferred_display_name"],
                "canonical_name": row["canonical_name"],
                "is_fixed_dhh": row["is_fixed_dhh"],
                "notes": row["roster_notes"],
            }
            for row in rows
        ]
    )


def fetch_projection_inventory(
    connection: sqlite3.Connection,
    projection_season: str | None = None,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if projection_season:
        where_clause = "WHERE hp.projection_season = ?"
        params.append(projection_season)
    return pd.read_sql_query(
        f"""
        SELECT
            hp.projection_season,
            pm.preferred_display_name AS player,
            hp.projection_source,
            hp.current_plate_appearances AS current_pa,
            hp.career_plate_appearances AS career_pa,
            hp.weighted_prior_plate_appearances AS weighted_prior_pa,
            hp.current_season_weight,
            hp.projected_on_base_rate,
            hp.projected_total_base_rate,
            hp.projected_extra_base_hit_rate
        FROM hitter_projections hp
        JOIN player_metadata pm ON pm.player_id = hp.player_id
        {where_clause}
        ORDER BY hp.projection_season DESC, LOWER(pm.preferred_display_name)
        """,
        connection,
        params=params,
    )


def fetch_projection_source_counts(
    connection: sqlite3.Connection,
    projection_season: str | None = None,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if projection_season:
        where_clause = "WHERE projection_season = ?"
        params.append(projection_season)
    return pd.read_sql_query(
        f"""
        SELECT projection_source, COUNT(*) AS players
        FROM hitter_projections
        {where_clause}
        GROUP BY projection_source
        ORDER BY players DESC, projection_source
        """,
        connection,
        params=params,
    )


def fetch_available_projection_rows(
    connection: sqlite3.Connection,
    projection_season: str,
    available_names: list[str],
) -> pd.DataFrame:
    rows = select_game_day_projections(
        connection=connection,
        projection_season=projection_season,
        available_player_names=available_names,
    )
    return pd.DataFrame(
        [
            {
                "player": row.preferred_display_name,
                "canonical_name": row.canonical_name,
                "projection_source": row.projection_source,
                "fixed_dhh": row.is_fixed_dhh,
                "proj_obp": row.projected_on_base_rate,
                "proj_tb_rate": row.projected_total_base_rate,
                "proj_run_rate": row.projected_run_rate,
                "proj_rbi_rate": row.projected_rbi_rate,
                "proj_xbh_rate": row.projected_extra_base_hit_rate,
            }
            for row in rows
        ]
    )


def run_optimizer(
    connection: sqlite3.Connection,
    projection_season: str,
    game_date: str,
    available_player_names: list[str],
    mode: str,
    simulations: int,
    seed: int,
) -> OptimizationResult:
    rules = load_league_rules(DEFAULT_LEAGUE_RULES_PATH)
    return optimize_lineup(
        connection=connection,
        projection_season=projection_season,
        game_date=game_date,
        league_rules=rules,
        simulations=simulations,
        seed=seed,
        mode=mode,
        available_player_names_override=available_player_names,
    )


def evaluate_manual_lineup(
    connection: sqlite3.Connection,
    *,
    projection_season: str,
    ordered_player_names: list[str],
    available_player_names: list[str],
    simulations: int,
    seed: int,
) -> SimulationSummary:
    rules = load_league_rules(DEFAULT_LEAGUE_RULES_PATH)
    lineup = build_simulation_lineup_from_order(
        connection=connection,
        projection_season=projection_season,
        ordered_player_names=ordered_player_names,
        available_player_names=available_player_names,
    )
    return simulate_lineup(
        lineup=lineup,
        league_rules=rules,
        simulations=simulations,
        seed=seed,
    )


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _title_from_markdown(markdown: str) -> str:
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return ""


def _writeup_remaining_phrase(remaining: int) -> str:
    if remaining == 1:
        return "1 away"
    return f"{remaining} away"


def _format_team_record(wins: int, losses: int) -> str:
    return f"{wins}-{losses}"


def _fetch_active_roster_names(
    connection: sqlite3.Connection,
    season_name: str = DEFAULT_ACTIVE_ROSTER_SEASON,
) -> list[str]:
    rows = fetch_active_roster_rows(connection=connection, season_name=season_name)
    return [str(row["preferred_display_name"]) for row in rows]


def _finalize_record_leaderboard(
    dataframe: pd.DataFrame,
    value_column: str,
) -> pd.DataFrame:
    if dataframe.empty:
        columns = [column for column in dataframe.columns]
        labeled_columns = ["#", *[DISPLAY_COLUMN_LABELS.get(column, column) for column in columns]]
        return pd.DataFrame(columns=labeled_columns)

    result = dataframe.copy().reset_index(drop=True)
    result.insert(0, "#", range(1, len(result) + 1))
    if "season" in result.columns:
        result.loc[:, "season"] = result["season"].map(lambda value: _compact_season_label(str(value)))
    ordered_columns = ["#", *[column for column in result.columns if column != "#"]]
    result = result[ordered_columns]
    return result.rename(columns={column: DISPLAY_COLUMN_LABELS.get(column, column) for column in result.columns})


def _headliner_from_board(dataframe: pd.DataFrame | None, active_names: set[str]) -> dict[str, object]:
    if dataframe is None or dataframe.empty:
        return {"label": "", "player": "No data", "canonical_name": "", "value": "", "context": "", "is_active": False}
    row = dataframe.iloc[0].to_dict()
    value_columns = [
        column
        for column in dataframe.columns
        if column not in {"#", "Date", "Time", "Team", "Opponent", "Season", "Player", "PA", "canonical_name"}
    ]
    value_column = value_columns[0] if value_columns else ""
    context_parts: list[str] = []
    if "Date" in row and str(row["Date"]).strip():
        context_parts.append(str(row["Date"]))
    if "Time" in row and str(row["Time"]).strip():
        context_parts.append(str(row["Time"]))
    if "Opponent" in row and str(row["Opponent"]).strip():
        context_parts.append(f"vs {row['Opponent']}")
    if "Season" in row:
        context_parts.append(_compact_season_label(str(row["Season"])))
    if "PA" in row and value_column in RATE_RECORD_COLUMNS:
        context_parts.append(f"PA {row['PA']}")
    value = row.get(value_column, "")
    return {
        "player": row.get("Player", ""),
        "canonical_name": row.get("canonical_name", ""),
        "value": value,
        "formatted_value": _format_record_value(value_column, value),
        "context": " | ".join(context_parts),
        "value_label": value_column,
        "is_active": str(row.get("Player", "")) in active_names,
    }


def _format_record_value(value_label: str, value: object) -> str:
    if value in ("", None):
        return ""
    if value_label in RATE_RECORD_COLUMNS:
        return f"{float(value):.3f}"
    if value_label in COUNTING_RECORD_COLUMNS or value_label == "PA":
        return f"{int(round(float(value)))}"
    return str(value)


def _format_game_context(game_date: str, game_time: str, opponent: str) -> str:
    parts: list[str] = []
    if game_date:
        parts.append(game_date)
    if game_time:
        parts.append(game_time)
    if opponent:
        parts.append(f"vs {opponent}")
    return " ".join(parts).strip()


def _milestone_urgency_label(remaining: object) -> str:
    if remaining in (None, "") or pd.isna(remaining):
        return ""
    numeric_remaining = int(remaining)
    if numeric_remaining <= 1:
        return "1 away"
    if numeric_remaining <= 5:
        return "2-5 away"
    if numeric_remaining <= 10:
        return "6-10 away"
    return ""


def _add_milestone_club_context(
    dataframe: pd.DataFrame,
    club_reference_totals: dict[str, pd.Series],
    *,
    threshold_column: str = "next_milestone",
) -> pd.DataFrame:
    result = dataframe.copy()
    thresholds = result[threshold_column]
    club_sizes = result.apply(
        lambda row: _club_size_for_row(club_reference_totals, row["stat"], row[threshold_column]),
        axis=1,
    )
    return result.assign(
        club_size=club_sizes,
        club_label=club_sizes.combine(
            thresholds,
            lambda club_size, milestone_threshold: _club_label_for_row(club_size, milestone_threshold),
        ),
        is_first_time_milestone=club_sizes == 0,
        next_milestone_sort=thresholds.fillna(-1),
    )


def _club_size_for_row(
    club_reference_totals: dict[str, pd.Series],
    stat_label: object,
    next_milestone: object,
) -> int:
    if stat_label in (None, "") or next_milestone in (None, "") or pd.isna(next_milestone):
        return 0
    totals = club_reference_totals.get(str(stat_label))
    if totals is None or totals.empty:
        return 0
    return int((totals >= int(next_milestone)).sum())


def _club_label_for_row(club_size: object, next_milestone: object) -> str:
    numeric_club_size = int(club_size or 0)
    if next_milestone in (None, "") or pd.isna(next_milestone):
        return ""
    if numeric_club_size == 0:
        return f"First to {int(next_milestone)}"
    return f"{numeric_club_size} in club"
