"""Export the dashboard database to JSON artifacts for the public site (site/src/data).

Run from the repo root as part of the weekly sync, after the local DB has been reloaded:

    python scripts/export_site_data.py

Reuses the tested stats layer in src.dashboard.data (PA-based OBP, Game Score, seed race,
POTW) so the public site and the Streamlit dashboard always agree on every number.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.dashboard.data import (  # noqa: E402
    DEFAULT_DASHBOARD_SEASON,
    DEFAULT_DB_PATH,
    DEFAULT_SCHEDULE_TEAM_NAME,
    fetch_active_roster,
    fetch_career_milestones,
    fetch_career_stats,
    fetch_franchise_opponent_ledger,
    fetch_next_game,
    fetch_player_of_the_week,
    fetch_potw_history,
    fetch_potw_leaderboard,
    fetch_record_leaderboards,
    fetch_records_and_milestones_watch,
    fetch_schedule_games,
    fetch_seasons,
    fetch_seed_race,
    fetch_single_game_feats,
    fetch_single_game_score_leaders,
    fetch_single_game_stats,
    fetch_single_season_stats,
    fetch_team_weekly_results,
    get_connection,
)

OUT_DIR = Path(__file__).resolve().parents[1] / "site" / "src" / "data"

SEASON_ORDER = {"spring": 0, "summer": 1, "fall": 2}


def season_label(name: str) -> str:
    return name.replace("Maple Tree ", "").strip() or name


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")


def season_sort_key(name: str):
    year = int(m.group(1)) if (m := re.search(r"(20\d{2})", name)) else 0
    rank = next((v for k, v in SEASON_ORDER.items() if k in name.lower()), 0)
    return (year, rank)


def records(df: pd.DataFrame | None, columns: list[str] | None = None) -> list[dict]:
    if df is None or df.empty:
        return []
    if columns:
        df = df[[c for c in columns if c in df.columns]]
    # to_json round-trip converts numpy scalars to natives and NaN to null.
    return json.loads(df.to_json(orient="records"))


def dump(name: str, payload) -> None:
    path = OUT_DIR / name
    path.write_text(json.dumps(payload, indent=1, allow_nan=False), encoding="utf-8")
    print(f"  wrote {name} ({path.stat().st_size:,} bytes)")


def team_line(rows: pd.DataFrame) -> dict:
    """Team totals with the same PA-based rate formulas as data.py."""
    def total(col: str) -> int:
        return int(rows[col].fillna(0).sum()) if col in rows.columns else 0

    pa, ab, h, bb = total("pa"), total("ab"), total("hits"), total("bb")
    hbp, sh, tb = total("hbp"), total("sh"), total("tb")
    obp_den = pa - sh
    obp = (h + bb + hbp) / obp_den if obp_den else 0.0
    slg = tb / ab if ab else 0.0
    return {
        "games": int(rows["games"].max()) if "games" in rows.columns and len(rows) else 0,
        "pa": pa, "ab": ab, "hits": h, "1b": total("1b"), "2b": total("2b"),
        "3b": total("3b"), "hr": total("hr"), "bb": bb, "r": total("r"),
        "rbi": total("rbi"), "tb": tb,
        "avg": round(h / ab, 4) if ab else 0.0,
        "obp": round(obp, 4), "slg": round(slg, 4), "ops": round(obp + slg, 4),
    }


def percentile_of(pool: pd.Series, value) -> int | None:
    pool = pd.to_numeric(pool, errors="coerce").dropna()
    if value is None or pd.isna(value) or pool.empty:
        return None
    below = int((pool < value).sum())
    ties = int((pool == value).sum())
    pct = round(100.0 * (below + 0.5 * ties) / len(pool))
    return int(min(99, max(1, pct)))


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    connection = get_connection(Path(DEFAULT_DB_PATH))

    seasons = sorted(fetch_seasons(connection), key=season_sort_key, reverse=True)
    season_meta = [
        {"name": s, "label": season_label(s), "slug": slugify(season_label(s))}
        for s in seasons
    ]
    current = DEFAULT_DASHBOARD_SEASON if DEFAULT_DASHBOARD_SEASON in seasons else seasons[0]

    season_stats = fetch_single_season_stats(connection)
    career = fetch_career_stats(connection)
    games = fetch_single_game_stats(connection)
    roster = fetch_active_roster(connection)
    potw_board = fetch_potw_leaderboard(connection)
    potw_history = fetch_potw_history(connection)

    active = set(roster["canonical_name"])

    def clean_name(value: str) -> str:
        # Initialisms come through title-cased ("Jj", "Aj") — restore all-caps.
        value = str(value)
        return value.upper() if len(value) <= 2 else value

    display_name: dict[str, str] = {}  # canonical -> site display name (from stats rows)

    # ---- schedule.json (+ per-game result lookup and per-season records) ----
    schedule_cols = ["week_label", "game_date", "game_time", "opponent_name", "home_away",
                     "location_or_field", "status", "completed_flag", "is_bye", "result",
                     "runs_for", "runs_against"]
    schedule_out: list[dict] = []
    result_lookup: dict[tuple, dict] = {}
    season_record: dict[str, str] = {}
    for meta in season_meta:
        # The franchise name changes by season (Soviet Sluggers -> Smoking Bunts -> ... ->
        # Maple Tree), so resolve each season's team name from its own schedule rows.
        row = connection.execute(
            "SELECT team_name FROM schedule_games WHERE season = ? LIMIT 1", (meta["name"],)
        ).fetchone()
        season_team = str(row["team_name"]) if row else DEFAULT_SCHEDULE_TEAM_NAME
        sched = fetch_schedule_games(
            connection, season=meta["name"], team_name=season_team,
            view_filter="All")
        rows = records(sched, schedule_cols)
        wins = sum(1 for r in rows if (r.get("result") or "").upper() == "W")
        losses = sum(1 for r in rows if (r.get("result") or "").upper() == "L")
        season_record[meta["name"]] = f"{wins}-{losses}"
        schedule_out.append({**meta, "record": season_record[meta["name"]], "games": rows})
        for r in rows:
            if r.get("result"):
                result_lookup[(meta["name"], r["game_date"], r.get("game_time"))] = {
                    "result": f"{r['result']} {int(r['runs_for'])}-{int(r['runs_against'])}",
                    "ha": (r.get("home_away") or "").lower(),
                }
    dump("schedule.json", schedule_out)

    # ---- season_stats.json ----
    stats_cols = ["player", "canonical_name", "games", "pa", "ab", "hits", "1b", "2b", "3b",
                  "hr", "bb", "r", "rbi", "tb", "avg", "obp", "slg", "ops"]
    seasons_out = []
    for meta in season_meta:
        rows = season_stats[season_stats["season"] == meta["name"]]
        if rows.empty:
            continue
        player_rows = records(rows.sort_values("ops", ascending=False), stats_cols)
        for r in player_rows:
            r["slug"] = slugify(r["canonical_name"])
            r["player"] = clean_name(r["player"])
        seasons_out.append({
            **meta,
            "record": season_record.get(meta["name"], ""),
            "team": team_line(rows),
            "players": player_rows,
        })
    dump("season_stats.json", seasons_out)

    # ---- players.json ----
    pool = season_stats[season_stats["pa"] >= 20].copy()
    pool = pool.assign(iso=pool["slg"] - pool["avg"], bb_pct=100.0 * pool["bb"] / pool["pa"])
    current_rows = season_stats[season_stats["season"] == current].set_index("canonical_name")

    potw_counts = dict(zip(potw_board.get("canonical_name", []), potw_board.get("potw", [])))
    best_weeks = dict(zip(potw_board.get("canonical_name", []), potw_board.get("best_week", [])))

    log_cols = ["game_date", "season", "opponent", "game_time", "pa", "ab", "hits", "1b", "2b",
                "3b", "hr", "bb", "r", "rbi", "tb", "game_score"]
    players_out = []
    for _, c in career.iterrows():
        canonical = str(c["canonical_name"])
        slug = slugify(canonical)
        name = clean_name(c["player"])
        display_name[canonical] = name

        p_seasons = records(
            season_stats[season_stats["canonical_name"] == canonical]
            .sort_values("season", key=lambda s: s.map(season_sort_key)),
            ["season", *stats_cols])
        for row in p_seasons:
            row["label"] = season_label(row["season"])
            row["season_slug"] = slugify(row["label"])

        log = games[games["canonical_name"] == canonical].sort_values(
            ["game_date", "game_time"], ascending=False)
        log_rows = records(log, log_cols)
        for row in log_rows:
            row["label"] = season_label(row["season"])
            info = result_lookup.get((row["season"], row["game_date"], row.get("game_time")))
            row["result"] = info["result"] if info else None
            row["ha"] = info["ha"] if info else None
            row.pop("game_time", None)

        percentiles = None
        if canonical in current_rows.index:
            cur = current_rows.loc[canonical]
            iso = float(cur["slg"]) - float(cur["avg"])
            bb_pct = 100.0 * float(cur["bb"]) / float(cur["pa"]) if cur["pa"] else 0.0
            percentiles = {
                "season": season_label(current),
                "metrics": [
                    {"key": "avg", "label": "AVG", "value": float(cur["avg"]),
                     "pct": percentile_of(pool["avg"], cur["avg"])},
                    {"key": "obp", "label": "OBP", "value": float(cur["obp"]),
                     "pct": percentile_of(pool["obp"], cur["obp"])},
                    {"key": "slg", "label": "SLG", "value": float(cur["slg"]),
                     "pct": percentile_of(pool["slg"], cur["slg"])},
                    {"key": "ops", "label": "OPS", "value": float(cur["ops"]),
                     "pct": percentile_of(pool["ops"], cur["ops"])},
                    {"key": "iso", "label": "ISO", "value": iso,
                     "pct": percentile_of(pool["iso"], iso)},
                    {"key": "bb_pct", "label": "BB%", "value": bb_pct,
                     "pct": percentile_of(pool["bb_pct"], bb_pct)},
                ],
            }

        career_row = {k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v))
                      for k, v in c.items()}
        players_out.append({
            "name": name,
            "canonical": canonical,
            "slug": slug,
            "active": canonical in active,
            "career": career_row,
            "seasons": p_seasons,
            "game_log": log_rows,
            "percentiles": percentiles,
            "potw": int(potw_counts.get(canonical, 0)),
            "best_week": float(best_weeks[canonical]) if canonical in best_weeks else None,
        })
    players_out.sort(key=lambda p: (-int(p["career"].get("games") or 0), p["name"]))
    dump("players.json", players_out)

    # ---- potw.json ----
    weekly_all = fetch_team_weekly_results(connection)
    weekly_map = {
        (str(r["season"]), str(r["game_date"])): str(r["result_display"])
        for _, r in weekly_all.iterrows()
    }
    board = records(potw_board)
    for r in board:
        r["slug"] = slugify(r["canonical_name"])
        r["player"] = display_name.get(r["canonical_name"], clean_name(r["player"]))
    history = records(potw_history)
    for r in history:
        r["slug"] = slugify(r["canonical_name"])
        r["player"] = display_name.get(r["canonical_name"], clean_name(r["player"]))
        r["label"] = season_label(r["season"])
        r["team_result"] = weekly_map.get((r["season"], r["game_date"]))
    dump("potw.json", {"leaderboard": board, "history": history})

    # ---- records.json (the record book) ----
    RATE_LABELS = {"AVG", "OBP", "SLG", "OPS"}
    # Single-game rate records are noise (every 4-for-4 is a 1.000 AVG) and PA/AB
    # single-game "records" are dull — same call as the season-review PDF.
    SINGLE_GAME_EXCLUDE = {"PA", "AB", *RATE_LABELS}

    def board_rows(df: pd.DataFrame, label: str) -> list[dict]:
        rows = []
        for _, r in df.iterrows():
            value = r[label]
            rows.append({
                "rank": int(r["#"]),
                "player": clean_name(r["Player"]),
                "slug": slugify(r["canonical_name"]),
                "value": (None if pd.isna(value)
                          else round(float(value), 4) if label in RATE_LABELS
                          else int(value)),
                "season": str(r["Season"]) if "Season" in df.columns else None,
                "date": str(r["Date"]) if "Date" in df.columns else None,
                "opponent": str(r["Opponent"]) if "Opponent" in df.columns else None,
            })
        return rows

    records_out: dict[str, list] = {}
    for scope in ("career", "single_season", "single_game"):
        boards = fetch_record_leaderboards(
            connection, scope=scope, limit=5,
            min_pa=0 if scope == "single_game" else 20)
        records_out[scope] = [
            {"label": label, "rows": board_rows(df, label)}
            for label, df in boards.items()
            if not df.empty and not (scope == "single_game" and label in SINGLE_GAME_EXCLUDE)
        ]
    dump("records.json", records_out)

    # ---- hof.json (single-game hall of fame) ----
    gs_cols = ["player", "canonical_name", "game_date", "season", "opponent",
               "pa", "ab", "hits", "2b", "3b", "hr", "bb", "r", "rbi", "tb", "game_score"]
    gs_rows = records(fetch_single_game_score_leaders(connection, limit=10), gs_cols)
    for r in gs_rows:
        r["slug"] = slugify(r["canonical_name"])
        r["player"] = clean_name(r["player"])
        r["label"] = season_label(r["season"])
    feat_boards = []
    for label, df in fetch_single_game_feats(connection).items():
        rows = records(df, ["player", "canonical_name", "game_date", "season",
                            "opponent", "pa", "hits", "hr", "rbi", "tb"])
        for r in rows:
            r["slug"] = slugify(r["canonical_name"])
            r["player"] = clean_name(r["player"])
            r["label"] = season_label(r["season"])
        feat_boards.append({"label": label, "rows": rows})
    dump("hof.json", {"game_scores": gs_rows, "feats": feat_boards})

    # ---- milestones.json (active-roster milestone watch) ----
    ms = fetch_career_milestones(connection, active_only=True)
    ms_rows = records(
        ms.sort_values(["remaining", "stat"]),
        ["player", "canonical_name", "stat", "current_total", "next_milestone_display",
         "remaining", "progress_to_next", "club_label"])
    for r in ms_rows:
        r["slug"] = slugify(r["canonical_name"])
        r["player"] = clean_name(r["player"])
        r["progress_to_next"] = round(float(r["progress_to_next"] or 0), 3)
    dump("milestones.json", ms_rows)

    # ---- rivalry.json (franchise vs every opponent, all-time) ----
    ledger_rows = records(fetch_franchise_opponent_ledger(connection))
    for r in ledger_rows:
        wins, losses, ties = int(r["wins"]), int(r["losses"]), int(r.get("ties") or 0)
        r["record"] = f"{wins}-{losses}" + (f"-{ties}" if ties else "")
    ledger_rows.sort(key=lambda r: (-int(r["games"]), str(r["opponent"]).lower()))
    dump("rivalry.json", ledger_rows)

    # ---- meta.json ----
    race = fetch_seed_race(connection, current, team_name=DEFAULT_SCHEDULE_TEAM_NAME)
    board_rows = records(race["standings"])
    potw_now = fetch_player_of_the_week(connection, current)
    if potw_now:
        potw_now["slug"] = slugify(potw_now["canonical_name"])
        potw_now["player"] = display_name.get(
            potw_now["canonical_name"], clean_name(potw_now["player"]))
    weekly = fetch_team_weekly_results(connection, current)
    weekly_rows = records(
        weekly.sort_values("game_date", ascending=False),
        ["game_date", "result_display", "wins", "losses", "runs_for", "runs_against"])
    next_game = fetch_next_game(connection, season=current, team_name=DEFAULT_SCHEDULE_TEAM_NAME)
    next_out = None
    if next_game:
        next_out = {k: str(next_game.get(k) or "") for k in (
            "week_label", "date_display", "time_display", "opponent_display",
            "home_away_display", "location_or_field")}

    completed_dates = games["game_date"].dropna()
    meta = {
        "team": DEFAULT_SCHEDULE_TEAM_NAME,
        "generated": datetime.now().isoformat(timespec="seconds"),
        "data_through": str(completed_dates.max()) if not completed_dates.empty else None,
        "current_season": next(m for m in season_meta if m["name"] == current),
        "seasons": season_meta,
        "record": season_record.get(current, "0-0"),
        "seed_race": {
            "headline": race["headline"],
            "team_seed": race["team_seed"],
            "leader": race["leader"],
            "games_played_total": race["games_played_total"],
            "board": board_rows,
        },
        "potw": potw_now,
        "weekly_results": weekly_rows,
        "milestones": fetch_records_and_milestones_watch(connection, current),
        "next_game": next_out,
    }
    dump("meta.json", meta)

    print(f"Export complete -> {OUT_DIR}")


if __name__ == "__main__":
    main()
