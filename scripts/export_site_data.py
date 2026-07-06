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
    MILESTONE_LADDERS,
    extend_milestone_ladder,
    fetch_active_roster,
    fetch_advanced_analytics_view,
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
    for _, c in career.iterrows():
        display_name[str(c["canonical_name"])] = clean_name(c["player"])

    # ---- advanced analytics (reuses the dashboard's wOBA / wRC+ / RAR engine) ----
    season_adv: dict[tuple[str, str], pd.Series] = {}
    for s in seasons:
        adv_df, _ = fetch_advanced_analytics_view(connection, view_mode="Season", selected_season=s)
        for _, r in adv_df.iterrows():
            season_adv[(s, str(r["canonical_name"]))] = r
    career_adv_df, _ = fetch_advanced_analytics_view(connection, view_mode="Career")
    if "canonical_name" not in career_adv_df.columns:
        name_to_canonical = dict(zip(career["player"].astype(str), career["canonical_name"].astype(str)))
        career_adv_df = career_adv_df.assign(
            canonical_name=career_adv_df["player"].astype(str).map(name_to_canonical))
    career_adv = {str(r["canonical_name"]): r for _, r in career_adv_df.iterrows()}
    gs_season = games.groupby(["season", "canonical_name"])["game_score"].mean()
    gs_career = games.groupby("canonical_name")["game_score"].mean()

    # ---- batted-ball profiles (HHB / LD% / FB% / GB%) from the raw GameChanger CSVs ----
    # These live only in the season exports, not the DB. The header row repeats column
    # names across Batting/Pitching/Fielding sections — first occurrence = batting.
    import csv as _csv

    name_to_canon = {str(p).strip().lower(): str(c)
                     for p, c in zip(career["player"], career["canonical_name"])}
    bb_season: dict[tuple[str, str], dict] = {}
    csv_dir = Path(__file__).resolve().parents[1] / "data" / "raw" / "season_csv"
    for csv_path in sorted(csv_dir.glob("*.csv")):
        season_name = csv_path.stem.replace(" Stats", "").strip()
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
            all_rows = list(_csv.reader(fh))
        if len(all_rows) < 3:
            continue
        idx: dict[str, int] = {}
        for i, col in enumerate(all_rows[1]):
            idx.setdefault(col, i)

        def _num(row: list, col: str) -> float:
            i = idx.get(col)
            raw = row[i] if i is not None and i < len(row) else ""
            try:
                return float(str(raw).replace("%", "").strip() or 0)
            except ValueError:
                return 0.0

        def _cell(row: list, col: str) -> str:
            i = idx.get(col)
            return (row[i] if i is not None and i < len(row) else "").strip()

        # GameChanger roster names drift across eras: Tristan was "Teo" in '21-'22,
        # Joey is "Snaxx" in '25-'26, and some names sit in the Last column.
        BB_ALIASES = {"teo": "tristan", "snaxx": "snaxx"}
        for row in all_rows[2:]:
            if not row or (row[0] or "").strip().lower() == "totals":
                continue
            first, last = _cell(row, "First"), _cell(row, "Last")
            name = first or last
            if not name:
                continue
            canon = None
            for key in (name.lower(), f"{first} {last}".strip().lower(),
                        f"{last} {first}".strip().lower()):
                canon = BB_ALIASES.get(key) or name_to_canon.get(key)
                if canon:
                    break
            if canon is None:
                print(f"  ! batted-ball: no identity match for '{name}' in {season_name}")
                continue
            batted = max(_num(row, "AB") - _num(row, "SO") + _num(row, "SF"), 0.0)
            if batted <= 0:
                continue
            hhb = _num(row, "HHB")
            bb_season[(season_name, canon)] = {
                "batted": batted, "hhb": hhb, "hh": hhb / batted,
                "ld": _num(row, "LD%") / 100.0,
                "fb": _num(row, "FB%") / 100.0,
                "gb": _num(row, "GB%") / 100.0,
            }
    career_bb: dict[str, dict] = {}
    for (_s, canon), d in bb_season.items():
        agg = career_bb.setdefault(canon, {"batted": 0.0, "hhb": 0.0, "ld": 0.0, "fb": 0.0, "gb": 0.0})
        agg["batted"] += d["batted"]
        agg["hhb"] += d["hhb"]
        for k in ("ld", "fb", "gb"):
            agg[k] += d[k] * d["batted"]
    for canon, agg in career_bb.items():
        batted = agg["batted"] or 1.0
        agg["hh"] = agg["hhb"] / batted
        for k in ("ld", "fb", "gb"):
            agg[k] /= batted
    print(f"  batted-ball profiles: {len(bb_season)} player-seasons, {len(career_bb)} careers")

    def opt(value, digits: int = 4):
        if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
            return None
        return round(float(value), digits)

    def adv_fields(stat_row, season_name: str | None, canonical: str) -> dict:
        """Analytics fields for one player-season (or career when season_name is None)."""
        adv = career_adv.get(canonical) if season_name is None else season_adv.get((season_name, canonical))
        gs_val = (gs_career.get(canonical) if season_name is None
                  else gs_season.get((season_name, canonical)))
        pa = float(stat_row.get("pa") or 0)
        return {
            "woba": opt(adv["woba"]) if adv is not None else None,
            "wrc_plus": opt(adv["wrc_plus"], 1) if adv is not None else None,
            "iso": opt(float(stat_row["slg"]) - float(stat_row["avg"])) if stat_row.get("slg") is not None else None,
            "bb_rate": opt(float(stat_row["bb"]) / pa) if pa else None,
            "xbh_rate": opt((float(stat_row["2b"]) + float(stat_row["3b"]) + float(stat_row["hr"])) / pa) if pa else None,
            "gs_avg": opt(gs_val, 2),
        }

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
        advanced = []
        for r in player_rows:
            a = season_adv.get((meta["name"], r["canonical_name"]))
            if a is None:
                continue
            advanced.append({
                "player": r["player"], "slug": r["slug"], "pa": r["pa"],
                **adv_fields(r, meta["name"], r["canonical_name"]),
                "rar": opt(a["rar"], 1), "owar": opt(a["owar"], 2),
                "archetype": str(a["archetype"]),
            })
        advanced.sort(key=lambda x: -(x["rar"] if x["rar"] is not None else -999.0))
        seasons_out.append({
            **meta,
            "record": season_record.get(meta["name"], ""),
            "team": team_line(rows),
            "players": player_rows,
            "advanced": advanced,
        })
    dump("season_stats.json", seasons_out)

    # ---- players.json ----
    current_rows = season_stats[season_stats["season"] == current].set_index("canonical_name")

    METRICS = [("avg", "AVG"), ("obp", "OBP"), ("slg", "SLG"), ("ops", "OPS"), ("iso", "ISO"),
               ("woba", "wOBA"), ("wrc_plus", "wRC+"), ("bb_rate", "BB%"),
               ("xbh_rate", "XBH%"), ("gs", "GS/G"),
               ("hh_rate", "HH%"), ("ld_rate", "LD%"), ("fb_rate", "FB%"), ("gb_rate", "GB%")]

    def stat_vals(row, season_name: str | None, canonical: str) -> dict:
        a = adv_fields(row, season_name, canonical)
        prof = (career_bb.get(canonical) if season_name is None
                else bb_season.get((season_name, canonical)))
        return {"avg": float(row["avg"]), "obp": float(row["obp"]), "slg": float(row["slg"]),
                "ops": float(row["ops"]), "iso": a["iso"], "woba": a["woba"],
                "wrc_plus": a["wrc_plus"], "bb_rate": a["bb_rate"], "xbh_rate": a["xbh_rate"],
                "gs": a["gs_avg"],
                "hh_rate": opt(prof["hh"]) if prof else None,
                "ld_rate": opt(prof["ld"]) if prof else None,
                "fb_rate": opt(prof["fb"]) if prof else None,
                "gb_rate": opt(prof["gb"]) if prof else None}

    def _adv_series(df, key_fn, source, column):
        return df.apply(
            lambda r: float(source[key_fn(r)][column]) if key_fn(r) in source else float("nan"),
            axis=1)

    pool_df = season_stats[season_stats["pa"] >= 20]
    _skey = lambda r: (str(r["season"]), str(r["canonical_name"]))  # noqa: E731
    pool_vals = {
        "avg": pool_df["avg"], "obp": pool_df["obp"], "slg": pool_df["slg"], "ops": pool_df["ops"],
        "iso": pool_df["slg"] - pool_df["avg"],
        "bb_rate": pool_df["bb"] / pool_df["pa"],
        "xbh_rate": (pool_df["2b"] + pool_df["3b"] + pool_df["hr"]) / pool_df["pa"],
        "woba": _adv_series(pool_df, _skey, season_adv, "woba"),
        "wrc_plus": _adv_series(pool_df, _skey, season_adv, "wrc_plus"),
        "gs": pool_df.apply(
            lambda r: float(gs_season.get((str(r["season"]), str(r["canonical_name"])), float("nan"))),
            axis=1),
        **{f"{k}_rate": pool_df.apply(
            lambda r, k=k: float(bb_season.get(_skey(r), {}).get(k, float("nan"))), axis=1)
           for k in ("hh", "ld", "fb", "gb")},
    }
    career_pool_df = career[career["pa"] >= 50]
    _ckey = lambda r: str(r["canonical_name"])  # noqa: E731
    career_pool_vals = {
        "avg": career_pool_df["avg"], "obp": career_pool_df["obp"],
        "slg": career_pool_df["slg"], "ops": career_pool_df["ops"],
        "iso": career_pool_df["slg"] - career_pool_df["avg"],
        "bb_rate": career_pool_df["bb"] / career_pool_df["pa"],
        "xbh_rate": (career_pool_df["2b"] + career_pool_df["3b"] + career_pool_df["hr"]) / career_pool_df["pa"],
        "woba": _adv_series(career_pool_df, _ckey, career_adv, "woba"),
        "wrc_plus": _adv_series(career_pool_df, _ckey, career_adv, "wrc_plus"),
        "gs": career_pool_df.apply(
            lambda r: float(gs_career.get(str(r["canonical_name"]), float("nan"))), axis=1),
        **{f"{k}_rate": career_pool_df.apply(
            lambda r, k=k: float(career_bb.get(_ckey(r), {}).get(k, float("nan"))), axis=1)
           for k in ("hh", "ld", "fb", "gb")},
    }

    def pct_block(row, season_name: str | None, canonical: str, label: str, pools: dict) -> dict | None:
        vals = stat_vals(row, season_name, canonical)
        metrics = [
            {"key": key, "label": lab, "value": vals[key], "pct": percentile_of(pools[key], vals[key])}
            for key, lab in METRICS
            if vals.get(key) is not None
        ]
        return {"season": label, "metrics": metrics} if metrics else None

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

        my_seasons = season_stats[season_stats["canonical_name"] == canonical].sort_values(
            "season", key=lambda s: s.map(season_sort_key), ascending=False)
        season_blocks = []
        for _, srow in my_seasons.iterrows():
            label = season_label(str(srow["season"]))
            blk = pct_block(srow, str(srow["season"]), canonical, label, pool_vals)
            if blk:
                season_blocks.append({"slug": slugify(label), "label": label,
                                      "metrics": blk["metrics"]})
        career_block = None
        if float(c["pa"]) >= 50:
            blk = pct_block(c, None, canonical, "Career", career_pool_vals)
            if blk:
                career_block = {"slug": "career", "label": "Career", "metrics": blk["metrics"]}
        percentiles = (
            {"seasons": season_blocks, "career": career_block}
            if (season_blocks or career_block) else None
        )

        def _game_best(key: str) -> dict | None:
            top = None
            for g in log_rows:
                v = g.get(key)
                if v is not None and (top is None or v > top[0]):
                    top = (v, g)
            if top is None or (top[0] or 0) <= 0:
                return None
            return {"v": top[0], "date": top[1]["game_date"], "opponent": top[1]["opponent"]}

        def _season_best(key: str, min_pa: int = 0) -> dict | None:
            candidates = [s for s in p_seasons
                          if (s.get("pa") or 0) >= min_pa and s.get(key) is not None]
            if not candidates:
                return None
            top = max(candidates, key=lambda s: s[key])
            return {"v": top[key], "label": top["label"]} if (top[key] or 0) > 0 else None

        bests = {
            "game": {k: _game_best(k) for k in ("hits", "hr", "rbi", "r", "bb", "tb", "game_score")},
            "season": {
                **{k: _season_best(k) for k in ("hits", "hr", "rbi", "r", "bb", "tb")},
                "avg": _season_best("avg", 20),
                "ops": _season_best("ops", 20),
            },
        }

        for row in p_seasons:
            row.update(adv_fields(row, row["season"], canonical))

        career_row = {k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v))
                      for k, v in c.items()}
        career_row.update(adv_fields(career_row, None, canonical))
        players_out.append({
            "name": name,
            "canonical": canonical,
            "slug": slug,
            "active": canonical in active,
            "career": career_row,
            "seasons": p_seasons,
            "game_log": log_rows,
            "percentiles": percentiles,
            "bests": bests,
            "potw": int(potw_counts.get(canonical, 0)),
            "best_week": float(best_weeks[canonical]) if canonical in best_weeks else None,
        })
    players_out.sort(key=lambda p: (-int(p["career"].get("games") or 0), p["name"]))

    # ---- milestone events: when each career rung was actually cleared (dated) ----
    STAT_COLS = {"Games": None, "PA": "pa", "AB": "ab", "Hits": "hits", "Singles": "1b",
                 "Doubles": "2b", "Triples": "3b", "HR": "hr", "RBI": "rbi", "Runs": "r",
                 "Walks": "bb", "Total Bases": "tb"}
    reached: list[dict] = []
    ordered_games = games.sort_values(["game_date", "game_time"])
    for canon, grp in ordered_games.groupby("canonical_name"):
        cum: dict[str, float] = {}
        for _, g in grp.iterrows():
            for stat, col in STAT_COLS.items():
                inc = 1.0 if col is None else float(g[col] or 0)
                if inc <= 0:
                    continue
                prev = cum.get(stat, 0.0)
                cum[stat] = prev + inc
                for rung in extend_milestone_ladder(MILESTONE_LADDERS[stat], int(cum[stat])):
                    if prev < rung <= cum[stat]:
                        reached.append({
                            "player": display_name.get(str(canon), str(canon).title()),
                            "slug": slugify(str(canon)), "stat": stat, "milestone": int(rung),
                            "date": str(g["game_date"]), "opponent": str(g["opponent"]),
                            "label": season_label(str(g["season"])),
                        })
                    elif rung > cum[stat]:
                        break
    reached.sort(key=lambda r: r["date"], reverse=True)

    ms = fetch_career_milestones(connection, active_only=True)
    ms_rows = records(
        ms.sort_values(["remaining", "stat"]),
        ["player", "canonical_name", "stat", "current_total", "next_milestone_display",
         "remaining", "progress_to_next", "club_label"])
    for r in ms_rows:
        r["slug"] = slugify(r["canonical_name"])
        r["player"] = clean_name(r["player"])
        r["progress_to_next"] = round(float(r["progress_to_next"] or 0), 3)

    # ---- records.json (the record book, incl. streaks) ----
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

    def _short_date(iso: str) -> str:
        d = datetime.strptime(iso, "%Y-%m-%d")
        return f"{d:%b} {d.day} '{d:%y}"

    STREAK_DEFS = [
        ("Longest Hit Streak", lambda g: float(g["hits"] or 0) > 0),
        ("Longest On-Base Streak", lambda g: float(g["hits"] or 0) + float(g["bb"] or 0) > 0),
        ("Longest XBH Streak",
         lambda g: float(g["2b"] or 0) + float(g["3b"] or 0) + float(g["hr"] or 0) > 0),
        ("Longest HR Streak", lambda g: float(g["hr"] or 0) > 0),
    ]
    for label, flag in STREAK_DEFS:
        best: dict[str, tuple] = {}
        for canon, grp in ordered_games.groupby("canonical_name"):
            run, start, top = 0, None, (0, None, None)
            for _, g in grp.iterrows():
                if flag(g):
                    run += 1
                    start = start or str(g["game_date"])
                    if run > top[0]:
                        top = (run, start, str(g["game_date"]))
                else:
                    run, start = 0, None
            if top[0] > 1:  # a one-game "streak" isn't a record
                best[str(canon)] = top
        ranked = sorted(best.items(), key=lambda kv: (-kv[1][0], kv[0]))[:5]
        records_out["career"].append({"label": label, "rows": [{
            "rank": i + 1,
            "player": display_name.get(canon, canon.title()),
            "slug": slugify(canon),
            "value": int(length),
            "season": None, "date": None, "opponent": None,
            "span": (f"{_short_date(sd)} – {_short_date(ed)}" if sd != ed else _short_date(sd)),
        } for i, (canon, (length, sd, ed)) in enumerate(ranked)]})
    dump("records.json", records_out)

    # ---- attach honors + milestones to each player ----
    records_held: dict[str, list] = {}
    scope_names = {"career": "Career", "single_season": "Season", "single_game": "Game"}
    for scope, boards in records_out.items():
        for b in boards:
            for r in b["rows"]:
                if r["rank"] != 1:
                    continue
                context = r.get("span") or r.get("season") or (
                    f"{r['date']} vs {r['opponent']}" if r.get("date") else None)
                records_held.setdefault(r["slug"], []).append({
                    "scope": scope_names.get(scope, scope), "label": b["label"],
                    "value": r["value"], "context": context,
                })
    reached_by_slug: dict[str, list] = {}
    for ev in reached:
        reached_by_slug.setdefault(ev["slug"], []).append(ev)
    ms_by_slug: dict[str, list] = {}
    for r in ms_rows:
        ms_by_slug.setdefault(r["slug"], []).append(r)
    for p in players_out:
        p["honors"] = records_held.get(p["slug"], [])
        p["milestones"] = {
            "next": ms_by_slug.get(p["slug"], [])[:5],
            "reached": reached_by_slug.get(p["slug"], []),
        }

    dump("players.json", players_out)

    # ---- career_stats.json (Career view for the stats page) ----
    standard = []
    for p in players_out:
        c0 = p["career"]
        standard.append({
            "player": p["name"], "slug": p["slug"], "active": p["active"],
            "seasons": c0.get("seasons_played"),
            **{k: c0.get(k) for k in ("games", "pa", "ab", "hits", "1b", "2b", "3b", "hr",
                                       "bb", "r", "rbi", "tb", "avg", "obp", "slg", "ops")},
        })
    standard.sort(key=lambda r: -(r["ops"] or 0))
    adv_career_rows = []
    for p in players_out:
        a = career_adv.get(p["canonical"])
        if a is None:
            continue
        c0 = p["career"]
        adv_career_rows.append({
            "player": p["name"], "slug": p["slug"], "pa": c0.get("pa"),
            "woba": opt(a["woba"]), "wrc_plus": opt(a["wrc_plus"], 1),
            "iso": c0.get("iso"), "bb_rate": c0.get("bb_rate"), "xbh_rate": c0.get("xbh_rate"),
            "gs_avg": c0.get("gs_avg"),
            "rar": opt(a["rar"], 1), "owar": opt(a["owar"], 2), "archetype": str(a["archetype"]),
        })
    adv_career_rows.sort(key=lambda r: -(r["rar"] if r["rar"] is not None else -999.0))
    franchise = team_line(career)
    franchise["games"] = int(len(games[["season", "game_date", "game_time"]].drop_duplicates()))
    dump("career_stats.json", {
        "standard": standard,
        "advanced": adv_career_rows,
        "franchise": franchise,
        "seasons_count": len(seasons),
    })

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

    # ---- milestones.json (active watch + recently reached) ----
    dump("milestones.json", {"watch": ms_rows, "recent": reached[:15]})

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
