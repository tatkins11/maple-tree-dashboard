"""Game-facing stats export — the DATA CONTRACT for 'Maple Tree: The Game'.

Runs after the normal weekly stats update. Writes a versioned extract to the game's
inbox (C:/MapleTreeGame/data/imports/). This project OWNS the shape; the game consumes
it read-only and NEVER reads this project's internals. The imports folder is the only
crossing point.

Rules honored:
  * Shape is a contract — version lives in manifest.json; changes are versioned, never silent.
  * Idempotent — re-running overwrites cleanly (stale per-season files are pruned first).
  * Missing data is EXPLICIT null + a manifest note — never zero-filled (missing != bad).

Run:  python scripts/export_game_extract.py
"""
from __future__ import annotations

import csv
import json
import re
from datetime import date
from pathlib import Path

EXTRACT_VERSION = "1.0.2"
REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "site" / "src" / "data"
RAW = REPO / "data" / "raw" / "season_csv"
OUT = Path("C:/MapleTreeGame/data/imports")

# Park dimensions — confirmed by Brian 2026-07-23. The org plays the Boncosky complex;
# every field is symmetric ("all around"). Green is the big one at 350; Blue, Red and
# Yellow are 300. All four are stated figures, not estimates (v1.0.2 dropped the
# approximate flag after Brian confirmed). The brief said "three fields"; there are four.
_SYM = lambda ft: {"left_ft": ft, "center_ft": ft, "right_ft": ft, "symmetric": True}
PARK_DIMS: dict[str, dict | None] = {
    "Boncosky Green": _SYM(350),
    "Boncosky Blue": _SYM(300),
    "Boncosky Red": _SYM(300),
    "Boncosky Yellow": _SYM(300),
}
NOTES: list[str] = []


def load(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def year_of(slug, name):
    m = re.search(r"(20\d\d)", slug) or re.search(r"(20\d\d)", name)
    return int(m.group(1)) if m else None


# ------------------------------------------------------------------ batted-ball
def _name_map(players_j):
    m = {}
    for p in players_j:
        for k in (p.get("name"), p.get("canonical")):
            if k:
                m[k.strip().lower()] = p["slug"]
        for se in p.get("seasons", []):
            if se.get("player"):
                m[se["player"].strip().lower()] = p["slug"]
    return m


ALIAS = {"teo": "tristan", "snaxx": "snaxx", "joey": "snaxx"}


def bb_from_csv(path: Path, name2slug: dict) -> dict[str, dict]:
    """{slug: {ld,fb,gb,hh}} for a season CSV. Uses the FIRST (batting) header block;
    resolves the period-correct GameChanger name to a slug (+ historical aliases)."""
    try:
        rows = list(csv.reader(open(path, encoding="utf-8-sig")))
    except OSError:
        return {}
    if len(rows) < 3:
        return {}
    idx = {}
    for i, nm in enumerate(rows[1]):
        n = (nm or "").strip()
        if n and n not in idx:
            idx[n] = i
    if not {"First", "Last", "AB", "SO", "SF", "HHB", "LD%", "FB%", "GB%"} <= set(idx):
        return {}

    def cell(r, k):
        j = idx[k]
        return (r[j] if j < len(r) else "") or ""

    def num(r, k):
        try:
            return float(cell(r, k).replace(",", "") or 0)
        except ValueError:
            return 0.0

    out = {}
    for r in rows[2:]:
        if not r or (r[0] or "").strip().lower() == "totals":
            continue
        first, last = cell(r, "First").strip(), cell(r, "Last").strip()
        name = first or last
        if not name:
            continue
        slug = None
        for key in (name.lower(), f"{first} {last}".strip().lower(), f"{last} {first}".strip().lower()):
            slug = ALIAS.get(key) or name2slug.get(key)
            if slug:
                break
        if not slug:
            continue
        batted = max(num(r, "AB") - num(r, "SO") + num(r, "SF"), 0.0)
        if batted <= 0:
            continue
        out[slug] = {
            "ld": round(num(r, "LD%") / 100.0, 4),
            "fb": round(num(r, "FB%") / 100.0, 4),
            "gb": round(num(r, "GB%") / 100.0, 4),
            "hh": round(num(r, "HHB") / batted, 4),
            "balls_in_play": int(batted),
        }
    return out


def csv_for_season(season_name: str) -> Path | None:
    target = f"{season_name} stats".lower().replace("  ", " ")
    for p in RAW.glob("*.csv"):
        if p.stem.lower() == target or p.stem.lower().startswith(season_name.lower()):
            return p
    return None


# ------------------------------------------------------------------ build
def main():
    OUT.mkdir(parents=True, exist_ok=True)
    season_stats = load("season_stats.json")
    schedule = load("schedule.json")
    rivalry = load("rivalry.json")
    players_j = load("players.json")
    name2slug = _name_map(players_j)
    sched_by_slug = {s["slug"]: s for s in schedule}

    # prune stale per-season files (idempotent clean overwrite)
    for f in OUT.glob("players_*.json"):
        f.unlink()
    for f in OUT.glob("games_*.json"):
        f.unlink()

    NUM = ["games", "pa", "ab", "hits", "1b", "2b", "3b", "hr", "bb", "r", "rbi", "tb"]
    RATE = ["avg", "obp", "slg", "ops"]
    seasons_meta, row_counts = [], {"players_by_season": {}, "games_by_season": {}}
    bb_covered = bb_total = 0

    for s in season_stats:
        slug, name = s["slug"], s["name"]
        yr = year_of(slug, name)
        csvp = csv_for_season(name)
        bb = bb_from_csv(csvp, name2slug) if csvp else {}

        players = []
        for pl in s["players"]:
            psl = pl["slug"]
            line = {k: pl.get(k) for k in NUM}
            line.update({k: (round(float(pl[k]), 4) if pl.get(k) is not None else None) for k in RATE})
            prof = bb.get(psl)
            bb_total += 1
            if prof:
                bb_covered += 1
            players.append({
                "slug": psl, "player": pl["player"],
                "batting": line,
                # explicit-missing: null where GameChanger didn't track it, never 0
                "batted_ball": prof if prof else None,
                "spray": None,  # pull/center/oppo direction not tracked in source
            })
        (OUT / f"players_{slug}.json").write_text(
            json.dumps({"season": slug, "season_name": name, "year": yr, "players": players},
                       indent=2), encoding="utf-8")
        row_counts["players_by_season"][slug] = len(players)

        # games
        games = []
        for g in sched_by_slug.get(slug, {}).get("games", []):
            done = g.get("status") == "completed" and g.get("completed_flag")
            games.append({
                "date": g.get("game_date"), "time": g.get("game_time"),
                "week": g.get("week_label"), "opponent": g.get("opponent_name"),
                "park": g.get("location_or_field"), "home_away": g.get("home_away"),
                "is_bye": bool(g.get("is_bye")),
                "result": g.get("result") if done else None,
                "runs_for": int(g["runs_for"]) if done and g.get("runs_for") is not None else None,
                "runs_against": int(g["runs_against"]) if done and g.get("runs_against") is not None else None,
                "status": g.get("status"),
            })
        (OUT / f"games_{slug}.json").write_text(
            json.dumps({"season": slug, "season_name": name, "year": yr, "games": games},
                       indent=2), encoding="utf-8")
        row_counts["games_by_season"][slug] = len(games)
        seasons_meta.append({"slug": slug, "name": name, "label": s["label"], "year": yr,
                             "record": s["record"], "players": len(players), "games": len(games)})

    # rosters
    rosters = {s["slug"]: [{"slug": pl["slug"], "player": pl["player"]} for pl in s["players"]]
               for s in season_stats}
    (OUT / "rosters.json").write_text(json.dumps({"by_season": rosters}, indent=2), encoding="utf-8")

    # opponents (team-level real; player-level explicitly absent)
    opps = []
    for o in rivalry:
        opps.append({
            "opponent": o["opponent"], "games": o["games"], "record": o.get("record"),
            "wins": o["wins"], "losses": o["losses"], "ties": o.get("ties", 0),
            "runs_for": o["runs_for"], "runs_against": o["runs_against"], "run_diff": o["run_diff"],
            "first_played": o.get("first_played"), "last_played": o.get("last_played"),
            "hitters": None, "pitching": None,  # opponent player stats are not tracked
        })
    (OUT / "opponents.json").write_text(
        json.dumps({"note": "Team-level only. Opponent player/pitching stats are not tracked "
                            "in source (GameChanger records our team). Use run_for/run_against "
                            "and record for mock-team ratings.", "opponents": opps}, indent=2),
        encoding="utf-8")

    # parks
    park_games: dict[str, int] = {}
    for s in schedule:
        for g in s["games"]:
            f = g.get("location_or_field")
            if f:
                park_games[f] = park_games.get(f, 0) + 1
    parks = [{"name": p, "dimensions_ft": PARK_DIMS.get(p),
              "games_played": park_games[p]} for p in sorted(park_games)]
    (OUT / "parks.json").write_text(
        json.dumps({"note": "Boncosky complex, four fields, all symmetric ('all around'). "
                            "Green 350; Blue, Red and Yellow 300. Confirmed by Brian "
                            "2026-07-23 — these are stated figures, not estimates.",
                    "parks": parks}, indent=2), encoding="utf-8")

    if bb_total:
        NOTES.append(f"batted_ball coverage: {bb_covered}/{bb_total} player-seasons had LD/FB/GB/HH "
                     "in source; the rest are null (not tracked that season), never zero-filled.")
    NOTES.append("spray direction (pull/center/oppo) is null everywhere — not tracked in source.")
    NOTES.append("opponents.json is team-level BY DESIGN — opponent player/pitching stats are not "
                 "recorded in any source (GameChanger tracks our team only). hitters/pitching are "
                 "null permanently; this is not a pending gap.")
    NOTES.append("parks.json dimensions_ft are populated and confirmed exact (Boncosky Green 350 "
                 "all around; Blue, Red and Yellow 300). The brief said three fields; there are four.")
    NOTES.append("games_*.json carry score-level real results (runs_for/against, result). Per-game "
                 "batting box lines are a v1.1 enrichment (only recently-tracked games have them).")

    manifest = {
        "extract_version": EXTRACT_VERSION,
        "generated_at": date.today().isoformat(),
        # Contract history — changes are versioned here, never silent.
        "changelog": {
            "1.0.2": "Park dimensions confirmed exact — dropped the `approximate` flag "
                     "(Green 350, Blue/Red/Yellow 300, all stated figures). Also confirmed: "
                     "opponent stats are team-level by design, not a pending gap.",
            "1.0.1": "parks.json dimensions_ft populated. Data-fill only — NO shape change "
                     "from 1.0.0, so 1.0.0 consumers stay compatible.",
            "1.0.0": "Initial contract: manifest, per-season players/games, rosters, "
                     "opponents, parks.",
        },
        "source_project": "slowpitch_optimizer",
        "consumer": "Maple Tree: The Game",
        "seasons": seasons_meta,
        "row_counts": {
            **row_counts,
            "seasons": len(seasons_meta),
            "opponents": len(opps),
            "parks": len(parks),
        },
        "files": {
            "manifest.json": "this file — version, coverage, row counts, schema, notes.",
            "players_<season>.json": "{season, season_name, year, players:[{slug, player, "
                                     "batting:{games,pa,ab,hits,1b,2b,3b,hr,bb,r,rbi,tb,avg,obp,slg,ops}, "
                                     "batted_ball:{ld,fb,gb,hh,balls_in_play}|null, spray:null}]}",
            "rosters.json": "{by_season:{<season>:[{slug, player}]}}",
            "games_<season>.json": "{season, season_name, year, games:[{date,time,week,opponent,park,"
                                    "home_away,is_bye,result|null,runs_for|null,runs_against|null,status}]}",
            "opponents.json": "{note, opponents:[{opponent,games,record,wins,losses,ties,runs_for,"
                              "runs_against,run_diff,first_played,last_played,hitters:null,pitching:null}]}",
            "parks.json": "{note, parks:[{name, dimensions_ft:null, games_played}]}",
        },
        "types": {
            "rates (avg/obp/slg/ops)": "float rounded to 4dp, or null",
            "counts": "int",
            "batted_ball rates (ld/fb/gb/hh)": "float 0..1 (share of balls in play), or null when untracked",
            "missing": "null + a manifest note — never zero-filled",
        },
        "notes": NOTES,
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Game extract v{EXTRACT_VERSION} -> {OUT}")
    print(f"  {len(seasons_meta)} seasons | players+games per season | "
          f"{len(opps)} opponents | {len(parks)} parks")
    print(f"  batted-ball coverage: {bb_covered}/{bb_total} player-seasons")
    for f in sorted(OUT.glob("*.json")):
        print(f"    {f.name}  ({f.stat().st_size} B)")


if __name__ == "__main__":
    main()
