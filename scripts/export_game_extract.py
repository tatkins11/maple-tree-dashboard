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
import sqlite3
from datetime import date
from pathlib import Path

EXTRACT_VERSION = "1.2.0"
REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "site" / "src" / "data"
RAW = REPO / "data" / "raw" / "season_csv"
DB = REPO / "db" / "all_seasons_identity.sqlite"
BOXGAMES = REPO / "data" / "processed" / "game_boxscore_games.csv"
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


def real_innings() -> dict:
    """{(date, opponent, team_score): innings} — REAL innings read off the GameChanger
    linescore at sync time. Blank until a week is captured; historical games pending backfill."""
    out = {}
    if not BOXGAMES.exists():
        return out
    for r in csv.DictReader(open(BOXGAMES, encoding="utf-8-sig")):
        v = (r.get("innings") or "").strip()
        if not v:
            continue
        try:
            out[(r["game_date"], (r["opponent_name"] or "").strip(), int(r["team_score"]))] = float(v)
        except (ValueError, KeyError):
            continue
    return out


def per_game_box() -> dict:
    """{(season, date, opponent): [ {team_score, opponent_score, box} ]} — team batting
    line per game, summed from player_game_batting. Doubleheaders share a key, so the
    caller disambiguates on score."""
    if not DB.exists():
        return {}
    q = """
    select g.season, g.game_date, g.opponent_name, g.team_score, g.opponent_score,
           sum(b.plate_appearances), sum(b.at_bats),
           sum(b.singles + b.doubles + b.triples + b.home_runs),
           sum(b.doubles), sum(b.triples), sum(b.home_runs),
           sum(b.walks), sum(b.runs), sum(b.rbi),
           sum(b.outs), sum(b.sacrifice_flies)
    from games g join player_game_batting b on b.game_id = g.game_id
    group by g.game_id
    """
    out: dict = {}
    con = sqlite3.connect(DB)
    try:
        for (season, gdate, opp, ts, os_, pa, ab, hits, d2, d3, hr, bb, r, rbi, outs, sf) in con.execute(q):
            i = lambda v: int(v or 0)  # noqa: E731
            out.setdefault((season, gdate, (opp or "").strip()), []).append({
                "team_score": ts, "opponent_score": os_,
                "box": {"pa": i(pa), "ab": i(ab), "hits": i(hits), "2b": i(d2), "3b": i(d3),
                        "hr": i(hr), "bb": i(bb), "r": i(r), "rbi": i(rbi),
                        # DERIVE outs as (AB - H + SF). The scorebook's stored `outs`
                        # column is under-populated — it disagrees with AB-H in 76 of 82
                        # games, every season, always low (one 2021 game stored 1 out on
                        # 38 PA). Deriving keeps the box self-consistent.
                        "outs": max(i(ab) - i(hits) + i(sf), 0)},
            })
    finally:
        con.close()
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
    boxes = per_game_box()
    innings_real = real_innings()
    box_hit = box_miss = inn_known = 0

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
            # A game is done if it is FLAGGED complete and carries runs. Do NOT gate on the
            # status string: 2026 rows say "completed" but every pre-2026 row says "final",
            # which silently nulled 61 real results in v1.0.x.
            rf, ra = g.get("runs_for"), g.get("runs_against")
            done = bool(g.get("completed_flag")) and rf is not None and ra is not None
            rf_i = int(rf) if done else None
            ra_i = int(ra) if done else None

            box = None
            cands = boxes.get((name, g.get("game_date"), (g.get("opponent_name") or "").strip()), [])
            if len(cands) == 1:
                box = cands[0]["box"]
            elif cands:  # doubleheader — disambiguate on the score
                for cd in cands:
                    if cd["team_score"] == rf_i and cd["opponent_score"] == ra_i:
                        box = cd["box"]
                        break
            if done:
                if box:
                    box_hit += 1
                else:
                    box_miss += 1

            # Innings: GameChanger's real per-game innings were never imported (no linescore
            # in any source), so that stays null. The DERIVED estimate below is the usable
            # early-ending signal — a full game is ~21 outs, so a run-rule or time-capped
            # game lands well under that. Flagged unreliable when the box is clearly partial.
            inn_val = innings_real.get((g.get("game_date"), (g.get("opponent_name") or "").strip(), rf_i))
            if inn_val is not None:
                inn_known += 1
            est = None
            if box and box["outs"] > 0:
                est = {"value": round(box["outs"] / 3.0, 1),
                       "derived_from": "(batting outs + sacrifice flies) / 3",
                       "reliable": box["outs"] >= 9}

            games.append({
                "date": g.get("game_date"), "time": g.get("game_time"),
                "week": g.get("week_label"), "opponent": g.get("opponent_name"),
                "park": g.get("location_or_field"), "home_away": g.get("home_away"),
                "is_bye": bool(g.get("is_bye")),
                "result": g.get("result") if done else None,
                "runs_for": rf_i, "runs_against": ra_i,
                "status": g.get("status"),
                "box": box,
                "innings": inn_val,
                "innings_batted_est": est,
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
    NOTES.append(f"games_*.json carry real results for ALL six seasons (v1.1.0 fixed a done-test "
                 f"that gated on status=='completed' and silently nulled every pre-2026 game, which "
                 f"uses 'final'). Per-game `box` present for {box_hit} of {box_hit + box_miss} "
                 f"completed games; null for the rest.")
    NOTES.append(f"`innings` is REAL where present ({inn_known} of {box_hit + box_miss} completed "
                 "games) — read off the GameChanger linescore in the weekly box-score screenshots "
                 "and captured at sync time. Null means not yet backfilled, NOT zero innings. Where "
                 "null, use innings_batted_est. (The season CSVs' INN column is fielding innings by "
                 "position and is empty — it is not this.)")
    NOTES.append("`innings_batted_est` is DERIVED, not source: (AB - H + SF) / 3. It does NOT use the "
                 "scorebook's stored `outs` column, which is under-populated (disagrees with AB-H "
                 "in 76 of 82 games, always low). Use it as "
                 "the early-ending signal — a full game is ~21 outs, so run-rule/time-capped games "
                 "sit well below. `reliable:false` marks boxes with under 9 outs, which are almost "
                 "certainly incomplete scorebook entries rather than 3-inning games — do not read "
                 "those as slaughters without cross-checking the score margin.")
    NOTES.append("OUT-ACCOUNTING behind innings_batted_est (what is and isn't counted): "
                 "SF = an out, COUNTED. FC = batter safe but a runner is retired, so the out total "
                 "is unchanged — negligible, ignored. GIDP = 2 outs, but the column is populated "
                 "ONLY for 2021 (0 elsewhere means untracked, NOT zero double plays) — deliberately "
                 "EXCLUDED so all six seasons use one formula; including it would inflate 2021 "
                 "alone to an impossible 10.3-inning max. Runners retired on the bases are not "
                 "tracked at all. Net: the estimate misses some real outs (GIDP, baserunning) and "
                 "adds some false ones (ROE, untracked per game), which partly cancel.")
    NOTES.append("KNOWN BIAS on innings_batted_est: reached-on-error is not tracked per game, so "
                 "(AB-H+SF) counts those at-bats as outs and the estimate runs slightly HIGH "
                 "(one game reads 8.0 in a 7-inning league). Use it to RANK games short-vs-long — "
                 "it is a relative early-ending signal, not a literal inning count. The scorebook's "
                 "own `outs` column is worse and deliberately unused: for older imports it was "
                 "never populated (a 2021 game stores 0 outs for 10 of 11 hitters), and where it "
                 "was populated it counts batter-outs only, excluding fielder's choice and ROE.")

    manifest = {
        "extract_version": EXTRACT_VERSION,
        "generated_at": date.today().isoformat(),
        # Contract history — changes are versioned here, never silent.
        "changelog": {
            "1.2.0": "`innings` is now REAL where captured, not permanently null. GameChanger's "
                     "linescore is in the weekly box-score screenshots and is now recorded at sync "
                     "time (new `innings` column on game_boxscore_games.csv). Week 5 backfilled "
                     "from its linescores (5 and 6). Historical games stay null pending backfill; "
                     "innings_batted_est remains the fallback signal.",
            "1.1.3": "Docs only. Full out-accounting recorded (per Brian): SF is an out and IS "
                     "counted; GIDP is 2 outs but is tracked ONLY in 2021, so it is deliberately "
                     "EXCLUDED — folding it in would inflate that one season against the other "
                     "five (max jumps to an impossible 10.3 innings) and break cross-season "
                     "comparability; baserunning outs are untracked; FC is negligible and "
                     "self-cancelling. Formula stays uniform at (AB-H+SF)/3.",
            "1.1.2": "Docs only, no data change: innings_batted_est now states its known bias. "
                     "(AB-H+SF) counts reached-on-error as an out because ROE is not tracked "
                     "per game, so the estimate runs slightly HIGH — one game reads 8.0 in a "
                     "7-inning league. Treat it as a RELATIVE short-vs-long signal, not a "
                     "literal inning count.",
            "1.1.1": "FIX: per-game `outs` (and therefore innings_batted_est) is now DERIVED "
                     "as (AB - H + SF). v1.1.0 trusted the scorebook's stored `outs` column, "
                     "which is under-populated — it disagrees with AB-H in 76 of 82 games and "
                     "produced impossible estimates (a full 38-PA game read as 0.7 innings). "
                     "All 82 games now estimate cleanly; zero fall below the reliability floor.",
            "1.1.0": "FIX + new fields. Per-game results now emit for ALL SIX seasons: the "
                     "done-test gated on status=='completed', but every pre-2026 row uses "
                     "'final', which silently nulled 61 real results in v1.0.x. Adds per-game "
                     "`box` (team batting line from the club scorebook), `innings` (always null "
                     "— see notes) and `innings_batted_est` (DERIVED early-ending signal).",
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
                                    "home_away,is_bye,result|null,runs_for|null,runs_against|null,status,"
                                    "box:{pa,ab,hits,2b,3b,hr,bb,r,rbi,outs}|null, innings:null, "
                                    "innings_batted_est:{value,derived_from,reliable}|null}]}",
            "opponents.json": "{note, opponents:[{opponent,games,record,wins,losses,ties,runs_for,"
                              "runs_against,run_diff,first_played,last_played,hitters:null,pitching:null}]}",
            "parks.json": "{note, parks:[{name, dimensions_ft:null, games_played}]}",
        },
        "types": {
            "rates (avg/obp/slg/ops)": "float rounded to 4dp, or null",
            "counts": "int",
            "batted_ball rates (ld/fb/gb/hh)": "float 0..1 (share of balls in play), or null when untracked",
            "missing": "null + a manifest note — never zero-filled",
            "innings": "always null (never captured); use innings_batted_est, which is DERIVED",
        },
        "notes": NOTES,
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Game extract v{EXTRACT_VERSION} -> {OUT}")
    print(f"  {len(seasons_meta)} seasons | players+games per season | "
          f"{len(opps)} opponents | {len(parks)} parks")
    print(f"  batted-ball coverage: {bb_covered}/{bb_total} player-seasons")
    print(f"  per-game box lines: {box_hit}/{box_hit + box_miss} completed games")
    print(f"  REAL innings (from linescore): {inn_known}/{box_hit + box_miss}")
    for f in sorted(OUT.glob("*.json")):
        print(f"    {f.name}  ({f.stat().st_size} B)")


if __name__ == "__main__":
    main()
