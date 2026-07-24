"""Week 3 (2026-07-08) data sync — Maple Tree split with Brew Crew.

Game 1 (6:30, away): Maple Tree 10, Brew Crew 5  (W)  — Corey ejected (Hennessy on the bench)
Game 2 (7:30, home): Brew Crew 11, Maple Tree 4  (L)  — Kives hurt his ankle running into the fence

Strategy:
  * Game 1 per-player lines are hand-transcribed from the GameChanger screenshot and
    self-verified against the game-1 team totals.
  * Game 2 per-player lines are DERIVED exactly as (new season CSV - old season CSV - game1),
    then self-verified against the game-2 team totals. This avoids screenshot-read ambiguity.

Updates (idempotent — safe to re-run): box score games+batting, team_schedule g1/g2,
league_schedule 10 games, and copies the new season CSV into data/raw.
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NEW_CSV = Path("C:/Users/TristanAtkins/Downloads/Maple Tree Summer 2026 Stats (1).csv")
OLD_CSV = REPO / "data/raw/season_csv/Maple Tree Summer 2026 Stats.csv"
BAT = REPO / "data/processed/game_boxscore_batting.csv"
GAMES = REPO / "data/processed/game_boxscore_games.csv"
TEAM = REPO / "data/processed/team_schedule.csv"
LEAGUE = REPO / "data/processed/league_schedule_games.csv"

STATS = ["pa", "ab", "h", "1b", "2b", "3b", "hr", "rbi", "r", "bb", "so", "sf", "fc"]
# 0-based column index of each stat in the GameChanger season CSV
COL = {"pa": 4, "ab": 5, "h": 10, "1b": 11, "2b": 12, "3b": 13, "hr": 14,
       "rbi": 15, "r": 16, "bb": 17, "so": 18, "sf": 22, "fc": 24}
ROSTER = {"Corey", "Duff", "Glove", "Jason", "JJ", "Joel", "Kives", "Porter", "Tim", "Tristan", "Walsh"}

G1_KEY = "maple-tree-summer-2026-07-08-1830-brew-crew"
G2_KEY = "maple-tree-summer-2026-07-08-1930-brew-crew"

# --- Game 1 (verified vs team totals) --------------------------------------
G1_ORDER = ["Glove", "Kives", "Tristan", "Tim", "JJ", "Porter", "Corey", "Joel", "Walsh", "Duff", "Jason"]
GAME1 = {
    #            pa ab  h 1b 2b 3b hr rbi r bb so sf fc
    "Glove":   (4, 4, 3, 3, 0, 0, 0, 0, 2, 0, 0, 0, 0),
    "Kives":   (4, 4, 3, 2, 1, 0, 0, 0, 2, 0, 0, 0, 0),
    "Tristan": (4, 4, 3, 0, 2, 1, 0, 4, 3, 0, 0, 0, 0),
    "Tim":     (4, 4, 2, 1, 0, 0, 1, 3, 1, 0, 0, 0, 0),
    "JJ":      (4, 4, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0),
    "Porter":  (4, 4, 2, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0),
    "Corey":   (3, 3, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0),
    "Joel":    (3, 3, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Walsh":   (3, 3, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1),
    "Duff":    (3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Jason":   (3, 3, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0),
}
G1_TEAM = dict(pa=39, ab=39, h=18, **{"1b": 11, "2b": 4, "3b": 2}, hr=1, rbi=10, r=10, bb=0, so=1, sf=0, fc=1)
# Game 2 team totals read off the screenshot (used only as an independent check)
G2_TEAM = dict(pa=31, ab=29, h=10, **{"1b": 9, "2b": 1, "3b": 0}, hr=0, rbi=4, r=4, bb=0, so=0, sf=2, fc=3)
G2_ORDER = ["Glove", "Kives", "Tristan", "Tim", "JJ", "Porter", "Joel", "Walsh", "Duff", "Jason"]  # no Corey


def parse_season(path: Path) -> dict[str, dict[str, int]]:
    out = {}
    for r in csv.reader(open(path, encoding="utf-8-sig")):
        if len(r) > 24 and r[2] in ROSTER:
            out[r[2]] = {s: int(r[COL[s]]) for s in STATS}
    return out


def team_totals(lines: dict[str, tuple]) -> dict[str, int]:
    return {s: sum(v[i] for v in lines.values()) for i, s in enumerate(STATS)}


def check(label, got, want):
    bad = {s: (got[s], want[s]) for s in STATS if got[s] != want[s]}
    assert not bad, f"{label} MISMATCH {bad}"
    print(f"  {label} team totals OK: " + " ".join(f"{s.upper()}{got[s]}" for s in STATS))


def main(write=False):
    old, new = parse_season(OLD_CSV), parse_season(NEW_CSV)
    print("Parsed season CSVs:", len(old), "old players,", len(new), "new players")

    # verify game 1
    check("GAME1", team_totals(GAME1), G1_TEAM)

    # derive game 2 = (new - old) - game1
    game2 = {}
    for name in G2_ORDER:
        row = tuple((new[name][s] - old[name][s]) - GAME1[name][i] for i, s in enumerate(STATS))
        assert all(v >= 0 for v in row), f"NEGATIVE derived stat for {name}: {dict(zip(STATS, row))}"
        game2[name] = row
    # Corey played only game 1 -> his season delta must equal his game-1 line exactly
    corey_delta = tuple(new["Corey"][s] - old["Corey"][s] for s in STATS)
    assert corey_delta == GAME1["Corey"], f"Corey delta {corey_delta} != game1 {GAME1['Corey']}"
    print("  Corey played game 1 only — season delta matches game-1 line OK")
    check("GAME2", team_totals(game2), G2_TEAM)

    # print reconciliation
    print("\nGAME 2 derived lines (pa/ab/h/rbi/r):")
    for n in G2_ORDER:
        d = dict(zip(STATS, game2[n]))
        print(f"  {n:8} PA{d['pa']} AB{d['ab']} H{d['h']} 2B{d['2b']} 3B{d['3b']} HR{d['hr']} RBI{d['rbi']} R{d['r']}")

    if not write:
        print("\n[dry run] no files written. Re-run with write=True.")
        return

    # ---- write box score batting (rebuild, dropping any prior wk3 brew-crew rows) ----
    def bat_row(key, spot, name, v):
        d = dict(zip(STATS, v))
        outs = d["ab"] - d["h"]
        return [key, spot, name, d["pa"], d["ab"], d["h"], d["1b"], d["2b"], d["3b"],
                d["hr"], d["rbi"], d["r"], d["bb"], d["so"], d["sf"], d["fc"], 0, outs, ""]

    rows = list(csv.reader(open(BAT, encoding="utf-8-sig")))
    header, body = rows[0], [r for r in rows[1:] if r and r[0] not in (G1_KEY, G2_KEY)]
    for i, n in enumerate(G1_ORDER, 1):
        body.append(bat_row(G1_KEY, i, n, GAME1[n]))
    for i, n in enumerate(G2_ORDER, 1):
        body.append(bat_row(G2_KEY, i, n, game2[n]))
    with open(BAT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(body)
    print(f"\nWrote {BAT.name}: +{len(G1_ORDER)} game1 rows, +{len(G2_ORDER)} game2 rows")

    # ---- write box score games ----
    grows = list(csv.reader(open(GAMES, encoding="utf-8-sig")))
    gh, gb = grows[0], [r for r in grows[1:] if r and r[0] not in (G1_KEY, G2_KEY)]
    gb.append([G1_KEY, "Maple Tree Summer 2026", "Maple Tree", "2026-07-08", "6:30 PM", "Brew Crew", 10, 5,
               "Imported from GameChanger screenshot. Week 3 Game 1. Corey ejected (bottle of Hennessy on the bench).",
               "gamechanger_screenshot"])
    gb.append([G2_KEY, "Maple Tree Summer 2026", "Maple Tree", "2026-07-08", "7:30 PM", "Brew Crew", 4, 11,
               "Imported from GameChanger screenshot. Week 3 Game 2. Kives left early (ankle — ran into the fence).",
               "gamechanger_screenshot"])
    with open(GAMES, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(gh)
        w.writerows(gb)
    print(f"Wrote {GAMES.name}: 2 game rows")

    # ---- team_schedule g1/g2 results ----
    trows = list(csv.reader(open(TEAM, encoding="utf-8-sig")))
    th = trows[0]
    res = {
        "summer-2026-week-3-g1": ("W", 10, 5, "Week 3 Game 1 vs Brew Crew"),
        "summer-2026-week-3-g2": ("L", 4, 11, "Week 3 Game 2 vs Brew Crew"),
    }
    for r in trows[1:]:
        if r and r[0] in res:
            result, rf, ra, note = res[r[0]]
            r[11] = "completed"      # status
            r[12] = "1"               # completed_flag
            r[14] = result            # result
            r[15] = str(rf)           # runs_for
            r[16] = str(ra)           # runs_against
            r[17] = note              # notes
    with open(TEAM, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(th)
        w.writerows([r for r in trows[1:] if r])
    print(f"Wrote {TEAM.name}: g1=W 10-5, g2=L 4-11")

    # ---- league_schedule 10 games ----
    scores = {
        "summer-2026-week-3-g1": (4, 14), "summer-2026-week-3-g2": (5, 10),
        "summer-2026-week-3-g3": (15, 4), "summer-2026-week-3-g4": (10, 2),
        "summer-2026-week-3-g5": (4, 11), "summer-2026-week-3-g6": (12, 18),
        "summer-2026-week-3-g7": (16, 10), "summer-2026-week-3-g8": (0, 12),
        "summer-2026-week-3-g9": (14, 22), "summer-2026-week-3-g10": (16, 6),
    }
    lrows = list(csv.reader(open(LEAGUE, encoding="utf-8-sig")))
    lh = lrows[0]
    n = 0
    for r in lrows[1:]:
        if r and r[0] in scores:
            hr, ar = scores[r[0]]
            r[10] = "completed"   # status
            r[11] = "1"            # completed_flag
            r[12] = str(hr)        # home_runs
            r[13] = str(ar)        # away_runs
            n += 1
    with open(LEAGUE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(lh)
        w.writerows([r for r in lrows[1:] if r])
    print(f"Wrote {LEAGUE.name}: {n} league games scored")

    # ---- copy new season CSV into data/raw ----
    shutil.copyfile(NEW_CSV, OLD_CSV)
    print(f"Copied new season CSV -> {OLD_CSV.name}")
    print("\nDONE.")


if __name__ == "__main__":
    import sys
    main(write="--write" in sys.argv)
