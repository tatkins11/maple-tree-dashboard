"""Week 4 (2026-07-15) data sync — Maple Tree SWEEPS Zero to Hiro.

Game 1 (6:30, home): Maple Tree 13, Zero to Hiro 12  (W)  — survived a furious rally
Game 2 (7:30, away): Maple Tree 16, Zero to Hiro 11  (W)  — sweep sealed

Kives (ankle) and Glove (fever) sat. Snaxx and Slomka made their season debuts;
Corey returned from his one-game suspension. Lineup both games:
Tristan, Tim, JJ, Porter, Walsh, Corey, Joel, Snaxx, Duff, Slomka, Jason.

Same strategy as Week 3 (proven): Game 1 hand-transcribed + verified against its
team totals; Game 2 DERIVED = (new season CSV − old season CSV − game 1), verified
against Game 2's team totals. Snaxx/Slomka had no prior 2026 rows → old = zeros.
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NEW_CSV = Path("C:/Users/TristanAtkins/Downloads/Maple Tree Summer 2026 Stats (2).csv")
OLD_CSV = REPO / "data/raw/season_csv/Maple Tree Summer 2026 Stats.csv"
BAT = REPO / "data/processed/game_boxscore_batting.csv"
GAMES = REPO / "data/processed/game_boxscore_games.csv"
TEAM = REPO / "data/processed/team_schedule.csv"
LEAGUE = REPO / "data/processed/league_schedule_games.csv"

STATS = ["pa", "ab", "h", "1b", "2b", "3b", "hr", "rbi", "r", "bb", "so", "sf", "fc"]
COL = {"pa": 4, "ab": 5, "h": 10, "1b": 11, "2b": 12, "3b": 13, "hr": 14,
       "rbi": 15, "r": 16, "bb": 17, "so": 18, "sf": 22, "fc": 24}
ROSTER = {"Corey", "Duff", "Glove", "Jason", "JJ", "Joel", "Kives", "Porter",
          "Slomka", "Snaxx", "Tim", "Tristan", "Walsh"}

G1_KEY = "maple-tree-summer-2026-07-15-1830-zero-to-hiro"
G2_KEY = "maple-tree-summer-2026-07-15-1930-zero-to-hiro"

ORDER = ["Tristan", "Tim", "JJ", "Porter", "Walsh", "Corey", "Joel", "Snaxx", "Duff", "Slomka", "Jason"]

# --- Game 1 (hand-transcribed from screenshot; verified vs team totals) -----
GAME1 = {
    #            pa ab  h 1b 2b 3b hr rbi  r bb so sf fc
    "Tristan": (4, 4, 2, 1, 1, 0, 0, 0, 2, 0, 0, 0, 0),
    "Tim":     (4, 4, 2, 2, 0, 0, 0, 0, 2, 0, 0, 0, 0),
    "JJ":      (4, 4, 2, 2, 0, 0, 0, 3, 1, 0, 0, 0, 0),
    "Porter":  (4, 4, 4, 4, 0, 0, 0, 1, 1, 0, 0, 0, 0),
    "Walsh":   (4, 4, 2, 1, 1, 0, 0, 2, 1, 0, 0, 0, 2),
    "Corey":   (4, 3, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1),
    "Joel":    (3, 3, 2, 1, 1, 0, 0, 1, 2, 0, 0, 0, 0),
    "Snaxx":   (3, 3, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0),
    "Duff":    (3, 2, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0),
    "Slomka":  (3, 3, 3, 1, 1, 0, 1, 3, 2, 0, 0, 0, 0),
    "Jason":   (3, 3, 2, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0),
}
G1_TEAM = dict(pa=39, ab=37, h=22, **{"1b": 17, "2b": 4, "3b": 0}, hr=1, rbi=13, r=13,
               bb=0, so=0, sf=2, fc=3)
# Game 2 team totals from the screenshot (independent check only)
G2_TEAM = dict(pa=44, ab=43, h=22, **{"1b": 13, "2b": 6, "3b": 1}, hr=2, rbi=16, r=16,
               bb=1, so=0, sf=0, fc=2)


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
    zero = {s: 0 for s in STATS}
    print(f"Parsed season CSVs: {len(old)} old players, {len(new)} new players")

    check("GAME1", team_totals(GAME1), G1_TEAM)

    game2 = {}
    for name in ORDER:
        base = old.get(name, zero)
        row = tuple((new[name][s] - base[s]) - GAME1[name][i] for i, s in enumerate(STATS))
        assert all(v >= 0 for v in row), f"NEGATIVE derived stat for {name}: {dict(zip(STATS, row))}"
        game2[name] = row
    # Kives and Glove sat -> their season lines must be unchanged
    for sat in ("Kives", "Glove"):
        assert new[sat] == old[sat], f"{sat} season line changed but he did not play!"
    print("  Kives + Glove season lines unchanged (sat out) OK")
    check("GAME2", team_totals(game2), G2_TEAM)

    print("\nGAME 2 derived lines:")
    for n in ORDER:
        d = dict(zip(STATS, game2[n]))
        print(f"  {n:8} PA{d['pa']} AB{d['ab']} H{d['h']} 2B{d['2b']} 3B{d['3b']} HR{d['hr']} RBI{d['rbi']} R{d['r']} BB{d['bb']}")

    if not write:
        print("\n[dry run] no files written. Re-run with --write.")
        return

    def bat_row(key, spot, name, v):
        d = dict(zip(STATS, v))
        return [key, spot, name, d["pa"], d["ab"], d["h"], d["1b"], d["2b"], d["3b"],
                d["hr"], d["rbi"], d["r"], d["bb"], d["so"], d["sf"], d["fc"], 0,
                d["ab"] - d["h"], ""]

    rows = list(csv.reader(open(BAT, encoding="utf-8-sig")))
    header, body = rows[0], [r for r in rows[1:] if r and r[0] not in (G1_KEY, G2_KEY)]
    for i, n in enumerate(ORDER, 1):
        body.append(bat_row(G1_KEY, i, n, GAME1[n]))
    for i, n in enumerate(ORDER, 1):
        body.append(bat_row(G2_KEY, i, n, game2[n]))
    with open(BAT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(body)
    print(f"\nWrote {BAT.name}: +{len(ORDER)} rows per game")

    grows = list(csv.reader(open(GAMES, encoding="utf-8-sig")))
    gh, gb = grows[0], [r for r in grows[1:] if r and r[0] not in (G1_KEY, G2_KEY)]
    gb.append([G1_KEY, "Maple Tree Summer 2026", "Maple Tree", "2026-07-15", "6:30 PM", "Zero to Hiro", 13, 12,
               "Imported from GameChanger screenshot. Week 4 Game 1. Up 13-7 late, survived a 6-run Zero to Hiro "
               "charge. Slomka HR in season debut. No Kives (ankle) or Glove (fever); Corey and Snaxx returned.",
               "gamechanger_screenshot"])
    gb.append([G2_KEY, "Maple Tree Summer 2026", "Maple Tree", "2026-07-15", "7:30 PM", "Zero to Hiro", 16, 11,
               "Imported from GameChanger screenshot. Week 4 Game 2. Sweep sealed 16-11. Tristan career HR #50; "
               "Joel HR.", "gamechanger_screenshot"])
    with open(GAMES, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(gh)
        w.writerows(gb)
    print(f"Wrote {GAMES.name}: 2 game rows")

    trows = list(csv.reader(open(TEAM, encoding="utf-8-sig")))
    th = trows[0]
    res = {
        "summer-2026-week-4-g1": ("W", 13, 12, "Week 4 Game 1 vs Zero to Hiro"),
        "summer-2026-week-4-g2": ("W", 16, 11, "Week 4 Game 2 vs Zero to Hiro"),
    }
    for r in trows[1:]:
        if r and r[0] in res:
            result, rf, ra, note = res[r[0]]
            r[11], r[12], r[14], r[15], r[16], r[17] = "completed", "1", result, str(rf), str(ra), note
    with open(TEAM, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(th)
        w.writerows([r for r in trows[1:] if r])
    print(f"Wrote {TEAM.name}: g1=W 13-12, g2=W 16-11")

    # (home_runs, away_runs) mapped from the league CSV's home/away designations
    scores = {
        "summer-2026-week-4-g1": (0, 12), "summer-2026-week-4-g2": (13, 12),
        "summer-2026-week-4-g3": (18, 6), "summer-2026-week-4-g4": (10, 11),
        "summer-2026-week-4-g5": (11, 16), "summer-2026-week-4-g6": (8, 16),
        "summer-2026-week-4-g7": (9, 16), "summer-2026-week-4-g8": (16, 6),
        "summer-2026-week-4-g9": (26, 16), "summer-2026-week-4-g10": (8, 18),
    }
    lrows = list(csv.reader(open(LEAGUE, encoding="utf-8-sig")))
    lh = lrows[0]
    n = 0
    for r in lrows[1:]:
        if r and r[0] in scores:
            hr_, ar = scores[r[0]]
            r[10], r[11], r[12], r[13] = "completed", "1", str(hr_), str(ar)
            n += 1
    with open(LEAGUE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(lh)
        w.writerows([r for r in lrows[1:] if r])
    print(f"Wrote {LEAGUE.name}: {n} league games scored")

    shutil.copyfile(NEW_CSV, OLD_CSV)
    print(f"Copied new season CSV -> {OLD_CSV.name}")
    print("\nDONE.")


if __name__ == "__main__":
    import sys
    main(write="--write" in sys.argv)
