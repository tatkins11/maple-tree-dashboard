"""Week 6 (2026-07-29) data sync — a SPLIT WITH THE FIRST-PLACE TEAM.

Game 1 (6:30, home): Maple Tree 12, Sandlot Vibes 13 (L) — 19 hits, lost by one.
Game 2 (7:30, away): Maple Tree 13, Sandlot Vibes 10 (W) — 21 hits, beat the #1 seed.

Sandlot Vibes came in 7-1 and league-best at 17.6 runs a game. We held them to 13 and
10 and took the nightcap. Harm went 3-for-3 with a homer and 4 RBI in game 2; Glove
went 4-for-4 with a homer and three runs.

Lineup both games (12 deep, Joey out) — Brian's order, NOT the screenshot order:
Glove, Tristan, Harm, Tim, JJ, Kives, Porter, Walsh, Joel, Slomka, Duff, Corey.

Same proven strategy: Game 1 hand-transcribed and verified against its team totals;
Game 2 DERIVED = (new season CSV - old season CSV - game 1), verified against Game 2's
team totals.

Season-CSV parsing gotcha (carried from Week 5): Harm's name sits in the *Last* column
("","Harm",""), everyone else in *First* — parse_season falls back to r[1].
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NEW_CSV = Path("C:/Users/TristanAtkins/Downloads/Maple Tree Summer 2026 Stats (4).csv")
OLD_CSV = REPO / "data/raw/season_csv/Maple Tree Summer 2026 Stats.csv"
BAT = REPO / "data/processed/game_boxscore_batting.csv"
GAMES = REPO / "data/processed/game_boxscore_games.csv"
TEAM = REPO / "data/processed/team_schedule.csv"
LEAGUE = REPO / "data/processed/league_schedule_games.csv"

STATS = ["pa", "ab", "h", "1b", "2b", "3b", "hr", "rbi", "r", "bb", "so", "sf", "fc"]
COL = {"pa": 4, "ab": 5, "h": 10, "1b": 11, "2b": 12, "3b": 13, "hr": 14,
       "rbi": 15, "r": 16, "bb": 17, "so": 18, "sf": 22, "fc": 24}
ROSTER = {"Corey", "Duff", "Glove", "Harm", "Jason", "JJ", "Joel", "Kives", "Porter",
          "Slomka", "Snaxx", "Tim", "Tristan", "Walsh"}

G1_KEY = "maple-tree-summer-2026-07-29-1830-sandlot-vibes"
G2_KEY = "maple-tree-summer-2026-07-29-1930-sandlot-vibes"

ORDER = ["Glove", "Tristan", "Harm", "Tim", "JJ", "Kives",
         "Porter", "Walsh", "Joel", "Slomka", "Duff", "Corey"]
SAT = ("Snaxx", "Jason")

# --- Game 1 (hand-transcribed from the screenshot; verified vs team totals) ----
GAME1 = {
    #            pa ab  h 1b 2b 3b hr rbi  r bb so sf fc
    "Glove":   (4, 4, 2, 2, 0, 0, 0, 1, 2, 0, 0, 0, 0),
    "Tristan": (4, 3, 2, 1, 1, 0, 0, 1, 1, 0, 0, 1, 0),
    "Harm":    (4, 2, 1, 1, 0, 0, 0, 2, 0, 1, 0, 1, 0),
    "Tim":     (4, 4, 2, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0),
    "JJ":      (4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Kives":   (3, 3, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0),
    "Porter":  (3, 3, 2, 0, 0, 1, 1, 2, 1, 0, 0, 0, 0),
    "Walsh":   (3, 3, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0),
    "Joel":    (3, 3, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0),
    "Slomka":  (3, 3, 2, 1, 1, 0, 0, 0, 2, 0, 0, 0, 0),
    "Duff":    (3, 3, 3, 3, 0, 0, 0, 2, 1, 0, 0, 0, 0),
    "Corey":   (3, 3, 2, 2, 0, 0, 0, 2, 1, 0, 0, 0, 0),
}
G1_TEAM = dict(pa=41, ab=38, h=19, **{"1b": 15, "2b": 2, "3b": 1}, hr=1, rbi=12, r=12,
               bb=1, so=0, sf=2, fc=0)
# Game 2 team totals from its screenshot (independent check only)
G2_TEAM = dict(pa=41, ab=40, h=21, **{"1b": 14, "2b": 5, "3b": 0}, hr=2, rbi=13, r=13,
               bb=0, so=0, sf=1, fc=3)

# Real innings off each linescore. Both games ran the regulation seven — I first read
# Game 1 as eight by miscounting the R/H/E columns as a spare inning; Brian corrected it.
# Both rows reconcile at seven (SNDL 4+6+0+0+3+0+0=13, MPLT 0+0+6+2+3+0+1=12).
INNINGS = {G1_KEY: 7, G2_KEY: 7}

# Week 6 league results (Brian, 7/30). His paste lists the AWAY team first; the CSV
# stores home_runs/away_runs, so each entry below is (home, away) for THAT row's
# designation. Nuketown had the bye. Our two games cross-check the box scores.
LEAGUE_SCORES: dict[str, tuple[int, int]] = {
    "summer-2026-week-6-g1": (12, 13),   # home Maple Tree 12, away Sandlot Vibes 13
    "summer-2026-week-6-g2": (12, 22),   # home Como 12, away Zero to Hiro 22
    "summer-2026-week-6-g3": (11, 2),    # home Brew Crew 11, away Slaughtered in 3 2
    "summer-2026-week-6-g4": (10, 13),   # home Sandlot Vibes 10, away Maple Tree 13
    "summer-2026-week-6-g5": (21, 14),   # home Zero to Hiro 21, away Como 14
    "summer-2026-week-6-g6": (7, 11),    # home Slaughtered in 3 7, away Brew Crew 11
    "summer-2026-week-6-g7": (20, 6),    # home Wasted Talent 20, away Everything hurts 6
    "summer-2026-week-6-g8": (4, 18),    # home Mean Beanz 4, away Bleacher Bums 18
    "summer-2026-week-6-g9": (2, 21),    # home Everything hurts 2, away Wasted Talent 21
    "summer-2026-week-6-g10": (20, 10),  # home Bleacher Bums 20, away Mean Beanz 10
}


def parse_season(path: Path) -> dict[str, dict[str, int]]:
    out = {}
    for r in csv.reader(open(path, encoding="utf-8-sig")):
        if len(r) > 24:
            name = (r[2] or "").strip() or (r[1] or "").strip()
            if name in ROSTER:
                out[name] = {s: int(r[COL[s]]) for s in STATS}
    return out


def team_totals(lines: dict[str, tuple]) -> dict[str, int]:
    return {s: sum(v[i] for v in lines.values()) for i, s in enumerate(STATS)}


def check(label, got, want):
    bad = {s: (got[s], want[s]) for s in STATS if got[s] != want[s]}
    assert not bad, f"{label} MISMATCH {bad}"
    print(f"  {label} team totals OK: " + " ".join(f"{s.upper()}{got[s]}" for s in STATS))


def write_league():
    if not LEAGUE_SCORES:
        print(f"SKIPPED {LEAGUE.name}: no league results in hand yet.")
        return
    lrows = list(csv.reader(open(LEAGUE, encoding="utf-8-sig")))
    lh, n = lrows[0], 0
    for r in lrows[1:]:
        if r and r[0] in LEAGUE_SCORES:
            hr_, ar = LEAGUE_SCORES[r[0]]
            r[10], r[11], r[12], r[13] = "completed", "1", str(hr_), str(ar)
            n += 1
    with open(LEAGUE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(lh)
        w.writerows([r for r in lrows[1:] if r])
    print(f"Wrote {LEAGUE.name}: {n}/{len(LEAGUE_SCORES)} league games scored")


def main(write=False):
    old, new = parse_season(OLD_CSV), parse_season(NEW_CSV)
    zero = {s: 0 for s in STATS}
    print(f"Parsed season CSVs: {len(old)} old players, {len(new)} new players")
    assert "Harm" in new, "Harm missing from the new CSV — check the name-column fallback"

    check("GAME1", team_totals(GAME1), G1_TEAM)

    game2 = {}
    for name in ORDER:
        base = old.get(name, zero)
        row = tuple((new[name][s] - base[s]) - GAME1[name][i] for i, s in enumerate(STATS))
        assert all(v >= 0 for v in row), f"NEGATIVE derived stat for {name}: {dict(zip(STATS, row))}"
        game2[name] = row
    for sat in SAT:
        if sat in old or sat in new:
            assert new.get(sat, zero) == old.get(sat, zero), f"{sat} season line changed but he did not play!"
    print(f"  {', '.join(SAT)} season lines unchanged (sat out) OK")
    check("GAME2", team_totals(game2), G2_TEAM)

    print("\nGAME 2 derived lines (true batting order):")
    for n in ORDER:
        d = dict(zip(STATS, game2[n]))
        print(f"  {n:8} PA{d['pa']} AB{d['ab']} H{d['h']} 2B{d['2b']} 3B{d['3b']} "
              f"HR{d['hr']} RBI{d['rbi']} R{d['r']} BB{d['bb']}")

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
    inn_i = gh.index("innings") if "innings" in gh else None

    def game_row(key, time, rf, ra, note):
        row = [key, "Maple Tree Summer 2026", "Maple Tree", "2026-07-29", time,
               "Sandlot Vibes", rf, ra, note, "gamechanger_screenshot"]
        if inn_i is not None:
            row = row[:inn_i] + [INNINGS[key]] + row[inn_i:]
        return row

    gb.append(game_row(G1_KEY, "6:30 PM", 12, 13,
              "Imported from GameChanger screenshot. Week 6 Game 1. Nineteen hits and a "
              "one-run loss to the league leaders. Sandlot Vibes scored ten in the first "
              "two innings; we answered with six in the third and never quite closed it. "
              "Went the full eight innings — time expired with Maple Tree due up."))
    gb.append(game_row(G2_KEY, "7:30 PM", 13, 10,
              "Imported from GameChanger screenshot. Week 6 Game 2. Twenty-one hits to beat "
              "the first-place team 13-10 and split the night. Glove 4-for-4 with a homer "
              "and three runs; Harm 3-for-3 with a homer and four RBI."))
    with open(GAMES, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(gh)
        w.writerows(gb)
    print(f"Wrote {GAMES.name}: 2 game rows (innings {INNINGS[G1_KEY]} / {INNINGS[G2_KEY]})")

    trows = list(csv.reader(open(TEAM, encoding="utf-8-sig")))
    th = trows[0]
    res = {
        "summer-2026-week-6-g1": ("L", 12, 13, "Week 6 Game 1 vs Sandlot Vibes"),
        "summer-2026-week-6-g2": ("W", 13, 10, "Week 6 Game 2 vs Sandlot Vibes"),
    }
    for r in trows[1:]:
        if r and r[0] in res:
            result, rf, ra, note = res[r[0]]
            r[11], r[12], r[14], r[15], r[16], r[17] = "completed", "1", result, str(rf), str(ra), note
    with open(TEAM, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(th)
        w.writerows([r for r in trows[1:] if r])
    print(f"Wrote {TEAM.name}: g1=L 12-13, g2=W 13-10")

    write_league()

    shutil.copyfile(NEW_CSV, OLD_CSV)
    print(f"Copied new season CSV -> {OLD_CSV.name}")


if __name__ == "__main__":
    import sys
    if "--league-only" in sys.argv:
        if "--write" not in sys.argv:
            print("[dry run] league-only; re-run with --write to apply.")
        else:
            write_league()
    else:
        main(write="--write" in sys.argv)
