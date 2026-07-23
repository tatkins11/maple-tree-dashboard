"""Week 5 (2026-07-22) data sync — split with Bleacher Bums, and HARM'S NIGHT.

Game 1 (8:30, away): Maple Tree  2, Bleacher Bums 12  (L)  — bats never showed up
Game 2 (9:30, home): Maple Tree 17, Bleacher Bums  6  (W)  — 19 hits, split salvaged

NOTE: game 1 score is 2-12 (Brian's correction; the GameChanger linescore read 11).

Harm — an old roster name — was brought back and slotted 4th, bumping everyone behind
him down a spot. He went 3-for-3 with 3 HR and 10 RBI in game 2: a NEW franchise
single-game RBI record (broke Glove's 8, Fall 2025) and the 4th 3-HR game in club
history. He came in with a career line of 0-for-4.

Kives (out), JJ (out) and Slomka (out) did not play — their season lines must be
unchanged. Lineup both games:
Glove, Tristan, Tim, Harm, Porter, Joel, Walsh, Corey, Snaxx, Duff, Jason.

Same proven strategy: Game 1 hand-transcribed + verified against its team totals;
Game 2 DERIVED = (new season CSV − old season CSV − game 1), verified against Game 2's
team totals. Harm has no prior 2026 row → old = zeros.

PARSING GOTCHA (new this week): Harm's name sits in the CSV's *Last* column
("","Harm","") while every other player uses *First*. The stock r[2] lookup drops him
silently — parse_season now falls back to r[1].
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NEW_CSV = Path("C:/Users/TristanAtkins/Downloads/Maple Tree Summer 2026 Stats (3).csv")
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

G1_KEY = "maple-tree-summer-2026-07-22-2030-bleacher-bums"
G2_KEY = "maple-tree-summer-2026-07-22-2130-bleacher-bums"

ORDER = ["Glove", "Tristan", "Tim", "Harm", "Porter", "Joel", "Walsh", "Corey", "Snaxx", "Duff", "Jason"]
SAT = ("Kives", "JJ", "Slomka")

# --- Game 1 (hand-transcribed from screenshot; verified vs team totals) -----
GAME1 = {
    #            pa ab  h 1b 2b 3b hr rbi  r bb so sf fc
    "Glove":   (3, 3, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Tristan": (2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Tim":     (2, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Harm":    (2, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0),
    "Porter":  (2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
    "Joel":    (2, 2, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0),
    "Walsh":   (2, 2, 1, 0, 0, 0, 1, 2, 1, 0, 0, 0, 0),
    "Corey":   (2, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Snaxx":   (2, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0),
    "Duff":    (2, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Jason":   (2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
}
G1_TEAM = dict(pa=23, ab=21, h=7, **{"1b": 5, "2b": 1, "3b": 0}, hr=1, rbi=2, r=2,
               bb=2, so=0, sf=0, fc=2)
# Game 2 team totals from the screenshot (independent check only)
G2_TEAM = dict(pa=37, ab=33, h=19, **{"1b": 13, "2b": 2, "3b": 1}, hr=3, rbi=17, r=17,
               bb=2, so=0, sf=2, fc=2)

# League results not in hand yet — fill in and re-run to score the other games.
LEAGUE_SCORES: dict[str, tuple[int, int]] = {}


def parse_season(path: Path) -> dict[str, dict[str, int]]:
    """Name is normally the First column (r[2]); single-name guys like Harm land in
    Last (r[1]) instead, so fall back or he vanishes from the sync."""
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


def main(write=False):
    old, new = parse_season(OLD_CSV), parse_season(NEW_CSV)
    zero = {s: 0 for s in STATS}
    print(f"Parsed season CSVs: {len(old)} old players, {len(new)} new players")
    assert "Harm" in new, "Harm missing from the new season CSV — check the name-column fallback"
    print(f"  Harm parsed OK: {new['Harm']}")

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
    gb.append([G1_KEY, "Maple Tree Summer 2026", "Maple Tree", "2026-07-22", "8:30 PM", "Bleacher Bums", 2, 12,
               "Imported from GameChanger screenshot. Week 5 Game 1. Bats never showed up in a 2-12 loss; "
               "Walsh's two-run homer was the only damage. Harm returned to the roster batting 4th.",
               "gamechanger_screenshot"])
    gb.append([G2_KEY, "Maple Tree Summer 2026", "Maple Tree", "2026-07-22", "9:30 PM", "Bleacher Bums", 17, 6,
               "Imported from GameChanger screenshot. Week 5 Game 2. 19 hits in a 17-6 rout to split the night. "
               "HARM'S NIGHT: 3-for-3, 3 HR, 10 RBI — a new franchise single-game RBI record (broke Glove's 8) "
               "and the 4th three-homer game in club history. Tristan 4-for-4.",
               "gamechanger_screenshot"])
    with open(GAMES, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(gh)
        w.writerows(gb)
    print(f"Wrote {GAMES.name}: 2 game rows")

    trows = list(csv.reader(open(TEAM, encoding="utf-8-sig")))
    th = trows[0]
    res = {
        "summer-2026-week-5-g1": ("L", 2, 12, "Week 5 Game 1 vs Bleacher Bums"),
        "summer-2026-week-5-g2": ("W", 17, 6, "Week 5 Game 2 vs Bleacher Bums"),
    }
    for r in trows[1:]:
        if r and r[0] in res:
            result, rf, ra, note = res[r[0]]
            r[11], r[12], r[14], r[15], r[16], r[17] = "completed", "1", result, str(rf), str(ra), note
    with open(TEAM, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(th)
        w.writerows([r for r in trows[1:] if r])
    print(f"Wrote {TEAM.name}: g1=L 2-12, g2=W 17-6")

    if LEAGUE_SCORES:
        lrows = list(csv.reader(open(LEAGUE, encoding="utf-8-sig")))
        lh = lrows[0]
        n = 0
        for r in lrows[1:]:
            if r and r[0] in LEAGUE_SCORES:
                hr_, ar = LEAGUE_SCORES[r[0]]
                r[10], r[11], r[12], r[13] = "completed", "1", str(hr_), str(ar)
                n += 1
        with open(LEAGUE, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(lh)
            w.writerows([r for r in lrows[1:] if r])
        print(f"Wrote {LEAGUE.name}: {n} league games scored")
    else:
        print(f"SKIPPED {LEAGUE.name}: league results not in hand — "
              "fill LEAGUE_SCORES and re-run with --write to score the rest of the week.")

    shutil.copyfile(NEW_CSV, OLD_CSV)
    print(f"Copied new season CSV -> {OLD_CSV.name}")


if __name__ == "__main__":
    import sys
    main(write="--write" in sys.argv)
