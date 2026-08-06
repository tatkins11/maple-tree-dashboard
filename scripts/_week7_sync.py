"""Week 7 (2026-08-05) data sync — SPLIT WITH WASTED TALENT, the other way around.

Game 1 (6:30, away): Maple Tree  4, Wasted Talent 17 (L) — run-ruled in four innings.
                     A single run in each of the four; they hung ten in the third.
Game 2 (7:30, home): Maple Tree 22, Wasted Talent 16 (W) — TWENTY-NINE HITS, most in
                     any game this season (previous high 21, last week). Up 22-9 after
                     six; home seventh not needed (X). Seven innings.

Glove's game 2: 5-for-5, two doubles, TWO homers, 5 RBI, 5 runs — against the #1 seed.

Lineup both games (10 deep; Harm, Kives, Snaxx, Slomka all out) — Brian's final order:
Glove, Tristan, Tim, Porter, JJ, Walsh, Duff, Corey, Joel, Jason.

Same proven strategy: Game 1 hand-transcribed and verified against its team totals;
Game 2 DERIVED = (new season CSV - old season CSV - game 1), verified against Game 2's
team totals.

League results NOT in yet — LEAGUE_SCORES carries only our two games. Run
`--league-only` after Brian sends the rest (edit LEAGUE_SCORES first).
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NEW_CSV = Path("C:/Users/TristanAtkins/Downloads/Maple Tree Summer 2026 Stats (5).csv")
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

G1_KEY = "maple-tree-summer-2026-08-05-1830-wasted-talent"
G2_KEY = "maple-tree-summer-2026-08-05-1930-wasted-talent"

ORDER = ["Glove", "Tristan", "Tim", "Porter", "JJ", "Walsh",
         "Duff", "Corey", "Joel", "Jason"]
SAT = ("Harm", "Kives", "Snaxx", "Slomka")

# --- Game 1 (hand-transcribed from the screenshot; verified vs team totals) ----
GAME1 = {
    #            pa ab  h 1b 2b 3b hr rbi  r bb so sf fc
    "Glove":   (3, 2, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0),
    "Tristan": (3, 3, 2, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0),
    "Tim":     (3, 3, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0),
    "Porter":  (2, 2, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1),
    "JJ":      (2, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2),
    "Walsh":   (2, 2, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0),
    "Duff":    (2, 2, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0),
    "Corey":   (2, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0),
    "Joel":    (2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    "Jason":   (2, 2, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1),
}
G1_TEAM = dict(pa=23, ab=21, h=10, **{"1b": 8, "2b": 1, "3b": 1}, hr=0, rbi=4, r=4,
               bb=1, so=0, sf=1, fc=4)
# Game 2 team totals from its screenshot (independent check only)
G2_TEAM = dict(pa=46, ab=45, h=29, **{"1b": 16, "2b": 7, "3b": 2}, hr=4, rbi=22, r=22,
               bb=0, so=0, sf=1, fc=2)

# Real innings off each linescore. Game 1 was run-ruled after four; game 2 went the
# full seven with the home half of the 7th not needed (X).
INNINGS = {G1_KEY: 4, G2_KEY: 7}

# Only our own games — the rest of the league's Week 7 results are not in yet.
LEAGUE_SCORES: dict[str, tuple[int, int]] = {
    "summer-2026-week-7-g3": (17, 4),    # home Wasted Talent 17, away Maple Tree 4
    "summer-2026-week-7-g6": (22, 16),   # home Maple Tree 22, away Wasted Talent 16
}


def parse_season(path: Path) -> dict[str, dict[str, int]]:
    """Single-name players (Harm) sit in the Last column — fall back to r[1]."""
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
        row = [key, "Maple Tree Summer 2026", "Maple Tree", "2026-08-05", time,
               "Wasted Talent", rf, ra, note, "gamechanger_screenshot"]
        if inn_i is not None:
            row = row[:inn_i] + [INNINGS[key]] + row[inn_i:]
        return row

    gb.append(game_row(G1_KEY, "6:30 PM", 4, 17,
              "Imported from GameChanger screenshot. Week 7 Game 1. Run-ruled in four by "
              "the league leaders, 4-17 — a single run in each inning while Wasted Talent "
              "hung ten in the third. The bats saved everything for the nightcap."))
    gb.append(game_row(G2_KEY, "7:30 PM", 22, 16,
              "Imported from GameChanger screenshot. Week 7 Game 2. Beat the #1 seed 22-16 "
              "on TWENTY-NINE hits, the most in any game this season (previous high 21). "
              "Glove: 5-for-5, two doubles, two homers, 5 RBI, 5 runs. Up 22-9 through six; "
              "the home seventh was not required."))
    with open(GAMES, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(gh)
        w.writerows(gb)
    print(f"Wrote {GAMES.name}: 2 game rows (innings {INNINGS[G1_KEY]} / {INNINGS[G2_KEY]})")

    trows = list(csv.reader(open(TEAM, encoding="utf-8-sig")))
    th = trows[0]
    res = {
        "summer-2026-week-7-g1": ("L", 4, 17, "Week 7 Game 1 at Wasted Talent"),
        "summer-2026-week-7-g2": ("W", 22, 16, "Week 7 Game 2 vs Wasted Talent"),
    }
    for r in trows[1:]:
        if r and r[0] in res:
            result, rf, ra, note = res[r[0]]
            r[11], r[12], r[14], r[15], r[16], r[17] = "completed", "1", result, str(rf), str(ra), note
    with open(TEAM, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(th)
        w.writerows([r for r in trows[1:] if r])
    print(f"Wrote {TEAM.name}: g1=L 4-17, g2=W 22-16")

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
