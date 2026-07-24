"""One-off: fold the 2026-06-17 playoff game (Maple Tree 8, Bullseyes 18) into the data.

Edits four source files:
  1. data/processed/game_boxscore_games.csv      -- append the game row
  2. data/processed/game_boxscore_batting.csv     -- append 12 batting rows
  3. data/raw/season_csv/Maple Tree Spring 2026 Stats.csv -- add the playoff to each
     player's season counting totals + recompute AVG/OBP/SLG/OPS (season includes playoffs)
  4. data/processed/team_schedule.csv             -- mark the playoff game completed (L 8-18)

Idempotent: skips box-score appends if the game_key already exists.
"""
from __future__ import annotations

import csv
from pathlib import Path

GAME_KEY = "maple-tree-spring-2026-06-17-1930-bullseyes"
SEASON = "Maple Tree Spring 2026"

# Verified box score (reconciles exactly with GameChanger's Team row).
# name: (pa, ab, h, b1, b2, b3, hr, rbi, r, bb, so)  -- lineup order = screenshot order (not tracked)
BOX = [
    ("Snaxx",   2, 2, 2, 2, 0, 0, 0, 0, 1, 0, 0),
    ("Corey",   3, 3, 2, 2, 0, 0, 0, 0, 0, 0, 0),
    ("Glove",   3, 3, 2, 2, 0, 0, 0, 1, 2, 0, 0),
    ("Kives",   3, 1, 1, 1, 0, 0, 0, 2, 1, 2, 0),
    ("Jason",   2, 2, 1, 1, 0, 0, 0, 0, 1, 0, 0),
    ("JJ",      3, 3, 1, 0, 0, 0, 1, 2, 1, 0, 0),
    ("Tristan", 3, 2, 1, 0, 0, 0, 1, 2, 2, 1, 0),
    ("Tim",     3, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0),
    ("Porter",  3, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0),
    ("Joel",    3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    ("Walsh",   3, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0),
    ("Duff",    3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1),
]
# columns: name idx -> pa1 ab2 h3 b1=4 b2=5 b3=6 hr7 rbi8 r9 bb10 so11


def tb(row):
    return row[4] + 2 * row[5] + 3 * row[6] + 4 * row[7]  # 1B + 2*2B + 3*3B + 4*HR


ROOT = Path(__file__).resolve().parents[1]
GAMES_CSV = ROOT / "data/processed/game_boxscore_games.csv"
BATTING_CSV = ROOT / "data/processed/game_boxscore_batting.csv"
SEASON_CSV = ROOT / "data/raw/season_csv/Maple Tree Spring 2026 Stats.csv"
SCHEDULE_CSV = ROOT / "data/processed/team_schedule.csv"


def append_game_row():
    existing = GAMES_CSV.read_text(encoding="utf-8")
    if GAME_KEY in existing:
        print("games.csv: game_key already present, skip")
        return
    note = ("Imported from GameChanger game stats screenshot shared in chat. "
            "Playoff Round 1 (single elimination) vs #3 Bullseyes; season-ending loss.")
    row = [GAME_KEY, SEASON, "Maple Tree", "2026-06-17", "7:30 PM", "Bullseyes",
           "8", "18", note, "gamechanger_screenshot"]
    with GAMES_CSV.open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(row)
    print("games.csv: appended game row")


def append_batting_rows():
    existing = BATTING_CSV.read_text(encoding="utf-8")
    if GAME_KEY in existing:
        print("batting.csv: game_key already present, skip")
        return
    out = []
    for spot, b in enumerate(BOX, start=1):
        name, pa, ab, h, b1, b2, b3, hr, rbi, r, bb, so = b
        outs = max(0, ab - h - so)  # fc=gidp=0 this game; importer re-derives anyway
        # header: game_key,lineup_spot,player_name,pa,ab,h,1b,2b,3b,hr,rbi,r,bb,so,sf,fc,gidp,outs,notes
        out.append([GAME_KEY, spot, name, pa, ab, h, b1, b2, b3, hr, rbi, r, bb, so, 0, 0, 0, outs, ""])
    with BATTING_CSV.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for row in out:
            w.writerow(row)
    print(f"batting.csv: appended {len(out)} batting rows")


def update_season_csv():
    deltas = {b[0]: b for b in BOX}
    rows = list(csv.reader(SEASON_CSV.open(encoding="utf-8")))
    header = rows[1]
    # Column names repeat across the Batting/Pitching/Fielding sections (GP, H, R,
    # BB, SO, HR, 1B...). Keep the FIRST occurrence, which is the batting section.
    idx: dict[str, int] = {}
    for i, name in enumerate(header):
        idx.setdefault(name, i)

    def fmt_rate(x):
        return f"{x:.3f}".lstrip("0") if x < 1 else f"{x:.3f}"  # .659 ; 2.253

    updated = 0
    for row in rows[2:]:
        if len(row) <= idx["First"]:
            continue
        name = row[idx["First"]].strip()
        if name not in deltas:
            continue
        _, pa, ab, h, b1, b2, b3, hr, rbi, r, bb, so = deltas[name]
        adds = {"GP": 1, "PA": pa, "AB": ab, "H": h, "1B": b1, "2B": b2, "3B": b3,
                "HR": hr, "RBI": rbi, "R": r, "BB": bb, "SO": so, "TB": tb(deltas[name])}
        for col, add in adds.items():
            row[idx[col]] = str(int(float(row[idx[col]] or 0)) + add)
        # recompute rates to match the app (OBP keyed off PA; HBP/SH are 0 here)
        H = int(row[idx["H"]]); AB = int(row[idx["AB"]]); BB = int(row[idx["BB"]])
        PA = int(row[idx["PA"]]); TB = int(row[idx["TB"]])
        avg = H / AB if AB else 0.0
        obp = (H + BB) / PA if PA else 0.0
        slg = TB / AB if AB else 0.0
        row[idx["AVG"]] = fmt_rate(avg)
        row[idx["OBP"]] = fmt_rate(obp)
        row[idx["SLG"]] = fmt_rate(slg)
        row[idx["OPS"]] = fmt_rate(obp + slg)
        updated += 1
    with SEASON_CSV.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)
    print(f"season csv: updated {updated} player rows (expected 12)")


def mark_schedule_complete():
    rows = list(csv.reader(SCHEDULE_CSV.open(encoding="utf-8")))
    header = rows[0]
    idx = {name.lstrip("﻿"): i for i, name in enumerate(header)}
    pid = "spring-2026-playoffs-r1-mt-vs-bullseyes"
    changed = False
    for row in rows[1:]:
        if row and row[idx["game_id"]] == pid:
            row[idx["status"]] = "completed"
            row[idx["completed_flag"]] = "1"
            row[idx["result"]] = "L"
            row[idx["runs_for"]] = "8"
            row[idx["runs_against"]] = "18"
            changed = True
    if changed:
        with SCHEDULE_CSV.open("w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerows(rows)
        print("team_schedule.csv: playoff row marked completed (L 8-18)")
    else:
        print("team_schedule.csv: playoff row NOT found!")


if __name__ == "__main__":
    append_game_row()
    append_batting_rows()
    update_season_csv()
    mark_schedule_complete()
