"""Set up Maple Tree's Summer 2026 regular-season schedule in team_schedule.csv.

Week 1 bye, then 6 doubleheaders (Weeks 2-7). Idempotent (skips if already present).
Division defaults to 'Blue Division' (carry-over from Spring) until confirmed.
"""
from __future__ import annotations

import csv
from pathlib import Path

CSV = Path(__file__).resolve().parents[1] / "data/processed/team_schedule.csv"
SEASON = "Maple Tree Summer 2026"
LEAGUE = "Wednesday Men's"
DIV = "Blue Division"  # FLAG: not given for Summer; carried over from Spring

# (week, date, time, field, opponent, home_away)  -- doubleheaders listed as two rows
GAMES = [
    ("Week 1", "2026-06-24", "", "", "", "bye"),
    ("Week 2", "2026-07-01", "8:30 PM", "Boncosky Blue", "Como", "home"),
    ("Week 2", "2026-07-01", "9:30 PM", "Boncosky Blue", "Como", "away"),
    ("Week 3", "2026-07-08", "6:30 PM", "Boncosky Red", "Brew Crew", "away"),
    ("Week 3", "2026-07-08", "7:30 PM", "Boncosky Red", "Brew Crew", "home"),
    ("Week 4", "2026-07-15", "6:30 PM", "Boncosky Green", "Zero to Hiro", "home"),
    ("Week 4", "2026-07-15", "7:30 PM", "Boncosky Green", "Zero to Hiro", "away"),
    ("Week 5", "2026-07-22", "8:30 PM", "Boncosky Green", "Bleacher Bums", "away"),
    ("Week 5", "2026-07-22", "9:30 PM", "Boncosky Green", "Bleacher Bums", "home"),
    ("Week 6", "2026-07-29", "6:30 PM", "Boncosky Green", "Sandlot Vibes", "home"),
    ("Week 6", "2026-07-29", "7:30 PM", "Boncosky Green", "Sandlot Vibes", "away"),
    ("Week 7", "2026-08-05", "6:30 PM", "Boncosky Yellow", "Wasted Talent", "away"),
    ("Week 7", "2026-08-05", "7:30 PM", "Boncosky Yellow", "Wasted Talent", "home"),
]


def main() -> None:
    if "summer-2026-week" in CSV.read_text(encoding="utf-8"):
        print("summer rows already present, skip")
        return
    rows = []
    counters: dict[str, int] = {}
    for week, date, time, field, opp, ha in GAMES:
        wk = week.split()[1]
        if ha == "bye":
            gid = f"summer-2026-week-{wk}-bye"
            rows.append([gid, SEASON, LEAGUE, DIV, week, date, "", "Maple Tree", "", "bye", "",
                         "scheduled", "0", "1", "", "", "", "Doubleheader bye week", "team_schedule.csv"])
        else:
            counters[wk] = counters.get(wk, 0) + 1
            gid = f"summer-2026-week-{wk}-g{counters[wk]}"
            rows.append([gid, SEASON, LEAGUE, DIV, week, date, time, "Maple Tree", opp, ha, field,
                         "scheduled", "0", "0", "", "", "", "", "team_schedule.csv"])
    with CSV.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for row in rows:
            w.writerow(row)
    print(f"appended {len(rows)} Maple Tree Summer 2026 schedule rows")


if __name__ == "__main__":
    main()
