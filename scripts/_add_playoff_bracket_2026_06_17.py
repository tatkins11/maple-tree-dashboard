"""One-off: add the Spring 2026 Blue Division playoff bracket to league_schedule_games.csv.

Single-elimination, all Wed 2026-06-17. Higher seed = home team.
Final standings: 1 cacheouts (champ), 2 No Dice, T3 Soft Ballz/Bullseyes, T5 Wasted
Talent/Wasted Potential/Maple Tree. Idempotent (skips if playoff rows already present).
"""
from __future__ import annotations

import csv
from pathlib import Path

CSV = Path(__file__).resolve().parents[1] / "data/processed/league_schedule_games.csv"
SEASON = "Maple Tree Spring 2026"
LEAGUE = "Wednesday Men's"
DIV = "Blue Division"

# (id, time, field, home, away, home_runs, away_runs, summary, note)
GAMES = [
    ("spring-2026-playoffs-g1", "6:30 PM", "Boncosky Yellow", "Wasted Talent", "Soft Ballz", 13, 18,
     "Soft Ballz 18, Wasted Talent 13", "Playoff Round 1 (Game 1): [5] Soft Ballz def. [4] Wasted Talent."),
    ("spring-2026-playoffs-g2", "7:30 PM", "Boncosky Yellow", "cacheouts", "Wasted Potential", 7, 0,
     "cacheouts 7, Wasted Potential 0", "Playoff Round 1 (Game 2): [2] cacheouts def. [7] Wasted Potential."),
    ("spring-2026-playoffs-g3", "7:30 PM", "Boncosky Red", "Bullseyes", "Maple Tree", 18, 8,
     "Bullseyes 18, Maple Tree 8", "Playoff Round 1 (Game 3): [3] Bullseyes def. [6] Maple Tree."),
    ("spring-2026-playoffs-g4", "8:30 PM", "Boncosky Yellow", "No Dice", "Soft Ballz", 27, 14,
     "No Dice 27, Soft Ballz 14", "Playoff Semifinal (Game 4): [1] No Dice def. [5] Soft Ballz."),
    ("spring-2026-playoffs-g5", "8:30 PM", "Boncosky Red", "cacheouts", "Bullseyes", 29, 13,
     "cacheouts 29, Bullseyes 13", "Playoff Semifinal (Game 5): [2] cacheouts def. [3] Bullseyes."),
    ("spring-2026-playoffs-g6", "9:30 PM", "", "No Dice", "cacheouts", 16, 29,
     "cacheouts 29, No Dice 16", "Playoff Championship (Game 6): [2] cacheouts def. [1] No Dice for the title."),
]


def main() -> None:
    existing = CSV.read_text(encoding="utf-8")
    if "spring-2026-playoffs-g1" in existing:
        print("playoff rows already present, skip")
        return
    rows = []
    for gid, time, field, home, away, hr, ar, summary, note in GAMES:
        rows.append([gid, SEASON, LEAGUE, DIV, "Playoffs", "2026-06-17", time, field,
                     home, away, "completed", "1", hr, ar, summary, note, "playoff_bracket"])
    with CSV.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for row in rows:
            w.writerow(row)
    print(f"appended {len(rows)} playoff rows to league_schedule_games.csv")


if __name__ == "__main__":
    main()
