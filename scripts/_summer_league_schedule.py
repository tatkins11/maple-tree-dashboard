"""Summer 2026: set division to 'Recreational' and load the full league schedule.

- Fixes division_name on the Maple Tree Summer rows already in team_schedule.csv.
- Appends every league game (all teams, Weeks 1-7) to league_schedule_games.csv.
Idempotent.
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEAM = ROOT / "data/processed/team_schedule.csv"
LEAGUE = ROOT / "data/processed/league_schedule_games.csv"
SEASON = "Maple Tree Summer 2026"
LEAGUE_NAME = "Wednesday Men's"
DIV = "Recreational"

# Weeks: (week_num, date, [(time, field, away, home), ...])
WEEKS = [
    ("1", "2026-06-24", [
        ("6:30 PM", "Boncosky Blue", "Wasted Talent", "Mean Beanz"),
        ("6:30 PM", "Boncosky Green", "Everything hurts", "Sandlot Vibes"),
        ("6:30 PM", "Boncosky Yellow", "Slaughtered in 3", "Nuketown"),
        ("7:30 PM", "Boncosky Blue", "Mean Beanz", "Wasted Talent"),
        ("7:30 PM", "Boncosky Green", "Sandlot Vibes", "Everything hurts"),
        ("7:30 PM", "Boncosky Yellow", "Nuketown", "Slaughtered in 3"),
        ("8:30 PM", "Boncosky Blue", "Bleacher Bums", "Zero to Hiro"),
        ("8:30 PM", "Boncosky Green", "Brew Crew", "Como"),
        ("9:30 PM", "Boncosky Blue", "Zero to Hiro", "Bleacher Bums"),
        ("9:30 PM", "Boncosky Green", "Como", "Brew Crew"),
    ]),
    ("2", "2026-07-01", [
        ("6:30 PM", "Boncosky Green", "Brew Crew", "Zero to Hiro"),
        ("6:30 PM", "Boncosky Red", "Mean Beanz", "Nuketown"),
        ("7:30 PM", "Boncosky Green", "Zero to Hiro", "Brew Crew"),
        ("7:30 PM", "Boncosky Red", "Nuketown", "Mean Beanz"),
        ("8:30 PM", "Boncosky Blue", "Como", "Maple Tree"),
        ("8:30 PM", "Boncosky Green", "Slaughtered in 3", "Everything hurts"),
        ("8:30 PM", "Boncosky Red", "Sandlot Vibes", "Bleacher Bums"),
        ("9:30 PM", "Boncosky Blue", "Maple Tree", "Como"),
        ("9:30 PM", "Boncosky Green", "Everything hurts", "Slaughtered in 3"),
        ("9:30 PM", "Boncosky Red", "Bleacher Bums", "Sandlot Vibes"),
    ]),
    ("3", "2026-07-08", [
        ("6:30 PM", "Boncosky Green", "Wasted Talent", "Slaughtered in 3"),
        ("6:30 PM", "Boncosky Red", "Maple Tree", "Brew Crew"),
        ("6:30 PM", "Boncosky Yellow", "Como", "Bleacher Bums"),
        ("7:30 PM", "Boncosky Green", "Slaughtered in 3", "Wasted Talent"),
        ("7:30 PM", "Boncosky Red", "Brew Crew", "Maple Tree"),
        ("7:30 PM", "Boncosky Yellow", "Bleacher Bums", "Como"),
        ("8:30 PM", "Boncosky Green", "Nuketown", "Sandlot Vibes"),
        ("8:30 PM", "Boncosky Red", "Zero to Hiro", "Everything hurts"),
        ("9:30 PM", "Boncosky Green", "Sandlot Vibes", "Nuketown"),
        ("9:30 PM", "Boncosky Red", "Everything hurts", "Zero to Hiro"),
    ]),
    ("4", "2026-07-15", [
        ("6:30 PM", "Boncosky Blue", "Bleacher Bums", "Slaughtered in 3"),
        ("6:30 PM", "Boncosky Green", "Zero to Hiro", "Maple Tree"),
        ("6:30 PM", "Boncosky Yellow", "Everything hurts", "Mean Beanz"),
        ("7:30 PM", "Boncosky Blue", "Slaughtered in 3", "Bleacher Bums"),
        ("7:30 PM", "Boncosky Green", "Maple Tree", "Zero to Hiro"),
        ("7:30 PM", "Boncosky Yellow", "Mean Beanz", "Everything hurts"),
        ("8:30 PM", "Boncosky Blue", "Sandlot Vibes", "Brew Crew"),
        ("8:30 PM", "Boncosky Green", "Nuketown", "Wasted Talent"),
        ("9:30 PM", "Boncosky Blue", "Brew Crew", "Sandlot Vibes"),
        ("9:30 PM", "Boncosky Green", "Wasted Talent", "Nuketown"),
    ]),
    ("5", "2026-07-22", [
        ("6:30 PM", "Boncosky Blue", "Nuketown", "Zero to Hiro"),
        ("6:30 PM", "Boncosky Green", "Como", "Everything hurts"),
        ("7:30 PM", "Boncosky Blue", "Zero to Hiro", "Nuketown"),
        ("7:30 PM", "Boncosky Green", "Everything hurts", "Como"),
        ("8:30 PM", "Boncosky Blue", "Mean Beanz", "Slaughtered in 3"),
        ("8:30 PM", "Boncosky Green", "Maple Tree", "Bleacher Bums"),
        ("8:30 PM", "Boncosky Red", "Wasted Talent", "Sandlot Vibes"),
        ("9:30 PM", "Boncosky Blue", "Slaughtered in 3", "Mean Beanz"),
        ("9:30 PM", "Boncosky Green", "Bleacher Bums", "Maple Tree"),
        ("9:30 PM", "Boncosky Red", "Sandlot Vibes", "Wasted Talent"),
    ]),
    ("6", "2026-07-29", [
        ("6:30 PM", "Boncosky Green", "Sandlot Vibes", "Maple Tree"),
        ("6:30 PM", "Boncosky Red", "Zero to Hiro", "Como"),
        ("6:30 PM", "Boncosky Yellow", "Slaughtered in 3", "Brew Crew"),
        ("7:30 PM", "Boncosky Green", "Maple Tree", "Sandlot Vibes"),
        ("7:30 PM", "Boncosky Red", "Como", "Zero to Hiro"),
        ("7:30 PM", "Boncosky Yellow", "Brew Crew", "Slaughtered in 3"),
        ("8:30 PM", "Boncosky Red", "Everything hurts", "Wasted Talent"),
        ("8:30 PM", "Boncosky Yellow", "Bleacher Bums", "Mean Beanz"),
        ("9:30 PM", "Boncosky Red", "Wasted Talent", "Everything hurts"),
        ("9:30 PM", "Boncosky Yellow", "Mean Beanz", "Bleacher Bums"),
    ]),
    ("7", "2026-08-05", [
        ("6:30 PM", "Boncosky Blue", "Brew Crew", "Nuketown"),
        ("6:30 PM", "Boncosky Green", "Mean Beanz", "Como"),
        ("6:30 PM", "Boncosky Yellow", "Maple Tree", "Wasted Talent"),
        ("7:30 PM", "Boncosky Blue", "Nuketown", "Brew Crew"),
        ("7:30 PM", "Boncosky Green", "Como", "Mean Beanz"),
        ("7:30 PM", "Boncosky Yellow", "Wasted Talent", "Maple Tree"),
    ]),
]


def fix_team_division():
    rows = list(csv.reader(TEAM.open(encoding="utf-8")))
    header = rows[0]
    idx = {n.lstrip("﻿"): i for i, n in enumerate(header)}
    changed = 0
    for row in rows[1:]:
        if row and row[idx["season"]] == SEASON and row[idx["division_name"]] != DIV:
            row[idx["division_name"]] = DIV
            changed += 1
    with TEAM.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)
    print(f"team_schedule.csv: set division=Recreational on {changed} Summer rows")


def add_league_games():
    if "summer-2026-week" in LEAGUE.read_text(encoding="utf-8"):
        print("league_schedule_games.csv: summer rows already present, skip")
        return
    out = []
    for wk, date, games in WEEKS:
        for gi, (time, field, away, home) in enumerate(games, start=1):
            gid = f"summer-2026-week-{wk}-g{gi}"
            # cols: league_game_id,season,league_name,division_name,week_label,game_date,game_time,
            #       location_or_field,home_team,away_team,status,completed_flag,home_runs,away_runs,
            #       result_summary,notes,source
            out.append([gid, SEASON, LEAGUE_NAME, DIV, f"Week {wk}", date, time, field, home, away,
                        "scheduled", "0", "", "", "", "", "league_schedule_games.csv"])
    with LEAGUE.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for row in out:
            w.writerow(row)
    print(f"league_schedule_games.csv: appended {len(out)} Summer league games")


if __name__ == "__main__":
    fix_team_division()
    add_league_games()
