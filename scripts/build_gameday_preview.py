"""Reusable weekly Gameday Preview generator (2-page PDF).

Everything is auto-detected from the exported site data (site/src/data/*.json) plus
a live opponent-scouting query, so the only thing you must supply is the batting
order. Run export_site_data.py first so the numbers match the website.

    # simplest — auto-detects the next game, its doubleheader times, and game week:
    python scripts/build_gameday_preview.py --lineup "Glove,Kives,Tristan,Tim,JJ,Porter,Corey,Joel,Walsh,Duff,Jason"

    # override anything:
    python scripts/build_gameday_preview.py --lineup "..." --opponent "Sandlot Vibes" \
        --date 2026-07-22 --week-label "Game Week 5"

Output: data/writeups/maple-tree-<season>/maple-tree-gameday-preview-<date>.pdf
"""
from __future__ import annotations

import argparse
import json
import re
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "site" / "src" / "data"
CARDS_DIR = REPO / "site" / "public" / "cards"
ASSET_CACHE = Path(tempfile.gettempdir()) / "mapletree_preview_assets"

# palette (The Maple Tree Tap)
BARK, BARK2 = HexColor("#2c1a0d"), HexColor("#4a2e15")
SAND, PAPER, WHITE = HexColor("#efece0"), HexColor("#f8f7f2"), HexColor("#ffffff")
INK, MUTED, MAPLE = HexColor("#20261f"), HexColor("#77705f"), HexColor("#c2410c")
TAN, CREAM, GREEN, LINE = HexColor("#d9c9a8"), HexColor("#f2e9d8"), HexColor("#15803d"), HexColor("#d8d3c2")
STRIPE = HexColor("#f1eee2")
W, H = letter

STAT_WORD = {"Hits": "hit", "Singles": "single", "Doubles": "double", "Triples": "triple",
             "HR": "HR", "RBI": "RBI", "Runs": "run", "Walks": "walk", "Total Bases": "total base",
             "PA": "PA", "AB": "AB", "Games": "game"}
ABBR = {"HR", "RBI", "PA", "AB"}
NUMWORD = {1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five", 6: "Six", 7: "Seven"}


# ---------- small helpers ----------
def load(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def slugify(v):
    return re.sub(r"[^a-z0-9]+", "-", str(v).lower()).strip("-")


def r3(v):
    if v is None:
        return "-"
    s = f"{float(v):.3f}"
    return s[1:] if s.startswith("0.") else s


def signed(v):
    v = int(round(v))
    return f"+{v}" if v > 0 else str(v)


def ordinal(n):
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def oxford(items):
    items = list(items)
    if len(items) <= 1:
        return "".join(items)
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


def milestone_phrase(name, rem, stat, disp, with_name=True):
    word = STAT_WORD.get(stat, stat.lower())
    if word not in ABBR and rem != 1:
        word += "s"
    head = f"{name} " if with_name else ""
    return f"{head}{rem} {word} from {disp}"


# ---------- pdf text helpers ----------
def _txt(c, x, y, s, font="Helvetica", size=10, color=INK, cs=0, align="l"):
    c.setFont(font, size)
    c.setFillColor(color)
    fn = {"l": c.drawString, "r": c.drawRightString, "c": c.drawCentredString}[align]
    if cs:
        fn(x, y, s, charSpace=cs)
    else:
        fn(x, y, s)


def wrap(c, x, y, txt, width, font="Helvetica", size=9.5, leading=12.5, color=INK):
    c.setFont(font, size)
    c.setFillColor(color)
    line = ""
    for w_ in txt.split():
        trial = (line + " " + w_).strip()
        if c.stringWidth(trial, font, size) > width and line:
            c.drawString(x, y, line)
            y -= leading
            line = w_
        else:
            line = trial
    if line:
        c.drawString(x, y, line)
        y -= leading
    return y


def section_title(c, x, y, label, width):
    _txt(c, x, y, label.upper(), "Helvetica-Bold", 11, BARK, cs=1.2)
    c.setStrokeColor(BARK2)
    c.setLineWidth(2)
    c.line(x, y - 6, x + width, y - 6)


# ---------- assets ----------
def prep_logo():
    ASSET_CACHE.mkdir(parents=True, exist_ok=True)
    dst = ASSET_CACHE / "logo.png"
    if not dst.exists():
        Image.open(REPO / "site" / "public" / "brand" / "maple-tree-tap.webp").save(dst)
    return str(dst)


def prep_card(asset):
    dst = ASSET_CACHE / f"card-{asset}.png"
    if not dst.exists():
        img = Image.open(CARDS_DIR / f"{asset}.webp").convert("RGBA")
        img.thumbnail((240, 360), Image.LANCZOS)
        mask = Image.new("L", img.size, 0)
        ImageDraw.Draw(mask).rounded_rectangle([0, 0, img.size[0] - 1, img.size[1] - 1], radius=14, fill=255)
        img.putalpha(mask)
        img.save(dst)
    return str(dst)


# ---------- data resolution ----------
def resolve_context(args):
    meta = load("meta.json")
    schedule = load("schedule.json")
    season_name = args.season or meta["current_season"]["name"]
    season_meta = next(s for s in schedule if s["name"] == season_name)
    games = season_meta["games"]

    # game-days in chronological order (dedupe doubleheaders)
    playdays = sorted({g["game_date"] for g in games if not g["is_bye"]})

    # default target = first upcoming (no result) non-bye game
    upcoming = sorted(
        (g for g in games if not g["is_bye"] and not g.get("result")),
        key=lambda g: (g["game_date"], g["game_time"] or ""))
    date = args.date or (upcoming[0]["game_date"] if upcoming else playdays[-1])

    day_games = sorted((g for g in games if g["game_date"] == date and not g["is_bye"]),
                       key=lambda g: g["game_time"] or "")
    opponent = args.opponent or (day_games[0]["opponent_name"] if day_games else None)
    if opponent is None:
        raise SystemExit("Could not detect an opponent — pass --opponent.")

    times = args.times or " & ".join(g["game_time"] for g in day_games if g["game_time"]) or "TBD"
    field = args.field or (day_games[0]["location_or_field"] if day_games else "TBD")
    ha = {g["home_away"] for g in day_games}
    prep = "at" if ha == {"away"} else "vs"

    if args.week_label:
        week_label = args.week_label
    else:
        idx = playdays.index(date) if date in playdays else len(playdays) - 1
        week_label = f"Game Week {idx + 1}"

    return {
        "meta": meta, "schedule": schedule, "season_name": season_name,
        "date": date, "opponent": opponent, "times": times, "field": field,
        "prep": prep, "week_label": week_label,
        "date_pretty": _date_pretty(date), "day_games": day_games,
    }


def _date_pretty(iso):
    from datetime import datetime
    d = datetime.fromisoformat(iso)
    return f"{d.strftime('%A, %B')} {d.day}, {d.year}"


def opponent_recent(opponent, season):
    """Live scouting: opponent's most recent results. Empty list if the DB isn't reachable."""
    try:
        import sys
        sys.path.insert(0, str(REPO))
        from src.dashboard.data import (DEFAULT_DB_PATH, fetch_league_team_recent_results,
                                        get_connection)
        con = get_connection(Path(DEFAULT_DB_PATH))
        df = fetch_league_team_recent_results(con, season=season, team_name=opponent, limit=3)
        if df is None or df.empty or "team_result_display" not in df.columns:
            return []
        return [str(v) for v in df["team_result_display"].tolist()]
    except Exception as exc:  # pragma: no cover - scouting is best-effort
        print(f"  (scouting query skipped: {exc})")
        return []


# ---------- build ----------
def main():
    ap = argparse.ArgumentParser(description="Maple Tree weekly gameday preview PDF")
    ap.add_argument("--lineup", required=True, help="comma-separated batting order (names or slugs)")
    ap.add_argument("--opponent")
    ap.add_argument("--date", help="YYYY-MM-DD")
    ap.add_argument("--times")
    ap.add_argument("--field")
    ap.add_argument("--week-label")
    ap.add_argument("--season")
    ap.add_argument("--out")
    args = ap.parse_args()

    ctx = resolve_context(args)
    meta = ctx["meta"]
    season_name = ctx["season_name"]
    season_slug = meta["current_season"]["slug"] if season_name == meta["current_season"]["name"] \
        else slugify(season_name.replace("Maple Tree ", ""))

    season = load("season_stats.json")
    players = load("players.json")
    career_stats = load("career_stats.json")
    rivalry = load("rivalry.json")
    cards = load("cards.json")

    summer = next((s for s in season if s["name"] == season_name), None)
    srows = {p["slug"]: p for p in (summer["players"] if summer else [])}
    prows = {p["slug"]: p for p in players}
    arch = {r["slug"]: r.get("archetype", "") for r in career_stats["advanced"]}
    by_name = {p["name"].lower(): p["slug"] for p in players}
    valid_slugs = {p["slug"] for p in players}

    def resolve(token):
        t = token.strip()
        if t.lower() in by_name:
            return by_name[t.lower()]
        s = slugify(t)
        if s in valid_slugs:
            return s
        raise SystemExit(f"Unknown player in lineup: '{token}'")

    lineup = [resolve(tok) for tok in args.lineup.split(",") if tok.strip()]

    def pick_card(slug):
        cs = [c for c in cards if c["slug"] == slug]
        ms = [c for c in cs if c["kind"] == "milestone"]
        return (ms or cs or [None])[0]

    def closest_milestone(slug):
        nxt = (prows[slug].get("milestones") or {}).get("next") or []
        nxt = [m for m in nxt if m.get("remaining") is not None]
        # fewest remaining, then the bigger round number on ties (100 RBI over 20 2B).
        return min(nxt, key=lambda m: (m["remaining"], -m["next_milestone_display"])) if nxt else None

    board = meta["seed_race"]["board"]
    us = next((r for r in board if r["is_team"]), None)
    opp_row = next((r for r in board if r["team_name"] == ctx["opponent"]), None)
    riv = next((r for r in rivalry if r["opponent"].lower() == ctx["opponent"].lower()), None)
    potw = meta.get("potw")

    # milestone chases within one swing (remaining <= 3), across the lineup
    chases = []
    for slug in lineup:
        m = closest_milestone(slug)
        if m and m["remaining"] <= 3:
            chases.append((prows[slug]["name"], m["remaining"], m["stat"], m["next_milestone_display"]))
    chases.sort(key=lambda x: x[1])

    # ---- storylines (data-driven) ----
    stories = []
    if us and opp_row:
        our_rec, opp_rec = meta["record"], f"{int(opp_row['wins'])}-{int(opp_row['losses'])}"
        if us["losses"] == 0 and opp_row["losses"] == 0:
            lead = "Unbeaten collision."
        elif us["seed"] and opp_row["seed"] and abs(us["seed"] - opp_row["seed"]) <= 1:
            lead = "Seeding on the line."
        else:
            lead = "The matchup."
        stories.append((lead, (
            f"Maple Tree enters {our_rec} (#{us['seed']} seed, {signed(us['run_diff'])}); "
            f"{ctx['opponent']} is {opp_rec} (#{opp_row['seed']}, {signed(opp_row['run_diff'])}). "
            f"{'Back-to-back seeds — a straight-up ladder fight.' if us['seed'] and opp_row['seed'] and abs(us['seed'] - opp_row['seed']) <= 1 else 'Seeding points are on the table.'}")))
    if summer and summer["team"]["games"]:
        t = summer["team"]
        stories.append(("The bats so far.", (
            f"A {r3(t['avg'])} team average, {int(t['r'])} runs and a {r3(t['ops'])} OPS "
            f"through {int(t['games'])} game{'s' if t['games'] != 1 else ''} this season.")))
    swing = [(nm, disp, stat) for (nm, rem, stat, disp) in chases if rem == 1]
    if swing:
        def mword(stat):
            w = STAT_WORD.get(stat, stat.lower())
            return w if w in ABBR else w + "s"
        parts = [f"{nm} ({disp} {mword(stat)})" for nm, disp, stat in swing[:6]]
        cnt = len(swing)
        stories.append(("Milestone night looming.", (
            f"{NUMWORD.get(cnt, str(cnt))} hitter{'s' if cnt != 1 else ''} "
            f"sit{'' if cnt != 1 else 's'} one swing from a career round number — {oxford(parts)}.")))
    elif chases:
        nm, rem, stat, disp = chases[0]
        stories.append(("Milestone watch.",
                        (f"{nm} leads the roster's chases — " + milestone_phrase(None, rem, stat, disp, with_name=False) + ".")))
    if riv:
        yrs = f"{riv['first_played'][:4]}–{riv['last_played'][:4]}"
        verb = "outscoring" if riv["run_diff"] >= 0 else "outscored by"
        stories.append(("The rivalry.", (
            f"Maple Tree is {riv['record']} all-time against {ctx['opponent']} "
            f"({int(riv['games'])} meetings, {yrs}), {verb} them "
            f"{int(riv['runs_for'])}–{int(riv['runs_against'])}.")))
    else:
        stories.append(("New blood.", (
            f"Maple Tree has never faced {ctx['opponent']} in any era of the franchise. "
            "First impressions count.")))
    stories = stories[:4]

    # ---- stat tiles ----
    tiles = []
    if summer and summer["team"]["games"]:
        t = summer["team"]
        rank = 1 + sum(1 for r in board if r["run_diff"] > (us["run_diff"] if us else 0))
        tiles = [
            (r3(t["avg"]), "TEAM AVG", f"through {int(t['games'])} games"),
            (str(int(t["r"])), "RUNS SCORED", f"in a {meta['record']} start"),
            (signed(us["run_diff"]) if us else "-", "RUN DIFFERENTIAL", f"{ordinal(rank)} in the league"),
        ]

    recent = opponent_recent(ctx["opponent"], season_name)

    logo = prep_logo()
    out = Path(args.out) if args.out else (
        REPO / "data" / "writeups" / f"maple-tree-{season_slug}"
        / f"maple-tree-gameday-preview-{ctx['date']}.pdf")
    out.parent.mkdir(parents=True, exist_ok=True)
    c = pdfcanvas.Canvas(str(out), pagesize=letter)
    c.setTitle(f"Maple Tree Gameday Preview - {ctx['date_pretty']}")

    # ===== PAGE 1 =====
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 96, W, 96, stroke=0, fill=1)
    c.drawImage(logo, 36, H - 88, width=76, height=76, mask="auto")
    _txt(c, 128, H - 40, f"MAPLE TREE SOFTBALL  ·  {meta['current_season']['label'].upper()}", "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, 128, H - 66, "GAMEDAY PREVIEW", "Helvetica-Bold", 28, WHITE, cs=1)
    _txt(c, 128, H - 82, "Wednesday Men's League  ·  Recreational Division  ·  Boncosky Park", "Helvetica", 8.5, TAN)

    py = H - 200
    c.setFillColor(WHITE)
    c.setStrokeColor(LINE)
    c.setLineWidth(1)
    c.roundRect(36, py, W - 72, 88, 8, stroke=1, fill=1)
    _txt(c, 208, py + 58, "MAPLE TREE", "Helvetica-Bold", 21, BARK, align="c")
    _txt(c, 208, py + 42, f"{meta['record']}"
         + (f"  ·  #{us['seed']} seed  ·  {signed(us['run_diff'])} run diff" if us else ""),
         "Helvetica", 9, MUTED, align="c")
    _txt(c, 306, py + 56, ctx["prep"].upper(), "Helvetica-Bold", 10, MAPLE, cs=1, align="c")
    _txt(c, 404, py + 58, ctx["opponent"].upper(), "Helvetica-Bold", 21, BARK, align="c")
    if opp_row:
        _txt(c, 404, py + 42, f"{int(opp_row['wins'])}-{int(opp_row['losses'])}  ·  "
             f"#{opp_row['seed']} seed  ·  {signed(opp_row['run_diff'])} run diff", "Helvetica", 9, MUTED, align="c")
    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(52, py + 32, W - 52, py + 32)
    _txt(c, W / 2, py + 18, f"{ctx['date_pretty']}   ·   {ctx['times']}   ·   {ctx['field']}",
         "Helvetica-Bold", 11, INK, align="c")
    if not riv:
        _txt(c, W / 2, py + 5, "FIRST MEETING IN FRANCHISE HISTORY", "Helvetica-Bold", 7.5, MAPLE, cs=1.5, align="c")

    ty = py - 66
    for i, (val, label, sub) in enumerate(tiles):
        x0 = 36 + i * 184
        c.setFillColor(WHITE)
        c.setStrokeColor(LINE)
        c.roundRect(x0, ty, 172, 54, 6, stroke=1, fill=1)
        _txt(c, x0 + 12, ty + 40, label, "Helvetica-Bold", 7.5, MUTED, cs=1)
        _txt(c, x0 + 12, ty + 16, val, "Helvetica-Bold", 23, BARK)
        _txt(c, x0 + 162, ty + 16, sub, "Helvetica", 7.5, MUTED, align="r")

    col_y = (ty - 28) if tiles else (py - 40)
    section_title(c, 36, col_y, "The storylines", 318)
    sy = col_y - 24
    for lead, body in stories:
        _txt(c, 36, sy, lead, "Helvetica-Bold", 10, BARK)
        sy = wrap(c, 36, sy - 13, body, 318, "Helvetica", 9.5, 12.5, INK) - 10

    def rail_box(y0, h_, title):
        c.setFillColor(WHITE)
        c.setStrokeColor(LINE)
        c.roundRect(370, y0, 206, h_, 6, stroke=1, fill=1)
        c.setFillColor(SAND)
        c.roundRect(370, y0 + h_ - 20, 206, 20, 6, stroke=0, fill=1)
        c.rect(370, y0 + h_ - 20, 206, 10, stroke=0, fill=1)
        _txt(c, 380, y0 + h_ - 14, title, "Helvetica-Bold", 8, BARK, cs=1)

    b1y = col_y - 100
    rail_box(b1y, 96, f"SCOUTING {ctx['opponent'].upper()}")
    ly = b1y + 62
    scout_lines = []
    if opp_row:
        scout_lines.append(f"{int(opp_row['wins'])}-{int(opp_row['losses'])}  ·  "
                           f"{signed(opp_row['run_diff'])} diff  ·  #{opp_row['seed']} seed")
    scout_lines += recent[:2]
    scout_lines.append(("Owns the all-time series " + riv["record"] + " vs us.") if riv else "No book on them yet - watch game one.")
    for line in scout_lines[:4]:
        _txt(c, 380, ly, line, "Helvetica", 9, INK)
        ly -= 14

    if potw:
        b2y = b1y - 110
        rail_box(b2y, 96, "REIGNING PLAYER OF THE WEEK")
        _txt(c, 380, b2y + 58, potw["player"], "Helvetica-Bold", 15, BARK)
        _txt(c, 380, b2y + 42, f"{potw['hits']}-for-{potw['ab']}, {potw['hr']} HR, {potw['rbi']} RBI, {potw['r']} R", "Helvetica", 9.5, INK)
        _txt(c, 380, b2y + 28, f"vs {potw['opponents']}  ·  Game Score {potw['game_score']:.1f}"
             + (f" ({potw['games']}-game)" if potw.get('games', 1) > 1 else ""), "Helvetica", 8.5, MUTED)
        m = closest_milestone(potw["slug"])
        if m and m["remaining"] <= 3:
            _txt(c, 380, b2y + 12, "Also " + milestone_phrase(potw["player"].split()[0], m["remaining"], m["stat"], m["next_milestone_display"], with_name=False) + ".", "Helvetica-Oblique", 8.5, MAPLE)

    # seed race table
    st_y = 268
    section_title(c, 36, st_y, "Race to the #1 seed", W - 72)
    cols = [("SEED", 58, "r"), ("W", 336, "r"), ("L", 376, "r"), ("RF", 426, "r"),
            ("RA", 476, "r"), ("DIFF", 530, "r"), ("LEFT", 574, "r")]
    hy = st_y - 24
    _txt(c, 76, hy, "TEAM", "Helvetica-Bold", 7, MUTED)
    for label, x, _ in cols:
        _txt(c, x, hy, label, "Helvetica-Bold", 7, MUTED, align="r")
    ry, row_h = hy - 8, 17
    for i, r in enumerate(board[:6]):
        y0 = ry - row_h * (i + 1)
        highlight = r["is_team"]
        if highlight:
            c.setFillColor(SAND)
            c.rect(36, y0 - 4, W - 72, row_h, stroke=0, fill=1)
            c.setFillColor(MAPLE)
            c.rect(36, y0 - 4, 3, row_h, stroke=0, fill=1)
        bold = r["is_team"] or r["team_name"] == ctx["opponent"]
        f = "Helvetica-Bold" if bold else "Helvetica"
        _txt(c, 58, y0, str(int(r["seed"])), f, 9, MUTED, align="r")
        _txt(c, 76, y0, r["team_name"], f, 9.5, BARK if bold else INK)
        for key, x in [("wins", 336), ("losses", 376), ("runs_for", 426), ("runs_against", 476)]:
            _txt(c, x, y0, str(int(r[key])), f, 9.5, align="r")
        d = int(r["run_diff"])
        _txt(c, 530, y0, signed(d), f, 9.5, GREEN if d > 0 else MUTED, align="r")
        _txt(c, 574, y0, str(int(r["games_remaining"])), f, 9.5, align="r")
    note_y = ry - row_h * 6 - 18
    _txt(c, 36, note_y, f"Wednesday's opponent in bold. Seeded by win %, then run differential — every team makes the playoffs.", "Helvetica", 7.5, MUTED)

    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(36, 64, W - 36, 64)
    _txt(c, 36, 50, "MAPLE TREE SOFTBALL", "Helvetica-Bold", 8, BARK, cs=1)
    _txt(c, W - 36, 50, "mapletreesoftball.netlify.app  ·  The Maple Tree Tap - Cary, Illinois", "Helvetica", 8, MUTED, align="r")
    c.showPage()

    # ===== PAGE 2 : LINEUP =====
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 84, W, 84, stroke=0, fill=1)
    _txt(c, 36, H - 38, f"{meta['current_season']['label'].upper()}  ·  {ctx['week_label'].upper()}", "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, 36, H - 64, "THE LINEUP", "Helvetica-Bold", 26, WHITE, cs=1)
    _txt(c, W - 36, H - 38, f"{ctx['prep']} {ctx['opponent']}", "Helvetica-Bold", 12, CREAM, align="r")
    _txt(c, W - 36, H - 56, f"{ctx['date_pretty'].split(',')[0]}, {ctx['date_pretty'].split(', ')[1]}  ·  {ctx['times']}  ·  {ctx['field']}", "Helvetica", 9, TAN, align="r")

    head_y = H - 108
    _txt(c, 104, head_y, "HITTER", "Helvetica-Bold", 7, MUTED, cs=1)
    for label, x in [("AVG", 368), ("OPS", 414), ("H", 446), ("HR", 478), ("RBI", 514)]:
        _txt(c, x, head_y, label, "Helvetica-Bold", 7, MUTED, align="r")
    _txt(c, 572, head_y, "wRC+", "Helvetica-Bold", 7, MUTED, align="r")
    _txt(c, 322, head_y + 11, meta["current_season"]["label"].upper(), "Helvetica-Bold", 6.5, MUTED, cs=1)
    _txt(c, 572, head_y + 11, "CAREER", "Helvetica-Bold", 6.5, MUTED, align="r")

    top, row_h = head_y - 10, 56
    for i, slug in enumerate(lineup):
        p, s = prows[slug], srows.get(slug)
        y0 = top - row_h * (i + 1)
        if i % 2 == 0:
            c.setFillColor(STRIPE)
            c.rect(36, y0, W - 72, row_h, stroke=0, fill=1)
        _txt(c, 50, y0 + row_h / 2 - 6, str(i + 1), "Helvetica-Bold", 17, BARK, align="c")
        art_h = 48
        card = pick_card(slug)
        if card:
            path = prep_card(card["asset"])
            img = Image.open(path)
            aw = art_h * (img.size[0] / img.size[1])
            c.drawImage(path, 66, y0 + (row_h - art_h) / 2, width=aw, height=art_h, mask="auto")
        else:
            c.setFillColor(BARK2)
            c.roundRect(66, y0 + (row_h - art_h) / 2, 32, art_h, 4, stroke=0, fill=1)
            _txt(c, 82, y0 + row_h / 2 - 4, p["name"][0], "Helvetica-Bold", 15, CREAM, align="c")
            _txt(c, 82, y0 + 7, "CARD TBD", "Helvetica", 4.2, TAN, align="c")
        ny = y0 + row_h / 2 + 4
        _txt(c, 106, ny, p["name"], "Helvetica-Bold", 12.5, BARK)
        m = closest_milestone(slug)
        if m and m["remaining"] <= 5:
            word = STAT_WORD.get(m["stat"], m["stat"].lower())
            if word not in ABBR and m["remaining"] != 1:
                word += "s"
            tag, tcolor = f"{m['remaining']} from {m['next_milestone_display']} career {word}", MAPLE
        elif p.get("potw"):
            tag, tcolor = f"{int(p['potw'])}x career Player of the Week", MUTED
        else:
            tag, tcolor = arch.get(slug, ""), MUTED
        _txt(c, 106, ny - 13, tag, "Helvetica", 8, tcolor)
        if s:
            for key, x in [("avg", 368), ("ops", 414)]:
                _txt(c, x, ny - 4, r3(s[key]), "Helvetica", 10, align="r")
            for key, x in [("hits", 446), ("hr", 478), ("rbi", 514)]:
                _txt(c, x, ny - 4, str(int(s[key])), "Helvetica", 10, align="r")
        else:
            for x in [368, 414, 446, 478, 514]:
                _txt(c, x, ny - 4, "-", "Helvetica", 10, MUTED, align="r")
        wrc = p["career"].get("wrc_plus")
        _txt(c, 572, ny - 4, str(int(round(wrc))) if wrc else "-", "Helvetica-Bold", 10.5, BARK, align="r")

    fy = top - row_h * len(lineup) - 16
    keys = []
    if not riv:
        keys.append("Nobody has a book on anybody — win game one")
    if swing:
        keys.append(f"{len(swing)} hitter{'s' if len(swing) != 1 else ''} one swing from a milestone")
    keys.append("Feed the middle of the order")
    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(36, fy, W - 36, fy)
    _txt(c, 36, fy - 14, "KEYS TO THE NIGHT", "Helvetica-Bold", 7.5, BARK, cs=1)
    _txt(c, 36, fy - 27, "   ·   ".join(keys) + ".", "Helvetica", 8.5, INK)
    _txt(c, W - 36, fy - 14, "mapletreesoftball.netlify.app/cards", "Helvetica", 7.5, MUTED, align="r")
    c.showPage()
    c.save()

    print(f"\nGameday preview -> {out}")
    print(f"  {ctx['prep']} {ctx['opponent']}  ·  {ctx['date_pretty']}  ·  {ctx['times']}  ·  {ctx['field']}")
    print(f"  {ctx['week_label']}  ·  lineup: {', '.join(prows[s]['name'] for s in lineup)}")


if __name__ == "__main__":
    main()
