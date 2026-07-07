"""Reusable weekly Postgame Recap generator (2-page PDF).

Recaps the most recent completed game day: scoreboards, the stars, milestones
actually reached that night, updated standings (page 1), and full box scores for
each game (page 2). Reuses the shared drawing kit from build_gameday_preview.

    python scripts/build_postgame_recap.py          # auto = latest completed game day
    python scripts/build_postgame_recap.py --date 2026-07-01

Run export_site_data.py first so the numbers match the website.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PIL import Image
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for the shared kit
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # for src.dashboard.data

from build_gameday_preview import (  # noqa: E402  (shared drawing kit)
    BARK, BARK2, CREAM, GREEN, INK, LINE, MAPLE, MUTED, PAPER, SAND, STRIPE, TAN, WHITE,
    _txt, load, ordinal, oxford, prep_card, prep_logo, r3, section_title, signed, slugify, wrap,
)

REPO = Path(__file__).resolve().parents[1]
W, H = letter

MS_WORD = {"Hits": "hit", "HR": "home run", "RBI": "RBI", "Total Bases": "total base",
           "Runs": "run", "Doubles": "double", "Triples": "triple", "Walks": "walk",
           "Singles": "single", "PA": "plate appearance", "AB": "at-bat", "Games": "game"}
# Rank reached milestones by magnitude, weighted so rare events (HR, triples) punch
# up and volume stats (PA/AB/Games) settle down. score = milestone * weight.
MS_WEIGHT = {"HR": 3.0, "Triples": 3.0, "Doubles": 2.0, "RBI": 1.2, "Hits": 1.0, "Runs": 1.0,
             "Walks": 1.0, "Singles": 0.8, "Total Bases": 0.7, "PA": 0.3, "AB": 0.3, "Games": 0.2}


def ms_score(e):
    return e["milestone"] * MS_WEIGHT.get(e["stat"], 1.0)


def ms_word(stat, plural=False):
    w = MS_WORD.get(stat, stat.lower())
    if plural and stat != "RBI":
        w += "s"
    return w


def main():
    ap = argparse.ArgumentParser(description="Maple Tree weekly postgame recap PDF")
    ap.add_argument("--date", help="YYYY-MM-DD (default: latest completed game day)")
    ap.add_argument("--season")
    ap.add_argument("--out")
    args = ap.parse_args()

    meta = load("meta.json")
    schedule = load("schedule.json")
    players = load("players.json")
    milestones = load("milestones.json")
    cards = load("cards.json")
    season_name = args.season or meta["current_season"]["name"]
    season_meta = next(s for s in schedule if s["name"] == season_name)

    played = [g for g in season_meta["games"] if not g["is_bye"] and g.get("result")]
    if not played:
        raise SystemExit("No completed games found to recap.")
    date = args.date or max(g["game_date"] for g in played)
    day = sorted((g for g in played if g["game_date"] == date), key=lambda g: g["game_time"] or "")
    if not day:
        raise SystemExit(f"No completed team games on {date}.")
    opponent = day[0]["opponent_name"]
    field = day[0]["location_or_field"]

    from datetime import datetime
    d = datetime.fromisoformat(date)
    date_pretty = f"{d.strftime('%A, %B')} {d.day}, {d.year}"

    name_of = {p["canonical"]: p["name"] for p in players}
    prows = {p["slug"]: p for p in players}

    # ---- per-game box scores from the DB (authoritative, ordered by game time) ----
    from src.dashboard.data import DEFAULT_DB_PATH, fetch_single_game_stats, get_connection
    con = get_connection(Path(DEFAULT_DB_PATH))
    gs_df = fetch_single_game_stats(con, seasons=[season_name])
    gs_df = gs_df[gs_df["game_date"] == date]

    boxes = []  # one per game, ordered by time
    for g in day:
        rows = gs_df[gs_df["game_time"] == g["game_time"]].sort_values("lineup_spot")
        lines = []
        for _, r in rows.iterrows():
            lines.append({
                "name": name_of.get(str(r["canonical_name"]), str(r["player"])),
                "canonical": str(r["canonical_name"]),
                **{k: int(r[k] or 0) for k in ("ab", "hits", "2b", "3b", "hr", "bb", "r", "rbi", "tb")},
                "gs": float(r["game_score"] or 0),
            })
        tot = {k: sum(l[k] for l in lines) for k in ("ab", "hits", "2b", "3b", "hr", "bb", "r", "rbi", "tb")}
        boxes.append({
            "time": g["game_time"], "ha": g["home_away"], "result": g["result"],
            "rf": int(g["runs_for"]), "ra": int(g["runs_against"]), "lines": lines, "tot": tot,
        })

    wins = sum(1 for b in boxes if b["result"] == "W")
    n = len(boxes)
    if n == 2:
        outcome_verb = ("swept" if wins == 2 else "split with" if wins == 1 else "were swept by")
        headline = (f"MAPLE TREE SWEEPS {opponent.upper()}, 2-0" if wins == 2
                    else f"MAPLE TREE SPLITS WITH {opponent.upper()}" if wins == 1
                    else f"{opponent.upper()} SWEEPS MAPLE TREE, 2-0")
    else:
        outcome_verb = "beat" if wins else "fell to"
        headline = f"MAPLE TREE {'DEFEATS' if wins else 'FALLS TO'} {opponent.upper()}"
    day_rf = sum(b["rf"] for b in boxes)
    day_ra = sum(b["ra"] for b in boxes)
    day_hits = sum(b["tot"]["hits"] for b in boxes)

    # ---- stars: combined line across the day, by total game score ----
    combo = {}
    for b in boxes:
        for l in b["lines"]:
            c = combo.setdefault(l["canonical"], {"name": l["name"], "canonical": l["canonical"],
                                                   "ab": 0, "hits": 0, "hr": 0, "rbi": 0, "r": 0,
                                                   "gs": 0.0, "g": 0})
            for k in ("ab", "hits", "hr", "rbi", "r"):
                c[k] += l[k]
            c["gs"] += l["gs"]
            c["g"] += 1
    stars = sorted(combo.values(), key=lambda c: -c["gs"])

    def star_line(c):
        parts = [f"{c['hits']}-for-{c['ab']}"]
        if c["hr"]:
            parts.append(f"{c['hr']} HR")
        if c["rbi"]:
            parts.append(f"{c['rbi']} RBI")
        if c["r"]:
            parts.append(f"{c['r']} R")
        return ", ".join(parts)

    # ---- milestones reached that night, ranked by weighted magnitude ----
    reached = [e for e in milestones["recent"] if e["date"] == date]
    reached.sort(key=lambda e: -ms_score(e))

    board = meta["seed_race"]["board"]
    us = next((r for r in board if r["is_team"]), None)
    potw = meta.get("potw")

    def pick_card(slug):
        cs = [c for c in cards if c["slug"] == slug]
        ms = [c for c in cs if c["kind"] == "milestone"]
        return (ms or cs or [None])[0]

    # ---- storylines ----
    stories = []
    scores = " and ".join(f"{b['rf']}-{b['ra']}" for b in boxes)
    stories.append(("The result.", (
        f"Maple Tree {outcome_verb} {opponent} {'on the night' if n > 1 else ''}, {scores} — "
        f"{day_rf} run{'s' if day_rf != 1 else ''} on {day_hits} hits at {field}.")))
    if stars:
        body = f"{stars[0]['name']} led the way at {star_line(stars[0])}"
        if len(stars) > 1 and stars[1]["gs"] > 0:
            body += f", and {stars[1]['name']} backed him up with {star_line(stars[1])}"
        stories.append(("The stars.", body + "."))
    if reached:
        marquee = [f"{e['player']}'s {ordinal(e['milestone'])} career {ms_word(e['stat'])}"
                   for e in reached if e["stat"] != "Games"][:3]
        if marquee:
            extra = len(reached) - len(marquee)
            tail = f" — plus {extra} more round numbers on the night" if extra > 0 else ""
            stories.append(("Milestone parade.", f"A big night for the record book: {oxford(marquee)}{tail}."))
    if us:
        stories.append(("Where it leaves us.", (
            f"Maple Tree sits {meta['record']} — the #{us['seed']} seed at {signed(us['run_diff'])} "
            f"run differential, {int(us['games_remaining'])} to play.")))
    stories = stories[:4]

    logo = prep_logo()
    season_slug = meta["current_season"]["slug"]
    out = Path(args.out) if args.out else (
        REPO / "data" / "writeups" / f"maple-tree-{season_slug}"
        / f"maple-tree-postgame-recap-{date}.pdf")
    out.parent.mkdir(parents=True, exist_ok=True)
    c = pdfcanvas.Canvas(str(out), pagesize=letter)
    c.setTitle(f"Maple Tree Postgame Recap - {date_pretty}")

    # ===== PAGE 1 =====
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 96, W, 96, stroke=0, fill=1)
    c.drawImage(logo, 36, H - 88, width=76, height=76, mask="auto")
    _txt(c, 128, H - 40, f"MAPLE TREE SOFTBALL  ·  {meta['current_season']['label'].upper()}", "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, 128, H - 66, "POSTGAME RECAP", "Helvetica-Bold", 28, WHITE, cs=1)
    _txt(c, 128, H - 82, date_pretty, "Helvetica", 8.5, TAN)

    # result panel with a scoreboard per game
    ph = 120
    py = H - 104 - ph
    c.setFillColor(WHITE)
    c.setStrokeColor(LINE)
    c.setLineWidth(1)
    c.roundRect(36, py, W - 72, ph, 8, stroke=1, fill=1)
    headline = f"MAPLE TREE {['SWEEPS', 'SPLITS WITH', 'DROPS TWO TO'][2 - wins] if n == 2 else ('DEFEATS' if wins else 'FALLS TO')} {opponent.upper()}"
    if n == 2 and wins == 2:
        headline = f"MAPLE TREE SWEEPS {opponent.upper()}, 2-0"
    _txt(c, W / 2, py + ph - 22, headline, "Helvetica-Bold", 15, BARK, align="c")
    c.setStrokeColor(LINE)
    c.setLineWidth(0.6)
    c.line(60, py + ph - 34, W - 60, py + ph - 34)
    n_boxes = len(boxes)
    bw, gap = 210, 26
    total_w = n_boxes * bw + (n_boxes - 1) * gap
    x_start = (W - total_w) / 2
    for i, b in enumerate(boxes):
        bx = x_start + i * (bw + gap)
        by = py + 14
        _txt(c, bx + bw / 2, by + 62, f"GAME {i + 1}  ·  {b['time']}  ·  {'HOME' if b['ha'] == 'home' else 'AWAY'}",
             "Helvetica-Bold", 7.5, MUTED, cs=1, align="c")
        res_color = GREEN if b["result"] == "W" else MAPLE
        for j, (team, score) in enumerate([("Maple Tree", b["rf"]), (opponent, b["ra"])]):
            yy = by + 42 - j * 20
            win_side = (b["result"] == "W" and j == 0) or (b["result"] == "L" and j == 1)
            _txt(c, bx + 14, yy, team, "Helvetica-Bold" if win_side else "Helvetica", 12,
                 BARK if win_side else MUTED)
            _txt(c, bx + bw - 14, yy, str(score), "Helvetica-Bold", 15,
                 BARK if win_side else MUTED, align="r")
        c.setFillColor(res_color)
        c.circle(bx + 10, by + 4, 3, stroke=0, fill=1)
        _txt(c, bx + 18, by + 1, f"FINAL  ·  {b['result']} {b['rf']}-{b['ra']}", "Helvetica-Bold", 8, res_color)

    # stat tiles
    tiles = [(str(day_rf), "RUNS SCORED", f"across {n} game{'s' if n != 1 else ''}"),
             (str(day_hits), "TEAM HITS", f"in the {['sweep', 'split', 'day'][2 - wins] if n == 2 else 'game'}"),
             (signed(day_rf - day_ra), "RUN DIFFERENTIAL", "on the night")]
    ty = py - 66
    for i, (val, label, sub) in enumerate(tiles):
        x0 = 36 + i * 184
        c.setFillColor(WHITE)
        c.setStrokeColor(LINE)
        c.roundRect(x0, ty, 172, 54, 6, stroke=1, fill=1)
        _txt(c, x0 + 12, ty + 40, label, "Helvetica-Bold", 7.5, MUTED, cs=1)
        _txt(c, x0 + 12, ty + 16, val, "Helvetica-Bold", 23, BARK)
        _txt(c, x0 + 162, ty + 16, sub, "Helvetica", 7.5, MUTED, align="r")

    # storylines (left) + rail (right)
    col_y = ty - 28
    section_title(c, 36, col_y, "How it happened", 318)
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

    # player of the game (with card art)
    star = stars[0] if stars else None
    pog = (combo.get(potw["slug"]) if potw else star) or star
    pog_slug = potw["slug"] if potw else (slugify(star["canonical"]) if star else "")
    b1y = col_y - 116
    rail_box(b1y, 112, "PLAYER OF THE GAME")
    if pog:
        card = pick_card(pog_slug)
        tx = 380
        if card:
            path = prep_card(card["asset"])
            img = Image.open(path)
            ah = 74
            aw = ah * (img.size[0] / img.size[1])
            c.drawImage(path, 380, b1y + 16, width=aw, height=ah, mask="auto")
            tx = 380 + aw + 12
        _txt(c, tx, b1y + 74, pog["name"], "Helvetica-Bold", 15, BARK)
        _txt(c, tx, b1y + 58, star_line(pog), "Helvetica", 9.5, INK)
        if potw:
            _txt(c, tx, b1y + 44, f"Game Score {potw['game_score']:.1f}"
                 + (f" ({potw['games']}-game)" if potw.get("games", 1) > 1 else ""), "Helvetica", 8.5, MUTED)
        _txt(c, tx, b1y + 26, "Player of the Week", "Helvetica-Oblique", 8.5, MAPLE)

    # milestones reached
    if reached:
        b2y = b1y - 130
        h2 = 124
        rail_box(b2y, h2, "MILESTONES REACHED")
        ly = b2y + h2 - 36
        for e in reached[:6]:
            _txt(c, 380, ly, "•", "Helvetica-Bold", 9, MAPLE)
            _txt(c, 392, ly, f"{e['player']} — {e['milestone']} career {ms_word(e['stat'], plural=True)}",
                 "Helvetica", 8.5, INK)
            ly -= 13.5
        if len(reached) > 6:
            _txt(c, 392, ly, f"+ {len(reached) - 6} more career milestones", "Helvetica-Oblique", 8, MUTED)

    # standings after
    st_y = 250
    section_title(c, 36, st_y, "Standings after", W - 72)
    cols = [("SEED", 58, "r"), ("W", 336, "r"), ("L", 376, "r"), ("RF", 426, "r"),
            ("RA", 476, "r"), ("DIFF", 530, "r"), ("LEFT", 574, "r")]
    hy = st_y - 24
    _txt(c, 76, hy, "TEAM", "Helvetica-Bold", 7, MUTED)
    for label, x, _a in cols:
        _txt(c, x, hy, label, "Helvetica-Bold", 7, MUTED, align="r")
    ry, row_h = hy - 8, 17
    for i, r in enumerate(board[:6]):
        y0 = ry - row_h * (i + 1)
        bold = r["is_team"] or r["team_name"] == opponent
        if r["is_team"]:
            c.setFillColor(SAND)
            c.rect(36, y0 - 4, W - 72, row_h, stroke=0, fill=1)
            c.setFillColor(MAPLE)
            c.rect(36, y0 - 4, 3, row_h, stroke=0, fill=1)
        f = "Helvetica-Bold" if bold else "Helvetica"
        _txt(c, 58, y0, str(int(r["seed"])), f, 9, MUTED, align="r")
        _txt(c, 76, y0, r["team_name"], f, 9.5, BARK if bold else INK)
        for key, x in [("wins", 336), ("losses", 376), ("runs_for", 426), ("runs_against", 476)]:
            _txt(c, x, y0, str(int(r[key])), f, 9.5, align="r")
        dd = int(r["run_diff"])
        _txt(c, 530, y0, signed(dd), f, 9.5, GREEN if dd > 0 else MUTED, align="r")
        _txt(c, 574, y0, str(int(r["games_remaining"])), f, 9.5, align="r")

    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(36, 64, W - 36, 64)
    _txt(c, 36, 50, "MAPLE TREE SOFTBALL", "Helvetica-Bold", 8, BARK, cs=1)
    _txt(c, W - 36, 50, "mapletreesoftball.netlify.app  ·  The Maple Tree Tap - Cary, Illinois", "Helvetica", 8, MUTED, align="r")
    c.showPage()

    # ===== PAGE 2 : BOX SCORES =====
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 72, W, 72, stroke=0, fill=1)
    _txt(c, 36, H - 34, f"{opponent.upper()}  ·  {date_pretty.split(',')[0].upper()}, {date_pretty.split(', ')[1].upper()}", "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, 36, H - 58, "THE BOX SCORE", "Helvetica-Bold", 24, WHITE, cs=1)

    STATCOLS = [("AB", 366), ("H", 396), ("2B", 424), ("3B", 452), ("HR", 482),
                ("BB", 512), ("R", 540), ("RBI", 570), ("GS", 602)]
    y = H - 96
    for i, b in enumerate(boxes):
        _txt(c, 36, y, f"GAME {i + 1}", "Helvetica-Bold", 11, BARK, cs=1)
        _txt(c, 96, y, f"Maple Tree {b['rf']}, {opponent} {b['ra']}  ·  {b['time']}  ·  "
             f"{'Home' if b['ha'] == 'home' else 'Away'}", "Helvetica", 9.5, MUTED)
        res_color = GREEN if b["result"] == "W" else MAPLE
        _txt(c, W - 36, y, b["result"], "Helvetica-Bold", 11, res_color, align="r")
        c.setStrokeColor(BARK2)
        c.setLineWidth(1.5)
        c.line(36, y - 6, W - 36, y - 6)
        hy = y - 20
        _txt(c, 44, hy, "HITTER", "Helvetica-Bold", 7, MUTED, cs=0.5)
        for label, x in STATCOLS:
            _txt(c, x, hy, label, "Helvetica-Bold", 7, MUTED, align="r")
        ry2 = hy - 6
        rh = 15.5
        for j, l in enumerate(b["lines"]):
            yy = ry2 - rh * (j + 1)
            if j % 2 == 0:
                c.setFillColor(STRIPE)
                c.rect(36, yy - 3.5, W - 72, rh, stroke=0, fill=1)
            _txt(c, 44, yy, f"{j + 1}. {l['name']}", "Helvetica", 9, INK)
            for (label, x), key in zip(STATCOLS[:-1], ("ab", "hits", "2b", "3b", "hr", "bb", "r", "rbi")):
                _txt(c, x, yy, str(l[key]) if l[key] else "-", "Helvetica", 9,
                     INK if l[key] else MUTED, align="r")
            _txt(c, 602, yy, f"{l['gs']:.1f}", "Helvetica-Bold", 9,
                 MAPLE if l["gs"] >= 3 else INK, align="r")
        # team totals
        tyy = ry2 - rh * (len(b["lines"]) + 1)
        c.setStrokeColor(BARK2)
        c.setLineWidth(1)
        c.line(36, tyy + rh - 3.5, W - 36, tyy + rh - 3.5)
        _txt(c, 44, tyy, "TEAM", "Helvetica-Bold", 9, BARK)
        for (label, x), key in zip(STATCOLS[:-1], ("ab", "hits", "2b", "3b", "hr", "bb", "r", "rbi")):
            _txt(c, x, tyy, str(b["tot"][key]), "Helvetica-Bold", 9, BARK, align="r")
        _txt(c, 602, tyy, str(b["rf"]), "Helvetica-Bold", 9, BARK, align="r")
        y = tyy - 40

    _txt(c, 36, 54, "GS = Game Score — single-game offensive impact.  ·  Box scores from the club scorebook.",
         "Helvetica", 8, MUTED)
    _txt(c, W - 36, 54, "mapletreesoftball.netlify.app", "Helvetica", 8, MUTED, align="r")
    c.showPage()
    c.save()

    print(f"\nPostgame recap -> {out}")
    print(f"  {outcome_verb} {opponent}  ·  {date_pretty}  ·  {scores}")
    print(f"  stars: {', '.join(s['name'] for s in stars[:3])}  ·  milestones reached: {len(reached)}")


if __name__ == "__main__":
    main()
