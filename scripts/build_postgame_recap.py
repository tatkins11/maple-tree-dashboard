"""Reusable weekly Postgame Recap generator (up to 4 pages).

Recaps the most recent completed game day:
  page 1 — scoreboards, stat tiles, storylines, Player of the Game, every
           milestone reached that night (auto-sized box)
  page 2 — Around the League: full standings + every score from the week
  page 3 — full box scores per game
  page 4 — The Card Corner: the week's special-edition card drops (auto-detected
           from cards whose caption mentions the week_label, or --feature-cards)

    python scripts/build_postgame_recap.py          # auto = latest completed game day
    python scripts/build_postgame_recap.py --date 2026-07-01
    ... --story "Lead.|Body" --stories-only         # full editorial control

Run export_site_data.py first so the numbers match the website.
Reuses the shared drawing kit from build_gameday_preview.
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


def prep_card_hd(asset):
    """Full-resolution card art with rounded corners, for the Card Corner page."""
    from build_gameday_preview import ASSET_CACHE, CARDS_DIR
    from PIL import ImageDraw
    dst = ASSET_CACHE / f"card-hd-{asset}.png"
    if not dst.exists():
        img = Image.open(CARDS_DIR / f"{asset}.webp").convert("RGBA")
        mask = Image.new("L", img.size, 0)
        ImageDraw.Draw(mask).rounded_rectangle([0, 0, img.size[0] - 1, img.size[1] - 1], radius=36, fill=255)
        img.putalpha(mask)
        img.save(dst)
    return dst


def main():
    ap = argparse.ArgumentParser(description="Maple Tree weekly postgame recap PDF")
    ap.add_argument("--date", help="YYYY-MM-DD (default: latest completed game day)")
    ap.add_argument("--season")
    ap.add_argument("--out")
    ap.add_argument("--story", action="append", default=[],
                    help='Extra storyline as "Lead.|Body text" — inserted after The stars.')
    ap.add_argument("--no-stars-story", action="store_true",
                    help="Skip the auto 'The stars.' storyline (frees room for custom stories)")
    ap.add_argument("--stories-only", action="store_true",
                    help="Use only the --story entries (full editorial control, no auto storylines)")
    ap.add_argument("--feature-cards",
                    help="Comma-separated card assets for a Card Corner page 3 "
                         "(default: special cards whose caption mentions this week)")
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
    week_label = day[0].get("week_label") or ""

    # featured cards for the Card Corner page (explicit flag, else auto-detect
    # special editions whose caption references this week)
    if args.feature_cards:
        wanted = [a.strip() for a in args.feature_cards.split(",") if a.strip()]
        featured = [c for c in cards if c["asset"] in wanted]
    else:
        featured = [c for c in cards
                    if c["kind"] == "special" and week_label
                    and week_label.lower() in (c.get("caption") or "").lower()]
    featured = featured[:2]

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

    # franchise rank: how many players (ever) sit at/above this threshold. Totals only
    # grow, so the club size IS the new member's ordinal — as long as no two players
    # cross the same milestone on the same night (then the count ties them, acceptably).
    career_std = load("career_stats.json")["standard"]
    MS_FIELD = {"Hits": "hits", "HR": "hr", "RBI": "rbi", "Runs": "r", "Doubles": "2b",
                "Triples": "3b", "Singles": "1b", "Walks": "bb", "Total Bases": "tb",
                "PA": "pa", "AB": "ab", "Games": "games"}

    def ms_rank(stat, thr):
        fld = MS_FIELD.get(stat)
        if not fld:
            return None
        return sum(1 for p in career_std if float(p.get(fld) or 0) >= thr) or None

    board = meta["seed_race"]["board"]
    us = next((r for r in board if r["is_team"]), None)
    potw = meta.get("potw")

    def pick_card(slug):
        cs = [c for c in cards if c["slug"] == slug]
        ms = [c for c in cs if c["kind"] == "milestone"]
        return (ms or cs or [None])[0]

    # ---- storylines ----
    stories = []
    custom = []
    for s in args.story:
        lead, _, body = s.partition("|")
        if body.strip():
            custom.append((lead.strip(), body.strip()))
    scores = " and ".join(f"{b['rf']}-{b['ra']}" for b in boxes)
    stories.append(("The result.", (
        f"Maple Tree {outcome_verb} {opponent} {'on the night' if n > 1 else ''}, {scores} — "
        f"{day_rf} run{'s' if day_rf != 1 else ''} on {day_hits} hits at {field}.")))
    if stars and not args.no_stars_story:
        body = f"{stars[0]['name']} led the way at {star_line(stars[0])}"
        if len(stars) > 1 and stars[1]["gs"] > 0:
            body += f", and {stars[1]['name']} backed him up with {star_line(stars[1])}"
        stories.append(("The stars.", body + "."))
    stories.extend(custom)
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
    if args.stories_only and custom:
        stories = custom
    else:
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

    # storylines (left) + rail (right) — never let stories run into the standings
    from reportlab.pdfbase.pdfmetrics import stringWidth

    def wrap_count(txt, width, font="Helvetica", size=9.5):
        lines, cur = 1, ""
        for word in txt.split():
            t = (cur + " " + word).strip()
            if stringWidth(t, font, size) <= width:
                cur = t
            else:
                lines += 1
                cur = word
        return lines

    col_y = ty - 28
    section_title(c, 36, col_y, "How it happened", 318)
    sy = col_y - 24
    floor = 84  # keep clear of the page footer
    for lead, body in stories:
        need = 13 + wrap_count(body, 318) * 12.5 + 10
        if sy - need < floor:
            break
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

    # milestones reached — every one of them, box sized to fit
    if reached:
        h2 = 40 + len(reached) * 13.5 + 8
        b2y = b1y - h2 - 12
        rail_box(b2y, h2, "MILESTONES REACHED")
        ly = b2y + h2 - 36
        for e in reached:
            rank = ms_rank(e["stat"], e["milestone"])
            tag = f" · {ordinal(rank)} ever" if rank else ""
            _txt(c, 380, ly, "•", "Helvetica-Bold", 9, MAPLE)
            _txt(c, 392, ly, f"{e['player']} — {e['milestone']} career {ms_word(e['stat'], plural=True)}{tag}",
                 "Helvetica", 8.2, INK)
            ly -= 13.5

    def page_footer():
        c.setStrokeColor(LINE)
        c.setLineWidth(0.75)
        c.line(36, 64, W - 36, 64)
        _txt(c, 36, 50, "MAPLE TREE SOFTBALL", "Helvetica-Bold", 8, BARK, cs=1)
        _txt(c, W - 36, 50, "mapletreesoftball.netlify.app  ·  The Maple Tree Tap - Cary, Illinois",
             "Helvetica", 8, MUTED, align="r")

    page_footer()
    c.showPage()

    # ===== PAGE 2 : THE LEAGUE — full standings + every score from the week =====
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 72, W, 72, stroke=0, fill=1)
    _txt(c, 36, H - 34, f"STANDINGS & SCORES  ·  {week_label.upper() or date_pretty.upper()}",
         "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, 36, H - 58, "AROUND THE LEAGUE", "Helvetica-Bold", 24, WHITE, cs=1)
    _txt(c, W - 36, H - 58, "Wednesday Men's · Recreational", "Helvetica-Oblique", 9, TAN, align="r")

    st_y = H - 104
    section_title(c, 36, st_y, f"Standings after {week_label or 'this week'}", W - 72)
    cols = [("SEED", 58, "r"), ("W", 336, "r"), ("L", 376, "r"), ("RF", 426, "r"),
            ("RA", 476, "r"), ("DIFF", 530, "r"), ("LEFT", 574, "r")]
    hy = st_y - 24
    _txt(c, 76, hy, "TEAM", "Helvetica-Bold", 7, MUTED)
    for label, x, _a in cols:
        _txt(c, x, hy, label, "Helvetica-Bold", 7, MUTED, align="r")
    ry, row_h = hy - 8, 17
    for i, r in enumerate(board):
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

    lg = con.execute(
        "SELECT game_time, location_or_field, home_team, away_team, home_runs, away_runs "
        "FROM league_schedule_games WHERE season=? AND week_label=? AND completed_flag=1 "
        "ORDER BY game_time, location_or_field", (season_name, week_label)).fetchall()
    if lg:
        sc_y = ry - row_h * (len(board) + 1) - 34
        section_title(c, 36, sc_y, f"{week_label} scores", W - 72)
        gy = sc_y - 26
        for gt, loc, home, away, hr_, ar_ in lg:
            win, wr, lose, lr = (home, hr_, away, ar_) if hr_ > ar_ else (away, ar_, home, hr_)
            ours = "Maple Tree" in (home, away)
            f = "Helvetica-Bold" if ours else "Helvetica"
            if ours:
                c.setFillColor(SAND)
                c.rect(36, gy - 4, W - 72, 16, stroke=0, fill=1)
                c.setFillColor(MAPLE)
                c.rect(36, gy - 4, 3, 16, stroke=0, fill=1)
            _txt(c, 44, gy, f"{win} {int(wr)}, {lose} {int(lr)}", f, 9.5, BARK if ours else INK)
            _txt(c, W - 44, gy, f"{gt}  ·  {loc}", "Helvetica", 8, MUTED, align="r")
            gy -= 16.5

    page_footer()
    c.showPage()

    # ===== PAGE 3 : BOX SCORES =====
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

    # ===== THE MILESTONE PARADE (its own page on big record-book nights) =====
    if len(reached) >= 4:
        c.setFillColor(PAPER)
        c.rect(0, 0, W, H, stroke=0, fill=1)
        c.setFillColor(BARK)
        c.rect(0, H - 84, W, 84, stroke=0, fill=1)
        _txt(c, 36, H - 36, f"{meta['current_season']['label'].upper()}  ·  {week_label.upper() or date_pretty.upper()}",
             "Helvetica-Bold", 8.5, TAN, cs=2)
        _txt(c, 36, H - 64, "THE MILESTONE PARADE", "Helvetica-Bold", 26, WHITE, cs=1)
        _txt(c, W - 36, H - 40, f"{len(reached)} career milestones fell in one night", "Helvetica-Oblique", 9, TAN, align="r")
        firsts = sum(1 for e in reached if ms_rank(e["stat"], e["milestone"]) == 1)
        if firsts:
            _txt(c, W - 36, H - 58, f"including {firsts} franchise first{'s' if firsts != 1 else ''}",
                 "Helvetica-Bold", 10, CREAM, align="r")

        ry3 = H - 116
        row_h3 = min(52, (ry3 - 80) / max(len(reached), 1))
        for i, e in enumerate(reached):
            y0 = ry3 - row_h3 * (i + 1)
            rank = ms_rank(e["stat"], e["milestone"])
            first = rank == 1
            if i % 2 == 0:
                c.setFillColor(STRIPE)
                c.rect(36, y0, W - 72, row_h3, stroke=0, fill=1)
            badge = MAPLE if first else BARK2
            c.setFillColor(badge)
            c.circle(58, y0 + row_h3 / 2, 13, stroke=0, fill=1)
            _txt(c, 58, y0 + row_h3 / 2 - 3.5, ordinal(rank) if rank else "—", "Helvetica-Bold",
                 8 if rank and rank < 10 else 7, WHITE, align="c")
            ty3 = y0 + row_h3 / 2 + 3
            _txt(c, 84, ty3, f"{e['player']} — {e['milestone']} career {ms_word(e['stat'], plural=True)}",
                 "Helvetica-Bold", 12, BARK)
            sub = (f"{ordinal(rank)} player in franchise history" if rank else "franchise record book") \
                + f"  ·  vs {e.get('opponent') or opponent}"
            _txt(c, 84, ty3 - 13, sub, "Helvetica", 8.5, MAPLE if first else MUTED)
            if first:
                _txt(c, W - 44, y0 + row_h3 / 2 - 3, "FRANCHISE FIRST", "Helvetica-Bold", 8, MAPLE,
                     cs=1.5, align="r")
        c.setStrokeColor(LINE)
        c.setLineWidth(0.75)
        c.line(36, 64, W - 36, 64)
        _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  THE RECORD BOOK GREW TONIGHT", "Helvetica-Bold", 8, BARK, cs=1)
        _txt(c, W - 36, 50, "full ladders at mapletreesoftball.netlify.app/milestones", "Helvetica", 8, MUTED, align="r")
        c.showPage()

    # ===== PAGE : THE CARD CORNER (when the week minted new special editions) =====
    if featured:
        c.setFillColor(PAPER)
        c.rect(0, 0, W, H, stroke=0, fill=1)
        c.setFillColor(BARK)
        c.rect(0, H - 72, W, 72, stroke=0, fill=1)
        kick = f"{meta['current_season']['label'].upper()}  ·  {week_label.upper() or date_pretty.upper()}  ·  SPECIAL EDITIONS"
        _txt(c, 36, H - 34, kick, "Helvetica-Bold", 8.5, TAN, cs=2)
        _txt(c, 36, H - 58, "THE CARD CORNER", "Helvetica-Bold", 24, WHITE, cs=1)
        _txt(c, W - 36, H - 58, "fresh drops from the clubhouse printer", "Helvetica-Oblique", 9, TAN, align="r")

        cw = 236
        xs = [(W - cw) / 2] if len(featured) == 1 else [45, W - 45 - cw]
        for x0, card in zip(xs, featured):
            path = prep_card_hd(card["asset"])
            img = Image.open(path)
            ch = cw * (img.size[1] / img.size[0])
            top = H - 100
            c.drawImage(str(path), x0, top - ch, width=cw, height=ch, mask="auto")
            c.setStrokeColor(LINE)
            c.setLineWidth(1)
            c.roundRect(x0 - 7, top - ch - 7, cw + 14, ch + 14, 12, stroke=1, fill=0)

            ty2 = top - ch - 30
            _txt(c, x0, ty2, (card.get("series") or "Special Edition").upper(),
                 "Helvetica-Bold", 8, MAPLE, cs=1.5)
            _txt(c, x0, ty2 - 17, card["player"], "Helvetica-Bold", 14, BARK)
            _txt(c, x0, ty2 - 30, card.get("caption") or "", "Helvetica", 8.5, MUTED)
            yy = wrap(c, x0, ty2 - 47, card.get("flavor") or "", cw, "Helvetica", 8.5, 11.5, INK) - 9
            c.setStrokeColor(LINE)
            c.setLineWidth(0.6)
            c.line(x0, yy + 4, x0 + cw, yy + 4)
            yy -= 9
            for f in (card.get("facts") or [])[:4]:
                _txt(c, x0, yy, str(f[0]).upper(), "Helvetica-Bold", 7, MUTED, cs=0.5)
                _txt(c, x0 + cw, yy, str(f[1]), "Helvetica", 8.5, INK, align="r")
                yy -= 13

        c.setStrokeColor(LINE)
        c.setLineWidth(0.75)
        c.line(36, 64, W - 36, 64)
        _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  TRADING CARDS", "Helvetica-Bold", 8, BARK, cs=1)
        _txt(c, W - 36, 50, "flip every card at mapletreesoftball.netlify.app/cards", "Helvetica", 8, MUTED, align="r")
        c.showPage()

    c.save()

    print(f"\nPostgame recap -> {out}")
    print(f"  {outcome_verb} {opponent}  ·  {date_pretty}  ·  {scores}")
    print(f"  stars: {', '.join(s['name'] for s in stars[:3])}  ·  milestones reached: {len(reached)}")
    if featured:
        print(f"  card corner: {', '.join(c_['asset'] for c_ in featured)}")


if __name__ == "__main__":
    main()
