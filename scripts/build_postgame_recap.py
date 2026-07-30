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
from reportlab.lib.colors import HexColor
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
        # The week's drop is BOTH: event/special cards captioned for this week, AND every
        # milestone card whose milestone actually fell tonight. Matching only on a "Week N"
        # caption missed the entire milestone batch, since those are captioned by rank.
        reached_keys = {(e["slug"], e["stat"], e["milestone"])
                        for e in milestones["recent"] if e["date"] == date}
        specials = [c for c in cards
                    if c["kind"] == "special" and week_label
                    and week_label.lower() in (c.get("caption") or "").lower()]
        miles = sorted([c for c in cards
                        if c["kind"] == "milestone"
                        and (c.get("slug"), c.get("stat"), c.get("value")) in reached_keys],
                       key=lambda x: -(x.get("rating") or 0))
        seen, featured = set(), []
        for card_ in specials + miles:          # specials headline, milestones follow
            if card_["asset"] not in seen:
                seen.add(card_["asset"])
                featured.append(card_)

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
    cols = [("SEED", 58, "r"), ("W", 336, "r"), ("L", 376, "r"), ("RS", 426, "r"),
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
        hero = featured[:2]          # marquee treatment, full write-up
        xs = [(W - cw) / 2] if len(hero) == 1 else [45, W - 45 - cw]
        for x0, card in zip(xs, hero):
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

        # ===== THE FULL DROP: every other card minted this week, as a grid =====
        rest, GCW, COLS, PER = featured[2:], 112, 4, 12
        gap = (W - 72 - COLS * GCW) / (COLS - 1)
        for pi in range(0, len(rest), PER):
            chunk = rest[pi:pi + PER]
            c.setFillColor(PAPER)
            c.rect(0, 0, W, H, stroke=0, fill=1)
            c.setFillColor(BARK)
            c.rect(0, H - 72, W, 72, stroke=0, fill=1)
            more = f"  ·  {pi // PER + 2}" if len(rest) > PER else ""
            _txt(c, 36, H - 34, f"{meta['current_season']['label'].upper()}  ·  "
                 f"{week_label.upper() or date_pretty.upper()}  ·  THE FULL DROP{more}",
                 "Helvetica-Bold", 8.5, TAN, cs=2)
            _txt(c, 36, H - 58, "EVERY NEW CARD THIS WEEK", "Helvetica-Bold", 24, WHITE, cs=1)
            _txt(c, W - 36, H - 58, f"{len(featured)} cards minted", "Helvetica-Oblique", 9, TAN, align="r")
            for i, card in enumerate(chunk):
                x0 = 36 + (i % COLS) * (GCW + gap)
                path = prep_card_hd(card["asset"])
                im = Image.open(path)
                gch = GCW * (im.size[1] / im.size[0])
                y0 = (H - 100) - (i // COLS) * (gch + 38) - gch
                c.drawImage(str(path), x0, y0, width=GCW, height=gch, mask="auto")
                c.setStrokeColor(LINE)
                c.setLineWidth(0.8)
                c.roundRect(x0 - 4, y0 - 4, GCW + 8, gch + 8, 7, stroke=1, fill=0)
                _txt(c, x0, y0 - 17, card["player"], "Helvetica-Bold", 9, BARK)
                sub = (f"{card.get('value')} {card.get('stat')}" if card.get("kind") == "milestone"
                       else (card.get("series") or "Special Edition"))
                _txt(c, x0, y0 - 28, str(sub), "Helvetica", 7.5, MUTED)
            c.setStrokeColor(LINE)
            c.setLineWidth(0.75)
            c.line(36, 64, W - 36, 64)
            _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  TRADING CARDS", "Helvetica-Bold", 8, BARK, cs=1)
            _txt(c, W - 36, 50, "flip every card at mapletreesoftball.netlify.app/cards",
                 "Helvetica", 8, MUTED, align="r")
            c.showPage()

    # ===== THE SEED RACE: where the season can still end up =====================
    race_path = REPO / "site" / "src" / "data" / "seed_race.json"
    if race_path.exists():
        import json as _json
        race = _json.loads(race_path.read_text(encoding="utf-8"))
        rteams = race["teams"]
        rus = next((t for t in rteams if t["is_team"]), None)
        if rus:
            def _pct(v):
                if v <= 0:
                    return "-"
                return "<0.1%" if v < 0.001 else f"{v * 100:.1f}%"

            c.setFillColor(PAPER)
            c.rect(0, 0, W, H, stroke=0, fill=1)
            c.setFillColor(BARK)
            c.rect(0, H - 96, W, 96, stroke=0, fill=1)
            _txt(c, 36, H - 34, f"{meta['current_season']['label'].upper()}  ·  "
                 f"{(week_label or date_pretty).upper()}  ·  WHAT IS LEFT",
                 "Helvetica-Bold", 8.5, TAN, cs=2)
            _txt(c, 36, H - 66, "THE SEED RACE", "Helvetica-Bold", 26, WHITE, cs=1)
            _txt(c, W - 36, H - 40, f"{race['games_remaining']} games remain",
                 "Helvetica-Bold", 11, WHITE, align="r")
            _txt(c, W - 36, H - 58, f"all {race['branches']:,} outcomes counted",
                 "Helvetica-Oblique", 8.5, TAN, align="r")

            # headline tiles
            ty = H - 200
            tiles = [("CHANCE AT THE #1 SEED", _pct(rus["p_top_seed"]), MAPLE),
                     ("FIRST-ROUND BYE", _pct(rus["p_first_round_bye"]), BARK),
                     ("MOST LIKELY FINISH", f"#{max(race['our_seed_odds'], key=lambda o: o['p'])['seed']}", BARK)]
            tw = (W - 72 - 24) / 3
            for i, (lbl, val, col) in enumerate(tiles):
                x0 = 36 + i * (tw + 12)
                c.setFillColor(CREAM)
                c.setStrokeColor(LINE)
                c.setLineWidth(0.8)
                c.roundRect(x0, ty, tw, 78, 7, stroke=1, fill=1)
                _txt(c, x0 + 14, ty + 56, lbl, "Helvetica-Bold", 7.5, MUTED, cs=1.4)
                _txt(c, x0 + 14, ty + 22, val, "Helvetica-Bold", 30, col)

            # what the seed buys
            y = ty - 24
            section_title(c, 36, y, "What the seed actually buys", W - 72)
            y -= 22
            y = wrap(c, 36, y, "Every club makes the playoffs and all ten games are Wednesday, "
                     "19 August, so this was never a race to get in. Seeds one through five skip "
                     "the 6:30 play-in round, and that bye is already all but banked. What is "
                     "still live is which half of the bracket Maple Tree land in.",
                     W - 72, "Helvetica", 9.5, 12.5, INK)
            y -= 6
            halves = [("TOP HALF  ·  #1, #4, #5",
                       "#4 and #5 open against each other and the winner draws the top seed in "
                       "the semi-final. Landing here means going through the best team in the "
                       "league just to reach the final."),
                      ("BOTTOM HALF  ·  #2, #3",
                       "#3 rides with #2 and cannot meet #1 before the final. That makes the step "
                       "from #3 to #4 the most expensive one left on the board — worth more than "
                       "the step from #2 to #3.")]
            hw = (W - 72 - 14) / 2
            for i, (head, body) in enumerate(halves):
                x0 = 36 + i * (hw + 14)
                c.setFillColor(SAND)
                c.setStrokeColor(LINE)
                c.roundRect(x0, y - 74, hw, 72, 6, stroke=1, fill=1)
                _txt(c, x0 + 12, y - 22, head, "Helvetica-Bold", 8.5, BARK, cs=1)
                wrap(c, x0 + 12, y - 38, body, hw - 24, "Helvetica", 8.3, 10.5, INK)
            y -= 92

            # tiebreakers
            tb = race.get("our_tiebreakers") or []
            if tb:
                section_title(c, 36, y, "The tiebreakers we already own", W - 72)
                y -= 20
                y = wrap(c, 36, y, "A tie is broken by head-to-head record, then head-to-head run "
                         "differential, then fewest runs allowed. Overall run differential never "
                         "enters it, so the +26 on the standings page decides nothing.",
                         W - 72, "Helvetica", 9, 11.5, MUTED)
                y -= 4
                for lbl, x in [("SERIES", 300), ("RUNS", 372), ("H2H DIFF", 444)]:
                    _txt(c, x, y, lbl, "Helvetica-Bold", 7, MUTED, align="r")
                _txt(c, 52, y, "OPPONENT", "Helvetica-Bold", 7, MUTED)
                _txt(c, W - 36, y, "TIEBREAK", "Helvetica-Bold", 7, MUTED, align="r")
                y -= 4
                for i, t in enumerate(tb):
                    y0 = y - 16
                    if i % 2 == 0:
                        c.setFillColor(STRIPE)
                        c.rect(36, y0, W - 72, 16, stroke=0, fill=1)
                    _txt(c, 52, y0 + 5, t["opponent"], "Helvetica", 9, INK)
                    _txt(c, 300, y0 + 5, f"{t['w']}-{t['l']}", "Helvetica", 9, INK, align="r")
                    _txt(c, 372, y0 + 5, f"{t['runs_for']}-{t['runs_against']}", "Helvetica", 9, INK, align="r")
                    d_ = t["h2h_diff"]
                    _txt(c, 444, y0 + 5, f"+{d_}" if d_ > 0 else str(d_), "Helvetica-Bold", 9,
                         GREEN if d_ > 0 else MAPLE, align="r")
                    _txt(c, W - 36, y0 + 5, "Maple Tree" if t["we_hold"] else t["opponent"],
                         "Helvetica-Bold", 8.5, GREEN if t["we_hold"] else MAPLE, align="r")
                    y -= 16
                y -= 6
                lost_ = [t["opponent"] for t in tb if not t["we_hold"]]
                if lost_:
                    y = wrap(c, 36, y, "The one we lose is to " + ", ".join(lost_) +
                             " — and it is the tie most likely to matter, because they finish "
                             "against the bottom of the table while we finish against the top.",
                             W - 72, "Helvetica-Oblique", 8.6, 11, MAPLE)
                y -= 8

            # ---- page break: the odds get their own sheet ----------------------
            c.setStrokeColor(LINE)
            c.setLineWidth(0.75)
            c.line(36, 64, W - 36, 64)
            _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  THE SEED RACE", "Helvetica-Bold", 8, BARK, cs=1)
            _txt(c, W - 36, 50, "continued", "Helvetica-Oblique", 8, MUTED, align="r")
            c.showPage()

            c.setFillColor(PAPER)
            c.rect(0, 0, W, H, stroke=0, fill=1)
            c.setFillColor(BARK)
            c.rect(0, H - 96, W, 96, stroke=0, fill=1)
            _txt(c, 36, H - 34, f"{meta['current_season']['label'].upper()}  ·  "
                 f"{(week_label or date_pretty).upper()}  ·  WHAT IS LEFT",
                 "Helvetica-Bold", 8.5, TAN, cs=2)
            _txt(c, 36, H - 66, "HOW IT ENDS", "Helvetica-Bold", 26, WHITE, cs=1)
            _txt(c, W - 36, H - 40, "Seed and title odds", "Helvetica-Bold", 11, WHITE, align="r")
            _txt(c, W - 36, H - 58, "every branch weighted", "Helvetica-Oblique", 8.5, TAN, align="r")
            y = H - 132

            # three ways our doubleheader goes
            scen = race.get("scenarios") or []
            if scen:
                section_title(c, 36, y, "The three ways our doubleheader goes", W - 72)
                y -= 20
                y = wrap(c, 36, y, "Both our games are treated as coin flips — a fairer planning "
                         "assumption than the model's read of a two-game sample. Everyone else "
                         "keeps their modelled odds. Each panel is conditional: given that the "
                         "night goes this way, here is where we land.",
                         W - 72, "Helvetica", 9, 11.5, MUTED)
                y -= 8
                cw = (W - 72 - 24) / 3
                top = y
                for i, sc in enumerate(scen):
                    x0 = 36 + i * (cw + 12)
                    best_ = sc["wins"] == 2
                    c.setFillColor(CREAM if best_ else SAND)
                    c.setStrokeColor(MAPLE if best_ else LINE)
                    c.setLineWidth(1.2 if best_ else 0.8)
                    c.roundRect(x0, top - 150, cw, 148, 8, stroke=1, fill=1)
                    _txt(c, x0 + 13, top - 22, sc["label"], "Helvetica-Bold", 12, BARK)
                    _txt(c, x0 + cw - 13, top - 22, sc["final_record"], "Helvetica", 9.5, MUTED, align="r")
                    _txt(c, x0 + 13, top - 52, f"#{sc['likeliest_seed']}", "Helvetica-Bold", 26,
                         MAPLE if best_ else BARK)
                    _txt(c, x0 + 52, top - 52, "most likely seed", "Helvetica-Bold", 7, MUTED, cs=0.8)
                    yy = top - 70
                    for x_ in sc["seeds"]:
                        _txt(c, x0 + 22, yy - 6, f"#{x_['seed']}", "Helvetica-Bold", 7.5, MUTED, align="r")
                        c.setFillColor(HexColor("#e6e1d2"))
                        c.roundRect(x0 + 28, yy - 8, cw - 76, 6, 3, stroke=0, fill=1)
                        c.setFillColor(MAPLE if best_ else HexColor("#a8a290"))
                        c.roundRect(x0 + 28, yy - 8, max(x_["p"] * (cw - 76), 1.2), 6, 3, stroke=0, fill=1)
                        _txt(c, x0 + cw - 13, yy - 6, _pct(x_["p"]), "Helvetica", 7.5, INK, align="r")
                        yy -= 13
                    c.setStrokeColor(LINE)
                    c.setLineWidth(0.7)
                    c.line(x0 + 13, top - 120, x0 + cw - 13, top - 120)
                    top2 = sum(x["p"] for x in sc["seeds"] if x["seed"] <= 2)
                    for j, (lb, vv, col) in enumerate([("BYE", sc["p_bye"], BARK),
                                                       ("#2 OR BETTER", top2, MAPLE)]):
                        xx = x0 + 13 + j * ((cw - 26) / 2)
                        _txt(c, xx, top - 128, lb, "Helvetica-Bold", 6.5, MUTED, cs=0.6)
                        _txt(c, xx, top - 143, _pct(vv), "Helvetica-Bold", 10, col)
                y = top - 168
                y = wrap(c, 36, y, "The bye is only at risk if we lose both. Win one and it is "
                         "locked. Lose both and there is a real chance we slide to sixth and have "
                         "to play the 6:30 game; sweep and we reach third or better nine times out "
                         "of ten, which is the half of the bracket that avoids Wasted Talent until "
                         "the final.", W - 72, "Helvetica-Oblique", 8.8, 11, MAPLE)
                y -= 10

            # where we finish
            section_title(c, 36, y, "Where Maple Tree finish", W - 72)
            y -= 20
            odds = [o for o in race["our_seed_odds"] if o["p"] > 0.001]
            peak = max((o["p"] for o in odds), default=1.0)
            best = max(odds, key=lambda o: o["p"]) if odds else None
            for o in odds:
                _txt(c, 52, y - 9, f"#{o['seed']}", "Helvetica-Bold", 10,
                     MAPLE if best and o["seed"] == best["seed"] else BARK, align="r")
                c.setFillColor(HexColor("#e6e1d2"))
                c.roundRect(62, y - 12, 380, 9, 4.5, stroke=0, fill=1)
                fw = max(o["p"] / peak * 380, 1.5)
                c.setFillColor(MAPLE if best and o["seed"] == best["seed"] else HexColor("#a8a290"))
                c.roundRect(62, y - 12, fw, 9, 4.5, stroke=0, fill=1)
                _txt(c, 486, y - 9, _pct(o["p"]), "Helvetica-Bold", 10, BARK, align="r")
                y -= 17
            y -= 8

            # ---- page break: the bracket gets its own sheet --------------------
            c.setStrokeColor(LINE)
            c.setLineWidth(0.75)
            c.line(36, 64, W - 36, 64)
            _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  THE SEED RACE", "Helvetica-Bold", 8, BARK, cs=1)
            _txt(c, W - 36, 50, "continued", "Helvetica-Oblique", 8, MUTED, align="r")
            c.showPage()

            c.setFillColor(PAPER)
            c.rect(0, 0, W, H, stroke=0, fill=1)
            c.setFillColor(BARK)
            c.rect(0, H - 96, W, 96, stroke=0, fill=1)
            _txt(c, 36, H - 34, f"{meta['current_season']['label'].upper()}  ·  "
                 "WEDNESDAY 19 AUGUST  ·  ONE NIGHT, TEN GAMES",
                 "Helvetica-Bold", 8.5, TAN, cs=2)
            _txt(c, 36, H - 66, "THE BRACKET", "Helvetica-Bold", 26, WHITE, cs=1)
            _txt(c, W - 36, H - 40, "All eleven clubs qualify", "Helvetica-Bold", 11, WHITE, align="r")
            _txt(c, W - 36, H - 58, "seeds 1-5 skip the 6:30 round", "Helvetica-Oblique", 8.5, TAN, align="r")

            likely = {sc["wins"]: sc["likeliest_seed"] for sc in scen} if scen else {}
            mark = {v: k for k, v in likely.items()}    # seed -> how our night went
            SLAB = {0: "LOSE BOTH", 1: "SPLIT", 2: "WIN BOTH"}

            def node(x, yb, w, h, lines, hot=False, tag=None):
                c.setFillColor(CREAM if hot else SAND)
                c.setStrokeColor(MAPLE if hot else LINE)
                c.setLineWidth(1.3 if hot else 0.8)
                c.roundRect(x, yb, w, h, 5, stroke=1, fill=1)
                for i, (txt_, bold) in enumerate(lines):
                    _txt(c, x + 8, yb + h - 13 - i * 11, txt_,
                         "Helvetica-Bold" if bold else "Helvetica", 8.2, BARK if bold else INK)
                if tag:
                    _txt(c, x + w - 7, yb + h - 12, tag, "Helvetica-Bold", 6, MAPLE, align="r")

            def elbow(x1, y1, x2, y2):
                c.setStrokeColor(HexColor("#c9c2ae"))
                c.setLineWidth(0.9)
                xm = (x1 + x2) / 2
                c.line(x1, y1, xm, y1)
                c.line(xm, y1, xm, y2)
                c.line(xm, y2, x2, y2)

            C1, C2, C3, C4 = 40, 174, 314, 452
            BW, BH, SH = 118, 34, 28

            for gid, hi, lo, yy in [("G1", 8, 9, 596), ("G2", 7, 10, 452), ("G3", 6, 11, 308)]:
                node(C1, yy, BW, SH, [(f"#{hi}  v  #{lo}", True)])
                _txt(c, C1, yy - 10, f"{gid}  ·  6:30", "Helvetica", 6.5, MUTED)

            r2 = [("G5", [("#1", True), ("winner of G1", False)], 640, (1,)),
                  ("G4", [("#4", True), ("#5", True)], 540, (4, 5)),
                  ("G6", [("#2", True), ("winner of G2", False)], 420, (2,)),
                  ("G7", [("#3", True), ("winner of G3", False)], 300, (3,))]
            for gid, lines, yy, seeds in r2:
                hits = [sd for sd in seeds if sd in mark]
                tag = SLAB[mark[hits[0]]] if hits else None
                node(C2, yy, BW, BH, lines, hot=bool(hits), tag=tag)
                _txt(c, C2, yy - 10, f"{gid}  ·  7:30", "Helvetica", 6.5, MUTED)

            elbow(C1 + BW, 596 + SH / 2, C2, 640 + 10)
            elbow(C1 + BW, 452 + SH / 2, C2, 420 + 10)
            elbow(C1 + BW, 308 + SH / 2, C2, 300 + 10)

            node(C3, 578, BW, BH, [("winner G5", False), ("winner G4", False)])
            _txt(c, C3, 568, "G8  ·  8:30", "Helvetica", 6.5, MUTED)
            node(C3, 348, BW, BH, [("winner G6", False), ("winner G7", False)])
            _txt(c, C3, 338, "G9  ·  8:30", "Helvetica", 6.5, MUTED)
            elbow(C2 + BW, 640 + BH / 2, C3, 578 + BH - 11)
            elbow(C2 + BW, 540 + BH / 2, C3, 578 + 11)
            elbow(C2 + BW, 420 + BH / 2, C3, 348 + BH - 11)
            elbow(C2 + BW, 300 + BH / 2, C3, 348 + 11)

            c.setFillColor(BARK)
            c.roundRect(C4, 452, 124, 48, 6, stroke=0, fill=1)
            _txt(c, C4 + 12, 482, "THE FINAL", "Helvetica-Bold", 7.5, TAN, cs=1.2)
            _txt(c, C4 + 12, 464, "G10  ·  9:30 PM", "Helvetica-Bold", 11, WHITE)
            elbow(C3 + BW, 578 + BH / 2, C4, 452 + 34)
            elbow(C3 + BW, 348 + BH / 2, C4, 452 + 14)

            y = 262
            section_title(c, 36, y, "Why the half matters more than the seed", W - 72)
            y -= 22
            y = wrap(c, 36, y, "#1 sits with #4 and #5, so whichever of those two survives their "
                     "opener runs straight into the best team in the league. #2 and #3 share the "
                     "bottom half and cannot meet #1 before the final. That is the whole reason the "
                     "step from #3 to #4 costs more than the step from #2 to #3, and why a "
                     "doubleheader against Wasted Talent decides more than a place on the table.",
                     W - 72, "Helvetica", 9.2, 12, INK)
            if mark:
                y -= 4
                bits = [f"{SLAB[w].lower()} puts us on the #{sd} line" for sd, w in sorted(mark.items())]
                wrap(c, 36, y, "Highlighted above: " + "; ".join(bits) + ".",
                     W - 72, "Helvetica-Oblique", 9, 11.5, MAPLE)

            c.setStrokeColor(LINE)
            c.setLineWidth(0.75)
            c.line(36, 64, W - 36, 64)
            _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  THE BRACKET", "Helvetica-Bold", 8, BARK, cs=1)
            _txt(c, W - 36, 50, "all ten games Wednesday 19 August", "Helvetica", 8, MUTED, align="r")
            c.showPage()

            # ---- title odds on a clean sheet ----------------------------------
            c.setFillColor(PAPER)
            c.rect(0, 0, W, H, stroke=0, fill=1)
            c.setFillColor(BARK)
            c.rect(0, H - 96, W, 96, stroke=0, fill=1)
            _txt(c, 36, H - 34, f"{meta['current_season']['label'].upper()}  ·  "
                 f"{(week_label or date_pretty).upper()}  ·  WHAT IS LEFT",
                 "Helvetica-Bold", 8.5, TAN, cs=2)
            _txt(c, 36, H - 66, "THE FULL BOARD", "Helvetica-Bold", 26, WHITE, cs=1)
            _txt(c, W - 36, H - 40, "Top seed and bye odds", "Helvetica-Bold", 11, WHITE, align="r")
            _txt(c, W - 36, H - 58, "every branch weighted", "Helvetica-Oblique", 8.5, TAN, align="r")
            y = H - 140

            # the board
            section_title(c, 36, y, "Top seed and bye odds across the league", W - 72)
            y -= 20
            for lbl, x in [("W", 300), ("L", 340), ("#1 SEED", 440), ("BYE", 530)]:
                _txt(c, x, y, lbl, "Helvetica-Bold", 7, MUTED, align="r")
            _txt(c, 52, y, "TEAM", "Helvetica-Bold", 7, MUTED)
            y -= 4
            for i, t in enumerate(rteams):
                y0 = y - 17
                if t["is_team"]:
                    c.setFillColor(CREAM)
                    c.rect(36, y0, W - 72, 17, stroke=0, fill=1)
                    c.setFillColor(MAPLE)
                    c.rect(36, y0, 3, 17, stroke=0, fill=1)
                elif i % 2 == 0:
                    c.setFillColor(STRIPE)
                    c.rect(36, y0, W - 72, 17, stroke=0, fill=1)
                f = "Helvetica-Bold" if t["is_team"] else "Helvetica"
                _txt(c, 52, y0 + 5, t["team"], f, 9, BARK if t["is_team"] else INK)
                _txt(c, 300, y0 + 5, str(int(t["wins"])), f, 9, INK, align="r")
                _txt(c, 340, y0 + 5, str(int(t["losses"])), f, 9, INK, align="r")
                _txt(c, 440, y0 + 5, _pct(t["p_top_seed"]), "Helvetica-Bold", 9.5,
                     MAPLE if t["is_team"] else BARK, align="r")
                _txt(c, 530, y0 + 5, _pct(t["p_first_round_bye"]), f, 9, INK, align="r")
                y -= 17

            # ---- the two-body problem: our night and theirs --------------------
            jt = race.get("joint") or []
            riv = race.get("rival")
            if jt and riv:
                y -= 18
                section_title(c, 36, y, f"What it takes: our night and {riv}'s", W - 72)
                y -= 20
                y = wrap(c, 36, y, "The seed race is really a two-body problem. Our own result only "
                         "gets us so far — the rest depends on whether the club a game ahead of us "
                         f"drops anything to a winless side. We hold the head-to-head over {riv}, "
                         "so a tie goes our way.",
                         W - 72, "Helvetica", 9, 11.5, MUTED)
                y -= 4
                LBL = {0: "lose both", 1: "split", 2: "win both"}
                _txt(c, 52, y, "MAPLE TREE", "Helvetica-Bold", 7, MUTED)
                _txt(c, 168, y, riv.upper(), "Helvetica-Bold", 7, MUTED)
                _txt(c, 330, y, "CHANCE", "Helvetica-Bold", 7, MUTED, align="r")
                _txt(c, 424, y, "#2 OR BETTER", "Helvetica-Bold", 7, MUTED, align="r")
                _txt(c, W - 36, y, "LIKELIEST SEED", "Helvetica-Bold", 7, MUTED, align="r")
                y -= 4
                for i, j in enumerate(jt):
                    y0 = y - 17
                    good = j["p_seed_2_or_better"] > 0.2
                    if good:
                        c.setFillColor(CREAM)
                        c.rect(36, y0, W - 72, 17, stroke=0, fill=1)
                        c.setFillColor(MAPLE)
                        c.rect(36, y0, 3, 17, stroke=0, fill=1)
                    elif i % 2 == 0:
                        c.setFillColor(STRIPE)
                        c.rect(36, y0, W - 72, 17, stroke=0, fill=1)
                    f = "Helvetica-Bold" if good else "Helvetica"
                    _txt(c, 52, y0 + 5, LBL[j["our_wins"]], f, 9, BARK if good else INK)
                    _txt(c, 168, y0 + 5, LBL[j["rival_wins"]], f, 9, BARK if good else INK)
                    _txt(c, 330, y0 + 5, _pct(j["p"]), "Helvetica", 9, MUTED, align="r")
                    _txt(c, 424, y0 + 5, _pct(j["p_seed_2_or_better"]), "Helvetica-Bold", 9.5,
                         MAPLE if good else MUTED, align="r")
                    top = max(j["seeds"], key=lambda x: x["p"]) if j["seeds"] else None
                    _txt(c, W - 36, y0 + 5, f"#{top['seed']}" if top else "-", f, 9,
                         BARK if good else INK, align="r")
                    y -= 17
                y -= 10
                wrap(c, 36, y, "Two live paths to the second seed, both requiring help: sweep the "
                     f"doubleheader and have {riv} drop one, or split it and have them drop both. "
                     "The catch is that their last two are against the only winless club in the "
                     "league, which is why the unconditional number looks so thin.",
                     W - 72, "Helvetica-Oblique", 8.8, 11, MAPLE)

            c.setStrokeColor(LINE)
            c.setLineWidth(0.75)
            c.line(36, 64, W - 36, 64)
            _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  THE SEED RACE", "Helvetica-Bold", 8, BARK, cs=1)
            _txt(c, W - 36, 50, "full breakdown at mapletreesoftball.netlify.app/seed-race",
                 "Helvetica", 8, MUTED, align="r")
            c.showPage()

    c.save()

    print(f"\nPostgame recap -> {out}")
    print(f"  {outcome_verb} {opponent}  ·  {date_pretty}  ·  {scores}")
    print(f"  stars: {', '.join(s['name'] for s in stars[:3])}  ·  milestones reached: {len(reached)}")
    if featured:
        print(f"  card corner: {', '.join(c_['asset'] for c_ in featured)}")


if __name__ == "__main__":
    main()
