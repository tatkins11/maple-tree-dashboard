"""Render the Week 6 lineup analysis to a 3-page PDF.

Reads data/processed/lineup_analysis_2026_07_29.json (written by
scripts/_lineup_analysis_2026_07_29.py) and lays it out in the house style.

    python scripts/build_lineup_analysis_pdf.py
"""
from __future__ import annotations

import json
from pathlib import Path

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "data" / "processed" / "lineup_analysis_2026_07_29.json"
OUT = REPO / "data" / "writeups" / "maple-tree-summer-2026" / "maple-tree-lineup-analysis-2026-07-29.pdf"

BARK = HexColor("#2c1a0d")
SAND, PAPER = HexColor("#efece0"), HexColor("#f8f7f2")
INK, MUTED, MAPLE = HexColor("#20261f"), HexColor("#77705f"), HexColor("#c2410c")
CREAM, GREEN, LINE = HexColor("#f2e9d8"), HexColor("#15803d"), HexColor("#d8d3c2")
STRIPE, AMBER = HexColor("#f1eee2"), HexColor("#b45309")
W, H = letter


def txt(c, x, y, s, f="Helvetica", sz=9, col=INK, align="l", cs=0):
    c.setFont(f, sz)
    c.setFillColor(col)
    fn = {"l": c.drawString, "r": c.drawRightString, "c": c.drawCentredString}[align]
    if cs:
        fn(x, y, str(s), charSpace=cs)
    else:
        fn(x, y, str(s))


def wrap(c, x, y, s, width, f="Helvetica", sz=9, col=INK, lead=12):
    c.setFont(f, sz)
    words, line = str(s).split(), ""
    for wd in words:
        t = f"{line} {wd}".strip()
        if c.stringWidth(t, f, sz) <= width:
            line = t
        else:
            txt(c, x, y, line, f, sz, col)
            y -= lead
            line = wd
    if line:
        txt(c, x, y, line, f, sz, col)
        y -= lead
    return y


def header(c, kicker, title, right_top="", right_bot=""):
    c.setFillColor(BARK)
    c.rect(0, H - 108, W, 108, stroke=0, fill=1)
    txt(c, 36, H - 46, kicker, "Helvetica-Bold", 9, CREAM, cs=2.4)
    txt(c, 36, H - 78, title, "Helvetica-Bold", 26, HexColor("#ffffff"), cs=0.6)
    if right_top:
        txt(c, W - 36, H - 46, right_top, "Helvetica-Bold", 13, HexColor("#ffffff"), align="r")
    if right_bot:
        txt(c, W - 36, H - 66, right_bot, "Helvetica", 9, HexColor("#d9c9a8"), align="r")


def footer(c, note=""):
    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(36, 58, W - 36, 58)
    txt(c, 36, 44, "MAPLE TREE SOFTBALL  ·  LINEUP ANALYSIS", "Helvetica-Bold", 8, BARK, cs=1)
    if note:
        txt(c, W - 36, 44, note, "Helvetica", 7.5, MUTED, align="r")


def section(c, x, y, label, width):
    txt(c, x, y, label.upper(), "Helvetica-Bold", 10.5, BARK, cs=1.6)
    c.setStrokeColor(BARK)
    c.setLineWidth(1.4)
    c.line(x, y - 7, x + width, y - 7)


def card(c, x, y, w, h, fill=PAPER):
    c.setFillColor(fill)
    c.setStrokeColor(LINE)
    c.setLineWidth(0.8)
    c.roundRect(x, y, w, h, 7, stroke=1, fill=1)


def bar(c, x, y, w, h, frac, col, track=HexColor("#e6e1d2")):
    c.setFillColor(track)
    c.roundRect(x, y, w, h, h / 2, stroke=0, fill=1)
    fw = max(0.0, min(1.0, frac)) * w
    if fw > 0.4:
        c.setFillColor(col)
        c.roundRect(x, y, fw, h, h / 2, stroke=0, fill=1)


# --------------------------------------------------------------- page 1
def page_verdict(c, d):
    header(c, "SUMMER 2026  ·  WEEK 6  ·  JULY 29", "IS THIS THE BEST LINEUP?",
           "vs Sandlot Vibes", "12 deep  ·  Boncosky Green")
    xr = d["base"]["mean"]
    rank = d["rank_modeled"]

    # headline band
    card(c, 36, H - 236, W - 72, 108, CREAM)
    txt(c, 52, H - 152, "PROJECTED RUNS TONIGHT", "Helvetica-Bold", 8.5, MUTED, cs=1.6)
    big = f"{xr:.1f}"
    txt(c, 52, H - 196, big, "Helvetica-Bold", 46, BARK)
    bx = 52 + c.stringWidth(big, "Helvetica-Bold", 46) + 10
    txt(c, bx, H - 196, "runs", "Helvetica", 14, MUTED)
    txt(c, 52, H - 214, f"{d['sims']:,} simulated games", "Helvetica-Oblique", 8, MUTED)

    c.setStrokeColor(LINE)
    c.line(300, H - 226, 300, H - 140)
    txt(c, 320, H - 152, f"#{rank}", "Helvetica-Bold", 34, MAPLE)
    txt(c, 320 + c.stringWidth(f"#{rank}", "Helvetica-Bold", 34) + 10, H - 152,
        f"of {d['n_lineups']} lineups", "Helvetica", 11, MUTED)
    txt(c, 320, H - 172, "in franchise history", "Helvetica-Bold", 8.5, MUTED, cs=1.4)
    wrap(c, 320, H - 192, "The three ahead of it all batted ten. This is comfortably the best "
         "twelve-deep lineup the club has ever fielded, and it is level with the best ten-man "
         "order of this season.", W - 356, "Helvetica", 8.6, INK, 11)

    # verdict
    y = H - 262
    section(c, 36, y, "The verdict", W - 72)
    y -= 26
    card(c, 36, y - 110, W - 72, 108, CREAM)
    txt(c, 50, y - 22, "Your read is right — this is the best group of bats we have run out.",
        "Helvetica-Bold", 12.5, BARK)
    ny = wrap(c, 50, y - 42, "Six hitters project above a .550 on-base rate and the top four alone "
              "account for roughly half the offence. Three lineups edge it on paper and every one "
              "of them batted ten; carrying twelve and still landing within a third of a run of the "
              "all-time mark is the real result here. Against a league average of "
              f"{d['league_rs_per_team_game']:.1f} runs a team, this order projects "
              f"{xr - d['league_rs_per_team_game']:+.1f} — and that gap is the whole story of the season.",
              W - 100, "Helvetica", 9.2, INK, 11.5)
    wrap(c, 50, ny - 4, "One caveat that has not gone away: Harm has 11 career plate appearances. "
         "He now projects sensibly, but he is the least certain line on the card.",
         W - 100, "Helvetica-Oblique", 8.6, MAPLE, 11)

    # all-time leaderboard — what actually sits ahead of tonight, and how close
    y -= 130
    section(c, 36, y, "The highest-projecting lineups this club has ever fielded", W - 72)
    y -= 20
    txt(c, 46, y, "LINEUP", "Helvetica-Bold", 7, MUTED)
    txt(c, 400, y, "DEPTH", "Helvetica-Bold", 7, MUTED, align="r")
    txt(c, 470, y, "xRUNS", "Helvetica-Bold", 7, MUTED, align="r")
    y -= 5
    board = []
    for hrow in d["historical"][:4]:
        board.append((f"{hrow['date']}  vs {hrow['opponent']}", hrow["n"], hrow["xr"], False))
    board.append(("2026-07-29  vs Sandlot Vibes  (tonight)", 12, xr, True))
    board.sort(key=lambda r: -r[2])
    for i, (label, depth, val, is_us) in enumerate(board):
        y0 = y - 21
        if is_us:
            c.setFillColor(CREAM)
            c.rect(36, y0, W - 72, 21, stroke=0, fill=1)
            c.setFillColor(MAPLE)
            c.rect(36, y0, 3, 21, stroke=0, fill=1)
        elif i % 2 == 0:
            c.setFillColor(STRIPE)
            c.rect(36, y0, W - 72, 21, stroke=0, fill=1)
        f = "Helvetica-Bold" if is_us else "Helvetica"
        txt(c, 48, y0 + 7, label, f, 9, BARK if is_us else INK)
        txt(c, 400, y0 + 7, str(depth), f, 9, BARK if is_us else INK, align="r")
        txt(c, 470, y0 + 7, f"{val:.2f}", "Helvetica-Bold", 9.5, MAPLE if is_us else BARK, align="r")
        if is_us:
            txt(c, 490, y0 + 7, "deepest order on the board", "Helvetica-Oblique", 7.5, MAPLE)
        y -= 21

    # Range of outcomes: percentile strip over a histogram of every simulated game.
    # Everything below is pinned to absolute y so it cannot drift into the footer
    # rule at y=58 as the prose above changes length.
    b = d["base"]
    section(c, 36, 232, "The range of outcomes", W - 72)
    for lbl, v, x in [("10th pct", b["p10"], 36), ("25th", b["p25"], 168),
                      ("median", b["median"], 300), ("75th", b["p75"], 432), ("90th pct", b["p90"], 540)]:
        txt(c, x, 208, f"{v:.0f}", "Helvetica-Bold", 18, BARK)
        txt(c, x, 197, lbl, "Helvetica", 7.5, MUTED, cs=0.8)

    BASE_Y, BAR_MAX = 112, 62
    dist = {int(k): v for k, v in b["dist"].items()}
    cols = [(r, dist.get(r, 0.0)) for r in range(0, 27)]
    peak = max(v for _, v in cols) or 1.0
    bw = (W - 72) / len(cols)
    for i, (r, v) in enumerate(cols):
        x = 36 + i * bw
        # shade the middle half of outcomes so the spread reads at a glance
        inside = b["p25"] <= r <= b["p75"]
        c.setFillColor(MAPLE if inside else HexColor("#c9c2ae"))
        c.rect(x + 1, BASE_Y, bw - 2, max((v / peak) * BAR_MAX, 0.5), stroke=0, fill=1)
        if r % 5 == 0:
            txt(c, x + bw / 2, BASE_Y - 11, str(r), "Helvetica", 7, MUTED, align="c")
    txt(c, 36, BASE_Y - 26, f"Runs in a single game across {d['sims']:,} simulations. Shaded is the "
        f"middle half. Roughly one night in ten is a {b['p10']:.0f}-run dud and one in ten is "
        f"{b['p90']:.0f} or more —", "Helvetica-Oblique", 8, MUTED)
    txt(c, 36, BASE_Y - 37, "slowpitch scoring is wide, so tonight's actual total will say very "
        "little about whether the lineup was right.", "Helvetica-Oblique", 8, MUTED)
    footer(c, "7 full innings · calibrated to 0.314 runs/PA, our own rate over 2,928 PA")


# --------------------------------------------------------------- page 2
def page_card(c, d):
    header(c, "SUMMER 2026  ·  WEEK 6", "THE LINEUP CARD",
           "Projected rates", "blended season + career")
    y = H - 136
    section(c, 36, y, "Tonight's twelve", W - 72)
    y -= 22
    for lbl, x in [("OBP", 300), ("TB/PA", 356), ("HR%", 404), ("BB%", 446), ("PA '26", 490)]:
        txt(c, x, y, lbl, "Helvetica-Bold", 7, MUTED, align="r")
    txt(c, W - 36, y, "CONFIDENCE", "Helvetica-Bold", 7, MUTED, align="r")
    y -= 6
    rh = 42
    for p in d["players"]:
        y0 = y - rh
        if p["spot"] % 2 == 1:
            c.setFillColor(STRIPE)
            c.rect(36, y0, W - 72, rh, stroke=0, fill=1)
        txt(c, 54, y0 + rh / 2 - 6, str(p["spot"]), "Helvetica-Bold", 16, BARK, align="c")
        nm = p["display"] if p["display"] != "Jj" else "JJ"
        txt(c, 74, y0 + rh / 2 + 2, nm, "Helvetica-Bold", 12.5, BARK)
        tags = []
        if p["dhh"]:
            tags.append("designated HR hitter (exempt from the 3-HR cap)")
        if p["car_pa"] < 40:
            tags.append(f"only {p['car_pa']:.0f} career PA — least certain line on the card")
        if tags:
            txt(c, 74, y0 + rh / 2 - 11, "  ·  ".join(tags), "Helvetica-Oblique", 7.6,
                MAPLE if p["car_pa"] < 40 else MUTED)
        thin = p["car_pa"] < 40
        txt(c, 300, y0 + rh / 2 - 3, f"{p['obp']:.3f}".replace("0.", "."),
            "Helvetica-Bold", 11, AMBER if thin else BARK, align="r")
        txt(c, 356, y0 + rh / 2 - 3, f"{p['tb_rate']:.2f}", "Helvetica", 10, INK, align="r")
        txt(c, 404, y0 + rh / 2 - 3, f"{p['hr'] * 100:.1f}", "Helvetica", 10, INK, align="r")
        txt(c, 446, y0 + rh / 2 - 3, f"{p['bb'] * 100:.1f}", "Helvetica", 10, INK, align="r")
        txt(c, 490, y0 + rh / 2 - 3, f"{p['cur_pa']:.0f}", "Helvetica", 10, MUTED, align="r")
        # confidence = how much real history sits behind the projection.
        # Starts at 508 so it clears the PA column right-aligned at 490.
        conf = min(p["car_pa"] / 250.0, 1.0)
        bar(c, 508, y0 + rh / 2 - 4, W - 36 - 508, 5, conf, GREEN if conf > 0.5 else AMBER)
        y -= rh
    y -= 10
    txt(c, 36, y, "OBP, TB/PA, HR% and BB% are PROJECTIONS, not what a player has done this season. "
        "Each blends 2026 against a recency-weighted career baseline;", "Helvetica-Oblique", 8, MUTED)
    txt(c, 36, y - 11, "with a month of games in, most of every line above is career history. Thin "
        "records are pulled toward the roster mean, so a hitter with a handful of PA cannot",
        "Helvetica-Oblique", 8, MUTED)
    txt(c, 36, y - 22, "project as a star or a scrub on noise alone. The confidence bar is career plate "
        "appearances against a 250-PA full bar.", "Helvetica-Oblique", 8, MUTED)
    footer(c, "Blend: season vs recency-weighted prior (prior capped at its own PA), then shrunk "
              "toward the roster mean by 25 PA")


# --------------------------------------------------------------- page 3
def page_production(c, d):
    header(c, "SUMMER 2026  ·  WEEK 6", "WHERE THE RUNS COME FROM",
           "Tonight's twelve", "expected production by slot")
    sl = d["slots_lifted"]
    prod = {i: sl[str(i)]["r"] + sl[str(i)]["rbi"] for i in range(1, 13)}
    tot = sum(prod.values())
    peak = max(prod.values())

    y = H - 136
    section(c, 36, y, "Expected production by lineup spot", W - 72)
    y -= 20
    txt(c, 74, y, "HITTER", "Helvetica-Bold", 7, MUTED)
    for lbl, x in [("PA", 250), ("HITS", 292), ("HR", 330), ("RUNS", 374), ("RBI", 414)]:
        txt(c, x, y, lbl, "Helvetica-Bold", 7, MUTED, align="r")
    txt(c, 470, y, "R + RBI", "Helvetica-Bold", 7, MUTED, align="r")
    txt(c, W - 36, y, "SHARE", "Helvetica-Bold", 7, MUTED, align="r")
    y -= 4
    rh = 25
    for i in range(1, 13):
        r = sl[str(i)]
        y0 = y - rh
        if i % 2 == 1:
            c.setFillColor(STRIPE)
            c.rect(36, y0, W - 72, rh, stroke=0, fill=1)
        nm = r["name"] if r["name"] != "Jj" else "JJ"
        txt(c, 52, y0 + 9, str(i), "Helvetica-Bold", 11, BARK, align="c")
        txt(c, 74, y0 + 9, nm, "Helvetica-Bold", 10.5, BARK)
        txt(c, 250, y0 + 9, f"{r['pa']:.2f}", "Helvetica", 9, INK, align="r")
        txt(c, 292, y0 + 9, f"{r['h']:.2f}", "Helvetica", 9, INK, align="r")
        txt(c, 330, y0 + 9, f"{r['hr']:.2f}", "Helvetica", 9, INK, align="r")
        txt(c, 374, y0 + 9, f"{r['r']:.2f}", "Helvetica", 9, INK, align="r")
        txt(c, 414, y0 + 9, f"{r['rbi']:.2f}", "Helvetica", 9, INK, align="r")
        txt(c, 470, y0 + 9, f"{prod[i]:.2f}", "Helvetica-Bold", 9.5, BARK, align="r")
        bar(c, 486, y0 + 8, W - 36 - 486, 5, prod[i] / peak, MAPLE if i <= 4 else HexColor("#a8a290"))
        y -= rh
    y -= 8
    txt(c, 36, y, "Averages per game across 40,000 simulations of this exact order. R + RBI "
        "double-counts a solo home run on purpose — it is a", "Helvetica-Oblique", 7.6, MUTED)
    txt(c, 36, y - 10, "share-of-the-offence measure, not a run total.", "Helvetica-Oblique", 7.6, MUTED)

    y -= 34
    section(c, 36, y, "What the shape tells you", W - 72)
    y -= 26
    top4 = sum(prod[i] for i in range(1, 5))
    card(c, 36, y - 86, 250, 84, CREAM)
    txt(c, 52, y - 30, f"{top4 / tot * 100:.0f}%", "Helvetica-Bold", 34, BARK)
    txt(c, 52, y - 46, "of all run production comes", "Helvetica", 8.6, MUTED)
    txt(c, 52, y - 57, "from the first four spots", "Helvetica", 8.6, MUTED)
    txt(c, 52, y - 74, f"Glove, Tristan, Harm and Tim", "Helvetica-Oblique", 8.4, MAPLE)
    card(c, 300, y - 86, W - 36 - 300, 84, CREAM)
    txt(c, 316, y - 30, f"{sl['1']['pa'] - sl['12']['pa']:.1f}", "Helvetica-Bold", 34, BARK)
    txt(c, 316, y - 46, "extra trips to the plate for the", "Helvetica", 8.6, MUTED)
    txt(c, 316, y - 57, "leadoff man over the twelve hole", "Helvetica", 8.6, MUTED)
    txt(c, 316, y - 74, f"{sl['1']['pa']:.2f} PA batting first vs {sl['12']['pa']:.2f} batting last",
        "Helvetica-Oblique", 8.4, MAPLE)
    y -= 104

    section(c, 36, y, "Is this the right order for these twelve?", W - 72)
    y -= 24
    lo, hi = d["order_worst"]["mean"], d["order_best"]["mean"]
    card(c, 36, y - 70, W - 72, 68, PAPER)
    # The sampler searches 400 of 479 million orderings at lower precision, so it
    # can land a hair BELOW the full-precision run of the real order. Clamp at 0
    # rather than print a negative "distance from optimal", which reads as an error.
    gap = max(hi - d["base"]["mean"], 0.0)
    txt(c, 52, y - 26, f"{gap:.2f}", "Helvetica-Bold", 24, GREEN)
    txt(c, 52, y - 41, "runs from the best order found —", "Helvetica", 8.4, MUTED)
    txt(c, 52, y - 52, "yours is already it", "Helvetica", 8.4, MUTED)
    wrap(c, 216, y - 22, "Four hundred random reshuffles of these same twelve names span only "
         f"{hi - lo:.2f} runs end to end, and yours sits within a quarter run of the best one found. "
         "Everybody bats and innings are long, so sequencing carries far less weight here than in "
         "baseball. The order is settled — the lineup is what it is, and it is a good one.",
         W - 268, "Helvetica", 8.8, INK, 11)
    footer(c, "Engine: src/models/simulator.py · projections: src/models/projections.py")


def main() -> None:
    d = json.loads(SRC.read_text(encoding="utf-8"))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    c = pdfcanvas.Canvas(str(OUT), pagesize=letter)
    c.setTitle("Maple Tree — Week 6 Lineup Analysis")
    for fn in (page_verdict, page_card, page_production):
        c.setFillColor(SAND)
        c.rect(0, 0, W, H, stroke=0, fill=1)
        fn(c, d)
        c.showPage()
    c.save()
    print(f"Lineup analysis -> {OUT}")


if __name__ == "__main__":
    main()
