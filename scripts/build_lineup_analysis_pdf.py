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
    lo, hi = d["base"]["mean"], d["sensitivity_team_avg"]["mean"]

    # headline band
    card(c, 36, H - 236, W - 72, 108, CREAM)
    txt(c, 52, H - 152, "PROJECTED RUNS TONIGHT", "Helvetica-Bold", 8.5, MUTED, cs=1.6)
    big = f"{lo:.1f} – {hi:.1f}"
    txt(c, 52, H - 196, big, "Helvetica-Bold", 42, BARK)
    bx = 52 + c.stringWidth(big, "Helvetica-Bold", 42) + 10  # sit clear of the numerals
    txt(c, bx, H - 196, "runs", "Helvetica", 13, MUTED)
    txt(c, 52, H - 214, f"{d['sims']:,} simulated games", "Helvetica-Oblique", 8, MUTED)

    c.setStrokeColor(LINE)
    c.line(300, H - 226, 300, H - 140)
    txt(c, 320, H - 152, "WHY A RANGE AND NOT A NUMBER", "Helvetica-Bold", 8.5, MUTED, cs=1.4)
    wrap(c, 320, H - 168, "The whole answer hinges on one hitter. Harm has 11 career plate "
         "appearances. The model regresses his 4-for-4, 3-HR night against an 0-for-4 from "
         "2021 and lands on a .255 on-base rate. That is the model saying it knows nothing, "
         "not that Harm is bad.", W - 356, "Helvetica", 8.6, INK, 11)

    # the two readings
    y = H - 262
    section(c, 36, y, "The two honest readings", W - 72)
    y -= 30
    for label, val, rank, note, col in [
        ("Model as written", lo, d["rank_modeled"],
         "Trusts a 5-PA line from 2021 as Harm's baseline. Conservative to the point of wrong.", MUTED),
        ("Harm as an average bat in this lineup", hi, d["rank_lifted"],
         "Replaces the noise with the profile of a typical hitter in tonight's order.", MAPLE),
    ]:
        card(c, 36, y - 52, W - 72, 50, PAPER)
        txt(c, 50, y - 22, label, "Helvetica-Bold", 11, BARK)
        txt(c, 50, y - 38, note, "Helvetica", 8.2, MUTED)
        txt(c, W - 210, y - 26, f"{val:.2f}", "Helvetica-Bold", 20, col, align="r")
        txt(c, W - 200, y - 26, "xR", "Helvetica", 9, MUTED)
        txt(c, W - 50, y - 22, f"#{rank}", "Helvetica-Bold", 17, BARK, align="r")
        txt(c, W - 50, y - 38, f"of {d['n_lineups']} lineups", "Helvetica", 7.5, MUTED, align="r")
        y -= 62

    # verdict
    y -= 4
    section(c, 36, y, "The verdict", W - 72)
    y -= 26
    card(c, 36, y - 96, W - 72, 94, CREAM)
    txt(c, 50, y - 22, "Best group of bats we have fielded. Not the highest-scoring lineup.",
        "Helvetica-Bold", 12.5, BARK)
    ny = wrap(c, 50, y - 42, "Give Harm an average profile and this order ranks 6th out of 83 lineups "
              "in franchise history — top 7 percent. But three of the five ahead of it are ten- and "
              "eleven-man orders, and that is the catch: batting twelve is what costs it the top spot, "
              "not the quality of the twelve. Seven innings hands a team a fixed pile of plate "
              "appearances. Every name you add divides that pile further.",
              W - 100, "Helvetica", 9.2, INK, 11.5)
    txt(c, 50, ny - 2, f"League average tonight is {d['league_rs_per_team_game']:.1f} runs per team. "
        f"Either reading clears it.", "Helvetica-Oblique", 9, MAPLE)

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
    footer(c, "Monte Carlo over projected per-plate-appearance rates · 7 innings · 3-HR cap")


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
            tags.append(f"only {p['car_pa']:.0f} career PA — model has no read")
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
    txt(c, 36, y - 11, "with a month of games in, roughly three quarters of every line above is career "
        "history. The confidence bar is career plate appearances against a 250-PA full bar.",
        "Helvetica-Oblique", 8, MUTED)
    footer(c, "Projection blend: current PA / (current PA + 120) against a recency-weighted prior")


# --------------------------------------------------------------- page 3
def page_drivers(c, d):
    header(c, "SUMMER 2026  ·  WEEK 6", "WHAT ACTUALLY MOVES IT",
           "Two findings", "one of them counterintuitive")

    y = H - 138
    section(c, 36, y, "1. The batting order barely matters", W - 72)
    y -= 26
    lo, hi = d["order_worst"]["mean"], d["order_best"]["mean"]
    card(c, 36, y - 74, W - 72, 72, PAPER)
    txt(c, 52, y - 26, f"{hi - lo:.2f}", "Helvetica-Bold", 26, BARK)
    txt(c, 52, y - 42, "runs between the best and worst", "Helvetica", 8.4, MUTED)
    txt(c, 52, y - 54, "of 400 randomly sampled orderings", "Helvetica", 8.4, MUTED)
    wrap(c, 210, y - 22, "Shuffling these same twelve names into a deliberately terrible sequence "
         "costs about six tenths of a run. Tonight's order is already within a quarter run of the "
         "best arrangement the sampler found. In slowpitch everybody bats and innings are long, so "
         "sequencing has far less leverage than it does in baseball. Who is in the order is the "
         "decision that matters. The order itself is nearly free.",
         W - 262, "Helvetica", 9, INK, 11.5)
    y -= 92

    section(c, 36, y, "2. Every extra bat costs about seven tenths of a run", W - 72)
    y -= 26
    dc = d["depth_curve"]
    card(c, 36, y - 116, W - 72, 114, PAPER)
    bx, bw = 300, 210
    mx = max(dc.values())
    yy = y - 26
    for n in ["9", "10", "11", "12"]:
        v = dc[n]
        is_tonight = n == "12"
        txt(c, 62, yy - 3, f"bat {n}", "Helvetica-Bold" if is_tonight else "Helvetica", 9.5,
            BARK if is_tonight else INK)
        bar(c, 108, yy - 5, 146, 8, v / mx, MAPLE if is_tonight else HexColor("#a8a290"))
        txt(c, 288, yy - 3, f"{v:.2f}", "Helvetica-Bold" if is_tonight else "Helvetica", 9.5,
            MAPLE if is_tonight else INK, align="r")  # right-aligned clear of the 254 bar end
        if is_tonight:
            txt(c, 294, yy - 3, "← tonight", "Helvetica-Bold", 7.5, MAPLE)
        yy -= 22
    wrap(c, 362, y - 22, "Seven innings fixes how many plate appearances a team gets. Adding a name "
         "does not create new trips to the plate — it moves them from the hitters at the top to the "
         "hitters at the bottom. Going twelve deep instead of nine costs roughly two runs a game. "
         "That is the price of everyone playing, and it is a price worth paying in a rec league. "
         "It is just worth knowing the number.",
         W - 36 - 362, "Helvetica", 8.6, INK, 10.8)
    y -= 134

    section(c, 36, y, "The lineups ahead of tonight's", W - 72)
    y -= 22
    txt(c, 36, y, "LINEUP", "Helvetica-Bold", 7, MUTED)
    for lbl, x in [("DEPTH", 400), ("xRUNS", 470)]:
        txt(c, x, y, lbl, "Helvetica-Bold", 7, MUTED, align="r")
    txt(c, W - 36, y, "NOTE", "Helvetica-Bold", 7, MUTED, align="r")
    y -= 6
    top = d["historical"][:5]
    for i, hrow in enumerate(top):
        y0 = y - 20
        if i % 2 == 0:
            c.setFillColor(STRIPE)
            c.rect(36, y0, W - 72, 20, stroke=0, fill=1)
        txt(c, 46, y0 + 6, f"{hrow['date']}  vs {hrow['opponent']}", "Helvetica", 9, INK)
        txt(c, 400, y0 + 6, f"{hrow['n']}", "Helvetica", 9, INK, align="r")
        txt(c, 470, y0 + 6, f"{hrow['xr']:.2f}", "Helvetica-Bold", 9.5, BARK, align="r")
        txt(c, W - 36, y0 + 6, "shorter order" if hrow["n"] < 12 else "", "Helvetica-Oblique",
            7.5, MUTED, align="r")
        y -= 20
    y0 = y - 22
    c.setFillColor(CREAM)
    c.rect(36, y0, W - 72, 22, stroke=0, fill=1)
    c.setFillColor(MAPLE)
    c.rect(36, y0, 3, 22, stroke=0, fill=1)
    txt(c, 46, y0 + 7, "2026-07-29  vs Sandlot Vibes  (tonight)", "Helvetica-Bold", 9, BARK)
    txt(c, 400, y0 + 7, "12", "Helvetica-Bold", 9, BARK, align="r")
    txt(c, 470, y0 + 7, f"{d['sensitivity_team_avg']['mean']:.2f}", "Helvetica-Bold", 9.5, MAPLE, align="r")
    txt(c, W - 36, y0 + 7, "deepest order on the list", "Helvetica-Oblique", 7.5, MAPLE, align="r")
    y -= 44

    section(c, 36, y, "What to hold against it", W - 72)
    y -= 20
    for line in [
        "Every historical lineup is scored with each player's CURRENT projection, so this compares "
        "who was in the order and where — not how good those players were at the time.",
        "Sandlot Vibes are 7-1 and score 17.6 a game in an 11.6-run league. Nothing here models "
        "the opponent; it is our run production against a neutral defence.",
        "Harm is one hot week with no track record. If he is closer to the 2021 line than to last "
        "Wednesday, the honest number is the bottom of the range, not the top.",
    ]:
        y = wrap(c, 46, y, "·  " + line, W - 92, "Helvetica", 8.5, MUTED, 10.5) - 4
    footer(c, "Engine: src/models/simulator.py · projections: src/models/projections.py")


def main() -> None:
    d = json.loads(SRC.read_text(encoding="utf-8"))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    c = pdfcanvas.Canvas(str(OUT), pagesize=letter)
    c.setTitle("Maple Tree — Week 6 Lineup Analysis")
    for fn in (page_verdict, page_card, page_drivers):
        c.setFillColor(SAND)
        c.rect(0, 0, W, H, stroke=0, fill=1)
        fn(c, d)
        c.showPage()
    c.save()
    print(f"Lineup analysis -> {OUT}")


if __name__ == "__main__":
    main()
