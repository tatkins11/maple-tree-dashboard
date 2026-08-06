"""Week 7 postgame extras — two pages appended to the recap.

Page A: the seeding scenarios now that our regular season is done.
Page B: the expected playoff bracket (all games Wed 8/19, Brian's sheet).

Everything numeric is read from site/src/data/seed_race.json so the pages agree
with the site. Merged onto the recap PDF by the __main__ block.

    python scripts/_week7_postgame_extras.py
"""
from __future__ import annotations

import json
from pathlib import Path

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

REPO = Path(__file__).resolve().parents[1]
SR = json.loads((REPO / "site/src/data/seed_race.json").read_text(encoding="utf-8"))
EXTRAS = REPO / "data/writeups/maple-tree-summer-2026/_week7_extras.pdf"
RECAP = REPO / "data/writeups/maple-tree-summer-2026/maple-tree-postgame-recap-2026-08-05.pdf"

BARK = HexColor("#2c1a0d")
SAND, PAPER, WHITE = HexColor("#efece0"), HexColor("#f8f7f2"), HexColor("#ffffff")
INK, MUTED, MAPLE = HexColor("#20261f"), HexColor("#77705f"), HexColor("#c2410c")
TAN, CREAM, GREEN, LINE = HexColor("#d9c9a8"), HexColor("#f2e9d8"), HexColor("#15803d"), HexColor("#d8d3c2")
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


def wrap(c, x, y, s, width, f="Helvetica", sz=9, col=INK, lead=11.5):
    c.setFont(f, sz)
    line = ""
    for wd in str(s).split():
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
    c.setFillColor(SAND)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 104, W, 104, stroke=0, fill=1)
    txt(c, 36, H - 44, kicker, "Helvetica-Bold", 9, CREAM, cs=2.2)
    txt(c, 36, H - 76, title, "Helvetica-Bold", 25, WHITE, cs=0.5)
    if right_top:
        txt(c, W - 36, H - 44, right_top, "Helvetica-Bold", 13, WHITE, align="r")
    if right_bot:
        txt(c, W - 36, H - 64, right_bot, "Helvetica", 9, TAN, align="r")


def footer(c, note):
    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(36, 64, W - 36, 64)
    txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  WEEK 7 POSTGAME", "Helvetica-Bold", 8, BARK, cs=1)
    txt(c, W - 36, 50, note, "Helvetica", 8, MUTED, align="r")


def section(c, x, y, label, width):
    txt(c, x, y, label.upper(), "Helvetica-Bold", 11, BARK, cs=1.2)
    c.setStrokeColor(HexColor("#4a2e15"))
    c.setLineWidth(2)
    c.line(x, y - 6, x + width, y - 6)


def card(c, x, y, w, h, fill=PAPER):
    c.setFillColor(fill)
    c.setStrokeColor(LINE)
    c.setLineWidth(0.8)
    c.roundRect(x, y, w, h, 7, stroke=1, fill=1)


# ---------------------------------------------------------------- page A
def page_scenarios(c):
    us = next(t for t in SR["teams"] if t["is_team"])
    odds = {o["seed"]: o["p"] for o in SR["our_seed_odds"]}
    header(c, "WEEK 7 POSTGAME  ·  THE ROAD AHEAD", "THE SEEDING SCENARIOS",
           "8-4  ·  season complete", f"{SR['games_remaining']} league games decide our seed")

    # odds bars
    ty = H - 150
    section(c, 36, ty, "Where we finish", W - 72)
    yy = ty - 26
    peak = max(odds.values())
    for sd in sorted(odds):
        p = odds[sd]
        if p < 0.001:
            continue
        txt(c, 58, yy - 3, f"#{sd}", "Helvetica-Bold", 12, BARK, align="c")
        c.setFillColor(HexColor("#e6e1d2"))
        c.roundRect(80, yy - 8, 300, 13, 6, stroke=0, fill=1)
        c.setFillColor(MAPLE if p == peak else HexColor("#a8a290"))
        c.roundRect(80, yy - 8, max(300 * p / peak, 4), 13, 6, stroke=0, fill=1)
        txt(c, 390, yy - 3, f"{p:.1%}", "Helvetica-Bold", 11, MAPLE if p == peak else INK)
        yy -= 24
    wrap(c, 440, ty - 30, "The top-five bye is locked at 100% either way. "
         "What is left is which half of the bracket we live in.",
         W - 36 - 440, "Helvetica-Oblique", 8.4, MUTED, 10.5)

    # scenario cards
    sy = yy - 14
    section(c, 36, sy, "The four ways it plays out", W - 72)
    sy -= 22
    scen = [
        ("COMO BEAT BREW CREW TWICE", "~12%", GREEN,
         "Brew Crew fall to 7-5 and we take third outright. Como won their nightcap 15-6 "
         "this week, so it is not nothing."),
        ("BREW CREW SPLIT, BLEACHER BUMS SWEEP", "~16%", GREEN,
         "Both land on 8-4 with us. Three-way tie — and the rule requires a win over EVERY "
         "tied club. Brew Crew and Bleacher Bums have never played; we beat both. We take "
         "third on eligibility alone."),
        ("BREW CREW SPLIT, BLEACHER BUMS DON'T", "~30%", MAPLE,
         "Two-way tie at 8-4. They took the runs-allowed tiebreaker by TWO runs back in "
         "Week 3, and it holds. Fourth seed."),
        ("BREW CREW WIN BOTH", "~42%", MAPLE,
         "9-3 passes us outright. Fourth seed, no arguments, no tiebreakers."),
    ]
    ch = 74
    for i, (head, pct, col, body) in enumerate(scen):
        x = 36 + (i % 2) * ((W - 72) / 2 + 6)
        y0 = sy - ch - (i // 2) * (ch + 10)
        card(c, x, y0, (W - 72) / 2 - 6, ch, PAPER)
        txt(c, x + 12, y0 + ch - 17, head, "Helvetica-Bold", 8.6, col, cs=0.3)
        txt(c, x + (W - 72) / 2 - 18, y0 + ch - 17, pct, "Helvetica-Bold", 10, col, align="r")
        wrap(c, x + 12, y0 + ch - 32, body, (W - 72) / 2 - 30, "Helvetica", 8.2, INK, 10.2)
    sy = sy - 2 * ch - 10 - 24

    # the long-shot second seed
    card(c, 36, sy - 46, W - 72, 44, CREAM)
    txt(c, 50, sy - 18, "THE 1.9% LOTTERY TICKET", "Helvetica-Bold", 8.4, AMBER, cs=0.5)
    wrap(c, 50, sy - 31, "Wasted Talent drop both to Mean Beanz while Brew Crew land on 8-4: "
         "a three-way tie at the TOP, same eligibility trick, and we inherit the second seed. "
         "Nobody should plan around it. Everybody should know it exists.",
         W - 100, "Helvetica", 8.2, INK, 10)
    sy -= 66

    # rooting strip
    section(c, 36, sy, "Who to pull for, in order of leverage", W - 72)
    sy -= 20
    roots = [(r["home"], r["away"], (r.get("root_now") or {}).get("root_for"), r["max_swing"])
             for r in SR["rooting"] if (r.get("root_now") or {}).get("root_for")]
    for h, a, pick, sw in roots:
        txt(c, 46, sy, f"{h} vs {a}", "Helvetica", 9, INK)
        txt(c, 330, sy, f"→  {pick}", "Helvetica-Bold", 9.5, GREEN)
        txt(c, W - 46, sy, f"{sw:.2f} seed positions", "Helvetica", 8, MUTED, align="r")
        sy -= 15
    footer(c, "odds from the exhaustive enumeration on mapletreesoftball.netlify.app/seed-race")
    c.showPage()


# ---------------------------------------------------------------- page B
def page_bracket(c):
    proj = {r["seed"]: r["team"] for r in SR["projected_table"]}
    seedof = {r["team"]: r["seed"] for r in SR["projected_table"]}
    teams = {t["team"]: t for t in SR["teams"]}
    header(c, "WEDNESDAY 8/19  ·  ALL TEN GAMES, ONE NIGHT", "THE EXPECTED BRACKET",
           "Every club is in", "seeds 1-5 skip the 6:30 round")
    wrap(c, 36, H - 126, "Names below are each club's MOST LIKELY seed today — the bracket "
         "firms up when the make-up games finish. #1 and #2 sit in opposite halves and can only "
         "meet in the final; #3 rides with #2, while #4 and #5 open on each other for the right "
         "to face #1.", W - 72, "Helvetica", 8.8, INK, 11)

    def gcard(x, y, w, h, gno, time, field, line1, line2, hot=False):
        card(c, x, y, w, h, CREAM if hot else PAPER)
        if hot:
            c.setFillColor(MAPLE)
            c.rect(x, y, 3, h, stroke=0, fill=1)
        txt(c, x + 9, y + h - 13, f"GAME {gno}", "Helvetica-Bold", 7.4, MAPLE, cs=0.6)
        txt(c, x + w - 9, y + h - 13, f"{time} · {field}", "Helvetica", 7, MUTED, align="r")
        txt(c, x + 9, y + h - 27, line1, "Helvetica-Bold", 8.6, BARK)
        txt(c, x + 9, y + h - 40, line2, "Helvetica-Bold" if hot else "Helvetica", 8.6,
            BARK if hot else INK)

    nm = lambda s: f"#{s} {proj.get(s, '?')}"  # noqa: E731
    cw, chh = 172, 48
    # round columns
    txt(c, 36, H - 176, "PLAY-IN · 6:30", "Helvetica-Bold", 8, MUTED, cs=1)
    gcard(36, H - 232, cw, chh, 1, "6:30", "Yellow", nm(8), nm(9))
    gcard(36, H - 288, cw, chh, 2, "6:30", "Red", nm(7), nm(10))
    gcard(36, H - 344, cw, chh, 3, "6:30", "Blue", nm(6), nm(11))

    txt(c, 226, H - 176, "QUARTERS · 7:30", "Helvetica-Bold", 8, MUTED, cs=1)
    gcard(226, H - 232, cw, chh, 5, "7:30", "Yellow", nm(1), "winner of Game 1")
    gcard(226, H - 288, cw, chh, 4, "7:30", "Green", nm(4), nm(5), hot=proj.get(4) == "Maple Tree")
    gcard(226, H - 344, cw, chh, 6, "7:30", "Red", nm(2), "winner of Game 2")
    gcard(226, H - 400, cw, chh, 7, "7:30", "Blue", nm(3), "winner of Game 3")

    txt(c, 416, H - 176, "SEMIS · 8:30  /  FINAL · 9:30", "Helvetica-Bold", 8, MUTED, cs=1)
    gcard(416, H - 250, cw, chh, 8, "8:30", "Yellow", "Game 5 winner", "Game 4 winner", hot=True)
    gcard(416, H - 306, cw, chh, 9, "8:30", "Red", "Game 6 winner", "Game 7 winner")
    txt(c, 416, H - 330, "THE FINAL", "Helvetica-Bold", 8, MAPLE, cs=1)
    gcard(416, H - 384, cw, chh, 10, "9:30", "Yellow", "Game 8 winner", "Game 9 winner", hot=True)

    # our path
    py = H - 470
    section(c, 36, py, "Our night, both versions", W - 72)
    py -= 22
    half = (W - 72) / 2 - 6
    card(c, 36, py - 92, half, 90, CREAM)
    txt(c, 48, py - 18, "AS THE 4 SEED  ·  70% TODAY", "Helvetica-Bold", 8.4, MAPLE, cs=0.4)
    wrap(c, 48, py - 32, "7:30 on Green against Bleacher Bums — a club we split with and hold "
         "the tiebreaker over. Win, and the 8:30 semi is almost certainly Wasted Talent, who "
         "just run-ruled us and lost the nightcap. The hard half, with a rematch we want.",
         half - 24, "Helvetica", 8.2, INK, 10.2)
    card(c, 36 + half + 12, py - 92, half, 90, PAPER)
    txt(c, 48 + half + 12, py - 18, "AS THE 3 SEED  ·  28% TODAY", "Helvetica-Bold", 8.4, GREEN, cs=0.4)
    wrap(c, 48 + half + 12, py - 32, "7:30 on Blue against the 6/11 winner — most likely "
         "Slaughtered in 3, whom we swept by a combined 39-9. The semi is then likely Sandlot "
         "Vibes, the one contender we have hung 22 on. The soft half, and the whole reason the "
         "seed matters.", half - 24, "Helvetica", 8.2, INK, 10.2)
    py -= 112

    footer(c, "bracket per the league sheet · all ten games Wednesday 19 August")
    c.showPage()


def main():
    c = pdfcanvas.Canvas(str(EXTRAS), pagesize=letter)
    page_scenarios(c)
    page_bracket(c)
    c.save()

    import fitz
    rec = fitz.open(RECAP)
    ext = fitz.open(EXTRAS)
    rec.insert_pdf(ext)
    out = RECAP.with_suffix(".tmp.pdf")
    rec.save(out)
    rec.close()
    ext.close()
    out.replace(RECAP)
    EXTRAS.unlink()
    print(f"merged -> {RECAP}")


if __name__ == "__main__":
    main()
