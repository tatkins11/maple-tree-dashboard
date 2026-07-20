"""The Maple Tree Tap — WEEKLY CARD RELEASE newsletter (PDF).

A professionally-styled "card drop" release sheet showcasing every new card
minted for a game week — like a Topps/The Show program-drop announcement.
Recurring deliverable: run after minting the week's cards.

    python scripts/build_card_release.py --date 2026-07-15
    python scripts/build_card_release.py --assets tristan-50-hr,jj-100-r,...

By --date it auto-selects every milestone card whose (player, stat, value)
matches a milestone reached that night (+ any explicitly listed extras).
Cards are ranked by OVR and grouped into tiers (ruby > diamond > gold >
silver > bronze), the collector's chase hierarchy. Local deliverable
(not committed), like the other write-up PDFs.
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageDraw
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_gameday_preview import (  # noqa: E402  (shared drawing kit)
    BARK, BARK2, CREAM, INK, LINE, MAPLE, MUTED, PAPER, SAND, TAN, WHITE,
    _txt, load, prep_logo, wrap,
)
from card_frame import gem_tier_for  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
CARDS_DIR = REPO / "site" / "public" / "cards"
W, H = letter
GOLD = (198, 142, 30)
GOLD_HI = (232, 190, 96)

TIER_ORDER = ["ruby", "diamond", "gold", "silver", "bronze"]
TIER_RGB = {"ruby": (176, 28, 36), "diamond": (120, 175, 205), "gold": (214, 158, 30),
            "silver": (162, 168, 182), "bronze": (168, 104, 52)}  # bright, for the gem fills
TIER_HEX = {"ruby": HexColor("#a81c26"), "diamond": HexColor("#2f7fa6"),  # darker, readable on paper
            "gold": HexColor("#a9740f"), "silver": HexColor("#6c7484"), "bronze": HexColor("#93551f")}
TIER_LABEL = {"ruby": "RUBY — FRANCHISE FIRSTS", "diamond": "DIAMOND — ELITE CLUB",
              "gold": "GOLD", "silver": "SILVER", "bronze": "BRONZE"}
STAT_FRIENDLY = {"HR": "home runs", "Runs": "runs", "Games": "games", "Singles": "singles",
                 "RBI": "RBI", "AB": "at-bats", "Hits": "hits", "Doubles": "doubles",
                 "Triples": "triples", "Walks": "walks", "Total Bases": "total bases",
                 "PA": "plate appearances"}
_tmp = Path(tempfile.gettempdir()) / "mt_card_release"
_tmp.mkdir(exist_ok=True)


def card_png(asset, max_w=460):
    """webp -> rounded PNG path for reportlab."""
    dst = _tmp / f"{asset}.png"
    src = CARDS_DIR / f"{asset}.webp"
    img = Image.open(src).convert("RGBA")
    if img.width > max_w:
        img = img.resize((max_w, round(img.height * max_w / img.width)), Image.LANCZOS)
    mask = Image.new("L", img.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, img.size[0] - 1, img.size[1] - 1], radius=14, fill=255)
    img.putalpha(mask)
    img.save(dst)
    return str(dst), img.size


def draw_gem(c, cx, cy, r, tier, value):
    base = TIER_RGB[tier]
    c.setFillColorRGB(*[x / 255 for x in base])
    c.setStrokeColor(WHITE)
    c.setLineWidth(1.2)
    c.saveState()
    p = c.beginPath()
    p.moveTo(cx, cy + r); p.lineTo(cx + r, cy); p.lineTo(cx, cy - r); p.lineTo(cx - r, cy); p.close()
    c.drawPath(p, fill=1, stroke=1)
    c.restoreState()
    _txt(c, cx, cy - r * 0.32, str(value), "Helvetica-Bold", r * 0.95, WHITE, align="c")


def main():
    ap = argparse.ArgumentParser(description="Weekly card-release newsletter PDF")
    ap.add_argument("--date", help="game date YYYY-MM-DD — auto-selects that night's milestone cards")
    ap.add_argument("--assets", help="explicit comma-separated card assets (adds to/overrides --date)")
    ap.add_argument("--out")
    args = ap.parse_args()

    cards = {c["asset"]: c for c in load("cards.json")}
    milestones = load("milestones.json")
    meta = load("meta.json")

    picked = []
    if args.date:
        reached = [(e["slug"], e["stat"], e["milestone"]) for e in milestones["recent"]
                   if e["date"] == args.date]
        for a, c in cards.items():
            if c["kind"] == "milestone" and (c["slug"], c["stat"], c["value"]) in reached:
                picked.append(a)
    if args.assets:
        for a in args.assets.split(","):
            if a.strip() and a.strip() not in picked:
                picked.append(a.strip())
    if not picked:
        raise SystemExit("No cards selected — pass --date and/or --assets.")

    drop = sorted((cards[a] for a in picked), key=lambda c: -(c.get("rating") or 0))
    for c in drop:
        c["tier"] = gem_tier_for(c.get("rating") or 0)
    tier_counts = {t: sum(1 for c in drop if c["tier"] == t) for t in TIER_ORDER}
    tier_counts = {t: n for t, n in tier_counts.items() if n}
    date_pretty = (datetime.fromisoformat(args.date).strftime("%B %d, %Y") if args.date
                   else datetime.now().strftime("%B %d, %Y"))
    week = ""
    if args.date:
        sched = load("schedule.json")
        for s in sched:
            for g in s["games"]:
                if g.get("game_date") == args.date and g.get("week_label"):
                    week = g["week_label"]
                    break

    logo = prep_logo()
    season_slug = meta["current_season"]["slug"]
    out = Path(args.out) if args.out else (
        REPO / "data" / "writeups" / f"maple-tree-{season_slug}"
        / f"maple-tree-card-release-{args.date or 'latest'}.pdf")
    out.parent.mkdir(parents=True, exist_ok=True)
    c = pdfcanvas.Canvas(str(out), pagesize=letter)
    c.setTitle(f"Maple Tree Tap — Card Release {date_pretty}")

    def footer(pg):
        c.setStrokeColor(LINE); c.setLineWidth(0.75); c.line(36, 46, W - 36, 46)
        _txt(c, 36, 34, "THE MAPLE TREE TAP  ·  CARD RELEASE", "Helvetica-Bold", 7.5, BARK, cs=1)
        _txt(c, W / 2, 34, str(pg), "Helvetica", 8, MUTED, align="c")
        _txt(c, W - 36, 34, "mapletreesoftball.netlify.app/cards", "Helvetica", 8, MUTED, align="r")

    def masthead(title_lines, sub):
        c.setFillColor(PAPER); c.rect(0, 0, W, H, stroke=0, fill=1)
        c.setFillColor(BARK); c.rect(0, H - 150, W, 150, stroke=0, fill=1)
        c.setFillColorRGB(*[x / 255 for x in GOLD]); c.rect(0, H - 153, W, 3, stroke=0, fill=1)
        c.drawImage(logo, 40, H - 128, width=104, height=104, mask="auto")
        _txt(c, 168, H - 52, "THE MAPLE TREE TAP  ·  OFFICIAL CARD RELEASE", "Helvetica-Bold", 9.5, TAN, cs=2.5)
        yy = H - 88
        for t in title_lines:
            _txt(c, 168, yy, t, "Helvetica-Bold", 27, WHITE, cs=0.5); yy -= 30
        _txt(c, 168, H - 140, sub, "Helvetica", 9.5, TAN)

    # ===================== PAGE 1 — masthead + editorial + headliners =====================
    n = len(drop)
    breakdown = "  ·  ".join(f"{v} {TIER_LABEL[t].split(' ')[0].lower()}" for t, v in tier_counts.items())
    masthead([f"{week.upper()+' ' if week else ''}MILESTONE DROP".strip(),
              f"{n} NEW CARDS"],
             f"{date_pretty}  ·  {breakdown}")

    # editorial lead
    rubies = [c_ for c_ in drop if c_["tier"] == "ruby"]
    firsts = [c_ for c_ in drop if "1st player" in (c_.get("caption") or "").lower()]
    ry = H - 178
    _txt(c, 36, ry, "FROM THE FRONT OFFICE", "Helvetica-Bold", 8.5, MAPLE, cs=2)
    lead = (
        f"The record book took a beating this week. A single game night produced {n} career "
        f"milestones worthy of cardboard — {', '.join(TIER_LABEL[t].split(' — ')[0].lower()+'s: '+str(v) for t,v in tier_counts.items())}. "
        + (f"Headlining the drop, {rubies[0]['player']} becomes the first player in franchise history "
           f"to reach {rubies[0]['value']} {STAT_FRIENDLY.get(rubies[0]['stat'], rubies[0]['stat'].lower())} "
           f"— the rarest pull in the set and an automatic {rubies[0]['rating']} overall. " if rubies else "")
        + "Every card is minted from the player's real face and this season's real numbers. "
        "Ratings are earned, not given: 99s are reserved for franchise firsts, and the gem steps "
        "down with each name already in the club. Chase accordingly.")
    ny = wrap(c, 36, ry - 16, lead, W - 72, "Helvetica", 9.5, 13, INK)

    # headliners: up to 2 top cards, large, side by side
    heads = drop[:2] if len(drop) >= 2 else drop
    _txt(c, 36, ny - 8, "THE HEADLINERS", "Helvetica-Bold", 11, BARK, cs=1)
    c.setStrokeColor(BARK2); c.setLineWidth(1.5); c.line(36, ny - 14, W - 36, ny - 14)
    hy = ny - 26
    cw = 176
    xs = [70, W / 2 + 20]
    for x0, card in zip(xs, heads):
        path, (iw, ih) = card_png(card["asset"])
        dh = 250; dw = dh * iw / ih
        c.drawImage(path, x0, hy - dh, width=dw, height=dh, mask="auto")
        tx = x0 + dw + 16
        tw = (W - 36) - tx if x0 == xs[1] else (xs[1] - 10) - tx
        draw_gem(c, tx + 16, hy - 20, 16, card["tier"], card["rating"])
        _txt(c, tx + 40, hy - 25, TIER_LABEL[card["tier"]].split(" — ")[0], "Helvetica-Bold", 9,
             TIER_HEX[card["tier"]], cs=1)
        _txt(c, tx, hy - 52, card["player"], "Helvetica-Bold", 16, BARK)
        _txt(c, tx, hy - 70, f"{card['value']} {card['stat']}", "Helvetica-Bold", 11, MAPLE)
        yy2 = wrap(c, tx, hy - 86, card.get("caption", ""), tw, "Helvetica-Oblique", 8.5, 11, MUTED)
        wrap(c, tx, yy2 - 6, card.get("flavor", ""), tw, "Helvetica", 8.7, 11.5, INK)
    footer(1)
    c.showPage()

    # ===================== PAGES 2+ — the full set, grouped by tier =====================
    def new_page():
        c.setFillColor(PAPER); c.rect(0, 0, W, H, stroke=0, fill=1)
        c.setFillColor(BARK); c.rect(0, H - 60, W, 60, stroke=0, fill=1)
        _txt(c, 36, H - 30, "THE COMPLETE SET", "Helvetica-Bold", 15, WHITE, cs=1)
        _txt(c, W - 36, H - 30, f"{week+' · ' if week else ''}{date_pretty}", "Helvetica-Oblique", 9, TAN, align="r")

    rest = drop[2:] if len(drop) >= 2 else []
    new_page()
    y = H - 92
    pg = 2
    ROW_H = 150
    last_tier = None
    for card in rest:
        if card["tier"] != last_tier:
            if y < 120 + 26:
                footer(pg); c.showPage(); pg += 1; new_page(); y = H - 92
            col = TIER_HEX[card["tier"]]
            _txt(c, 36, y, TIER_LABEL[card["tier"]], "Helvetica-Bold", 11, col, cs=1.5)
            c.setStrokeColor(col); c.setLineWidth(1.2); c.line(36, y - 6, W - 36, y - 6)
            y -= 24
            last_tier = card["tier"]
        if y - ROW_H < 60:
            footer(pg); c.showPage(); pg += 1; new_page(); y = H - 92
            _txt(c, 36, y, TIER_LABEL[card["tier"]] + " (cont.)", "Helvetica-Bold", 9, TIER_HEX[card["tier"]], cs=1)
            y -= 18
        # card thumb
        path, (iw, ih) = card_png(card["asset"], 300)
        dh = ROW_H - 12; dw = dh * iw / ih
        c.drawImage(path, 44, y - dh, width=dw, height=dh, mask="auto")
        tx = 44 + dw + 18
        draw_gem(c, tx + 13, y - 14, 13, card["tier"], card["rating"])
        _txt(c, tx + 34, y - 18, card["player"].upper(), "Helvetica-Bold", 14, BARK)
        _txt(c, tx, y - 40, f"{card['value']} {card['stat']}   ·   {card.get('caption','')}",
             "Helvetica-Bold", 9.5, MAPLE)
        by = wrap(c, tx, y - 56, card.get("flavor", ""), W - 36 - tx, "Helvetica", 8.8, 11.5, INK)
        if card.get("game_text"):
            wrap(c, tx, by - 4, card["game_text"], W - 36 - tx, "Helvetica-Oblique", 8, 10.5, MUTED)
        y -= ROW_H

    footer(pg)
    c.showPage()
    c.save()
    print(f"\nCard Release -> {out}")
    print(f"  {n} cards · {tier_counts} · {date_pretty}")


if __name__ == "__main__":
    main()
