"""One-page 'Mascot Vote' ballot PDF — all six concepts, checkbox each, team decides.

Reads the rendered concept previews from C:/Slowpitch/Mascot Concepts and lays them
out in the club's house style. Output lands next to the concepts.
"""
from __future__ import annotations

import sys
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_gameday_preview import (  # noqa: E402
    BARK, BARK2, CREAM, INK, LINE, MAPLE, MUTED, PAPER, SAND, TAN, WHITE, _txt, prep_logo,
)

W, H = letter
SRC = Path("C:/Slowpitch/Mascot Concepts")
OUT = SRC / "maple-tree-mascot-vote.pdf"

CONCEPTS = [
    ("01", "THE TAPPER", "The bar's namesake, armed. A mascot with main-character energy."),
    ("02", "THE FIERCE LEAF", "Cap-logo menace. Softball stitching included."),
    ("03", "THE RETRO BADGE", "Vintage taproom emblem — crossed bats, old soul."),
    ("04", "THE BIG SWING", "Full send. The leaves fly, the ball leaves."),
    ("05", "THE DIAMOND LEAF", "Minimalist — the ballfield lives inside the leaf."),
    ("06", "THE NAVY LEAF", "The trading-card colorway, for the traditionalists."),
]
FILES = {n: SRC / f"{n}-{slug}-preview.png" for n, slug in
         [("01", "the-tapper"), ("02", "fierce-leaf"), ("03", "retro-badge"),
          ("04", "swinging-mascot"), ("05", "minimal-diamond"), ("06", "navy-leaf")]}


def main():
    c = pdfcanvas.Canvas(str(OUT), pagesize=letter)
    c.setTitle("Maple Tree - The Mascot Vote")

    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 84, W, 84, stroke=0, fill=1)
    c.drawImage(prep_logo(), 36, H - 76, width=66, height=66, mask="auto")
    _txt(c, 116, H - 34, "MAPLE TREE SOFTBALL  ·  OFFICIAL TEAM BALLOT", "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, 116, H - 62, "THE MASCOT VOTE", "Helvetica-Bold", 27, WHITE, cs=1)
    _txt(c, W - 36, H - 34, "one vote per Tapper", "Helvetica-Oblique", 9, TAN, align="r")
    _txt(c, W - 36, H - 56, "ties settled by career batting average", "Helvetica-Oblique", 8, TAN, align="r")

    cell_w, cell_h, gap = 262, 188, 12
    x0s = [36, 36 + cell_w + gap]
    top = H - 98
    for i, (num, name, note) in enumerate(CONCEPTS):
        col, row = i % 2, i // 2
        x = x0s[col]
        y = top - (row + 1) * cell_h - row * gap
        c.setFillColor(WHITE)
        c.setStrokeColor(LINE)
        c.setLineWidth(1)
        c.roundRect(x, y, cell_w, cell_h, 8, stroke=1, fill=1)
        img = FILES[num]
        art = 134
        c.drawImage(str(img), x + (cell_w - art) / 2, y + cell_h - art - 8, width=art, height=art, mask="auto")
        # number badge + checkbox
        c.setFillColor(MAPLE)
        c.circle(x + 20, y + cell_h - 20, 12, stroke=0, fill=1)
        _txt(c, x + 20, y + cell_h - 23.5, num, "Helvetica-Bold", 10, WHITE, align="c")
        c.setFillColor(WHITE)
        c.setStrokeColor(BARK2)
        c.setLineWidth(1.6)
        c.rect(x + cell_w - 34, y + cell_h - 32, 18, 18, stroke=1, fill=1)
        _txt(c, x + 14, y + 28, name, "Helvetica-Bold", 11, BARK)
        _txt(c, x + 14, y + 15, note, "Helvetica", 7.6, MUTED)

    c.setFillColor(SAND)
    c.roundRect(36, 64, W - 72, 30, 6, stroke=0, fill=1)
    _txt(c, 48, 75, "WRITE-IN / REMIX IDEAS:", "Helvetica-Bold", 8, BARK, cs=1)
    c.setStrokeColor(MUTED)
    c.setLineWidth(0.7)
    c.line(175, 72, W - 48, 72)
    _txt(c, 36, 50, "MAPLE TREE SOFTBALL  ·  BRAND DEPARTMENT", "Helvetica-Bold", 8, BARK, cs=1)
    _txt(c, W - 36, 50, "all marks delivered as print-ready vectors  ·  The Maple Tree Tap - Cary, Illinois",
         "Helvetica", 7.5, MUTED, align="r")
    c.showPage()
    c.save()
    print(f"ballot -> {OUT}")


if __name__ == "__main__":
    main()
