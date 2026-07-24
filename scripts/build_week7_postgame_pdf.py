"""Render the Week 7 postgame recap as a clean, shareable PDF."""
from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Flowable, ListFlowable, ListItem, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

REPO = Path(r"C:\Slowpitch\slowpitch_optimizer")

INK = colors.HexColor("#334155")
LINE = colors.HexColor("#94a3b8")
HL = colors.HexColor("#2563eb")
HL_FILL = colors.HexColor("#dbeafe")
BOX_FILL = colors.HexColor("#f1f5f9")


class Bracket(Flowable):
    """Single-elimination playoff bracket, drawn to match a printed bracket sheet."""

    def __init__(self, width=6.9 * inch, height=3.5 * inch):
        super().__init__()
        self.width = width
        self.height = height

    def wrap(self, *args):
        return self.width, self.height

    def _box(self, c, x, y, label, w=118, h=22, highlight=False):
        c.saveState()
        c.setLineWidth(1.1 if highlight else 0.8)
        c.setStrokeColor(HL if highlight else LINE)
        c.setFillColor(HL_FILL if highlight else BOX_FILL)
        c.roundRect(x, y - h / 2, w, h, 3, stroke=1, fill=1)
        c.setFillColor(HL if highlight else INK)
        c.setFont("Helvetica-Bold" if highlight else "Helvetica", 8.5)
        c.drawString(x + 7, y - 3, label)
        c.restoreState()
        return x + w, y  # right-center anchor

    def _join(self, c, p1, p2, join_x, highlight=False):
        """Connect two feeder points into one output point at their vertical midpoint."""
        c.saveState()
        c.setLineWidth(1.2 if highlight else 0.9)
        c.setStrokeColor(HL if highlight else LINE)
        (x1, y1), (x2, y2) = p1, p2
        c.line(x1, y1, join_x, y1)
        c.line(x2, y2, join_x, y2)
        c.line(join_x, y1, join_x, y2)
        my = (y1 + y2) / 2.0
        c.restoreState()
        return join_x, my

    def _tag(self, c, x, y, text, highlight=False):
        c.saveState()
        c.setFillColor(HL if highlight else colors.HexColor("#64748b"))
        c.setFont("Helvetica-Oblique", 7)
        c.drawString(x + 3, y + 3, text)
        c.restoreState()

    def draw(self):
        c = self.canv
        H = self.height
        rows = [
            ("[1] No Dice", False),
            ("[4] Wasted Talent", False),
            ("[5] Soft Ballz", False),
            ("[2] cacheouts", False),
            ("[7] Wasted Potential", False),
            ("[3] Bullseyes", False),
            ("[6] Maple Tree", True),
        ]
        top = H - 14
        gap = (H - 26) / 6.0
        ys = [top - i * gap for i in range(7)]

        # seed boxes
        anchors = []
        for (label, hl), y in zip(rows, ys):
            anchors.append(self._box(c, 0, y, label, highlight=hl))
        a, b, cc, d, e, f, g = anchors

        # round 1 (G1 4v5, G2 2v7, G3 6v3 -> Maple Tree path highlighted)
        g1 = self._join(c, b, cc, 158)
        g2 = self._join(c, d, e, 158)
        g3 = self._join(c, f, g, 158, highlight=True)
        self._tag(c, 158, (b[1] + cc[1]) / 2, "G1 · 6:30")
        self._tag(c, 158, (d[1] + e[1]) / 2, "G2 · 7:30")
        self._tag(c, 158, (f[1] + g[1]) / 2, "G3 · 7:30 Red", highlight=True)

        # stubs out of round-1 joins
        c.setStrokeColor(LINE); c.setLineWidth(0.9)
        c.line(*g1, 182, g1[1]); c.line(*g2, 182, g2[1])
        c.setStrokeColor(HL); c.setLineWidth(1.2); c.line(*g3, 182, g3[1])

        # round 2 (semfinals): G4 = No Dice vs G1 winner ; G5 = G2 winner vs G3 winner
        g4 = self._join(c, a, (182, g1[1]), 250)
        g5 = self._join(c, (182, g2[1]), (182, g3[1]), 250, highlight=True)
        self._tag(c, 250, (a[1] + g1[1]) / 2, "G4 · 8:30")
        self._tag(c, 250, (g2[1] + g3[1]) / 2, "G5 · 8:30 Red", highlight=True)
        c.setStrokeColor(LINE); c.setLineWidth(0.9); c.line(*g4, 286, g4[1])
        c.setStrokeColor(HL); c.setLineWidth(1.2); c.line(*g5, 286, g5[1])

        # final: G6
        g6 = self._join(c, (286, g4[1]), (286, g5[1]), 348)
        self._tag(c, 348, (g4[1] + g5[1]) / 2, "G6 FINAL · 9:30")
        c.setStrokeColor(INK); c.setLineWidth(1.0); c.line(*g6, 430, g6[1])
        # champion box
        c.setStrokeColor(colors.HexColor("#ca8a04")); c.setFillColor(colors.HexColor("#fef9c3"))
        c.setLineWidth(1.0)
        c.roundRect(430, g6[1] - 13, 64, 26, 3, stroke=1, fill=1)
        c.setFillColor(colors.HexColor("#854d0e")); c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(462, g6[1] + 1, "LEAGUE")
        c.drawCentredString(462, g6[1] - 8, "CHAMPION")


def main() -> None:
    out = REPO / "data/writeups/maple-tree-spring-2026/week-7-postgame.pdf"
    out.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(out), pagesize=LETTER,
        leftMargin=0.8*inch, rightMargin=0.8*inch, topMargin=0.8*inch, bottomMargin=0.8*inch,
        title="Maple Tree — Week 7 Postgame Recap", author="Maple Tree Dashboard",
    )
    s = getSampleStyleSheet()
    title = ParagraphStyle("T", parent=s["Title"], fontSize=22, leading=26, spaceAfter=4, alignment=0)
    sub = ParagraphStyle("Sub", parent=s["Italic"], fontSize=10.5, leading=14, textColor=colors.HexColor("#475569"), spaceAfter=12)
    h1 = ParagraphStyle("H1", parent=s["Heading1"], fontSize=14, leading=18, spaceBefore=13, spaceAfter=6, textColor=colors.HexColor("#0f172a"))
    body = ParagraphStyle("B", parent=s["BodyText"], fontSize=10.5, leading=14.5, spaceAfter=8)
    bullet = ParagraphStyle("Bul", parent=body, leftIndent=16, bulletIndent=5, spaceAfter=4)
    callout = ParagraphStyle("C", parent=body, leftIndent=12, rightIndent=12, backColor=colors.HexColor("#f1f5f9"),
                             borderPadding=10, spaceAfter=10, textColor=colors.HexColor("#0f172a"))
    rec = ParagraphStyle("Rec", parent=body, leftIndent=12, rightIndent=12, backColor=colors.HexColor("#fef9c3"),
                         borderPadding=10, spaceAfter=10, textColor=colors.HexColor("#0f172a"))
    cap = ParagraphStyle("Cap", parent=body, fontSize=9, textColor=colors.HexColor("#475569"), spaceAfter=12)

    story: list = []
    story.append(Paragraph("Week 7 Postgame Recap", title))
    story.append(Paragraph("Regular-season finale — Maple Tree vs No Dice — Wed, June 3, 2026 — Boncosky Yellow", sub))

    story.append(Paragraph(
        "Week 7 was the regular-season finale, and Maple Tree closed the book the way most of the season's "
        "chapters read: a competitive game it could not quite finish, followed by a game that finished it. "
        "No Dice swept the doubleheader 11-9 and 17-5, the regular season ends at 2-10, and the franchise is "
        "now 0-10 all-time against No Dice. But here is the part that matters: <b>the regular season is over, "
        "and the playoffs do not care about it.</b> Maple Tree is in. Single elimination. Records reset to "
        "zero. And the Round 1 opponent is not the team that just swept this roster — it is a Bullseyes club "
        "Maple Tree already beat once this spring.",
        body,
    ))

    story.append(Paragraph("Week 7 Scoreboard", h1))
    sb = [["Game", "Time", "Result", "MT", "No Dice", "Notes"],
          ["1", "6:30 PM", "L", "9", "11", "Closest game ever vs No Dice"],
          ["2", "7:30 PM", "L", "5", "17", "16-2 No Dice over the last 4 innings"]]
    story.append(_tbl(sb, [0.5*inch, 0.8*inch, 0.6*inch, 0.5*inch, 0.85*inch, 2.7*inch]))
    story.append(Spacer(1, 6))
    story.append(ListFlowable([
        ListItem(Paragraph("Team line: 14 runs, 29 hits, 2 home runs across 68 plate appearances", bullet)),
        ListItem(Paragraph("All-time vs No Dice after Week 7: <b>0-10</b>", bullet)),
    ], bulletType="bullet", start="•"))

    story.append(Paragraph("Game 1 Recap", h1))
    story.append(Paragraph(
        "The opener was the most genuinely competitive game Maple Tree has played against No Dice in franchise "
        "history. Maple Tree put up five in the first, traded the rest of the way, finished with 18 hits and 9 "
        "runs, and lost by two — an 11-9 final that, against this opponent, qualifies as a moral near-miss. The "
        "team had never previously come within eight runs of No Dice; this time it came within two. Glove "
        "(3-for-4), JJ (3-for-3, double), and Tristan (3-for-4) all had three-hit games, and Tim added a double "
        "and three RBI. The margin came down to the usual culprit — No Dice scored in five of their first six "
        "innings and Maple Tree never got the one clean defensive frame that flips a two-run loss into a win.",
        body,
    ))

    story.append(Paragraph("Game 2 Recap", h1))
    story.append(Paragraph(
        "The nightcap reverted to form. No Dice scored four in the fourth, four in the fifth, four in the sixth, "
        "and four in the seventh — sixteen in the back half — and won 17-5. Maple Tree managed 11 hits and five "
        "runs and was outscored 16-2 over the final four innings. The bright spots were brief: Kives went "
        "2-for-3 with a home run and <b>four RBI</b>, Tim added two more doubles to cap a huge night, Tristan "
        "hit his 11th homer, and Glove tripled. Enough offense to be respectable, not enough run prevention to "
        "be competitive — the season in miniature.",
        body,
    ))

    story.append(Paragraph("Week MVP", h1))
    story.append(Paragraph(
        "<b>Tim is the Week 7 MVP, and it is not close.</b> Across the doubleheader he went <b>5-for-6 with "
        "three doubles, eight total bases, three runs, and three RBI</b> — the loudest two-game line on the team "
        "and a fitting cap to a strong second half. Kives earns runner-up for the four-RBI nightcap.",
        body,
    ))

    story.append(Paragraph("Records and Milestones", h1))
    story.append(Paragraph(
        "<b>Tim broke the single-season doubles record.</b> His three doubles in Week 7 pushed him to "
        "<b>10 doubles on the season</b>, surpassing the previous franchise mark of 8 (shared by Tristan in "
        "2021 &amp; 2022 and Porter in 2021). It is the only single-season counting record currently held by a "
        "2026 hitter — and because the team counts playoff games toward records, <b>the mark is not final.</b> "
        "Tim can keep padding it in the bracket.",
        rec,
    ))
    story.append(Paragraph("The regular season's final milestone ledger:", body))
    story.append(ListFlowable([
        ListItem(Paragraph("<b>Tim</b> crossed <b>50 career runs</b> (and set the new single-season doubles record).", bullet)),
        ListItem(Paragraph("<b>Kives</b> crossed <b>200 career plate appearances</b> (now 204).", bullet)),
        ListItem(Paragraph("<b>JJ</b> crossed <b>250 career plate appearances</b> (now 251).", bullet)),
        ListItem(Paragraph("<b>Corey</b> crossed <b>200 career at bats</b>.", bullet)),
        ListItem(Paragraph("<b>Jason</b> crossed <b>150 career at bats</b>.", bullet)),
        ListItem(Paragraph("<b>Joel</b> and <b>Joey</b> both crossed <b>100 career plate appearances</b>.", bullet)),
    ], bulletType="bullet", start="•"))
    story.append(Paragraph(
        "Because playoff games count, none of the single-season leaderboards are locked. The closest remaining "
        "chases: Tristan is four home runs shy of his own 15-HR single-season record, and JJ is four walks shy "
        "of the single-season walk record — long shots in a single-elimination run, but live on paper.",
        body,
    ))

    story.append(Paragraph("Final Regular-Season Standings (= Playoff Seeds)", h1))
    st = [["Seed", "Team", "W", "L"],
          ["1", "No Dice", "11", "1"], ["2", "cacheouts", "9", "3"], ["3", "Bullseyes", "7", "5"],
          ["4", "Wasted Talent", "6", "6"], ["5", "Soft Ballz", "5", "7"],
          ["6", "Maple Tree", "2", "10"], ["7", "Wasted Potential", "2", "10"]]
    story.append(_tbl(st, [0.6*inch, 2.0*inch, 0.6*inch, 0.6*inch], highlight_row=6))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "Maple Tree backed into the 6 seed over Wasted Potential on the tiebreak — both finished 2-10. Around "
        "the league: cacheouts hammered Soft Ballz twice (23-6, 17-2) to lock the 2 seed, Bullseyes and Wasted "
        "Talent split, and No Dice finished 11-1 as the runaway top seed.",
        body,
    ))

    story.append(PageBreak())
    story.append(Paragraph("The Playoff Bracket", h1))
    story.append(Paragraph("Single elimination — all games Wednesday, June 10 at Boncosky. Maple Tree's path is highlighted in blue.", body))
    story.append(Spacer(1, 4))
    story.append(Bracket())
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "<b>Maple Tree's path:</b> Round 1 is <b>Game 3 — #6 Maple Tree vs #3 Bullseyes, 7:30 PM at Boncosky "
        "Red.</b> Win, and the team advances to <b>Game 5 (8:30, Red)</b> against the winner of cacheouts vs "
        "Wasted Potential. Win that, and it is the <b>Game 6 final at 9:30.</b>",
        callout,
    ))

    story.append(Paragraph("Why This Is Not No Dice", h1))
    story.append(Paragraph(
        "Here is the encouraging part. Maple Tree just went 0-2 against a No Dice team it has never beaten in 10 "
        "tries. The Round 1 opponent is <b>not that team.</b> Maple Tree split with <b>Bullseyes</b> this season "
        "— winning the 25-24 Week 2 slugfest and losing the other meeting 15-5. Bullseyes finished 7-5, a "
        "good-but-beatable club, and exactly the kind of opponent the season-long data says Maple Tree can "
        "outscore on a good offensive night. The team's whole problem all year was the back half of "
        "doubleheaders and run prevention. The playoffs are <b>single games</b> — no nightcap collapse to worry "
        "about. The analytics said it months ago: this team wins when it scores 15. Against Bullseyes, that "
        "number is reachable. Against No Dice it never was.",
        body,
    ))

    story.append(Paragraph("Final Regular-Season Team Stats", h1))
    story.append(Paragraph(
        "Through 12 games, Maple Tree scored 140 runs and allowed 204 — <b>11.7 scored, 17.0 allowed per "
        "game</b> — and posted a .489/.518/.811 slash for a <b>1.329 team OPS</b> with 29 home runs. The story "
        "never changed: a top-of-the-division offense dragged underwater by bottom-of-the-division run "
        "prevention. The 2-10 record reflects the defense, not the bats. The bats are why there is still a "
        "season to play.",
        body,
    ))

    story.append(PageBreak())
    story.append(Paragraph("Final Spring 2026 Player Stats", h1))
    story.append(Paragraph("Sorted by OPS. Team line in bold. Records and stats remain open — playoff games count.", cap))
    ps = [
        ["#", "Player", "G", "PA", "AB", "H", "HR", "R", "RBI", "BB", "TB", "AVG", "OBP", "SLG", "OPS", "wRC+"],
        ["1", "Tristan", "12", "41", "37", "24", "11", "23", "26", "3", "59", ".649", ".675", "1.595", "2.270", "167"],
        ["2", "Tim", "12", "41", "39", "24", "4", "16", "18", "1", "46", ".615", ".625", "1.179", "1.804", "137"],
        ["3", "Glove", "12", "40", "39", "23", "3", "18", "21", "1", "41", ".590", ".600", "1.051", "1.651", "133"],
        ["4", "Kives", "12", "40", "40", "21", "5", "16", "22", "0", "43", ".525", ".525", "1.075", "1.600", "126"],
        ["5", "JJ", "12", "42", "37", "18", "2", "16", "6", "5", "29", ".486", ".548", ".784", "1.331", "98"],
        ["6", "Joel", "10", "30", "25", "11", "3", "11", "12", "4", "20", ".440", ".517", ".800", "1.317", "91"],
        ["7", "Corey", "12", "34", "31", "15", "0", "8", "7", "3", "20", ".484", ".529", ".645", "1.175", "91"],
        ["8", "Slomka", "6", "18", "18", "9", "0", "6", "3", "0", "10", ".500", ".500", ".556", "1.056", "85"],
        ["9", "Porter", "8", "25", "24", "12", "0", "5", "7", "1", "12", ".500", ".520", ".500", "1.020", "72"],
        ["10", "Walsh", "12", "35", "33", "14", "0", "5", "10", "2", "16", ".424", ".457", ".485", ".942", "70"],
        ["11", "Duff", "12", "34", "30", "10", "1", "8", "5", "4", "14", ".333", ".412", ".467", ".878", "65"],
        ["12", "Jason", "9", "25", "25", "9", "0", "7", "3", "0", "9", ".360", ".360", ".360", ".720", "52"],
        ["13", "Joey", "10", "24", "23", "6", "0", "1", "4", "0", "6", ".261", ".261", ".261", ".522", "35"],
        ["", "Team", "12", "429", "401", "196", "29", "140", "144", "24", "325", ".489", ".518", ".811", "1.329", "100"],
    ]
    widths = [0.24*inch, 0.78*inch, 0.27*inch, 0.36*inch, 0.36*inch, 0.30*inch, 0.33*inch, 0.30*inch,
              0.37*inch, 0.30*inch, 0.34*inch, 0.42*inch, 0.42*inch, 0.47*inch, 0.47*inch, 0.42*inch]
    story.append(_stats_tbl(ps, widths))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "wRC+ is indexed so 100 = team average; it weights how a hitter actually produces runs (linear "
        "weights), so it reorders the card slightly versus OPS. JJ's elite on-base but lighter slug lands him "
        "right at team-average run creation; Tristan's 167 means he created 67% more runs per plate appearance "
        "than the team norm.",
        cap,
    ))

    doc.build(story)
    print(f"wrote {out}")


def _tbl(rows, widths, highlight_row=None):
    t = Table(rows, colWidths=widths, repeatRows=1)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (2, 1), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.HexColor("#0f172a")),
    ])
    if highlight_row is not None:
        style.add("BACKGROUND", (0, highlight_row), (-1, highlight_row), colors.HexColor("#dbeafe"))
        style.add("FONTNAME", (0, highlight_row), (-1, highlight_row), "Helvetica-Bold")
    t.setStyle(style)
    return t


def _stats_tbl(rows, widths):
    t = Table(rows, colWidths=widths, repeatRows=1)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (1, 0), (1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#f8fafc")]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.HexColor("#0f172a")),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#fef3c7")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("LINEABOVE", (0, -1), (-1, -1), 1.0, colors.HexColor("#0f172a")),
    ])
    style.add("FONTNAME", (-1, 1), (-1, 1), "Helvetica-Bold")
    style.add("BACKGROUND", (-1, 1), (-1, 1), colors.HexColor("#dcfce7"))
    t.setStyle(style)
    return t


if __name__ == "__main__":
    main()
