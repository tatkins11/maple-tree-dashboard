"""Render the Playoff Round 1 pregame write-up as a clean, shareable PDF."""
from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    ListFlowable, ListItem, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

REPO = Path(r"C:\Slowpitch\slowpitch_optimizer")


def main() -> None:
    out = REPO / "data/writeups/maple-tree-spring-2026/playoff-round-1-pregame.pdf"
    out.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(out), pagesize=LETTER,
        leftMargin=0.8*inch, rightMargin=0.8*inch, topMargin=0.8*inch, bottomMargin=0.8*inch,
        title="Maple Tree — Playoff Round 1 Pregame", author="Maple Tree Dashboard",
    )
    s = getSampleStyleSheet()
    title = ParagraphStyle("T", parent=s["Title"], fontSize=21, leading=25, spaceAfter=4, alignment=0)
    sub = ParagraphStyle("Sub", parent=s["Italic"], fontSize=10.5, leading=14, textColor=colors.HexColor("#475569"), spaceAfter=12)
    h1 = ParagraphStyle("H1", parent=s["Heading1"], fontSize=14, leading=18, spaceBefore=13, spaceAfter=6, textColor=colors.HexColor("#0f172a"))
    body = ParagraphStyle("B", parent=s["BodyText"], fontSize=10.5, leading=14.5, spaceAfter=8)
    bullet = ParagraphStyle("Bul", parent=body, leftIndent=16, bulletIndent=5, spaceAfter=4)
    callout = ParagraphStyle("C", parent=body, leftIndent=12, rightIndent=12, backColor=colors.HexColor("#f1f5f9"),
                             borderPadding=10, spaceAfter=10, textColor=colors.HexColor("#0f172a"))
    rec = ParagraphStyle("Rec", parent=body, leftIndent=12, rightIndent=12, backColor=colors.HexColor("#fef9c3"),
                         borderPadding=10, spaceAfter=10, textColor=colors.HexColor("#0f172a"))
    cap = ParagraphStyle("Cap", parent=body, fontSize=9, textColor=colors.HexColor("#475569"), spaceAfter=10)

    story: list = []
    story.append(Paragraph("Playoff Pregame: #6 Maple Tree vs #3 Bullseyes", title))
    story.append(Paragraph("Round 1 · Single elimination · Wednesday, June 17, 2026 · 7:30 PM · Boncosky Red (rescheduled from June 10 rainout)", sub))

    story.append(Paragraph(
        "The regular season was a 2-10 grind that bought exactly one thing: a ticket to a single-elimination "
        "tournament where the slate is clean and the matchup is winnable. Tonight that ticket gets cashed. Of "
        "all the teams left in this bracket, <b>Bullseyes is the one the data says you can beat</b> — the "
        "franchise owns them, and this lineup is built to exploit them. Win and the season gets another chapter "
        "at 8:30. Lose and it's over. One game, one good offensive night, and the 2-10 record becomes a footnote.",
        body,
    ))

    story.append(Paragraph("Game Setup", h1))
    story.append(ListFlowable([
        ListItem(Paragraph("<b>7:30 PM — Boncosky Red</b> (Game 3 of the bracket)", bullet)),
        ListItem(Paragraph("Single elimination — no nightcap, no second game to give it back in", bullet)),
        ListItem(Paragraph("Opponent: <b>Bullseyes</b> (#3 seed, 7-5 regular season)", bullet)),
        ListItem(Paragraph("Win → <b>Game 5, 8:30 PM, Boncosky Red</b> vs the winner of #2 cacheouts and #7 Wasted Potential", bullet)),
        ListItem(Paragraph("11 available tonight — Duff and Slomka are off the card", bullet)),
    ], bulletType="bullet", start="•"))

    story.append(Paragraph("Tonight's Lineup", h1))
    story.append(Paragraph(
        "This order is built for <i>this</i> opponent. The four highest career marks against Bullseyes are "
        "stacked one through four. Numbers below are <b>career vs Bullseyes</b> — the only sample that matters "
        "tonight.",
        body,
    ))
    lineup = [
        ["#", "Player", "AVG", "OBP", "SLG", "HR", "RBI", "OPS"],
        ["1", "Glove", ".658", ".675", ".974", "1", "16", "1.649"],
        ["2", "Tristan", ".639", ".675", "1.833", "14", "36", "2.508"],
        ["3", "Tim", ".588", ".632", "1.706", "6", "15", "2.338"],
        ["4", "Kives", ".686", ".694", "1.286", "4", "22", "1.980"],
        ["5", "JJ", ".613", ".700", ".968", "2", "9", "1.668"],
        ["6", "Porter", ".594", ".629", ".906", "1", "7", "1.535"],
        ["7", "Joel", ".292", ".346", ".542", "2", "10", ".888"],
        ["8", "Corey", ".300", ".382", ".367", "0", "6", ".749"],
        ["9", "Walsh", ".500", ".667", ".500", "0", "1", "1.167"],
        ["10", "Snaxx", ".526", ".550", ".632", "0", "6", "1.182"],
        ["11", "Jason", ".348", ".483", ".348", "0", "3", ".831"],
    ]
    widths = [0.4*inch, 1.4*inch, 0.75*inch, 0.75*inch, 0.8*inch, 0.55*inch, 0.6*inch, 0.8*inch]
    story.append(_lineup_table(lineup, widths, highlight_rows=(1, 2, 3, 4)))
    story.append(Paragraph("Slash line, HR, RBI, and OPS are career totals versus Bullseyes.", cap))
    story.append(Paragraph(
        "The construction is the whole point. The four highest career OPS marks against Bullseyes — Tristan "
        "(2.508), Tim (2.338), Kives (1.980), Glove (1.649) — hit one through four. JJ and his <b>.700 on-base</b> "
        "sit fifth. And the order correctly weighs <b>Porter's Bullseyes history (1.535) over Joel's better season "
        "number</b>, because against this team Joel is only .888. This is not a lineup sorted by season stats — "
        "it's sorted by who beats Bullseyes, and it gets that right top to bottom.",
        body,
    ))

    story.append(Spacer(1, 4))
    story.append(Paragraph("The Bullseyes Book", h1))
    story.append(Paragraph(
        "This is the encouraging part, and it isn't a pep talk — it's the record book. <b>The franchise is 6-4 "
        "all-time against Bullseyes with a +4 run differential</b> (150 scored, 146 allowed). They are one of the "
        "few opponents Maple Tree actually owns. You split with them this very spring — the 25-24 Week 2 slugfest "
        "and a 5-15 dud. This is not No Dice, who you've never beaten in ten tries. This is a beatable team you "
        "have already beaten.",
        body,
    ))
    story.append(Paragraph("Every Maple Tree meeting with Bullseyes the last two seasons:", body))
    sb = [
        ["Result", "Score", "Maple Tree runs"],
        ["W", "24-14", "24"],
        ["W", "16-11", "16"],
        ["W", "25-24", "25  (this April)"],
        ["L", "13-14", "13"],
        ["L", "10-15", "10"],
        ["L", "8-20", "8"],
        ["L", "5-15", "5  (this April)"],
    ]
    story.append(_result_table(sb, [1.0*inch, 1.2*inch, 1.8*inch]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "The pattern is almost insulting in its clarity: <b>score 16 or more and you beat Bullseyes; get held "
        "under 13 and you lose.</b> There is no in-between. The wins are 24, 16, 25. The losses are 13, 10, 8, 5. "
        "The whole game tonight is whether your bats clear a number you've cleared against this exact team three "
        "times already.",
        callout,
    ))

    story.append(Paragraph("Keys Tonight", h1))
    story.append(ListFlowable([
        ListItem(Paragraph("<b>Score 16.</b> Not a vibe — a line drawn by seven games of history against this exact opponent. Your top four can get there in two innings.", bullet)),
        ListItem(Paragraph("<b>Survive one crooked inning.</b> Every Bullseyes win over Maple Tree came on a single 14-, 15-, or 20-run rally. You don't need a shutout — you need to keep one inning from becoming the game.", bullet)),
        ListItem(Paragraph("<b>It's one game.</b> The season-long killer was the nightcap collapse. There is no nightcap tonight. Nothing to give back. Empty the tank.", bullet)),
    ], bulletType="bullet", start="•"))

    story.append(Paragraph("Records Live Tonight", h1))
    story.append(Paragraph(
        "The team counts playoff games toward records, so nothing is final and everything is in play:",
        body,
    ))
    story.append(Paragraph(
        "• <b>Tim</b> owns the single-season doubles record at 10 — every double tonight extends it.<br/>"
        "• <b>Tristan</b> is <b>four home runs</b> from his own 15-HR single-season record, and he's facing the "
        "team he has already hit <b>14 career home runs</b> against. A multi-homer night is on the table.<br/>"
        "• <b>JJ</b> is <b>four walks</b> from the single-season walk record, and he runs a <b>.700 OBP against "
        "Bullseyes</b> specifically. They give him free passes.",
        rec,
    ))

    story.append(Paragraph("The Stakes", h1))
    story.append(Paragraph(
        "Win and you're playing at 8:30 in Game 5, against the winner of #2 cacheouts and #7 Wasted Potential. "
        "Lose and a season that fought to a 2-10 finish ends tonight at Boncosky Red. That's the entire weight of "
        "it, and it sits on a number — 16 — that this lineup has cleared against this opponent before.",
        body,
    ))
    story.append(Paragraph(
        "You're the 6 seed. You're also the better matchup play, with the better history, and a lineup engineered "
        "to do the one thing that beats Bullseyes. Score 16. Weather one inning. Leave it all out there. Go write "
        "the next chapter. <b>Let's go.</b>",
        body,
    ))

    doc.build(story)
    print(f"wrote {out}")


def _lineup_table(rows, widths, highlight_rows=()):
    t = Table(rows, colWidths=widths, repeatRows=1)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (1, 0), (1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.HexColor("#0f172a")),
    ])
    for r in highlight_rows:
        style.add("BACKGROUND", (0, r), (-1, r), colors.HexColor("#dbeafe"))
        style.add("FONTNAME", (1, r), (1, r), "Helvetica-Bold")
    t.setStyle(style)
    return t


def _result_table(rows, widths):
    t = Table(rows, colWidths=widths, repeatRows=1)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.HexColor("#0f172a")),
    ])
    for i, row in enumerate(rows[1:], start=1):
        if row[0] == "W":
            style.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#dcfce7"))
            style.add("TEXTCOLOR", (0, i), (0, i), colors.HexColor("#15803d"))
        else:
            style.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#fee2e2"))
            style.add("TEXTCOLOR", (0, i), (0, i), colors.HexColor("#b91c1c"))
        style.add("FONTNAME", (0, i), (0, i), "Helvetica-Bold")
    t.setStyle(style)
    return t


if __name__ == "__main__":
    main()
