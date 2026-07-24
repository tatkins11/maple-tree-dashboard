"""Maple Tree — Spring 2026 End-of-Season Review (branded, chart-rich PDF).

Sections: cover, season story, playoffs+standings, awards, team-by-year, this-season
team line, opponent report card, per-player pages, active-roster career leaderboard,
record book, season impact & highlights, glossary.

Run:  python scripts/build_season_review_pdf.py
"""
from __future__ import annotations

import sqlite3
import sys
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Flowable, Image as RLImage, KeepTogether, ListFlowable, ListItem, PageBreak,
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from src.dashboard.data import (
    fetch_active_roster, fetch_advanced_analytics_archetype_summary, fetch_advanced_analytics_view,
    fetch_career_stats,
    fetch_current_season_stats, fetch_franchise_opponent_ledger, fetch_player_game_log,
    fetch_player_milestone_context, fetch_player_record_context, fetch_record_leaderboards,
    fetch_seasons, fetch_single_game_stats, fetch_single_season_stats, fetch_team_summary,
    format_display_date, get_connection,
)

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "data/writeups/maple-tree-spring-2026/season-review-spring-2026.pdf"
SEASON = "Maple Tree Spring 2026"

# ---- Maple Tree theme -------------------------------------------------------
GREEN = colors.HexColor("#15803d")
DGREEN = colors.HexColor("#14532d")
LGREEN = colors.HexColor("#dcfce7")
MGREEN = colors.HexColor("#bbf7d0")
INK = colors.HexColor("#1f2937")
MUTED = colors.HexColor("#475569")
LINE = colors.HexColor("#cbd5e1")
GOLD = colors.HexColor("#ca8a04")
GOLDFILL = colors.HexColor("#fef9c3")
CREAM = colors.HexColor("#f6f8f4")
PALE = colors.HexColor("#f0fdf4")
HEX_GREEN = "#15803d"

SEASON_ABBR = {
    "Soviet Sluggers Summer 2021": "'21 Sov",
    "Smoking Bunts Summer 2022": "'22 Bunts",
    "Maple Tree Tappers Summer 2025": "'25 Tap",
    "Maple Tree Fall 2025": "'25 Fall",
    "Maple Tree Spring 2026": "'26 Spr",
}


def abbr(season: str) -> str:
    return SEASON_ABBR.get(season, season)


# ---- styles -----------------------------------------------------------------
_S = getSampleStyleSheet()
ST = {
    "cover_team": ParagraphStyle("ct", parent=_S["Title"], fontSize=44, leading=46,
                                 textColor=GREEN, alignment=TA_CENTER, spaceAfter=2),
    "cover_sub": ParagraphStyle("cs", parent=_S["Title"], fontSize=22, leading=26,
                                textColor=DGREEN, alignment=TA_CENTER, spaceAfter=10),
    "cover_tag": ParagraphStyle("ctag", parent=_S["Italic"], fontSize=12, leading=16,
                                textColor=MUTED, alignment=TA_CENTER, spaceAfter=6),
    "h1": ParagraphStyle("h1", parent=_S["Heading1"], fontSize=17, leading=21, textColor=DGREEN,
                         spaceBefore=2, spaceAfter=8),
    "h2": ParagraphStyle("h2", parent=_S["Heading2"], fontSize=12, leading=15, textColor=GREEN,
                         spaceBefore=5, spaceAfter=2),
    "body": ParagraphStyle("b", parent=_S["BodyText"], fontSize=10.3, leading=14.5, spaceAfter=8,
                           textColor=INK),
    "cap": ParagraphStyle("cap", parent=_S["BodyText"], fontSize=8.6, leading=11.5, textColor=MUTED,
                          spaceAfter=10),
    "bullet": ParagraphStyle("bul", parent=_S["BodyText"], fontSize=9.6, leading=13, leftIndent=14,
                             bulletIndent=4, spaceAfter=2, textColor=INK),
    "callout": ParagraphStyle("co", parent=_S["BodyText"], fontSize=10.3, leading=14.5, leftIndent=12,
                              rightIndent=12, backColor=PALE, borderPadding=10, spaceAfter=10,
                              borderColor=MGREEN, borderWidth=0.6, textColor=INK),
    "gold": ParagraphStyle("gold", parent=_S["BodyText"], fontSize=10.3, leading=14.5, leftIndent=12,
                           rightIndent=12, backColor=GOLDFILL, borderPadding=10, spaceAfter=10,
                           borderColor=GOLD, borderWidth=0.6, textColor=colors.HexColor("#713f12")),
    "pname": ParagraphStyle("pn", parent=_S["Heading1"], fontSize=19, leading=22, textColor=DGREEN,
                            spaceAfter=1),
    "parch": ParagraphStyle("pa", parent=_S["Italic"], fontSize=10.5, leading=13, textColor=GREEN,
                            spaceAfter=8),
}


# =====================================================================================
# data
# =====================================================================================
def gather(conn: sqlite3.Connection) -> dict:
    seasons = fetch_seasons(conn)  # newest first
    chrono = list(reversed(seasons))
    roster = fetch_active_roster(conn)
    active = list(zip(roster["preferred_display_name"], roster["canonical_name"]))

    cur = fetch_current_season_stats(conn, SEASON)
    adv, _meta = fetch_advanced_analytics_view(conn, view_mode="Season", selected_season=SEASON,
                                               min_pa=0, active_only=False)
    advc = adv[["canonical_name", "wrc_plus", "owar", "team_relative_ops", "iso",
                "archetype_label", "woba", "runs_above_replacement"]]
    cur = cur.merge(advc, on="canonical_name", how="left")
    cur = cur.sort_values("ops", ascending=False).reset_index(drop=True)

    team = fetch_team_summary(conn, SEASON)
    season_totals = pd.read_sql_query(
        "SELECT season, SUM(hits) h, SUM(singles) s1, SUM(doubles) s2, SUM(triples) s3 "
        "FROM season_batting_stats GROUP BY season", conn).set_index("season")
    team_by_year = []
    for s in chrono:
        t = fetch_team_summary(conn, s)
        row = {"season": s, **t}
        if s in season_totals.index:
            tt = season_totals.loc[s]
            row.update(hits=int(tt.h), s1=int(tt.s1), s2=int(tt.s2), s3=int(tt.s3))
        team_by_year.append(row)

    single_season = fetch_single_season_stats(conn)
    career = fetch_career_stats(conn).set_index("canonical_name")
    sg_season = fetch_single_game_stats(conn, seasons=[SEASON])

    # RISP / two-out from season_batting_stats
    risp = pd.read_sql_query(
        "SELECT pm.preferred_display_name player, pi.canonical_name, s.batting_average_risp risp, "
        "s.two_out_rbi t2 FROM season_batting_stats s JOIN player_metadata pm ON pm.player_id=s.player_id "
        "JOIN player_identity pi ON pi.player_id=s.player_id WHERE s.season=?",
        conn, params=[SEASON])

    # opponent results this season (Maple Tree schedule)
    opp = pd.read_sql_query(
        "SELECT opponent_name, result, runs_for, runs_against FROM schedule_games "
        "WHERE team_name='Maple Tree' AND season=? AND is_bye=0 AND completed_flag=1 "
        "AND opponent_name IS NOT NULL AND opponent_name<>''", conn, params=[SEASON])

    boards = {sc: fetch_record_leaderboards(conn, scope=sc, limit=5) for sc in
              ("career", "single_season", "single_game")}
    ledger = fetch_franchise_opponent_ledger(conn)

    # per-game team OPS trajectory (this season, chronological)
    g = sg_season.copy()
    traj = g.groupby(["game_date", "game_time"], dropna=False).agg(
        H=("hits", "sum"), BB=("bb", "sum"), AB=("ab", "sum"), PA=("pa", "sum"),
        TB=("tb", "sum")).reset_index().sort_values(["game_date", "game_time"]).reset_index(drop=True)
    traj = traj.assign(ops=((traj.H + traj.BB) / traj.PA.replace(0, 1)) + (traj.TB / traj.AB.replace(0, 1)))

    return dict(seasons=seasons, chrono=chrono, active=active, cur=cur, adv=adv, team=team,
                team_by_year=team_by_year, single_season=single_season, career=career,
                sg_season=sg_season, risp=risp, opp=opp, boards=boards, ledger=ledger, traj=traj)


# =====================================================================================
# helpers: charts + tables
# =====================================================================================
def chart(fig, width_in: float) -> RLImage:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    w, h = PILImage.open(buf).size
    buf.seek(0)
    iw = width_in * inch
    return RLImage(buf, width=iw, height=iw * h / w)


def hbar(labels, values, title, xlabel, width_in=6.6, value_fmt="{:.0f}", height=None):
    n = len(labels)
    fig, ax = plt.subplots(figsize=(width_in, height or max(1.6, 0.34 * n + 0.6)))
    y = range(n)
    ax.barh(list(y), values, color=HEX_GREEN, height=0.66)
    ax.set_yticks(list(y)); ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel, fontsize=8.5)
    if title:
        ax.set_title(title, fontsize=10, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.tick_params(labelsize=8)
    vmax = max(values) if values else 1
    for i, v in zip(y, values):
        ax.text(v + vmax * 0.012, i, value_fmt.format(v), va="center", fontsize=7.6, color="#334155")
    ax.set_xlim(0, vmax * 1.14)
    return chart(fig, width_in)


def line_chart(x, y, title, ylabel, width_in=6.6):
    fig, ax = plt.subplots(figsize=(width_in, 2.4))
    ax.plot(range(len(x)), y, "-o", color=HEX_GREEN, linewidth=2, markersize=4)
    ax.axhline(sum(y) / len(y), color="#9ca3af", linestyle="--", linewidth=0.9)
    ax.set_xticks(range(len(x))); ax.set_xticklabels(x, fontsize=7, rotation=45, ha="right")
    ax.set_ylabel(ylabel, fontsize=8.5)
    ax.set_title(title, fontsize=10, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.tick_params(labelsize=8)
    return chart(fig, width_in)


def stat_table(rows, widths, total_row=False, highlight_rows=()):
    t = Table(rows, colWidths=widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), DGREEN),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.4),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (1, 0), (1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, CREAM]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.7, DGREEN),
    ]
    if total_row:
        style += [("BACKGROUND", (0, -1), (-1, -1), LGREEN), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                  ("LINEABOVE", (0, -1), (-1, -1), 0.9, DGREEN)]
    for r in highlight_rows:
        style += [("BACKGROUND", (0, r), (-1, r), GOLDFILL), ("FONTNAME", (0, r), (-1, r), "Helvetica-Bold")]
    t.setStyle(TableStyle(style))
    return t


def simple_table(rows, widths, header=True, highlight_row=None):
    t = Table(rows, colWidths=widths, repeatRows=1 if header else 0)
    style = [
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (2, 1), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 1 if header else 0), (-1, -1), [colors.white, CREAM]),
    ]
    if header:
        style += [("BACKGROUND", (0, 0), (-1, 0), DGREEN), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                  ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("LINEBELOW", (0, 0), (-1, 0), 0.7, DGREEN)]
    if highlight_row is not None:
        style += [("BACKGROUND", (0, highlight_row), (-1, highlight_row), LGREEN),
                  ("FONTNAME", (0, highlight_row), (-1, highlight_row), "Helvetica-Bold")]
    t.setStyle(TableStyle(style))
    return t


def f3(x):
    try:
        return f"{float(x):.3f}".lstrip("0") if 0 <= float(x) < 1 else f"{float(x):.3f}"
    except (TypeError, ValueError):
        return "-"


def i0(x):
    try:
        return str(int(round(float(x))))
    except (TypeError, ValueError):
        return "-"


# =====================================================================================
# sections
# =====================================================================================
def sec_cover(story, d):
    team = d["team"]
    story.append(Spacer(1, 0.8 * inch))
    # banner
    bt = ParagraphStyle("bt", parent=_S["Title"], fontSize=46, leading=48, textColor=colors.white,
                        alignment=TA_CENTER, spaceAfter=2)
    bs = ParagraphStyle("bs", parent=_S["Title"], fontSize=16, leading=20,
                        textColor=colors.HexColor("#dcfce7"), alignment=TA_CENTER, spaceAfter=0)
    banner = Table([[Paragraph("MAPLE TREE", bt)], [Paragraph("S P R I N G &nbsp; 2 0 2 6", bs)]],
                   colWidths=[7.1 * inch])
    banner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), GREEN), ("TOPPADDING", (0, 0), (-1, 0), 22),
        ("BOTTOMPADDING", (0, -1), (-1, -1), 18), ("TOPPADDING", (0, 1), (-1, 1), 0),
        ("LINEBELOW", (0, -1), (-1, -1), 3, GOLD),
    ]))
    story.append(banner)
    story.append(Spacer(1, 6))
    story.append(Paragraph("Season in Review &nbsp;·&nbsp; Wednesday Men's League &nbsp;·&nbsp; "
                           "Blue Division &nbsp;·&nbsp; 5th franchise season", ST["cover_tag"]))
    story.append(Spacer(1, 0.4 * inch))

    # 2-row KPI panel (label row + value row, twice)
    def kpi_rows(labels, values):
        return [labels, values]
    panel = (kpi_rows(["RECORD", "PLAYOFF FINISH", "GAMES", "TEAM OPS"],
                      ["2-11", "T-5th", i0(team["team_games"]), f3(team["ops"])])
             + kpi_rows(["RUNS", "HOME RUNS", "TEAM AVG", "TEAM OBP"],
                        [i0(team["runs"]), i0(team["home_runs"]), f3(team["avg"]), f3(team["obp"])]))
    t = Table(panel, colWidths=[1.7 * inch] * 4)
    t.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        # label rows (0, 2)
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8), ("FONTSIZE", (0, 2), (-1, 2), 8),
        ("TEXTCOLOR", (0, 0), (-1, 0), GREEN), ("TEXTCOLOR", (0, 2), (-1, 2), GREEN),
        ("TOPPADDING", (0, 0), (-1, 0), 10), ("TOPPADDING", (0, 2), (-1, 2), 12),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 0), ("BOTTOMPADDING", (0, 2), (-1, 2), 0),
        # value rows (1, 3)
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"), ("FONTNAME", (0, 3), (-1, 3), "Helvetica-Bold"),
        ("FONTSIZE", (0, 1), (-1, 1), 22), ("FONTSIZE", (0, 3), (-1, 3), 22),
        ("TEXTCOLOR", (0, 1), (-1, 1), DGREEN), ("TEXTCOLOR", (0, 3), (-1, 3), DGREEN),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 10), ("BOTTOMPADDING", (0, 3), (-1, 3), 12),
        ("LINEBELOW", (0, 1), (-1, 1), 0.5, LINE),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.5 * inch))
    story.append(Paragraph("Loud bats, tough finishes — a <b>1.305-OPS</b> offense that punched into the "
                           "playoffs before running into Bullseyes in Round 1.",
                           ParagraphStyle("hl", parent=ST["cover_tag"], fontSize=13, leading=17,
                                          textColor=DGREEN)))
    story.append(Spacer(1, 4))
    story.append(Paragraph("Team history &nbsp;·&nbsp; this season &nbsp;·&nbsp; every active player "
                           "&nbsp;·&nbsp; the record book &nbsp;·&nbsp; milestones", ST["cover_tag"]))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("Maple Tree Dashboard &nbsp;·&nbsp; Generated 06/18/26", ST["cap"]))
    story.append(PageBreak())


def sec_story(story, d):
    team = d["team"]
    story.append(Paragraph("The Season Story", ST["h1"]))
    story.append(Paragraph(
        "Spring 2026 was a season of split personality. Maple Tree fielded one of the loudest offenses in "
        "the Blue Division — a <b>{ops} team OPS</b> with <b>{hr} home runs</b> and <b>{runs} runs</b> across "
        "the year — and still finished the regular season <b>2-10</b>. The bats were never the problem; run "
        "prevention was. Night after night the team hung points on the board and could not get the one clean "
        "defensive inning that turns a shootout into a win.".format(
            ops=f3(team["ops"]), hr=i0(team["home_runs"]), runs=i0(team["runs"])), ST["body"]))
    story.append(Paragraph(
        "It was enough to back into the playoffs as the <b>6 seed</b>. Round 1 drew Bullseyes — a team Maple "
        "Tree had split with in the regular season and genuinely believed it could outscore. Instead the bats "
        "went quiet at the worst possible time: <b>8 runs on 10 hits</b>, a season-ending <b>18-8</b> loss, and "
        "a tie for 5th. cacheouts went on to win the league over top-seeded No Dice.", ST["body"]))
    story.append(Paragraph(
        "But the box scores tell a better story than the standings. Tristan put together an MVP season, Tim "
        "rewrote the doubles record, and a half-dozen hitters posted lines that would start on most rosters in "
        "the division. This review lays out all of it — where this season sits in franchise history, what each "
        "hitter did, and the records and milestones that moved.", ST["body"]))
    story.append(Paragraph("Season at a Glance", ST["h2"]))
    g = [["Games", "Record", "Runs", "HR", "Hits", "AVG", "OBP", "SLG", "OPS"],
         [i0(team["team_games"]), "2-11", i0(team["runs"]), i0(team["home_runs"]),
          i0(d["cur"]["hits"].sum()), f3(team["avg"]), f3(team["obp"]), f3(team["slg"]), f3(team["ops"])]]
    story.append(stat_table(g, [0.66 * inch, 0.8 * inch, 0.66 * inch, 0.55 * inch, 0.66 * inch,
                                0.7 * inch, 0.7 * inch, 0.7 * inch, 0.7 * inch]))
    story.append(Spacer(1, 12))
    traj = d["traj"]
    labels = [format_display_date(x) for x in traj["game_date"]]
    story.append(line_chart(labels, list(traj["ops"]), "The shape of the season — team OPS by game", "OPS", 6.8))
    story.append(Spacer(1, 4))
    story.append(Paragraph("Maple Tree cleared a 1.000 team OPS in nearly every game it played — the bats "
                           "rarely went quiet. The dashed line is the season average.", ST["cap"]))
    story.append(PageBreak())


def sec_playoffs(story, d):
    story.append(Paragraph("Playoffs &amp; Final Standings", ST["h1"]))
    story.append(Paragraph(
        "Single elimination, all games Wednesday, June 17. Maple Tree (6 seed) fell to 3-seed Bullseyes 18-8 "
        "in Round 1. cacheouts ran the table to the title.", ST["body"]))
    story.append(Paragraph("Final League Standings", ST["h2"]))
    standings = [["Place", "Team", "Result"],
                 ["1st", "cacheouts", "Champions (beat No Dice 29-16)"],
                 ["2nd", "No Dice", "Runner-up"],
                 ["T-3rd", "Soft Ballz", "Lost semifinal"],
                 ["T-3rd", "Bullseyes", "Lost semifinal"],
                 ["T-5th", "Wasted Talent", "Lost Round 1"],
                 ["T-5th", "Wasted Potential", "Lost Round 1"],
                 ["T-5th", "Maple Tree", "Lost Round 1 (to Bullseyes 18-8)"]]
    story.append(simple_table(standings, [0.9 * inch, 1.7 * inch, 4.0 * inch], highlight_row=7))
    story.append(Spacer(1, 10))
    story.append(Paragraph("The Bracket", ST["h2"]))
    bracket = [["Round 1", "", ""],
               ["Game 1 · 6:30", "Soft Ballz 18, Wasted Talent 13", "SB advances"],
               ["Game 2 · 7:30", "cacheouts 7, Wasted Potential 0", "cacheouts advances"],
               ["Game 3 · 7:30", "Bullseyes 18, Maple Tree 8", "Bullseyes advances"],
               ["Semifinals", "", ""],
               ["Game 4 · 8:30", "No Dice 27, Soft Ballz 14", "No Dice → final"],
               ["Game 5 · 8:30", "cacheouts 29, Bullseyes 13", "cacheouts → final"],
               ["Final", "", ""],
               ["Game 6 · 9:30", "cacheouts 29, No Dice 16", "cacheouts champions"]]
    t = simple_table(bracket, [1.3 * inch, 3.1 * inch, 2.2 * inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("BACKGROUND", (0, 0), (-1, 0), DGREEN), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("SPAN", (0, 0), (-1, 0)), ("SPAN", (0, 4), (-1, 4)), ("SPAN", (0, 7), (-1, 7)),
        ("BACKGROUND", (0, 4), (-1, 4), GREEN), ("TEXTCOLOR", (0, 4), (-1, 4), colors.white),
        ("FONTNAME", (0, 4), (-1, 4), "Helvetica-Bold"),
        ("BACKGROUND", (0, 7), (-1, 7), GREEN), ("TEXTCOLOR", (0, 7), (-1, 7), colors.white),
        ("FONTNAME", (0, 7), (-1, 7), "Helvetica-Bold"),
        ("BACKGROUND", (0, 3), (-1, 3), LGREEN), ("FONTNAME", (0, 3), (0, 3), "Helvetica-Bold"),
        ("ROWBACKGROUNDS", (0, 1), (-1, 3), [colors.white, CREAM]),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
    ]))
    story.append(t)
    story.append(PageBreak())


def compute_awards(d):
    cur = d["cur"].copy()
    ss = d["single_season"]
    sg = d["sg_season"]
    awards = []
    featured = set()

    def take(name, candidates, line_fn):
        # candidates: iterable of (canonical, player, payload), best first
        for canonical, player, payload in candidates:
            if canonical and canonical not in featured:
                featured.add(canonical)
                awards.append((name, player, line_fn(payload)))
                return

    def ranked(df, sort_cols, ascending=False):
        s = df.sort_values(sort_cols, ascending=ascending)
        return [(r["canonical_name"], r["player"], r) for _, r in s.iterrows()]

    # 1. MVP — wRC+
    take("Team MVP", ranked(cur, "wrc_plus"),
         lambda r: f"wRC+ {i0(r['wrc_plus'])} · {f3(r['ops'])} OPS · {i0(r['hr'])} HR — the team's run-creation "
                   f"engine, tops in OPS, on-base, and homers.")

    # 2. Game of the Year — best single game by an eligible player
    best_games = sg.sort_values("game_score", ascending=False).drop_duplicates("canonical_name")
    take("Game of the Year", [(r["canonical_name"], r["player"], r) for _, r in best_games.iterrows()],
         lambda r: f"{format_display_date(r['game_date'])} vs {r['opponent']}: {i0(r['hits'])}-for-{i0(r['ab'])}, "
                   f"{i0(r['hr'])} HR, {i0(r['rbi'])} RBI — a {r['game_score']:.1f} Game Score.")

    # 3. Most Improved — OPS jump over the player's previous season
    piv = ss.pivot_table(index="canonical_name", columns="season", values="ops", aggfunc="first")
    name_of = dict(zip(ss["canonical_name"], ss["player"]))
    order = {s: i for i, s in enumerate(d["chrono"])}
    improved = []
    for canonical in piv.index:
        played = sorted(piv.loc[canonical].dropna().index, key=lambda s: order.get(s, 99))
        if SEASON in played and played.index(SEASON) > 0:
            prev = played[played.index(SEASON) - 1]
            delta = float(piv.loc[canonical, SEASON] - piv.loc[canonical, prev])
            improved.append((canonical, name_of.get(canonical, canonical),
                             {"delta": delta, "prev_label": abbr(prev),
                              "prev": piv.loc[canonical, prev], "cur": piv.loc[canonical, SEASON]}))
    improved.sort(key=lambda x: x[2]["delta"], reverse=True)
    take("Most Improved", [c for c in improved if c[2]["delta"] > 0],
         lambda r: f"OPS up {r['delta']:+.3f} from {r['prev_label']} ({f3(r['prev'])} → {f3(r['cur'])}) — "
                   f"the season's biggest leap forward.")

    # 4. Silver Slugger — power (HR, then TB)
    take("Silver Slugger", ranked(cur, ["hr", "tb"]),
         lambda r: f"{i0(r['hr'])} home runs and {i0(r['tb'])} total bases — the lineup's biggest power threat.")

    # 5. Mr. Clutch — RISP
    risp = d["risp"].merge(cur[["canonical_name"]], on="canonical_name", how="inner")
    take("Mr. Clutch", ranked(risp[risp["risp"] > 0], ["risp", "t2"]),
         lambda r: f"Hit {f3(r['risp'])} with runners in scoring position and {i0(r['t2'])} two-out RBI — "
                   f"money when it counted.")

    # 6. Iron Man — Duff (durability); 8 played all 13
    maxg = int(cur["games"].max())
    n_iron = int((cur["games"] == maxg).sum())
    duff_first = [(r["canonical_name"], r["player"], r) for _, r in
                  cur[cur["canonical_name"] == "duff"].iterrows()]
    take("Iron Man", duff_first + ranked(cur[cur["games"] == maxg], "pa"),
         lambda r: f"All {i0(r['games'])} games, never a night off — one of {n_iron} who went wire-to-wire. "
                   f"Show up, grind, repeat.")

    # 7. On-Base Spark — OBP
    take("On-Base Spark", ranked(cur, "obp"),
         lambda r: f"A {f3(r['obp'])} on-base with {i0(r['bb'])} walks — refused to give away outs.")

    # 8. Spark Plug — runs scored
    take("Spark Plug", ranked(cur, "r"),
         lambda r: f"Crossed the plate {i0(r['r'])} times — always around to start or finish a rally.")

    # 9. The Singles King — Jason's themed award (replaces the original "Tough Out",
    #    which had gone to Slomka). Reserve Slomka so Unsung Hero below doesn't re-feature him.
    featured.add("slomka")
    take("The Singles King", ranked(cur[cur["canonical_name"] == "jason"], "ops"),
         lambda r: f"All {i0(r['hits'])} of his hits were singles — a symmetrical {f3(r['avg'])}/"
                   f"{f3(r['obp'])}/{f3(r['slg'])} slash with no extra-base frills. Pure bat-to-ball, and "
                   f"around to score {i0(r['r'])} times. Find grass, beat it out, repeat.")

    # 10. Unsung Hero — best remaining bat
    take("Unsung Hero", ranked(cur, "ops"),
         lambda r: f"{f3(r['ops'])} OPS across {i0(r['pa'])} trips — quietly productive every time up.")

    return awards


def sec_awards(story, d):
    story.append(Paragraph("Awards &amp; Superlatives", ST["h1"]))
    story.append(Paragraph("The hardware from a season the bats can be proud of.", ST["cap"]))
    for name, who, line in compute_awards(d):
        block = Table([[Paragraph(f"<b>{name}</b>", ParagraphStyle("aw", parent=ST["body"], fontSize=11,
                                                                      textColor=DGREEN, spaceAfter=0)),
                        Paragraph(f"<b>{who}</b>", ParagraphStyle("awn", parent=ST["body"], fontSize=13,
                                                                  textColor=GREEN, alignment=TA_LEFT, spaceAfter=0))],
                       [Paragraph(line, ParagraphStyle("awl", parent=ST["body"], spaceAfter=0)), ""]],
                      colWidths=[2.0 * inch, 4.6 * inch])
        block.setStyle(TableStyle([
            ("SPAN", (0, 1), (1, 1)), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("BACKGROUND", (0, 0), (-1, -1), PALE), ("BOX", (0, 0), (-1, -1), 0.6, MGREEN),
            ("LEFTPADDING", (0, 0), (-1, -1), 10), ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LINEAFTER", (0, 0), (0, 0), 0.6, MGREEN),
        ]))
        story.append(block)
        story.append(Spacer(1, 6))
    story.append(PageBreak())


def sec_team_by_year(story, d):
    story.append(Paragraph("Team by the Year", ST["h1"]))
    story.append(Paragraph("Every season in franchise history (all team-name eras count as one club). "
                           "This season is highlighted.", ST["cap"]))
    rows = [["Season", "G", "PA", "Runs", "H", "1B", "2B", "3B", "HR", "AVG", "OBP", "SLG", "OPS"]]
    hl = None
    for i, t in enumerate(d["team_by_year"], start=1):
        if t["season"] == SEASON:
            hl = i
        rows.append([abbr(t["season"]), i0(t["team_games"]), i0(t["plate_appearances"]), i0(t["runs"]),
                     i0(t.get("hits", 0)), i0(t.get("s1", 0)), i0(t.get("s2", 0)), i0(t.get("s3", 0)),
                     i0(t["home_runs"]), f3(t["avg"]), f3(t["obp"]), f3(t["slg"]), f3(t["ops"])])
    story.append(stat_table(rows, [0.92 * inch, 0.4 * inch, 0.5 * inch, 0.5 * inch, 0.42 * inch, 0.4 * inch,
                                   0.4 * inch, 0.4 * inch, 0.4 * inch, 0.56 * inch, 0.56 * inch, 0.56 * inch,
                                   0.56 * inch], highlight_rows=(hl,) if hl else ()))
    story.append(Spacer(1, 8))
    # rank callout
    by_ops = sorted(d["team_by_year"], key=lambda t: t["ops"], reverse=True)
    rank = [t["season"] for t in by_ops].index(SEASON) + 1
    n = len(by_ops)
    story.append(Paragraph(
        f"This year's <b>{f3(d['team']['ops'])} team OPS</b> ranks <b>{rank} of {n}</b> in franchise history — "
        f"an offense that held its own against any Maple Tree team ever fielded.", ST["callout"]))
    story.append(Spacer(1, 6))
    labels = [abbr(t["season"]) for t in d["team_by_year"]]
    story.append(chart(_year_ops_fig(labels, [t["ops"] for t in d["team_by_year"]]), 6.6))
    story.append(PageBreak())


def _year_ops_fig(labels, ops):
    fig, ax = plt.subplots(figsize=(6.6, 2.4))
    bars = ax.bar(range(len(labels)), ops, color=HEX_GREEN, width=0.6)
    if "'26 Spr" in labels:
        bars[labels.index("'26 Spr")].set_color("#ca8a04")
    ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels, fontsize=8.5)
    ax.set_ylabel("Team OPS", fontsize=8.5)
    ax.set_title("Team OPS by season", fontsize=10, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    for i, v in enumerate(ops):
        ax.text(i, v + 0.02, f"{v:.3f}", ha="center", fontsize=7.6, color="#334155")
    ax.set_ylim(0, max(ops) * 1.16)
    return fig


def sec_this_season(story, d):
    cur = d["cur"]
    story.append(Paragraph("This Season — Team Batting Line", ST["h1"]))
    story.append(Paragraph("Every hitter, sorted by OPS. wRC+ indexes run creation to 100 = team average.",
                           ST["cap"]))
    head = ["Player", "G", "PA", "AB", "H", "1B", "2B", "3B", "HR", "R", "RBI", "BB", "TB",
            "AVG", "OBP", "SLG", "OPS", "wRC+"]
    rows = [head]
    for _, r in cur.iterrows():
        rows.append([r["player"], i0(r["games"]), i0(r["pa"]), i0(r["ab"]), i0(r["hits"]), i0(r["1b"]),
                     i0(r["2b"]), i0(r["3b"]), i0(r["hr"]), i0(r["r"]), i0(r["rbi"]), i0(r["bb"]),
                     i0(r["tb"]), f3(r["avg"]), f3(r["obp"]), f3(r["slg"]), f3(r["ops"]), i0(r["wrc_plus"])])
    team = d["team"]
    rows.append(["TEAM", i0(team["team_games"]), i0(team["plate_appearances"]), i0(cur["ab"].sum()),
                 i0(cur["hits"].sum()), i0(cur["1b"].sum()), i0(cur["2b"].sum()), i0(cur["3b"].sum()),
                 i0(team["home_runs"]), i0(team["runs"]), i0(cur["rbi"].sum()), i0(cur["bb"].sum()),
                 i0(cur["tb"].sum()), f3(team["avg"]), f3(team["obp"]), f3(team["slg"]), f3(team["ops"]), "100"])
    widths = [0.66, 0.24, 0.3, 0.3, 0.27, 0.27, 0.27, 0.27, 0.28, 0.27, 0.32, 0.27, 0.3,
              0.37, 0.37, 0.39, 0.42, 0.4]
    story.append(stat_table(rows, [w * inch for w in widths], total_row=True))
    story.append(Spacer(1, 10))
    top = cur.head(8)
    story.append(chart(_top_ops_fig(list(top["player"])[::-1], list(top["ops"])[::-1]), 6.6))
    story.append(PageBreak())


def _top_ops_fig(labels, ops):
    fig, ax = plt.subplots(figsize=(6.6, 2.7))
    ax.barh(range(len(labels)), ops, color=HEX_GREEN, height=0.66)
    ax.set_yticks(range(len(labels))); ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_xlabel("OPS", fontsize=8.5)
    ax.set_title("Top of the order — OPS leaders", fontsize=10, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    for i, v in enumerate(ops):
        ax.text(v + 0.02, i, f"{v:.3f}", va="center", fontsize=7.6, color="#334155")
    ax.set_xlim(0, max(ops) * 1.14)
    return fig


def _wrcplus_fig(labels, vals):
    fig, ax = plt.subplots(figsize=(6.6, 3.3))
    ax.barh(range(len(labels)), vals, color=HEX_GREEN, height=0.66)
    ax.axvline(100, color="#64748b", linestyle="--", linewidth=1)
    ax.text(100, len(labels) - 0.3, " team avg", fontsize=7, color="#64748b", va="top")
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_xlabel("wRC+  (100 = team average)", fontsize=8.5)
    ax.set_title("Run creation — wRC+", fontsize=10, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    for i, v in enumerate(vals):
        ax.text(v + 3, i, i0(v), va="center", fontsize=7.6, color="#334155")
    ax.set_xlim(0, max(vals) * 1.13)
    return fig


def _hittermap_fig(cur):
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    x = cur["obp"].astype(float)
    y = cur["iso"].astype(float)
    ax.axvline(float(x.mean()), color="#cbd5e1", linewidth=0.9, zorder=1)
    ax.axhline(float(y.mean()), color="#cbd5e1", linewidth=0.9, zorder=1)
    ax.scatter(x, y, s=80, color=HEX_GREEN, alpha=0.85, edgecolor="white", linewidth=0.8, zorder=3)
    for _, r in cur.iterrows():
        ax.annotate(str(r["player"]), (float(r["obp"]), float(r["iso"])), fontsize=6.8,
                    xytext=(4, 3), textcoords="offset points", color="#334155")
    ax.set_xlabel("OBP  (on-base →)", fontsize=8.5)
    ax.set_ylabel("ISO  (power ↑)", fontsize=8.5)
    ax.set_title("The hitter map — on-base vs. power", fontsize=10, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    return fig


def sec_advanced(story, d):
    cur = d["cur"]
    story.append(Paragraph("Advanced Analytics", ST["h1"]))
    story.append(Paragraph("Beyond the back of the card — these weight <i>how</i> a hitter produces, not just "
                           "how often, and grade it against the rest of the lineup.", ST["body"]))
    story.append(ListFlowable([
        ListItem(Paragraph("<b>wRC+</b> — runs created, indexed so 100 = team average (150 = 50% better than "
                           "the team norm).", ST["bullet"])),
        ListItem(Paragraph("<b>wOBA</b> — weighted on-base average; every way of reaching base valued by how "
                           "much it truly helps score.", ST["bullet"])),
        ListItem(Paragraph("<b>ISO</b> — isolated power (SLG − AVG). <b>OPS+</b> — OPS indexed to the team. "
                           "<b>oWAR</b> — offensive wins above a replacement bat. <b>RAR</b> — runs above "
                           "replacement.", ST["bullet"])),
    ], bulletType="bullet", start="•"))
    story.append(Spacer(1, 6))
    adv = cur.sort_values("wrc_plus", ascending=False)
    head = ["Player", "PA", "wRC+", "wOBA", "ISO", "OPS+", "oWAR", "RAR", "Archetype"]
    rows = [head]
    for _, r in adv.iterrows():
        rows.append([r["player"], i0(r["pa"]), i0(r["wrc_plus"]), f3(r["woba"]), f3(r["iso"]),
                     i0(r["team_relative_ops"]), f"{float(r['owar']):.1f}",
                     f"{float(r['runs_above_replacement']):.1f}", str(r.get("archetype_label", ""))])
    story.append(stat_table(rows, [w * inch for w in
                 [0.74, 0.34, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 1.66]]))
    story.append(Spacer(1, 10))
    story.append(chart(_wrcplus_fig(list(adv["player"])[::-1], list(adv["wrc_plus"])[::-1]), 6.6))
    story.append(PageBreak())

    story.append(Paragraph("Hitter Profiles", ST["h1"]))
    story.append(Paragraph("Each hitter gets a one-word identity from blending on-base skill and power — from "
                           "<b>Cornerstones</b> (elite at both) down to <b>Depth Bats</b>.", ST["body"]))
    arch = fetch_advanced_analytics_archetype_summary(d["adv"])
    arows = [["Profile", "Hitters", "Avg OBP", "Avg SLG", "Avg oWAR"]]
    for _, r in arch.iterrows():
        arows.append([r["archetype"], i0(r["hitters"]), f3(r["avg_obp"]), f3(r["avg_slg"]),
                      f"{float(r['avg_owar']):.2f}"])
    story.append(simple_table(arows, [1.9 * inch, 0.9 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch]))
    story.append(Spacer(1, 12))
    story.append(chart(_hittermap_fig(cur), 6.4))
    story.append(Spacer(1, 4))
    story.append(Paragraph("Right = gets on base more; up = more power. The crosshairs mark the team average — "
                           "the top-right quadrant is where the Cornerstones live.", ST["cap"]))
    story.append(PageBreak())


def sec_opponents(story, d):
    story.append(Paragraph("Opponent Report Card", ST["h1"]))
    story.append(Paragraph("How Maple Tree fared against the division this spring, and the all-time ledger "
                           "for context.", ST["cap"]))
    opp = d["opp"]
    agg = []
    for name, g in opp.groupby("opponent_name"):
        w = int((g["result"].str.upper() == "W").sum())
        losses = int((g["result"].str.upper() == "L").sum())
        rf = int(g["runs_for"].sum()); ra = int(g["runs_against"].sum())
        agg.append((name, len(g), w, losses, rf, ra, rf - ra))
    agg.sort(key=lambda x: x[6], reverse=True)
    rows = [["Opponent", "GP", "W", "L", "RF", "RA", "Diff"]]
    for name, gp, w, ls, rf, ra, diff in agg:
        rows.append([name, str(gp), str(w), str(ls), str(rf), str(ra), f"{diff:+d}"])
    story.append(stat_table(rows, [1.9 * inch, 0.55 * inch, 0.5 * inch, 0.5 * inch, 0.6 * inch,
                                   0.6 * inch, 0.7 * inch]))
    story.append(Spacer(1, 10))
    story.append(Paragraph("All-Time vs the Field (franchise, all eras)", ST["h2"]))
    led = d["ledger"].sort_values("games", ascending=False).head(12)
    rows = [["Opponent", "G", "W", "L", "T", "Win%", "RF", "RA", "Diff"]]
    for _, r in led.iterrows():
        rows.append([r["opponent"], i0(r["games"]), i0(r["wins"]), i0(r["losses"]), i0(r["ties"]),
                     f3(r["win_pct"]), i0(r["runs_for"]), i0(r["runs_against"]), f"{int(r['run_diff']):+d}"])
    story.append(stat_table(rows, [1.7 * inch, 0.42 * inch, 0.42 * inch, 0.42 * inch, 0.42 * inch,
                                   0.6 * inch, 0.55 * inch, 0.55 * inch, 0.66 * inch]))
    story.append(PageBreak())


def _player_data(conn, canonical, display, d):
    ss = d["single_season"]
    order = {s: i for i, s in enumerate(d["chrono"])}
    pseasons = ss[ss["canonical_name"] == canonical].copy()
    pseasons = pseasons.assign(__o=pseasons["season"].map(order)).sort_values("__o")
    career = d["career"].loc[canonical] if canonical in d["career"].index else None
    adv_row = d["adv"][d["adv"]["canonical_name"] == canonical]
    log = fetch_player_game_log(conn, canonical)
    season_log = log[log["season"] == SEASON]
    mil = fetch_player_milestone_context(conn, canonical)
    rec = fetch_player_record_context(conn, canonical)
    return dict(display=display, pseasons=pseasons, career=career, adv_row=adv_row, log=log,
                season_log=season_log, mil=mil, rec=rec)


def sec_player(story, conn, canonical, display, d):
    p = _player_data(conn, canonical, display, d)
    flow = []
    archetype = ""
    advn = p["adv_row"]
    if not advn.empty:
        archetype = str(advn.iloc[0].get("archetype_label", "") or "")
    flow.append(Paragraph(display, ST["pname"]))
    if archetype:
        flow.append(Paragraph(f"Archetype: {archetype}", ST["parch"]))

    # season-by-season
    flow.append(Paragraph("Year by Year", ST["h2"]))
    head = ["Season", "G", "PA", "AB", "H", "1B", "2B", "3B", "HR", "R", "RBI", "BB", "TB",
            "AVG", "OBP", "SLG", "OPS"]
    rows = [head]
    for _, r in p["pseasons"].iterrows():
        rows.append([abbr(r["season"]), i0(r["games"]), i0(r["pa"]), i0(r["ab"]), i0(r["hits"]), i0(r["1b"]),
                     i0(r["2b"]), i0(r["3b"]), i0(r["hr"]), i0(r["r"]), i0(r["rbi"]), i0(r["bb"]), i0(r["tb"]),
                     f3(r["avg"]), f3(r["obp"]), f3(r["slg"]), f3(r["ops"])])
    car = p["career"]
    if car is not None:
        rows.append(["CAREER", i0(car["games"]), i0(car["pa"]), i0(car["ab"]), i0(car["hits"]), i0(car["1b"]),
                     i0(car["2b"]), i0(car["3b"]), i0(car["hr"]), i0(car["r"]), i0(car["rbi"]), i0(car["bb"]),
                     i0(car["tb"]), f3(car["avg"]), f3(car["obp"]), f3(car["slg"]), f3(car["ops"])])
    widths = [0.74, 0.28, 0.34, 0.34, 0.28, 0.28, 0.28, 0.28, 0.3, 0.28, 0.36, 0.28, 0.34,
              0.42, 0.42, 0.44, 0.48]
    flow.append(stat_table(rows, [w * inch for w in widths], total_row=car is not None))

    # advanced + season snapshot line
    if not advn.empty:
        a = advn.iloc[0]
        flow.append(Spacer(1, 6))
        flow.append(Paragraph(
            f"<b>Spring 2026 advanced:</b> wRC+ {i0(a['wrc_plus'])} &nbsp;·&nbsp; oWAR {a['owar']:.1f} "
            f"&nbsp;·&nbsp; OPS+ (team) {i0(a['team_relative_ops'])} &nbsp;·&nbsp; ISO {f3(a['iso'])}",
            ST["body"]))

    # OPS-by-season trend (multi-season players only)
    if len(p["pseasons"]) >= 2:
        labels = [abbr(s) for s in p["pseasons"]["season"]]
        flow.append(chart(_player_ops_fig(labels, list(p["pseasons"]["ops"])), 3.5))

    # highlights: this-season + career single-game highs
    hi = _player_highlights(p)
    if hi:
        flow.append(Paragraph("Highlights &amp; Career Bests", ST["h2"]))
        flow.append(ListFlowable([ListItem(Paragraph(h, ST["bullet"])) for h in hi],
                                 bulletType="bullet", start="•"))

    facts = _player_fun_facts(p, d, canonical)
    if facts:
        flow.append(Paragraph("By the Numbers", ST["h2"]))
        flow.append(ListFlowable([ListItem(Paragraph(f, ST["bullet"])) for f in facts],
                                 bulletType="bullet", start="•"))

    # milestones cleared + chasing
    bits = _player_milestones(p)
    if bits:
        flow.append(Paragraph("Milestones", ST["h2"]))
        flow.append(ListFlowable([ListItem(Paragraph(b, ST["bullet"])) for b in bits],
                                 bulletType="bullet", start="•"))

    # records
    recbits = _player_records(p)
    if recbits:
        flow.append(Paragraph("Record Book", ST["h2"]))
        flow.append(ListFlowable([ListItem(Paragraph(b, ST["bullet"])) for b in recbits],
                                 bulletType="bullet", start="•"))

    story.append(KeepTogether(flow[:2]))
    for f in flow[2:]:
        story.append(f)
    story.append(PageBreak())


def _player_ops_fig(labels, ops):
    fig, ax = plt.subplots(figsize=(3.9, 1.05))
    ax.bar(range(len(labels)), ops, color=HEX_GREEN, width=0.6)
    ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels, fontsize=7.5)
    ax.set_title("OPS by season", fontsize=8.5, color="#14532d", fontweight="bold", loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.tick_params(labelsize=7)
    mx = max(ops) if len(ops) else 1
    for i, v in enumerate(ops):
        ax.text(i, v + mx * 0.02, f"{v:.2f}", ha="center", fontsize=6.5, color="#334155")
    ax.set_ylim(0, mx * 1.2)
    return fig


def _player_fun_facts(p, d, canonical):
    out = []
    cur_row = d["cur"][d["cur"]["canonical_name"] == canonical]
    if not cur_row.empty:
        r = cur_row.iloc[0]
        xbh = int(r["2b"]) + int(r["3b"]) + int(r["hr"])
        if xbh > 0:
            out.append(f"<b>{xbh}</b> extra-base hits this spring ({i0(r['2b'])} 2B, {i0(r['3b'])} 3B, "
                       f"{i0(r['hr'])} HR)")

        def rank(col):
            order = d["cur"].sort_values(col, ascending=False).reset_index(drop=True)
            pos = order.index[order["canonical_name"] == canonical]
            return int(pos[0]) + 1 if len(pos) else None
        ranks = []
        ro = rank("ops")
        if ro:
            ranks.append(f"#{ro} OPS")
        rh = rank("hr")
        if rh and int(r["hr"]) > 0:
            ranks.append(f"#{rh} HR")
        rr = rank("rbi")
        if rr and int(r["rbi"]) > 0:
            ranks.append(f"#{rr} RBI")
        if ranks:
            out.append("Where he ranked on the team: " + " · ".join(ranks[:3]))
    slog = p["season_log"]
    if not slog.empty:
        slog = slog.sort_values(["game_date", "game_time"])
        mh = int((slog["hits"] >= 2).sum())
        mr = int((slog["rbi"] >= 2).sum())
        bits = []
        if mh:
            bits.append(f"{mh} multi-hit game{'s' if mh != 1 else ''}")
        if mr:
            bits.append(f"{mr} multi-RBI game{'s' if mr != 1 else ''}")
        if bits:
            out.append(" · ".join(bits))
        streak = run = 0
        for h in slog["hits"]:
            run = run + 1 if h >= 1 else 0
            streak = max(streak, run)
        if streak >= 3:
            out.append(f"Longest hitting streak: <b>{streak}</b> straight games with a hit")
    return out[:3]


def _player_highlights(p):
    out = []
    slog = p["season_log"]
    if not slog.empty:
        best = slog.sort_values("game_score", ascending=False).iloc[0]
        out.append(f"<b>Best game (2026):</b> {format_display_date(best['game_date'])} vs {best['opponent']} — "
                   f"{i0(best['hits'])}-for-{i0(best['ab'])}, {i0(best['hr'])} HR, {i0(best['rbi'])} RBI "
                   f"(Game Score {best['game_score']:.1f})")
        for col, lbl in [("hits", "hits"), ("hr", "home runs"), ("rbi", "RBI"), ("tb", "total bases")]:
            mx = int(slog[col].max())
            if mx > 0:
                out.append(f"<b>Season single-game high:</b> {mx} {lbl}")
                break
    log = p["log"]
    if not log.empty:
        for col, lbl in [("hr", "HR"), ("rbi", "RBI"), ("tb", "total bases"), ("hits", "hits")]:
            cmx = int(log[col].max())
            if cmx > 0:
                row = log.sort_values([col, "game_score"], ascending=False).iloc[0]
                out.append(f"<b>Career single-game best:</b> {cmx} {lbl} "
                           f"({format_display_date(row['game_date'])} vs {row['opponent']})")
                break
        out.append(f"<b>Top career Game Score:</b> {log['game_score'].max():.1f}")
    return out[:4]


def _player_milestones(p):
    out = []
    cleared = p["mil"].get("cleared", pd.DataFrame())
    upcoming = p["mil"].get("upcoming", pd.DataFrame())
    if cleared is not None and not cleared.empty:
        items = []
        for _, r in cleared.head(4).iterrows():
            disp = r.get("highest_cleared_milestone", r.get("next_milestone", ""))
            items.append(f"{i0(disp)} {r['stat']}")
        if items:
            out.append("<b>Cleared:</b> " + " · ".join(items))
    if upcoming is not None and not upcoming.empty:
        r = upcoming.iloc[0]
        out.append(f"<b>Chasing:</b> {i0(r['remaining'])} from {r.get('next_milestone_display', r['next_milestone'])} "
                   f"{r['stat']} (at {i0(r['current_total'])})")
    return out


def _player_records(p):
    out = []
    owned = p["rec"].get("owned", pd.DataFrame())
    placements = p["rec"].get("placements", pd.DataFrame())
    if owned is not None and not owned.empty:
        items = [f"{r['stat']} ({r['scope']})" for _, r in owned.head(5).iterrows()]
        out.append("<b>Holds the record:</b> " + " · ".join(items))
    if placements is not None and not placements.empty:
        top = placements[placements["rank"] <= 3].head(4)
        items = [f"#{i0(r['rank'])} {r['stat']} ({r['scope']})" for _, r in top.iterrows()]
        if items:
            out.append("<b>Top-3 all-time:</b> " + " · ".join(items))
    return out


def sec_career_leaderboard(story, d):
    story.append(Paragraph("Active Roster — Career Lines", ST["h1"]))
    story.append(Paragraph("Every active player's career totals across all franchise seasons, sorted by "
                           "career OPS (min 1 PA).", ST["cap"]))
    actives = {c for _, c in d["active"]}
    car = d["career"].reset_index()
    car = car[car["canonical_name"].isin(actives)].sort_values("ops", ascending=False)
    head = ["Player", "Sea", "G", "PA", "AB", "H", "1B", "2B", "3B", "HR", "R", "RBI", "BB", "TB",
            "AVG", "OBP", "SLG", "OPS"]
    rows = [head]
    for _, r in car.iterrows():
        rows.append([r["player"], i0(r["seasons_played"]), i0(r["games"]), i0(r["pa"]), i0(r["ab"]),
                     i0(r["hits"]), i0(r["1b"]), i0(r["2b"]), i0(r["3b"]), i0(r["hr"]), i0(r["r"]),
                     i0(r["rbi"]), i0(r["bb"]), i0(r["tb"]), f3(r["avg"]), f3(r["obp"]), f3(r["slg"]),
                     f3(r["ops"])])
    widths = [0.66, 0.3, 0.28, 0.32, 0.32, 0.28, 0.28, 0.28, 0.28, 0.3, 0.28, 0.34, 0.28, 0.32,
              0.4, 0.4, 0.42, 0.44]
    story.append(stat_table(rows, [w * inch for w in widths]))
    story.append(PageBreak())


def _record_mini(board, stat, scope):
    is_rate = stat in ("OPS", "AVG", "OBP", "SLG")
    ctx = scope != "career"
    data = [[stat.upper()] + [""] * (3 if ctx else 2)]
    for _, r in board.head(5).iterrows():
        val = f3(r[stat]) if is_rate else i0(r[stat])
        row = [i0(r["#"]), str(r["Player"]), val]
        if scope == "single_game":
            row.append(f"{r.get('Date', '')} {str(r.get('Opponent', ''))[:11]}")
        elif scope == "single_season":
            row.append(str(r.get("Season", "")))
        data.append(row)
    widths = [0.26, 1.18, 0.6, 1.16] if ctx else [0.34, 1.95, 0.85]
    t = Table(data, colWidths=[w * inch for w in widths])
    style = [
        ("SPAN", (0, 0), (-1, 0)),
        ("BACKGROUND", (0, 0), (-1, 0), GREEN), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, 0), 8.5),
        ("FONTSIZE", (0, 1), (-1, -1), 7.8),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"), ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2.6), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.6),
        ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, CREAM]),
        ("TEXTCOLOR", (2, 1), (2, -1), DGREEN), ("FONTNAME", (2, 1), (2, -1), "Helvetica-Bold"),
    ]
    if ctx:
        style += [("TEXTCOLOR", (3, 1), (3, -1), MUTED), ("FONTSIZE", (3, 1), (3, -1), 6.8)]
    t.setStyle(TableStyle(style))
    return t


def _grid2(minis):
    rows = []
    for i in range(0, len(minis), 2):
        pair = list(minis[i:i + 2])
        if len(pair) == 1:
            pair.append("")
        rows.append(pair)
    g = Table(rows, colWidths=[3.45 * inch, 3.45 * inch])
    g.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 11),
    ]))
    return g


def sec_record_book(story, d):
    counting = ["Hits", "Singles", "Doubles", "Triples", "HR", "RBI", "Runs", "Walks", "Total Bases"]
    full = ["Hits", "Singles", "Doubles", "Triples", "HR", "RBI", "Runs", "Walks", "Total Bases",
            "PA", "AB", "Games", "AVG", "OBP", "SLG", "OPS"]
    feature_by = {"single_game": counting, "single_season": full, "career": full}
    scopes = [("single_game", "Single-Game Records"), ("single_season", "Single-Season Records"),
              ("career", "Career Records")]
    for scope, label in scopes:
        story.append(Paragraph("The Record Book — " + label, ST["h1"]))
        note = "Every franchise leaderboard from the site — top 5 (all team-name eras as one club; playoffs included)."
        if scope == "single_game":
            note = "Single-game franchise leaders, top 5 — counting stats only (no rate records)."
        story.append(Paragraph(note + " Bold = the record value.", ST["cap"]))
        boards = d["boards"][scope]
        minis = [_record_mini(boards[stat], stat, scope) for stat in feature_by[scope]
                 if stat in boards and not boards[stat].empty]
        n = len(minis)
        pages = max(1, (n + 9) // 10)          # ~10 mini-tables per page
        per = (n + pages - 1) // pages          # balanced evenly across those pages
        for ci in range(0, n, per):
            story.append(_grid2(minis[ci:ci + per]))
            story.append(PageBreak())


def sec_impact(story, d):
    story.append(Paragraph("Season Impact &amp; Highlights", ST["h1"]))
    # records set in 2026 (single_season + single_game #1 from this season)
    set_lines = []
    for scope in ("single_season", "single_game"):
        for stat, b in d["boards"][scope].items():
            if b.empty or "Season" not in b.columns:
                continue
            top = b.iloc[0]
            if str(top["Season"]).strip() in (SEASON, abbr(SEASON), "Spr 2026", "2026 Spr"):
                kind = "single-season" if scope == "single_season" else "single-game"
                val = f3(top[stat]) if stat in ("OPS", "AVG", "OBP", "SLG") else i0(top[stat])
                set_lines.append(f"<b>{top['Player']}</b> holds the {kind} <b>{stat}</b> record ({val}).")
    story.append(Paragraph("Records Set or Held by 2026 Hitters", ST["h2"]))
    if set_lines:
        story.append(ListFlowable([ListItem(Paragraph(s, ST["bullet"])) for s in set_lines[:14]],
                                  bulletType="bullet", start="•"))
    else:
        story.append(Paragraph("No franchise records changed hands this season.", ST["body"]))

    # highlight games (top game scores this season)
    story.append(Paragraph("Highlight Games of the Spring", ST["h2"]))
    sg = d["sg_season"].sort_values("game_score", ascending=False).head(6)
    rows = [["Player", "Date", "Opponent", "Line", "GS"]]
    for _, r in sg.iterrows():
        line = f"{i0(r['hits'])}-{i0(r['ab'])}, {i0(r['hr'])}HR {i0(r['rbi'])}RBI {i0(r['r'])}R"
        rows.append([r["player"], format_display_date(r["game_date"]), r["opponent"], line, f"{r['game_score']:.1f}"])
    story.append(stat_table(rows, [0.95 * inch, 0.8 * inch, 1.3 * inch, 2.6 * inch, 0.55 * inch]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "The season's signature swing: <b>Joel's 2-homer, 7-RBI explosion on opening night vs Bullseyes</b>, "
        "still the loudest single game any Maple Tree hitter put up all spring.", ST["callout"]))
    story.append(PageBreak())


def sec_glossary(story, d):
    story.append(Paragraph("Glossary", ST["h1"]))
    items = [
        ("AVG", "Batting average — hits per at-bat."),
        ("OBP", "On-base percentage — how often a hitter reaches base (hits + walks per plate appearance)."),
        ("SLG", "Slugging — total bases per at-bat; rewards extra-base hits."),
        ("OPS", "On-base plus slugging. The quick all-in-one bat rating; .900+ is excellent."),
        ("wRC+", "Weighted Runs Created Plus. Run creation indexed so 100 = team average; 150 = 50% better."),
        ("ISO", "Isolated power — SLG minus AVG; pure extra-base pop."),
        ("oWAR", "Offensive Wins Above Replacement — runs a hitter adds over a bench-level bat, in wins."),
        ("Game Score", "Single-game performance rating from the batting line (hits, extra bases, walks, runs, RBI)."),
        ("RISP", "Runners In Scoring Position — batting average with runners on 2nd or 3rd."),
        ("Archetype", "A hitter profile (e.g., Patient Cornerstone, Power Bat) from on-base vs. power tendencies."),
    ]
    rows = [["Stat", "What it means"]]
    for k, v in items:
        rows.append([k, v])
    story.append(simple_table(rows, [1.0 * inch, 5.6 * inch]))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Maple Tree Dashboard · Spring 2026 Season Review", ST["cap"]))


# =====================================================================================
# main
# =====================================================================================
def main():
    conn = get_connection()
    d = gather(conn)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(OUT), pagesize=LETTER, leftMargin=0.7 * inch, rightMargin=0.7 * inch,
                            topMargin=0.7 * inch, bottomMargin=0.6 * inch,
                            title="Maple Tree — Spring 2026 Season Review", author="Maple Tree Dashboard")
    story = []
    sec_cover(story, d)
    sec_story(story, d)
    sec_playoffs(story, d)
    sec_awards(story, d)
    sec_team_by_year(story, d)
    sec_this_season(story, d)
    sec_advanced(story, d)
    sec_opponents(story, d)
    # player pages (active roster, ordered by current-season OPS)
    order = {c: i for i, c in enumerate(d["cur"]["canonical_name"])}
    actives = sorted(d["active"], key=lambda dc: order.get(dc[1], 999))
    for display, canonical in actives:
        sec_player(story, conn, canonical, display, d)
    sec_career_leaderboard(story, d)
    sec_record_book(story, d)
    sec_impact(story, d)
    if story and isinstance(story[-1], PageBreak):
        story.pop()
    doc.build(story)
    conn.close()
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
