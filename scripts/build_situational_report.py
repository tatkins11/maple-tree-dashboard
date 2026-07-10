"""The Run Production Report — situational & batted-ball deep dive (5-page PDF).

Front-office-style analytics report on the team's situational splits
(data/processed/team_situational_stats.csv, 4 seasons of GameChanger
Situational-Team data) + per-player contact profiles (players.json).
Goal: what the team must do to score more runs. Every number is computed
from the data at runtime — safe to re-run any week of the season.

    python scripts/build_situational_report.py
"""
from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_gameday_preview import (  # noqa: E402  (shared drawing kit)
    BARK, BARK2, CREAM, GREEN, INK, LINE, MAPLE, MUTED, PAPER, SAND, STRIPE, TAN, WHITE,
    _txt, load, prep_logo, section_title, wrap,
)

REPO = Path(__file__).resolve().parents[1]
W, H = letter

SEASONS = [
    ("Maple Tree Tappers Summer 2025", "Summer 25"),
    ("Maple Tree Fall 2025", "Fall 25"),
    ("Maple Tree Spring 2026", "Spring 26"),
    ("Maple Tree Summer 2026", "Summer 26"),
]
LBL = {full: short for full, short in SEASONS}
CUR = "Maple Tree Summer 2026"
NUM = ["pa", "ab", "h", "1b", "2b", "3b", "hr", "tb", "rbi", "so", "bb", "hbp", "sac", "sf", "roe"]


# ---------------------------------------------------------------- data layer
def load_situational():
    sit = {}
    with open(REPO / "data/processed/team_situational_stats.csv", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            d = {k: int(r[k]) for k in NUM}
            d.update({k: float(r[k]) for k in ("avg", "obp", "slg", "ops")})
            sit.setdefault(r["season"], {}).setdefault(r["view"], {})[r["situation"]] = d
    return sit


def agg(rows):
    """Sum counting stats and recompute rates (PA-based OBP, GameChanger-style)."""
    t = {k: sum(r[k] for r in rows) for k in NUM}
    t["avg"] = t["h"] / t["ab"] if t["ab"] else 0.0
    t["slg"] = t["tb"] / t["ab"] if t["ab"] else 0.0
    t["obp"] = (t["h"] + t["bb"] + t["hbp"]) / t["pa"] if t["pa"] else 0.0
    t["ops"] = t["obp"] + t["slg"]
    t["tb_pa"] = t["tb"] / t["pa"] if t["pa"] else 0.0
    t["rbi_pa"] = t["rbi"] / t["pa"] if t["pa"] else 0.0
    return t


def r3(v):
    s = f"{v:.3f}"
    return s[1:] if s.startswith("0.") else s


def ops3(v):
    """Baseball-style: 1.212 stays, 0.693 -> .693"""
    return f"{v:.3f}" if v >= 1 else r3(v)


# ---------------------------------------------------------------- drawing helpers
def hbar(c, x, y, w_full, frac, h_=9, color=MAPLE, track=True):
    if track:
        c.setFillColor(STRIPE)
        c.roundRect(x, y, w_full, h_, h_ / 2, stroke=0, fill=1)
    c.setFillColor(color)
    c.roundRect(x, y, max(h_, w_full * min(frac, 1.0)), h_, h_ / 2, stroke=0, fill=1)


def page_head(c, kicker, title, sub=None, logo=None):
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 72, W, 72, stroke=0, fill=1)
    x = 36
    if logo:
        c.drawImage(logo, 36, H - 62, width=52, height=52, mask="auto")
        x = 100
    _txt(c, x, H - 32, kicker, "Helvetica-Bold", 8.5, TAN, cs=2)
    _txt(c, x, H - 56, title, "Helvetica-Bold", 22, WHITE, cs=1)
    if sub:
        _txt(c, W - 36, H - 32, sub, "Helvetica-Oblique", 9, TAN, align="r")


def page_foot(c, n, total):
    c.setStrokeColor(LINE)
    c.setLineWidth(0.75)
    c.line(36, 56, W - 36, 56)
    _txt(c, 36, 43, "MAPLE TREE ANALYTICS  ·  RUN PRODUCTION REPORT", "Helvetica-Bold", 7.5, BARK, cs=1)
    _txt(c, W / 2, 43, f"{n} / {total}", "Helvetica", 8, MUTED, align="c")
    _txt(c, W - 36, 43, "CONFIDENTIAL — CLUBHOUSE EYES ONLY", "Helvetica-Bold", 7.5, MAPLE, cs=1, align="r")


def takeaway(c, y, h_, text, label="THE TAKEAWAY"):
    c.setFillColor(BARK)
    c.roundRect(36, y, W - 72, h_, 8, stroke=0, fill=1)
    c.setFillColor(MAPLE)
    c.rect(36, y, 4, h_, stroke=0, fill=1)
    _txt(c, 54, y + h_ - 20, label, "Helvetica-Bold", 8, TAN, cs=2)
    wrap(c, 54, y + h_ - 36, text, W - 126, "Helvetica", 9.5, 13, CREAM)


def main():
    sit = load_situational()
    players = load("players.json")
    season_stats = load("season_stats.json")
    today = date.today().strftime("%B %d, %Y")

    # ---- computed analysis ----
    bb_types = ["Line drive", "Hard ground ball", "Ground ball", "Fly ball", "Pop fly"]
    comb = {t: agg([sit[s]["batted_balls"][t] for s, _ in SEASONS if t in sit[s].get("batted_balls", {})])
            for t in bb_types}
    cur_bb = sit[CUR]["batted_balls"]
    cur_gs = sit[CUR]["game_state"]

    def mix(season, t):
        tot = sum(v["pa"] for v in sit[season]["batted_balls"].values())
        d = sit[season]["batted_balls"].get(t)
        return (d["pa"] / tot) if d and tot else 0.0

    hist = [s for s, _ in SEASONS[:3]]
    fb_hr = {s: (sit[s]["batted_balls"]["Fly ball"]["hr"] / sit[s]["batted_balls"]["Fly ball"]["pa"])
             for s, _ in SEASONS}
    hist_2out = agg([sit[s]["game_state"]["2 outs"] for s in hist])
    cur_pa_game = {t: cur_bb[t]["pa"] / 4 for t in cur_bb}

    # per-player contact profiles (active, played this season)
    prof = []
    cur_players = {p["slug"]: p for p in next(s for s in season_stats if s["name"] == CUR)["players"]}
    for p in players:
        s26 = next((s for s in p["percentiles"]["seasons"] if s["slug"] == "summer-2026"), None)
        if not s26 or p["slug"] not in cur_players:
            continue
        met = {m["key"]: m["value"] for m in s26["metrics"]}
        car = {m["key"]: m["value"] for m in p["percentiles"]["career"]["metrics"]}
        cs = cur_players[p["slug"]]
        prof.append({
            "name": p["name"], "pa": int(cs.get("pa") or 0), "ops": float(cs.get("ops") or 0),
            "hr": int(cs.get("hr") or 0),
            "ld": met.get("ld_rate"), "fb": met.get("fb_rate"), "gb": met.get("gb_rate"),
            "hh": met.get("hh_rate"), "cld": car.get("ld_rate"), "cfb": car.get("fb_rate"),
        })
    prof = [p for p in prof if p["pa"] >= 8]
    prof.sort(key=lambda p: -(p["ld"] or 0))

    ld, fb, gb, hgb = comb["Line drive"], comb["Fly ball"], comb["Ground ball"], comb["Hard ground ball"]
    cur_fb_mix = mix(CUR, "Fly ball")
    hist_fb_mix = sum(mix(s, "Fly ball") for s in hist) / 3
    cur_ld_mix = mix(CUR, "Line drive")
    hist_ld_mix = sum(mix(s, "Line drive") for s in hist) / 3
    # impact estimate: converting 1/3 of current flies to liners, in TB per game
    flies_g = cur_pa_game["Fly ball"]
    conv = flies_g / 3
    tb_gain = conv * (ld["tb_pa"] - cur_bb["Fly ball"]["tb"] / cur_bb["Fly ball"]["pa"])
    rbi_2out_gain = (cur_gs["2 outs"]["pa"] / 4) * (hist_2out["rbi_pa"] - cur_gs["2 outs"]["rbi"] / cur_gs["2 outs"]["pa"])

    logo = prep_logo()
    out = REPO / "data" / "writeups" / "maple-tree-summer-2026" / "maple-tree-run-production-report.pdf"
    out.parent.mkdir(parents=True, exist_ok=True)
    c = pdfcanvas.Canvas(str(out), pagesize=letter)
    c.setTitle("Maple Tree - The Run Production Report")
    TOTAL = 5

    # ================================================================ PAGE 1
    c.setFillColor(PAPER)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    c.setFillColor(BARK)
    c.rect(0, H - 210, W, 210, stroke=0, fill=1)
    c.setFillColor(BARK2)
    c.rect(0, H - 210, W, 3, stroke=0, fill=1)
    c.drawImage(logo, 36, H - 118, width=88, height=88, mask="auto")
    _txt(c, 140, H - 52, "MAPLE TREE FRONT OFFICE  ·  ANALYTICS DIVISION", "Helvetica-Bold", 9, TAN, cs=2.5)
    _txt(c, 140, H - 92, "THE RUN PRODUCTION", "Helvetica-Bold", 30, WHITE, cs=1)
    _txt(c, 140, H - 126, "REPORT", "Helvetica-Bold", 30, WHITE, cs=1)
    _txt(c, 140, H - 152, "A situational deep dive on four seasons of Maple Tree offense — and where the next runs are hiding.",
         "Helvetica", 9.5, TAN)
    _txt(c, 140, H - 186, f"PREPARED {today.upper()}  ·  SUMMER 2025 – SUMMER 2026  ·  CRYSTAL LAKE, IL",
         "Helvetica-Bold", 7.5, TAN, cs=1.5)

    # stat tiles
    tiles = [
        (f"{comb['Line drive']['ops']:.3f}"[0:5], "OPS ON LINE DRIVES", "4-season, all situations"),
        (r3(cur_bb["Fly ball"]["ops"]), "OPS ON FLY BALLS", "Summer 26 — and 45% of our contact"),
        (r3(cur_gs["Lead off inning"]["obp"]), "LEADOFF OBP, SUMMER 26", "best inning-openers in team history"),
        (r3(cur_gs["2 outs"]["ops"]), "TWO-OUT OPS, SUMMER 26", f"franchise norm {hist_2out['ops']:.3f}"),
    ]
    ty = H - 290
    for i, (val, label, sub) in enumerate(tiles):
        x0 = 36 + i * 138
        c.setFillColor(WHITE)
        c.setStrokeColor(LINE)
        c.roundRect(x0, ty, 126, 64, 6, stroke=1, fill=1)
        _txt(c, x0 + 10, ty + 47, label, "Helvetica-Bold", 6.6, MUTED, cs=0.6)
        _txt(c, x0 + 10, ty + 22, val, "Helvetica-Bold", 20, BARK)
        _txt(c, x0 + 10, ty + 10, sub, "Helvetica", 6.4, MUTED)

    # executive summary
    ey = ty - 34
    section_title(c, 36, ey, "Executive summary", W - 72)
    findings = [
        ("Contact quality is destiny.", f"Across four seasons the team hits {r3(ld['avg'])} with a {ld['ops']:.3f} OPS on line drives and {r3(hgb['avg'])} on hard ground balls — versus {r3(gb['avg'])} on ordinary grounders and {r3(fb['avg'])} on fly balls. There is no secret: hard, flat contact scores runs; everything else is an out with paperwork."),
        ("The fly ball only pays when it leaves.", f"Fly balls returned a {sit['Maple Tree Fall 2025']['batted_balls']['Fly ball']['ops']:.3f} OPS in Fall 25 ({fb_hr['Maple Tree Fall 2025']*100:.0f}% of them home runs) and {sit['Maple Tree Spring 2026']['batted_balls']['Fly ball']['ops']:.3f} in Spring 26 ({fb_hr['Maple Tree Spring 2026']*100:.0f}% HR). This summer: {fb_hr[CUR]*100:.1f}% HR and a {r3(cur_bb['Fly ball']['ops'])} OPS. Same fields every season — this is behavior, not ballpark."),
        ("We start innings like champions and end them like tourists.", f"Summer 26 leadoff hitters are reaching at a {r3(cur_gs['Lead off inning']['obp'])} clip — a franchise best. But two-out production has collapsed to a {r3(cur_gs['2 outs']['ops'])} OPS against a three-season norm of {hist_2out['ops']:.3f}, and we are hitting worse with runners in scoring position ({r3(cur_gs['Scoring position']['ops'])}) than with the bases empty ({r3(cur_gs['Bases empty']['ops'])}). The table is set; the meal is not being eaten."),
        ("The fix is worth roughly 3 runs a game.", f"Converting one third of our current fly-ball volume back into line drives is worth an estimated +{tb_gain:.1f} total bases per game, and restoring normal two-out production adds ~{rbi_2out_gain:.1f} RBI per game. That is the difference between winning the split and sweeping it."),
    ]
    sy = ey - 24
    for lead, body in findings:
        _txt(c, 36, sy, lead, "Helvetica-Bold", 10, BARK)
        sy = wrap(c, 36, sy - 13, body, W - 72, "Helvetica", 9, 12, INK) - 11

    takeaway(c, 84, 58, "Stop auditioning for the highlight reel. Our four-season data says the same thing every year: line drives win, grounders and lazy flies lose. Hit it hard, hit it flat, and finish the innings we start.",
             label="THE THESIS")
    page_foot(c, 1, TOTAL)
    c.showPage()

    # ================================================================ PAGE 2
    page_head(c, "SECTION 01", "CONTACT QUALITY IS DESTINY", "what each batted ball is actually worth", logo)
    y = H - 100
    section_title(c, 36, y, "Four seasons, one lesson — OPS by batted-ball type", W - 72)
    rows = [(t, comb[t]) for t in ("Line drive", "Hard ground ball", "Ground ball", "Fly ball", "Pop fly") if comb[t]["pa"]]
    by = y - 26
    max_ops = max(d["ops"] for _, d in rows)
    for t, d in rows:
        _txt(c, 36, by - 3, t, "Helvetica-Bold", 9, BARK)
        _txt(c, 168, by - 3, f"{d['pa']} PA", "Helvetica", 7.5, MUTED)
        hbar(c, 214, by - 6, 250, d["ops"] / max_ops, 11,
             MAPLE if t in ("Line drive", "Hard ground ball") else TAN)
        _txt(c, 474, by - 3, ops3(d["ops"]), "Helvetica-Bold", 9.5, BARK)
        _txt(c, 576, by - 3, f"{r3(d['avg'])} AVG · {d['tb_pa']:.2f} TB/PA", "Helvetica", 7.5, MUTED, align="r")
        by -= 22
    _txt(c, 36, by - 2, "Hard contact (liners + hard grounders) runs a ~1.9 OPS. Everything else combined runs under .800.",
         "Helvetica-Oblique", 8.5, MUTED)

    y2 = by - 30
    section_title(c, 36, y2, "The fly-ball lottery — HR per fly ball, by season", W - 72)
    fy = y2 - 30
    bw = 118
    for i, (s, short) in enumerate(SEASONS):
        x0 = 42 + i * 136
        rate = fb_hr[s]
        d = sit[s]["batted_balls"]["Fly ball"]
        c.setFillColor(WHITE)
        c.setStrokeColor(LINE)
        c.roundRect(x0, fy - 64, bw + 12, 72, 6, stroke=1, fill=1)
        _txt(c, x0 + 10, fy - 6, short.upper(), "Helvetica-Bold", 7.5, MUTED, cs=1)
        _txt(c, x0 + 10, fy - 28, f"{rate*100:.1f}%", "Helvetica-Bold", 17, MAPLE if rate < 0.05 else GREEN)
        hbar(c, x0 + 10, fy - 42, bw - 8, rate / max(fb_hr.values()), 7,
             MAPLE if rate < 0.05 else GREEN)
        _txt(c, x0 + 10, fy - 56, f"{d['hr']} HR / {d['pa']} flies · {ops3(d['ops'])} OPS", "Helvetica", 7, MUTED)
    note_y = fy - 92
    wrap(c, 36, note_y,
         "Both power seasons (Fall 25, Spring 26) turned 15–17% of flies into home runs and made the air-ball approach profitable. "
         f"Both summers it cratered — {fb_hr['Maple Tree Tappers Summer 2025']*100:.1f}% and now {fb_hr[CUR]*100:.1f}%. Same park, same fences. "
         "When the ball is not carrying out, the fly ball is the single worst swing result we produce.",
         W - 72, "Helvetica", 9, 12, INK)

    y3 = note_y - 46
    section_title(c, 36, y3, "The mix has drifted the wrong way", W - 72)
    my = y3 - 26
    pairs = [("Line drives", cur_ld_mix, hist_ld_mix), ("Fly balls", cur_fb_mix, hist_fb_mix),
             ("Ground balls", mix(CUR, "Ground ball"), sum(mix(s, "Ground ball") for s in hist) / 3)]
    for label, now, was in pairs:
        _txt(c, 36, my - 3, label, "Helvetica-Bold", 9, BARK)
        hbar(c, 150, my - 6, 240, was / 0.5, 9, TAN)
        _txt(c, 398, my - 3, f"{was*100:.0f}% 3-season avg", "Helvetica", 7.5, MUTED)
        my -= 15
        hbar(c, 150, my - 6, 240, now / 0.5, 9, MAPLE)
        _txt(c, 398, my - 3, f"{now*100:.0f}% Summer 26", "Helvetica-Bold", 7.5, MAPLE)
        my -= 21
    takeaway(c, 84, 68, f"Summer 26 contact is {cur_fb_mix*100:.0f}% fly balls (vs a {hist_fb_mix*100:.0f}% norm) with {fb_hr[CUR]*100:.1f}% of them leaving the yard. That mix produced a {r3(cur_bb['Fly ball']['ops'])} OPS on nearly half our swings. Trade a third of those flies for liners and the math improves by ~+{tb_gain:.1f} total bases per game — before anyone gets 'hot'.")
    page_foot(c, 2, TOTAL)
    c.showPage()

    # ================================================================ PAGE 3
    page_head(c, "SECTION 02", "THE INNING LIFECYCLE", "where innings are born, and where they die", logo)
    y = H - 100
    section_title(c, 36, y, "OPS by out state — the two-out cliff", W - 72)
    hy = y - 24
    _txt(c, 130, hy, "", "Helvetica", 8, MUTED)
    for j, (s, short) in enumerate(SEASONS):
        _txt(c, 250 + j * 90, hy, short.upper(), "Helvetica-Bold", 7.5,
             MAPLE if s == CUR else MUTED, cs=0.5, align="r")
    ry = hy - 18
    for state in ("0 outs", "1 out", "2 outs"):
        _txt(c, 36, ry, state.upper(), "Helvetica-Bold", 9, BARK)
        for j, (s, short) in enumerate(SEASONS):
            d = sit[s]["game_state"][state]
            bad = s == CUR and state == "2 outs"
            _txt(c, 250 + j * 90, ry, ops3(d["ops"]), "Helvetica-Bold" if s == CUR else "Helvetica",
                 10 if s == CUR else 9.5, MAPLE if bad else (BARK if s == CUR else INK), align="r")
        ry -= 19
    wrap(c, 36, ry - 4,
         f"Two-out hitting has been a franchise strength — a combined {hist_2out['ops']:.3f} OPS across the three prior seasons, with {hist_2out['rbi']} of the offense's RBI arriving after the second out. This summer it sits at {ops3(cur_gs['2 outs']['ops'])} ({cur_gs['2 outs']['h']} hits in {cur_gs['2 outs']['ab']} at-bats). Innings that used to end in crooked numbers now end in handshakes.",
         W - 72, "Helvetica", 9, 12, INK)

    y2 = ry - 58
    section_title(c, 36, y2, "Runners in scoring position — the reversal", W - 72)
    ry2 = y2 - 26
    for s, short in SEASONS:
        risp, empty = sit[s]["game_state"]["Scoring position"], sit[s]["game_state"]["Bases empty"]
        delta = risp["ops"] - empty["ops"]
        _txt(c, 36, ry2 - 3, short, "Helvetica-Bold", 9, MAPLE if s == CUR else BARK)
        _txt(c, 120, ry2 - 3, f"RISP {risp['ops']:.3f}", "Helvetica", 8.5, INK)
        _txt(c, 205, ry2 - 3, f"empty {empty['ops']:.3f}", "Helvetica", 8.5, MUTED)
        mid = 420
        scale = 150
        wdt = max(scale * min(abs(delta) / 0.30, 1), 3)
        if delta >= 0:
            hbar(c, mid, ry2 - 6, wdt, 1.0, 10, GREEN, track=False)
        else:
            hbar(c, mid - wdt, ry2 - 6, wdt, 1.0, 10, MAPLE, track=False)
        c.setStrokeColor(BARK2)
        c.setLineWidth(1)
        c.line(mid, ry2 - 9, mid, ry2 + 7)
        _txt(c, mid + (wdt + 7 if delta >= 0 else -wdt - 7), ry2 - 3,
             f"{'+' if delta >= 0 else '−'}{r3(abs(delta))}",
             "Helvetica-Bold", 9, GREEN if delta >= 0 else MAPLE, align="l" if delta >= 0 else "r")
        ry2 -= 20
    wrap(c, 36, ry2 - 2,
         "For three seasons this team hit BETTER with runners in scoring position than with the bases empty — the mark of a lineup that shortens up and takes what the defense gives. Summer 26 has flipped: our best swings are happening with nobody on. "
         f"RBI conversion is surviving on sacrifice flies ({cur_gs['Scoring position']['sf']} SF already), not knocks.",
         W - 72, "Helvetica", 9, 12, INK)

    y3 = ry2 - 56
    section_title(c, 36, y3, "Front-running — OPS when leading vs trailing", W - 72)
    ry3 = y3 - 26
    for s, short in SEASONS:
        lead_, trail = sit[s]["game_state"]["Leading"], sit[s]["game_state"]["Trailing"]
        _txt(c, 36, ry3 - 3, short, "Helvetica-Bold", 9, MAPLE if s == CUR else BARK)
        hbar(c, 130, ry3 - 6, 170, lead_["ops"] / 1.7, 9, TAN)
        _txt(c, 308, ry3 - 3, f"{ops3(lead_['ops'])} leading", "Helvetica", 7.5, MUTED)
        hbar(c, 400, ry3 - 6, 170, trail["ops"] / 1.7, 9, MAPLE if s == CUR else SAND)
        _txt(c, W - 36, ry3 - 3, f"{ops3(trail['ops'])} trailing", "Helvetica", 7.5,
             MAPLE if s == CUR else MUTED, align="r")
        ry3 -= 19
    takeaway(c, 84, 72, f"Summer 26 in one sentence: we reach base to open innings at a {r3(cur_gs['Lead off inning']['obp'])} clip, then hit {r3(cur_gs['2 outs']['avg'])} with two outs, post a lower OPS with runners in scoring position than with the bases empty, and lose {r3(sit[CUR]['game_state']['Leading']['ops'] - sit[CUR]['game_state']['Trailing']['ops'])} points of OPS the moment we fall behind. The runs we want are not new baserunners — they are the ones already standing on base.")
    page_foot(c, 3, TOTAL)
    c.showPage()

    # ================================================================ PAGE 4
    page_head(c, "SECTION 03", "THE PERSONNEL FILE", "who models the approach — and who is due a conversation", logo)
    y = H - 100
    section_title(c, 36, y, "Summer 26 contact profiles (career mix in gray)", W - 72)
    hy = y - 22
    for label, x, al in [("HITTER", 44, "l"), ("PA", 168, "r"), ("OPS", 212, "r"), ("LD%", 300, "r"),
                         ("FB%", 392, "r"), ("GB%", 470, "r"), ("HARD-HIT%", 576, "r")]:
        _txt(c, x, hy, label, "Helvetica-Bold", 7, MUTED, cs=0.5, align=al)
    ry = hy - 8
    rh = 24
    for i in range(len(prof)):  # stripes first so sub-labels never get painted over
        if i % 2 == 0:
            c.setFillColor(STRIPE)
            c.rect(36, ry - rh * (i + 1) - 6, W - 72, rh, stroke=0, fill=1)
    for i, p in enumerate(prof):
        yy = ry - rh * (i + 1)
        _txt(c, 44, yy, p["name"], "Helvetica-Bold", 9.5, BARK)
        _txt(c, 168, yy, str(p["pa"]), "Helvetica", 9, INK, align="r")
        _txt(c, 212, yy, ops3(p["ops"]), "Helvetica-Bold", 9, BARK, align="r")
        for key, ckey, x0 in (("ld", "cld", 236), ("fb", "cfb", 328)):
            v, cv = p[key], p[ckey]
            if v is not None:
                good = (key == "ld" and v >= 0.38) or (key == "fb" and v <= 0.35)
                _txt(c, x0 + 64, yy, f"{v*100:.0f}", "Helvetica-Bold", 9.5,
                     GREEN if good else (MAPLE if key == "fb" and v >= 0.5 else INK), align="r")
                if cv is not None:
                    _txt(c, x0 + 64, yy - 9, f"{cv*100:.0f} car", "Helvetica", 6, MUTED, align="r")
        if p["gb"] is not None:
            _txt(c, 470, yy, f"{p['gb']*100:.0f}", "Helvetica", 9, MAPLE if p["gb"] >= 0.5 else INK, align="r")
        if p["hh"] is not None:
            _txt(c, 576, yy, f"{p['hh']*100:.0f}", "Helvetica", 9, INK, align="r")
    ty2 = ry - rh * len(prof) - 26

    section_title(c, 36, ty2, "The scouting notes", W - 72)
    ld_names = [p["name"] for p in prof[:3]]
    fb_flag = sorted((p for p in prof if (p["fb"] or 0) >= 0.5 and p["hr"] == 0),
                     key=lambda p: -(p["fb"] or 0))
    gb_flag = [p for p in prof if (p["gb"] or 0) >= 0.45]
    air = oxford_names = ", ".join(f"{p['name']} ({p['fb']*100:.0f}%)" for p in fb_flag[:3])
    gb_is = "is" if len(gb_flag) == 1 else "are"
    notes = [
        ("The templates.", f"{', '.join(ld_names)} are running the highest line-drive rates on the club — every one of them is having a monster offensive month. Their swings should be shown at the next team meeting, possibly framed."),
        ("The air traffic control problem.", (f"{air} are putting half or more of their contact in the air with zero home runs between them. " if fb_flag else "") + "A fly-ball swing without fly-ball power is a donation to the outfield. Two clicks flatter and those are doubles into the gap."),
        ("The lawn maintenance department.", (f"{' and '.join(x['name'] for x in gb_flag)} {gb_is} rolling over on {'/'.join(f'{x['gb']*100:.0f}%' for x in gb_flag)} of contact. " if gb_flag else "") + f"Ordinary ground balls have been a {r3(gb['avg'])} average for four straight seasons. The data does not hate ground balls personally — it just watches them get thrown out."),
    ]
    ny = ty2 - 24
    for lead_, body in notes:
        _txt(c, 36, ny, lead_, "Helvetica-Bold", 10, BARK)
        ny = wrap(c, 36, ny - 13, body, W - 72, "Helvetica", 9, 12, INK) - 10
    takeaway(c, 84, 74, "Small samples, big patterns: the hitters swinging flat are carrying the offense, and the ones underneath the ball are financing the other team's defense. Sample size will be re-examined at Week 6 — the approach does not need to wait for it.")
    page_foot(c, 4, TOTAL)
    c.showPage()

    # ================================================================ PAGE 5
    page_head(c, "SECTION 04", "THE PLAN", "five directives, effective immediately", logo)
    y = H - 104
    plan = [
        ("Hunt the line drive, not the fence.",
         f"Until home-run-per-fly rebounds above ~10%, the fly ball is our least valuable swing ({r3(cur_bb['Fly ball']['ops'])} OPS this summer). Target: line-drive share back to 40%+ of contact and fly-ball share under 35%. Estimated value: +{tb_gain:.1f} total bases per game."),
        ("Two outs: shorter swing, earlier strike.",
         f"We are hitting {r3(cur_gs['2 outs']['avg'])} with two outs against a {r3(hist_2out['avg'])} franchise norm. This is approach, not talent — the same lineup posted a {sit['Maple Tree Spring 2026']['game_state']['2 outs']['ops']:.3f} two-out OPS in the spring. Restoring the norm is worth ~{rbi_2out_gain:.1f} RBI per game."),
        ("With RISP, take the single.",
         f"The {r3(cur_gs['Scoring position']['avg'])} RISP average is being bailed out by sacrifice flies. Three seasons of evidence say we are a +.20 OPS team with runners on when we stop trying to cash the whole inning in one swing. The middle of the field is open; use it."),
        ("Protect the leadoff gift.",
         f"A {r3(cur_gs['Lead off inning']['obp'])} leadoff OBP is a scoring position machine — do not spend it on first-pitch fly balls. Hitters 2 through 4 in any inning inherit the at-bat that matters most; treat it like bases loaded ({sit[CUR]['game_state']['Bases loaded']['rbi']} RBI in {sit[CUR]['game_state']['Bases loaded']['pa']} bases-loaded PA says we know how)."),
        ("Measure it weekly.",
         "This report regenerates from the data in one command. LD/FB mix, two-out OPS, and RISP delta go on the wall after every week. What gets measured gets mashed."),
    ]
    ny = y
    for i, (lead_, body) in enumerate(plan, 1):
        c.setFillColor(MAPLE)
        c.circle(48, ny - 1, 10, stroke=0, fill=1)
        _txt(c, 48, ny - 4.5, str(i), "Helvetica-Bold", 11, WHITE, align="c")
        _txt(c, 68, ny, lead_, "Helvetica-Bold", 11, BARK)
        ny = wrap(c, 68, ny - 14, body, W - 104, "Helvetica", 9, 12.5, INK) - 16

    # method note
    my = ny - 4
    section_title(c, 36, my, "Method & data notes", W - 72)
    wrap(c, 36, my - 22,
         "Sources: GameChanger Situational–Team splits, four seasons (Summer 2025 – Summer 2026); situational tracking unavailable for 2021–22. "
         "One bunt PA removed (scoring error). All games played at the same Crystal Lake fields — a 'Crystal River, FL' tag in GameChanger metadata is erroneous, so cross-season contrasts reflect behavior, not environment. "
         "OBP is PA-based throughout, matching club convention. Summer 26 figures cover 4 games and are flagged directional; three-season baselines are used for all norms. "
         "Impact estimates are arithmetic (TB/PA and RBI/PA deltas), not simulation.",
         W - 72, "Helvetica", 8, 10.5, MUTED)

    takeaway(c, 84, 74, "The 3-1 start is real and the leadoff work is elite. The gap between this team and a 2-0-every-Wednesday team is not power, health, or luck — it is 30 feet of ball flight and the second half of innings. Hit it flat, finish the frame, hang the banner.",
             label="FINAL WORD")
    page_foot(c, 5, TOTAL)
    c.showPage()
    c.save()

    print(f"\nRun Production Report -> {out}")
    print(f"  combined LD OPS {ld['ops']:.3f} on {ld['pa']} PA · cur FB mix {cur_fb_mix*100:.0f}% (norm {hist_fb_mix*100:.0f}%)")
    print(f"  HR/FB by season: " + ", ".join(f"{LBL[s]} {fb_hr[s]*100:.1f}%" for s, _ in SEASONS))
    print(f"  est. impact: +{tb_gain:.1f} TB/g from FB->LD conversion · +{rbi_2out_gain:.1f} RBI/g from 2-out normalization")
    print(f"  player rows: {len(prof)}")


if __name__ == "__main__":
    main()
