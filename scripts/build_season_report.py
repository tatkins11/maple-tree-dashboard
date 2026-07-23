"""Maple Tree — season stat report (3 pages, branded + visual).

  P1 THE TEAM     record, slash line, runs-by-game chart, game log, seed race
  P2 THE ROSTER   full per-player batting table with inline OPS bars
  P3 LEADERS      category leaderboards + advanced table + milestone watch

Run:  python scripts/build_season_report.py [--season summer-2026]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from reportlab.lib.colors import HexColor, Color
from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as rl_canvas

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "site" / "src" / "data"
LOGO = Path("C:/Slowpitch/Logo/Maple Tree Logo - restored transparent.png")

BARK, BARK2 = HexColor("#2c1a0d"), HexColor("#4a2e15")
INK, MUTED, MAPLE = HexColor("#20261f"), HexColor("#77705f"), HexColor("#c2410c")
PAPER, CARD, LINE = HexColor("#faf7f2"), HexColor("#ffffff"), HexColor("#e7ded0")
GOLD, GREEN, RED = HexColor("#b8860b"), HexColor("#15803d"), HexColor("#b91c1c")
W, H = letter
MINPA = 10  # qualifier for rate-stat leaderboards


def load(name):
    return json.loads((DATA / f"{name}.json").read_text(encoding="utf-8"))


def _txt(c, x, y, s, font="Helvetica", size=10, color=INK, cs=0, align="l"):
    c.setFont(font, size); c.setFillColor(color)
    fn = {"l": c.drawString, "r": c.drawRightString, "c": c.drawCentredString}[align]
    if cs:
        fn(x, y, str(s), charSpace=cs)
    else:
        fn(x, y, str(s))


def r3(v):
    return f"{float(v):.3f}".lstrip("0") if v else ".000"


def _bb_from_csv(path: Path):
    """Team batted-ball profile from a GameChanger season export, weighted by balls in
    play (AB - SO + SF). The header repeats names across Batting/Pitching/Fielding —
    always take the FIRST occurrence, which is the batting block."""
    import csv as _csv
    rows = list(_csv.reader(open(path, encoding="utf-8-sig")))
    if len(rows) < 3:
        return None
    idx = {}
    for i, nm in enumerate(rows[1]):
        n = (nm or "").strip()
        if n and n not in idx:
            idx[n] = i
    if not {"AB", "SO", "SF", "HHB", "LD%", "FB%", "GB%"} <= set(idx):
        return None
    tot = {"batted": 0.0, "hhb": 0.0, "ld": 0.0, "fb": 0.0, "gb": 0.0}
    for r in rows[2:]:
        if not r or (r[0] or "").strip().lower() == "totals":
            continue
        if not ((r[2] or "").strip() or (r[1] or "").strip()):
            continue

        def num(k):
            try:
                return float((r[idx[k]] or "0").replace(",", "") or 0)
            except (ValueError, IndexError):
                return 0.0
        batted = max(num("AB") - num("SO") + num("SF"), 0.0)
        if batted <= 0:
            continue
        tot["batted"] += batted
        tot["hhb"] += num("HHB")
        for k, col in (("ld", "LD%"), ("fb", "FB%"), ("gb", "GB%")):
            tot[k] += (num(col) / 100.0) * batted
    b = tot["batted"]
    if b <= 0:
        return None
    return {"batted": b, "hh": tot["hhb"] / b,
            **{k: tot[k] / b for k in ("ld", "fb", "gb")}}


def batted_ball(season_name: str):
    """(this season, career baseline across every season on file)."""
    root = REPO / "data" / "raw" / "season_csv"
    season, agg = None, {"batted": 0.0, "hhb": 0.0, "ld": 0.0, "fb": 0.0, "gb": 0.0}
    for p in sorted(root.glob("*.csv")):
        d = _bb_from_csv(p)
        if not d:
            continue
        if p.stem.lower().startswith(season_name.lower()):
            season = d
        agg["batted"] += d["batted"]
        agg["hhb"] += d["hh"] * d["batted"]
        for k in ("ld", "fb", "gb"):
            agg[k] += d[k] * d["batted"]
    b = agg["batted"]
    career = None if b <= 0 else {"batted": b, "hh": agg["hhb"] / b,
                                  **{k: agg[k] / b for k in ("ld", "fb", "gb")}}
    return season, career


def band(c, title, sub):
    c.setFillColor(BARK); c.rect(0, H - 96, W, 96, stroke=0, fill=1)
    if LOGO.exists():
        try:
            c.drawImage(ImageReader(str(LOGO)), 34, H - 84, width=70, height=70,
                        mask="auto", preserveAspectRatio=True)
        except Exception:
            pass
    _txt(c, 118, H - 42, title, "Helvetica-Bold", 27, HexColor("#ffffff"), cs=1)
    _txt(c, 118, H - 64, sub, "Helvetica", 10.5, HexColor("#d9c9b0"))


def foot(c, note):
    c.setStrokeColor(LINE); c.setLineWidth(1); c.line(36, 46, W - 36, 46)
    _txt(c, 36, 32, "MAPLE TREE SOFTBALL", "Helvetica-Bold", 8, BARK, cs=1)
    _txt(c, W - 36, 32, note, "Helvetica", 8, MUTED, align="r")


def panel(c, x, y, w, h, title=None):
    c.setFillColor(CARD); c.setStrokeColor(LINE); c.setLineWidth(1)
    c.roundRect(x, y, w, h, 7, stroke=1, fill=1)
    if title:
        _txt(c, x + 14, y + h - 20, title, "Helvetica-Bold", 9, BARK, cs=1.1)


def tile(c, x, y, w, h, label, value, sub=""):
    panel(c, x, y, w, h)
    _txt(c, x + 14, y + h - 20, label, "Helvetica-Bold", 8, MUTED, cs=1.1)
    if sub:  # same line as the label so it can never collide with the big value
        _txt(c, x + w - 12, y + h - 20, sub, "Helvetica", 7.2, MUTED, align="r")
    _txt(c, x + 14, y + 16, value, "Helvetica-Bold", 27, BARK)


def bar(c, x, y, w, h, frac, color, bg=HexColor("#efe7da")):
    c.setFillColor(bg); c.roundRect(x, y, w, h, h / 2, stroke=0, fill=1)
    fw = max(0.0, min(1.0, frac)) * w
    if fw > 1:
        c.setFillColor(color); c.roundRect(x, y, max(fw, h), h, h / 2, stroke=0, fill=1)


# ---------------------------------------------------------------- pages
def page_team(c, S, games, meta):
    t = S["team"]
    band(c, "SEASON REPORT", f"{S['name']}  ·  {t['games']} games  ·  through {meta.get('data_through','')}")
    rf = int(sum(g["runs_for"] for g in games)); ra = int(sum(g["runs_against"] for g in games))
    diff = rf - ra
    y = H - 190
    tw = (W - 72 - 3 * 12) / 4
    for i, (lab, val, sub) in enumerate([
        ("RECORD", S["record"], f"{len(games)} games"),
        ("TEAM AVG", r3(t["avg"]), f"{t['hits']} hits"),
        ("TEAM OPS", f"{t['ops']:.3f}", f"OBP {r3(t['obp'])}"),
        ("RUN DIFF", f"+{diff}" if diff >= 0 else str(diff), f"{rf} for / {ra} ag."),
    ]):
        tile(c, 36 + i * (tw + 12), y, tw, 74, lab, val, sub)

    # team line
    y2 = y - 118
    panel(c, 36, y2, W - 72, 104, "THE TEAM LINE")
    cells = [("PA", t["pa"]), ("AB", t["ab"]), ("H", t["hits"]), ("1B", t["1b"]), ("2B", t["2b"]),
             ("3B", t["3b"]), ("HR", t["hr"]), ("RBI", t["rbi"]), ("R", t["r"]), ("BB", t["bb"]),
             ("TB", t["tb"]), ("AVG", r3(t["avg"])), ("OBP", r3(t["obp"])), ("SLG", r3(t["slg"]))]
    cw = (W - 72 - 28) / len(cells)
    for i, (k, v) in enumerate(cells):
        cx = 36 + 14 + i * cw
        _txt(c, cx + cw / 2 - 6, y2 + 52, str(v), "Helvetica-Bold", 15, BARK, align="c")
        _txt(c, cx + cw / 2 - 6, y2 + 32, k, "Helvetica-Bold", 7.5, MUTED, cs=0.8, align="c")

    # runs by game
    y3 = y2 - 168
    panel(c, 36, y3, W - 72, 152, "RUNS BY GAME  ·  scored vs allowed")
    mx = max([max(g["runs_for"], g["runs_against"]) for g in games] + [1])
    n = len(games); gw = (W - 72 - 40) / n
    base = y3 + 34
    for i, g in enumerate(games):
        gx = 36 + 20 + i * gw
        bh = 84
        for j, (val, col) in enumerate(((g["runs_for"], MAPLE), (g["runs_against"], HexColor("#9aa19a")))):
            hgt = (val / mx) * bh
            c.setFillColor(col)
            c.rect(gx + 6 + j * (gw / 2 - 8), base, gw / 2 - 12, max(hgt, 1.5), stroke=0, fill=1)
            _txt(c, gx + 6 + j * (gw / 2 - 8) + (gw / 2 - 12) / 2, base + max(hgt, 1.5) + 4,
                 int(val), "Helvetica-Bold", 7.5, col, align="c")
        _txt(c, gx + gw / 2 - 6, base - 12, g["opponent_name"][:9], "Helvetica", 6.8, MUTED, align="c")
        _txt(c, gx + gw / 2 - 6, base - 22, g["result"], "Helvetica-Bold", 8,
             GREEN if g["result"] == "W" else RED, align="c")

    # game log
    y4 = y3 - 176
    panel(c, 36, y4, W - 72, 160, "GAME LOG")
    hy = y4 + 128
    for lab, cx, al in (("WK", 52, "l"), ("DATE", 96, "l"), ("OPPONENT", 168, "l"),
                        ("RES", 400, "c"), ("SCORE", 470, "c"), ("DIFF", 545, "r")):
        _txt(c, cx, hy, lab, "Helvetica-Bold", 7.5, MUTED, cs=0.8, align=al)
    ry = hy - 16
    for g in games:
        d = g["runs_for"] - g["runs_against"]
        _txt(c, 52, ry, (g.get("week_label") or "").replace("Week ", "W"), "Helvetica", 8.5, MUTED)
        _txt(c, 96, ry, g["game_date"][5:], "Helvetica", 8.5, INK)
        _txt(c, 168, ry, g["opponent_name"], "Helvetica-Bold", 9.5, INK)
        _txt(c, 400, ry, g["result"], "Helvetica-Bold", 9.5, GREEN if g["result"] == "W" else RED, align="c")
        _txt(c, 470, ry, f"{int(g['runs_for'])}-{int(g['runs_against'])}", "Helvetica", 9.5, INK, align="c")
        _txt(c, 545, ry, f"+{int(d)}" if d >= 0 else str(int(d)), "Helvetica-Bold", 9,
             GREEN if d >= 0 else RED, align="r")
        ry -= 15

    # batted-ball profile vs the all-time baseline
    y5 = 56
    panel(c, 36, y5, W - 72, 76, "BATTED-BALL PROFILE  ·  small = career baseline, all seasons")
    bbs, bbc = batted_ball(S["name"])
    if bbs:
        cells = [("LD%", "ld", "up"), ("FB%", "fb", None), ("GB%", "gb", "down"), ("HH%", "hh", "up")]
        cwid = (W - 72 - 28) / len(cells)
        for i, (lab, k, good) in enumerate(cells):
            cx = 36 + 14 + i * cwid
            v = bbs[k] * 100
            _txt(c, cx, y5 + 42, lab, "Helvetica-Bold", 7.5, MUTED, cs=0.9)
            _txt(c, cx, y5 + 16, f"{v:.1f}%", "Helvetica-Bold", 20, BARK)
            if bbc:
                cv = bbc[k] * 100; d = v - cv
                _txt(c, cx, y5 + 4, f"career {cv:.1f}%", "Helvetica", 6.8, MUTED)
                col = MUTED if good is None else (
                    GREEN if ((d >= 0) if good == "up" else (d <= 0)) else RED)
                _txt(c, cx + 76, y5 + 4, f"{d:+.1f}", "Helvetica-Bold", 6.8, col)
    else:
        _txt(c, 50, y5 + 32, "no batted-ball data on file for this season",
             "Helvetica-Oblique", 9, MUTED)
    foot(c, "Page 1 of 3  ·  the team")


def page_roster(c, S, meta):
    band(c, "THE ROSTER", f"{S['name']}  ·  every batter, every number")
    ps = sorted(S["players"], key=lambda p: (-p["pa"], -p["ops"]))
    cols = [("PLAYER", 44, "l", 78), ("GP", 132, "c", 0), ("PA", 160, "c", 0), ("AB", 188, "c", 0),
            ("H", 216, "c", 0), ("2B", 243, "c", 0), ("3B", 269, "c", 0), ("HR", 295, "c", 0),
            ("RBI", 325, "c", 0), ("R", 353, "c", 0), ("BB", 380, "c", 0), ("TB", 408, "c", 0),
            ("AVG", 447, "r", 0), ("OBP", 487, "r", 0), ("SLG", 527, "r", 0), ("OPS", 570, "r", 0)]
    top = H - 156
    _txt(c, 44, top + 30, "sorted by plate appearances  ·  category leaders in orange",
         "Helvetica-Oblique", 8, MUTED)
    c.setFillColor(BARK); c.rect(36, top - 6, W - 72, 22, stroke=0, fill=1)
    for lab, cx, al, _ in cols:
        _txt(c, cx, top + 1, lab, "Helvetica-Bold", 7.5, HexColor("#f3ead9"), cs=0.7, align=al)
    best = {k: max(p[k] for p in ps) for k in ("hits", "hr", "rbi", "r", "tb")}
    y = top - 26
    rh = 25.5
    for i, p in enumerate(ps):
        if i % 2 == 0:
            c.setFillColor(HexColor("#f4efe6")); c.rect(36, y - 7, W - 72, rh, stroke=0, fill=1)
        _txt(c, 44, y, p["player"], "Helvetica-Bold", 10, BARK)
        vals = [("GP", p["games"]), ("PA", p["pa"]), ("AB", p["ab"]), ("H", p["hits"]),
                ("2B", p["2b"]), ("3B", p["3b"]), ("HR", p["hr"]), ("RBI", p["rbi"]),
                ("R", p["r"]), ("BB", p["bb"]), ("TB", p["tb"])]
        for (lab, cx, al, _), (k, v) in zip(cols[1:12], vals):
            lead = (k == "H" and v == best["hits"]) or (k == "HR" and v == best["hr"] and v) or \
                   (k == "RBI" and v == best["rbi"]) or (k == "R" and v == best["r"]) or \
                   (k == "TB" and v == best["tb"])
            _txt(c, cx, y, int(v), "Helvetica-Bold" if lead else "Helvetica", 9.5,
                 MAPLE if lead else INK, align=al)
        for (lab, cx, al, _), v in zip(cols[12:], (p["avg"], p["obp"], p["slg"])):
            _txt(c, cx, y, r3(v), "Helvetica", 9.5, INK, align=al)
        _txt(c, 570, y, f"{p['ops']:.3f}", "Helvetica-Bold", 9.5, BARK, align="r")
        # OPS meter, kept INSIDE the row band (band spans y-7 .. y-7+rh) so it is
        # never sliced by the next row's shading, and short of the GP column.
        bar(c, 44, y - 5, 64, 3.2, min(p["ops"] / 2.0, 1.0), MAPLE)
        y -= rh
    t = S["team"]
    c.setFillColor(BARK); c.rect(36, y - 6, W - 72, 22, stroke=0, fill=1)
    _txt(c, 44, y + 1, "TEAM", "Helvetica-Bold", 9.5, HexColor("#f3ead9"))
    for (lab, cx, al, _), v in zip(cols[1:12], [t["games"], t["pa"], t["ab"], t["hits"], t["2b"],
                                                t["3b"], t["hr"], t["rbi"], t["r"], t["bb"], t["tb"]]):
        _txt(c, cx, y + 1, int(v), "Helvetica-Bold", 9, HexColor("#f3ead9"), align=al)
    for (lab, cx, al, _), v in zip(cols[12:], (t["avg"], t["obp"], t["slg"])):
        _txt(c, cx, y + 1, r3(v), "Helvetica-Bold", 9, HexColor("#f3ead9"), align=al)
    _txt(c, 570, y + 1, f"{t['ops']:.3f}", "Helvetica-Bold", 9, GOLD, align="r")
    foot(c, "Page 2 of 3  ·  the roster")


def page_leaders(c, S, meta):
    band(c, "LEADERS & ADVANCED", f"{S['name']}  ·  who is carrying the load")
    ps = S["players"]
    adv = {a["slug"]: a for a in S["advanced"]}
    qual = [p for p in ps if p["pa"] >= MINPA]

    boards = [("HITS", "hits", ps, 0), ("RBI", "rbi", ps, 0), ("RUNS", "r", ps, 0),
              ("HOME RUNS", "hr", ps, 0), ("AVG", "avg", qual, 3), ("OPS", "ops", qual, 3)]
    bw = (W - 72 - 24) / 3
    PH = 152
    for i, (title, key, pool, dec) in enumerate(boards):
        col, row = i % 3, i // 3
        x = 36 + col * (bw + 12)
        y = 508 - row * 168          # panel BOTTOM (row0 top=660, clear of the header band)
        panel(c, x, y, bw, PH, title + ("" if dec == 0 else f"   (min {MINPA} PA)"))
        top5 = sorted(pool, key=lambda p: -p[key])[:5]
        mx = max([p[key] for p in top5] + [0.001])
        ly = y + PH - 44
        for p in top5:
            v = p[key]
            _txt(c, x + 14, ly, p["player"][:14], "Helvetica-Bold", 8.5, INK)
            _txt(c, x + bw - 14, ly, r3(v) if dec else int(v), "Helvetica-Bold", 8.5, BARK, align="r")
            bar(c, x + 14, ly - 8, bw - 28, 4.5, v / mx, MAPLE)
            ly -= 21

    ay, ah = 96, 226
    panel(c, 36, ay, W - 72, ah, "ADVANCED  ·  wRC+ 100 = league-average bat")
    hdr = [("PLAYER", 52, "l"), ("PA", 172, "c"), ("wOBA", 226, "r"), ("wRC+", 278, "r"),
           ("ISO", 328, "r"), ("BB%", 380, "r"), ("XBH%", 432, "r"), ("oWAR", 484, "r"),
           ("ARCHETYPE", 502, "l")]
    hy = ay + ah - 46            # 26pt below the panel title
    for lab, cx, al in hdr:
        _txt(c, cx, hy, lab, "Helvetica-Bold", 7.5, MUTED, cs=0.7, align=al)
    c.setStrokeColor(LINE); c.setLineWidth(0.8); c.line(50, hy - 6, W - 50, hy - 6)
    ry = hy - 20
    for p in sorted(ps, key=lambda p: -(adv.get(p["slug"], {}).get("wrc_plus") or 0))[:11]:
        a = adv.get(p["slug"])
        if not a:
            continue
        wr = a["wrc_plus"]
        _txt(c, 52, ry, p["player"], "Helvetica-Bold", 9, BARK)
        _txt(c, 172, ry, int(a["pa"]), "Helvetica", 8.5, INK, align="c")
        _txt(c, 226, ry, r3(a["woba"]), "Helvetica", 8.5, INK, align="r")
        _txt(c, 278, ry, f"{wr:.0f}", "Helvetica-Bold", 9, MAPLE if wr >= 100 else MUTED, align="r")
        _txt(c, 328, ry, r3(a["iso"]), "Helvetica", 8.5, INK, align="r")
        _txt(c, 380, ry, f"{a['bb_rate']*100:.1f}", "Helvetica", 8.5, INK, align="r")
        _txt(c, 432, ry, f"{a['xbh_rate']*100:.1f}", "Helvetica", 8.5, INK, align="r")
        _txt(c, 484, ry, f"{a['owar']:.2f}", "Helvetica", 8.5, INK, align="r")
        _txt(c, 502, ry, a.get("archetype", ""), "Helvetica-Oblique", 8, MUTED)
        ry -= 14.2
    foot(c, "Page 3 of 3  ·  leaders & advanced")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="summer-2026")
    ap.add_argument("--out")
    args = ap.parse_args()

    seasons = load("season_stats")
    S = next(x for x in seasons if x["slug"] == args.season)
    meta = load("meta")
    sched = next(x for x in load("schedule") if x["slug"] == args.season)
    games = [g for g in sched["games"] if g.get("status") == "completed" and g.get("result")]

    out = Path(args.out) if args.out else (
        REPO / "data" / "writeups" / f"maple-tree-{args.season}" /
        f"maple-tree-season-report-{args.season}.pdf")
    out.parent.mkdir(parents=True, exist_ok=True)
    c = rl_canvas.Canvas(str(out), pagesize=letter)
    c.setFillColor(PAPER); c.rect(0, 0, W, H, stroke=0, fill=1)
    page_team(c, S, games, meta); c.showPage()
    c.setFillColor(PAPER); c.rect(0, 0, W, H, stroke=0, fill=1)
    page_roster(c, S, meta); c.showPage()
    c.setFillColor(PAPER); c.rect(0, 0, W, H, stroke=0, fill=1)
    page_leaders(c, S, meta); c.showPage()
    c.save()
    print(f"Season report -> {out}")
    print(f"  {S['name']}  {S['record']}  ·  {len(S['players'])} batters  ·  {len(games)} games")


if __name__ == "__main__":
    main()
