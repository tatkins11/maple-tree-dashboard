"""Generate the 'Prayers Up for Joey' special-edition injury card.

Uses Joey's own portrait (from his base card), warm-duotoned to a candlelight
tone, with an angel halo hovering above his head and the IR theme around it.
Built as an SVG and rendered crisp via pymupdf (which ignores gradients/filters,
so glows are stepped concentric shapes). One-off card art generator.
"""
from __future__ import annotations

import base64
import io
import math
from pathlib import Path

import fitz
from PIL import Image, ImageOps

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "site" / "public" / "cards" / "snaxx-injured.webp"
SRC = Path("C:/Slowpitch/Player Trading Cards/10f07928-30d9-40c2-8472-38720b3096d3.png")
W_, H_ = 720, 1008
CX = 360

BARK = "#1b1006"
BARK2 = "#2c1a0d"
GOLD = "#d9a95a"
GOLD_HI = "#f2d488"
CREAM = "#f4e8cc"
LIGHT = "#f8eccb"
MAROON = "#7c2130"
FLAME = "#f6b24a"
MUTED = "#b7975f"

# photo placement
PX, PY, PW, PH = 58, 84, 604, 576
HEAD_X = PX + PW * 0.50
HALO_Y = PY + 18


def photo_b64() -> str:
    src = Image.open(SRC).convert("RGB")
    w, h = src.size
    crop = src.crop((int(w * 0.11), int(h * 0.15), int(w * 0.865), int(h * 0.63)))
    crop = crop.resize((PW, PH), Image.LANCZOS)
    gray = ImageOps.autocontrast(crop.convert("L"), cutoff=1)
    duo = ImageOps.colorize(gray, black=(26, 15, 7), mid=(150, 108, 58),
                            white=(247, 235, 205), midpoint=126)
    blended = Image.blend(duo, crop, 0.20)
    buf = io.BytesIO()
    blended.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def logo_b64() -> str:
    img = Image.open(REPO / "site" / "public" / "brand" / "maple-tree-tap.webp").convert("RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;")


def rays(cx, cy, count=16, length=360):
    out = []
    for k in range(count):
        a = (2 * math.pi / count) * k - math.pi / 2
        wa = 0.03
        p1 = (cx + length * math.cos(a - wa), cy + length * math.sin(a - wa))
        p2 = (cx + length * math.cos(a + wa), cy + length * math.sin(a + wa))
        out.append(f'<polygon points="{cx},{cy} {p1[0]:.1f},{p1[1]:.1f} {p2[0]:.1f},{p2[1]:.1f}" '
                   f'fill="{LIGHT}" opacity="0.045"/>')
    return "\n".join(out)


def halo(cx, cy):
    out = []
    # glow
    for i, (rx, ry, op) in enumerate([(140, 46, 0.06), (118, 38, 0.10), (100, 31, 0.16)]):
        out.append(f'<ellipse cx="{cx}" cy="{cy}" rx="{rx}" ry="{ry}" fill="{LIGHT}" opacity="{op}"/>')
    # the ring
    out.append(f'<ellipse cx="{cx}" cy="{cy}" rx="96" ry="27" fill="none" stroke="{GOLD}" stroke-width="10"/>')
    out.append(f'<ellipse cx="{cx}" cy="{cy-2}" rx="96" ry="27" fill="none" stroke="{GOLD_HI}" stroke-width="4" opacity="0.9"/>')
    out.append(f'<ellipse cx="{cx}" cy="{cy-4}" rx="90" ry="23" fill="none" stroke="{CREAM}" stroke-width="1.5" opacity="0.7"/>')
    return "\n".join(out)


def candles(cx, y, n=5, spread=200):
    out = []
    for i in range(n):
        x = cx - spread / 2 + spread * i / (n - 1)
        h = 20 + (5 if i % 2 == 0 else 0)
        out.append(f'<ellipse cx="{x}" cy="{y-h-6}" rx="13" ry="17" fill="{FLAME}" opacity="0.26"/>')
        out.append(f'<rect x="{x-3.2}" y="{y-h}" width="6.4" height="{h}" rx="2.5" fill="{CREAM}" opacity="0.85"/>')
        out.append(f'<ellipse cx="{x}" cy="{y-h-3}" rx="2.8" ry="6" fill="{FLAME}"/>')
        out.append(f'<ellipse cx="{x}" cy="{y-h-4}" rx="1.2" ry="3" fill="{GOLD_HI}"/>')
    return "\n".join(out)


def T(x, y, s, size, *, font="serif", weight="normal", fill=CREAM, anchor="middle",
      spacing=0, italic=False, opacity=1.0):
    fam = "Georgia, 'Times New Roman', serif" if font == "serif" else "Helvetica, Arial, sans-serif"
    st = ' font-style="italic"' if italic else ""
    ls = f' letter-spacing="{spacing}"' if spacing else ""
    return (f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="{fam}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" opacity="{opacity}"{ls}{st}>{esc(s)}</text>')


def chart_row(y, label, value):
    return (T(90, y, label, 12, font="sans", weight="bold", fill=GOLD, anchor="start", spacing=1)
            + T(630, y, value, 13.5, font="sans", fill=CREAM, anchor="end"))


def build_svg():
    ph, lg = photo_b64(), logo_b64()
    P = [
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" '
        f'width="{W_}" height="{H_}" viewBox="0 0 {W_} {H_}">',
        f'<rect width="{W_}" height="{H_}" rx="34" fill="{BARK}"/>',
        rays(HEAD_X, HALO_Y + 30),
        # photo + its inner glow backdrop
        f'<ellipse cx="{HEAD_X}" cy="{PY+PH*0.4}" rx="330" ry="330" fill="{LIGHT}" opacity="0.05"/>',
        f'<image x="{PX}" y="{PY}" width="{PW}" height="{PH}" xlink:href="data:image/png;base64,{ph}"/>',
    ]
    # bottom fade of the photo into bark (stepped)
    for i in range(10):
        t = i / 9
        yy = PY + PH - 120 + 120 * t
        P.append(f'<rect x="{PX}" y="{yy:.1f}" width="{PW}" height="14" fill="{BARK}" opacity="{0.06+0.6*t:.2f}"/>')
    P += [
        f'<rect x="{PX}" y="{PY}" width="{PW}" height="{PH}" rx="6" fill="none" stroke="{GOLD}" stroke-width="2" opacity="0.7"/>',
        halo(HEAD_X, HALO_Y),
        # frames
        f'<rect x="10" y="10" width="{W_-20}" height="{H_-20}" rx="28" fill="none" stroke="{GOLD}" stroke-width="5"/>',
        f'<rect x="22" y="22" width="{W_-44}" height="{H_-44}" rx="20" fill="none" stroke="{GOLD}" stroke-width="1.2" opacity="0.55"/>',
        # IR pill + medical gem
        f'<rect x="40" y="42" width="180" height="30" rx="15" fill="{MAROON}"/>',
        T(130, 63, "INJURED RESERVE", 13, font="sans", weight="bold", fill=CREAM, spacing=1.5),
    ]
    gx, gy = 648, 66
    P += [
        f'<polygon points="{gx},{gy-32} {gx+32},{gy} {gx},{gy+32} {gx-32},{gy}" fill="{MAROON}" stroke="{GOLD}" stroke-width="2"/>',
        f'<rect x="{gx-3.6}" y="{gy-14}" width="7.2" height="28" rx="2" fill="{CREAM}"/>',
        f'<rect x="{gx-14}" y="{gy-3.6}" width="28" height="7.2" rx="2" fill="{CREAM}"/>',
    ]
    # titles
    P += [
        T(CX, 690, "— PRAYERS UP —", 14, font="sans", weight="bold", fill=GOLD, spacing=5),
        T(CX, 734, "PRAY FOR JOEY", 46, font="serif", weight="bold", fill=CREAM),
        T(CX, 760, "JOEY “SNAXX” STANLEY", 17, font="sans", weight="bold", fill=GOLD_HI, spacing=1),
        T(CX, 780, "MAPLE TREE  ·  SNACKS & MORALE  ·  R / R", 11.5, font="sans", fill=MUTED, spacing=1),
    ]
    # incident report
    by = 800
    P += [
        f'<rect x="58" y="{by}" width="604" height="120" rx="10" fill="#150c05" stroke="{GOLD}" stroke-width="1.3"/>',
        f'<rect x="58" y="{by}" width="604" height="28" rx="10" fill="{MAROON}"/>',
        f'<rect x="58" y="{by+14}" width="604" height="14" fill="{MAROON}"/>',
        T(CX, by + 19, "THE INCIDENT REPORT", 12.5, font="sans", weight="bold", fill=CREAM, spacing=2),
        chart_row(by + 50, "MECHANISM", "Cannonball into a 4-foot pool"),
        chart_row(by + 72, "DIAGNOSIS", "Bruised knee"),
        chart_row(by + 94, "STATUS", "Day-to-day · potential season-ending"),
        chart_row(by + 112, "PROGNOSIS", "Prayers strongly encouraged"),
    ]
    # career strip + footer
    P += [
        f'<line x1="120" y1="946" x2="600" y2="946" stroke="{GOLD}" stroke-width="0.6" opacity="0.4"/>',
        T(CX, 966, ".423 AVG   ·   .885 OPS   ·   41 H   ·   16 RBI   ·   3 SEASONS OF VIBES", 12,
          font="sans", weight="bold", fill=GOLD, spacing=0.5),
        T(CX, 990, "GET WELL SOON  ·  THE MAPLE TREE TAP  ·  CARY, ILLINOIS", 10,
          font="sans", weight="bold", fill=MUTED, spacing=1.2),
    ]
    P.append("</svg>")
    return "\n".join(P)


def main():
    svg_path = OUT.with_suffix(".svg")
    svg_path.write_text(build_svg(), encoding="utf-8")
    doc = fitz.open(str(svg_path))
    pix = doc[0].get_pixmap(matrix=fitz.Matrix(3, 3), alpha=False)
    img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
    img.thumbnail((640, 900), Image.LANCZOS)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    img.save(OUT, quality=88, method=6)
    svg_path.unlink(missing_ok=True)
    img.save(Path(r"C:/Users/TRISTA~1/AppData/Local/Temp/claude/C--Slowpitch/bc588138-5c92-42f1-bd89-f0ba9ddab939/scratchpad/joey_card.png"))
    print(f"wrote {OUT} ({OUT.stat().st_size // 1024} KB, {img.size[0]}x{img.size[1]})")


if __name__ == "__main__":
    main()
