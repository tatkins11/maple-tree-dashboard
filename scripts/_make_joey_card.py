"""Generate the 'Prayers Up for Joey' special-edition injury card as a crisp SVG,
render it with pymupdf, and save the web asset. One-off card art generator."""
from __future__ import annotations

import base64
import io
import math
from pathlib import Path

import fitz
from PIL import Image

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "site" / "public" / "cards" / "snaxx-injured.webp"
W_, H_ = 720, 1008
CX = 360

# palette
BARK = "#1b1006"
BARK2 = "#2c1a0d"
GOLD = "#d9a95a"
GOLD_HI = "#eecf8f"
CREAM = "#f4e8cc"
LIGHT = "#f7ebc9"
MAROON = "#7c2130"
MAPLE = "#c2410c"
FLAME = "#f6b24a"
MUTED = "#b7975f"


def logo_b64() -> str:
    img = Image.open(REPO / "site" / "public" / "brand" / "maple-tree-tap.webp").convert("RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;")


def glow(cx, cy, r_out, r_in, steps=26) -> str:
    """Stepped radial glow, bark -> light, brighter toward center."""
    out = []
    for i in range(steps):
        t = i / (steps - 1)
        r = r_out - (r_out - r_in) * t
        op = 0.03 + 0.62 * (t ** 2)
        out.append(f'<circle cx="{cx}" cy="{cy}" r="{r:.1f}" fill="{LIGHT}" opacity="{op:.3f}"/>')
    return "\n".join(out)


def rays(cx, cy, count=14, length=330) -> str:
    out = []
    for k in range(count):
        a = (2 * math.pi / count) * k
        wa = 0.028
        x1 = cx + length * math.cos(a - wa)
        y1 = cy + length * math.sin(a - wa)
        x2 = cx + length * math.cos(a + wa)
        y2 = cy + length * math.sin(a + wa)
        out.append(f'<polygon points="{cx},{cy} {x1:.1f},{y1:.1f} {x2:.1f},{y2:.1f}" '
                   f'fill="{LIGHT}" opacity="0.05"/>')
    return "\n".join(out)


def candles(cx, y, n=5, spread=190) -> str:
    out = []
    for i in range(n):
        x = cx - spread / 2 + spread * i / (n - 1)
        h = 26 + (6 if i % 2 == 0 else 0)
        out.append(f'<ellipse cx="{x}" cy="{y - h - 8}" rx="16" ry="20" fill="{FLAME}" opacity="0.28"/>')
        out.append(f'<rect x="{x-4}" y="{y-h}" width="8" height="{h}" rx="3" fill="{CREAM}" opacity="0.85"/>')
        out.append(f'<ellipse cx="{x}" cy="{y-h-3}" rx="3.4" ry="7" fill="{FLAME}"/>')
        out.append(f'<ellipse cx="{x}" cy="{y-h-4}" rx="1.4" ry="3.5" fill="{GOLD_HI}"/>')
    return "\n".join(out)


def text(x, y, s, size, *, font="serif", weight="normal", fill=CREAM, anchor="middle",
         spacing=0, italic=False, opacity=1.0):
    fam = "Georgia, 'Times New Roman', serif" if font == "serif" else "Helvetica, Arial, sans-serif"
    style = f' font-style="italic"' if italic else ""
    ls = f' letter-spacing="{spacing}"' if spacing else ""
    return (f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="{fam}" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}" opacity="{opacity}"{ls}{style}>'
            f'{esc(s)}</text>')


def chart_row(y, label, value):
    return (text(88, y, label, 12.5, font="sans", weight="bold", fill=GOLD, anchor="start", spacing=1)
            + text(632, y, value, 14, font="sans", fill=CREAM, anchor="end"))


def build_svg() -> str:
    lb = logo_b64()
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" '
        f'width="{W_}" height="{H_}" viewBox="0 0 {W_} {H_}">',
        f'<rect width="{W_}" height="{H_}" rx="34" fill="{BARK}"/>',
        # vignette-ish base
        f'<rect x="0" y="560" width="{W_}" height="448" fill="{BARK2}" opacity="0.5"/>',
        rays(CX, 250),
        glow(CX, 250, 300, 40),
        # frames
        f'<rect x="10" y="10" width="{W_-20}" height="{H_-20}" rx="28" fill="none" stroke="{GOLD}" stroke-width="5"/>',
        f'<rect x="22" y="22" width="{W_-44}" height="{H_-44}" rx="20" fill="none" stroke="{GOLD}" stroke-width="1.2" opacity="0.6"/>',
        # halo behind logo
        f'<circle cx="{CX}" cy="250" r="120" fill="{LIGHT}" opacity="0.16"/>',
        f'<image x="{CX-118}" y="132" width="236" height="236" xlink:href="data:image/png;base64,{lb}"/>',
    ]
    # top-left IL pill
    parts += [
        f'<rect x="40" y="42" width="176" height="30" rx="15" fill="{MAROON}"/>',
        text(128, 63, "INJURED RESERVE", 13, font="sans", weight="bold", fill=CREAM, spacing=1.5),
    ]
    # top-right gem with medical cross
    gx, gy = 648, 66
    parts += [
        f'<polygon points="{gx},{gy-34} {gx+34},{gy} {gx},{gy+34} {gx-34},{gy}" fill="{MAROON}" stroke="{GOLD}" stroke-width="2"/>',
        f'<rect x="{gx-4}" y="{gy-15}" width="8" height="30" rx="2" fill="{CREAM}"/>',
        f'<rect x="{gx-15}" y="{gy-4}" width="30" height="8" rx="2" fill="{CREAM}"/>',
    ]
    # titles
    parts += [
        text(CX, 452, "— PRAYERS UP —", 15, font="sans", weight="bold", fill=GOLD, spacing=5),
        text(CX, 508, "PRAY FOR JOEY", 58, font="serif", weight="bold", fill=CREAM),
        text(CX, 540, "JOEY “SNAXX” STANLEY", 19, font="sans", weight="bold", fill=GOLD_HI, spacing=1),
        text(CX, 562, "MAPLE TREE  ·  SNACKS & MORALE  ·  R / R", 12.5, font="sans", fill=MUTED, spacing=1.5),
    ]
    # incident chart box
    by = 588
    parts += [
        f'<rect x="60" y="{by}" width="600" height="176" rx="12" fill="#150c05" stroke="{GOLD}" stroke-width="1.4" opacity="0.98"/>',
        f'<rect x="60" y="{by}" width="600" height="30" rx="12" fill="{MAROON}"/>',
        f'<rect x="60" y="{by+16}" width="600" height="14" fill="{MAROON}"/>',
        text(CX, by + 20, "THE INCIDENT REPORT", 13, font="sans", weight="bold", fill=CREAM, spacing=2),
        chart_row(by + 58, "MECHANISM", "Cannonball into a 4-foot pool"),
        chart_row(by + 86, "DIAGNOSIS", "Bruised knee (left)"),
        chart_row(by + 114, "STATUS", "Day-to-day · Questionable · Heroic"),
        chart_row(by + 148, "EXPECTED RETURN", "When the swelling & chirping subside"),
        f'<line x1="80" y1="{by+128}" x2="640" y2="{by+128}" stroke="{GOLD}" stroke-width="0.6" opacity="0.4"/>',
    ]
    # career strip
    parts += [
        f'<line x1="90" y1="800" x2="630" y2="800" stroke="{GOLD}" stroke-width="0.8" opacity="0.5"/>',
        text(CX, 822, ".423 AVG   ·   .885 OPS   ·   41 H   ·   16 RBI   ·   3 SEASONS OF VIBES", 13,
             font="sans", weight="bold", fill=GOLD, spacing=0.5),
    ]
    # flavor
    parts += [
        text(CX, 858, "He saw four feet of water and feared nothing.", 13.5, italic=True, fill=CREAM),
        text(CX, 878, "The knee will heal. The legend of the shallow-end", 13.5, italic=True, fill=CREAM),
        text(CX, 898, "cannonball never will. Get well soon, Snaxx.", 13.5, italic=True, fill=CREAM),
    ]
    # candles + footer
    parts += [
        candles(CX, 946),
        text(CX, 978, "GET WELL SOON  ·  THE MAPLE TREE TAP  ·  CARY, ILLINOIS", 10.5,
             font="sans", weight="bold", fill=MUTED, spacing=1.5),
    ]
    parts.append("</svg>")
    return "\n".join(parts)


def main():
    svg = build_svg()
    svg_path = OUT.with_suffix(".svg")
    svg_path.write_text(svg, encoding="utf-8")
    doc = fitz.open(str(svg_path))
    pix = doc[0].get_pixmap(matrix=fitz.Matrix(3, 3), alpha=False)
    png_bytes = pix.tobytes("png")
    img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
    img.thumbnail((640, 900), Image.LANCZOS)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    img.save(OUT, quality=88, method=6)
    svg_path.unlink(missing_ok=True)
    # also drop a preview png in scratch
    prev = Path(r"C:/Users/TRISTA~1/AppData/Local/Temp/claude/C--Slowpitch/bc588138-5c92-42f1-bd89-f0ba9ddab939/scratchpad/joey_card.png")
    img.save(prev)
    print(f"wrote {OUT} ({OUT.stat().st_size // 1024} KB, {img.size[0]}x{img.size[1]})")


if __name__ == "__main__":
    main()
