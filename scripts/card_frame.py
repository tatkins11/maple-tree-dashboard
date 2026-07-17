"""Card frame compositor v2 — the EXTRAVAGANT one, matched to the club's card canon.

Layout grammar extracted from the existing 24-card set (MLB-The-Show energy):
top-left red MILESTONE pill + huge name + gold nickname; top-right faceted ruby
gem with glow; art full-bleed (the AI renders the colossal milestone number and
the big materialized stat word INSIDE the scene); bottom caption block with
color-highlighted keywords; leaf + TAPPERS wordmark bottom-left; rank flag
bottom-right; gold-dust pinstripe frame.

The compositor owns every FACTUAL string (names, captions, stats, ranks) so cards
regenerate from data; the art owns the drama.

    python scripts/card_frame.py --demo <art.png> <out.png>
"""
from __future__ import annotations

import math
import random
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

W, H = 1024, 1434
BARK_DEEP = (24, 13, 6)
GOLD = (222, 176, 92)
GOLD_HI = (250, 224, 150)
CREAM = (247, 238, 216)
MAPLE = (207, 74, 16)
RED = (176, 28, 36)
RED_DEEP = (110, 12, 22)
WHITE = (255, 253, 248)

FONTS = Path("C:/Windows/Fonts")


def F(name, size):
    return ImageFont.truetype(str(FONTS / name), size)


def _tw(draw, text, font):
    l, t, r, b = draw.textbbox((0, 0), text, font=font)
    return r - l


def _fit(draw, text, font_name, max_width, start, floor=24):
    size = start
    while size > floor:
        f = F(font_name, size)
        if _tw(draw, text, f) <= max_width:
            return f
        size -= 3
    return F(font_name, floor)


def _glow_text(card, xy, text, font, fill, glow_color, glow_radius=10, glow_alpha=160,
               stroke_width=0, stroke_fill=None):
    layer = Image.new("RGBA", card.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    d.text(xy, text, font=font, fill=(*glow_color, glow_alpha),
           stroke_width=max(stroke_width, 2), stroke_fill=(*glow_color, glow_alpha))
    card.alpha_composite(layer.filter(ImageFilter.GaussianBlur(glow_radius)))
    d2 = ImageDraw.Draw(card)
    kw = {}
    if stroke_width:
        kw = dict(stroke_width=stroke_width, stroke_fill=stroke_fill)
    d2.text(xy, text, font=font, fill=fill, **kw)


GEM_TIERS = {
    #        base            deep            light           lighter         glow             edge
    "ruby":   ((176, 28, 36), (110, 12, 22), (228, 62, 74), (255, 96, 106), (255, 40, 60), (255, 214, 220)),
    "gold":   ((214, 158, 30), (146, 98, 10), (238, 190, 66), (255, 222, 110), (255, 190, 40), (255, 240, 190)),
    "silver": ((162, 168, 182), (104, 110, 124), (204, 210, 222), (238, 242, 250), (210, 224, 255), (250, 252, 255)),
    "bronze": ((172, 104, 46), (108, 60, 20), (204, 138, 74), (232, 168, 104), (230, 140, 60), (246, 210, 170)),
}


def gem_tier_for(rating) -> str:
    r = int(rating)
    return "ruby" if r >= 95 else "gold" if r >= 85 else "silver" if r >= 75 else "bronze"


def _gem(card, cx, cy, r, value, tier="ruby"):
    """Faceted rating gem — tier sets the stone (ruby/gold/silver/bronze)."""
    base, deep, light, lighter, glow_c, edge = GEM_TIERS[tier]
    glow = Image.new("RGBA", card.size, (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.polygon([(cx, cy - r - 14), (cx + r + 14, cy), (cx, cy + r + 14), (cx - r - 14, cy)],
               fill=(*glow_c, 130))
    card.alpha_composite(glow.filter(ImageFilter.GaussianBlur(22)))
    d = ImageDraw.Draw(card)
    pts = [(cx, cy - r), (cx + r, cy), (cx, cy + r), (cx - r, cy)]
    d.polygon(pts, fill=base, outline=edge)
    top, right, bottom, left = pts
    table = [(cx - r * 0.42, cy - r * 0.1), (cx, cy - r * 0.52), (cx + r * 0.42, cy - r * 0.1),
             (cx, cy + r * 0.42)]
    d.polygon([top, table[0], table[1]], fill=light)
    d.polygon([top, table[1], table[2]], fill=lighter)
    d.polygon([left, table[0], table[3]], fill=deep)
    d.polygon([right, table[2], table[3]], fill=tuple(int(c * 0.82) for c in base))
    d.polygon(table, fill=tuple(int((b + l) / 2) for b, l in zip(base, light)))
    d.line([cx - r * 0.55, cy - r * 0.4, cx - r * 0.15, cy - r * 0.75], fill=(255, 252, 248), width=6)
    d.line([cx + r * 0.2, cy - r * 0.68, cx + r * 0.42, cy - r * 0.45], fill=(255, 248, 242), width=4)
    d.polygon(pts, outline=edge)
    val_f = _fit(d, str(value), "impact.ttf", r * 1.2, 84, 30)
    vw = _tw(d, str(value), val_f)
    _glow_text(card, (cx - vw / 2, cy - val_f.size * 0.58), str(value), val_f, WHITE,
               (255, 255, 255), glow_radius=6, glow_alpha=120,
               stroke_width=3, stroke_fill=deep)


def _sparkles(card, boxes, n, seed):
    rnd = random.Random(seed)
    layer = Image.new("RGBA", card.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for _ in range(n):
        x0, y0, x1, y1 = boxes[rnd.randrange(len(boxes))]
        x, y = rnd.uniform(x0, x1), rnd.uniform(y0, y1)
        s = rnd.uniform(3, 11)
        c = (255, 244, 200, rnd.randint(140, 230)) if rnd.random() < 0.7 else (255, 255, 255, 220)
        d.polygon([(x, y - s), (x + s * 0.22, y - s * 0.22), (x + s, y), (x + s * 0.22, y + s * 0.22),
                   (x, y + s), (x - s * 0.22, y + s * 0.22), (x - s, y), (x - s * 0.22, y - s * 0.22)],
                  fill=c)
    card.alpha_composite(layer.filter(ImageFilter.GaussianBlur(0.6)))
    card.alpha_composite(layer)


def _leaf_mark(card, x, y, h):
    """Team leaf + TAPPERS wordmark, bottom-left (from the transparent fierce-leaf svg raster)."""
    leaf_png = Path("C:/Slowpitch/Mascot Concepts/02-fierce-leaf-preview.png")
    d = ImageDraw.Draw(card)
    ly = y
    if leaf_png.exists():
        leaf = Image.open(leaf_png).convert("RGBA")
        # knock out the near-cream background so only the mark shows
        datas = leaf.getdata()
        bg = datas[0]
        leaf.putdata([(r, g, b, 0) if abs(r - bg[0]) + abs(g - bg[1]) + abs(b - bg[2]) < 40
                      else (r, g, b, a) for (r, g, b, a) in datas])
        leaf.thumbnail((h, h), Image.LANCZOS)
        card.alpha_composite(leaf, (x, y))
        ly = y + h + 6
    f1, f2 = F("seguibl.ttf", 17), F("seguibl.ttf", 24)
    d = ImageDraw.Draw(card)
    d.text((x - 2, ly), "MAPLE TREE", font=f1, fill=CREAM,
           stroke_width=1, stroke_fill=(8, 5, 2))
    d.text((x - 2, ly + 21), "TAPPERS", font=f2, fill=GOLD_HI,
           stroke_width=1, stroke_fill=(8, 5, 2))


def compose_card(art_path, out_path, *, player, gem, caption_lines, nickname=None,
                 series="MILESTONE", rank_flag=None, sparkle_seed=0,
                 variant="milestone"):
    """caption_lines: list of segments-lists; each segment = (text, color_key)
    with color_key in {'white','gold','red'}. Example:
        [[("HIT ", "white"), ("50 CAREER HOME RUNS", "gold")],
         [("1ST IN FRANCHISE HISTORY", "red"), (" · JULY 15, 2026", "white")]]
    """
    card = Image.new("RGBA", (W, H), (*BARK_DEEP, 255))

    # full-bleed art, cover-fit
    art = Image.open(art_path).convert("RGB")
    scale = max(W / art.width, H / art.height)
    art = art.resize((round(art.width * scale), round(art.height * scale)), Image.LANCZOS)
    ax, ay = (art.width - W) // 2, max((art.height - H) // 2, 0)
    card.paste(art.crop((ax, ay, ax + W, ay + H)), (0, 0))
    draw = ImageDraw.Draw(card)

    # ---- top scrim so the head elements pop ----
    scrim = Image.new("RGBA", (W, 260), (0, 0, 0, 0))
    sd = ImageDraw.Draw(scrim)
    for yy in range(260):
        sd.line([(0, yy), (W, yy)], fill=(*BARK_DEEP, int(150 * (1 - yy / 260))))
    card.alpha_composite(scrim, (0, 0))

    # ---- series pill (top-left, red gloss) ----
    m = 40
    pill_f = F("seguibl.ttf", 30)
    pt = series.upper()
    pw = _tw(draw, pt, pill_f) + 56
    draw.rounded_rectangle([m, m, m + pw, m + 56], 16, fill=RED, outline=GOLD_HI, width=3)
    draw.rounded_rectangle([m + 8, m + 6, m + pw - 8, m + 20], 8, fill=(255, 255, 255, 34))
    draw.text((m + 28, m + 11), pt, font=pill_f, fill=WHITE,
              stroke_width=2, stroke_fill=RED_DEEP)

    # ---- player name (huge, stacked if two words) ----
    ny = m + 70
    parts = player.upper().split()
    lines = [player.upper()] if len(parts) == 1 else [" ".join(parts[:-1]), parts[-1]]
    for line in lines:
        nf = _fit(draw, line, "ariblk.ttf", 560, 64, 40)
        _glow_text(card, (m, ny), line, nf, WHITE, (0, 0, 0), glow_radius=8, glow_alpha=200,
                   stroke_width=3, stroke_fill=(10, 6, 3))
        draw = ImageDraw.Draw(card)
        ny += nf.size + 6
    if nickname:
        nick_f = F("seguibl.ttf", 34)
        _glow_text(card, (m, ny + 2), nickname.upper(), nick_f, GOLD_HI, (0, 0, 0),
                   glow_radius=6, glow_alpha=190, stroke_width=2, stroke_fill=(10, 6, 3))

    # ---- rating gem (top-right; tier follows the rating) ----
    _gem(card, W - 118, 118, 84, gem, tier=gem_tier_for(gem) if str(gem).isdigit() else "ruby")
    draw = ImageDraw.Draw(card)

    # ---- bottom scrim + caption block ----
    bh = 250
    scrim2 = Image.new("RGBA", (W, bh), (0, 0, 0, 0))
    sd2 = ImageDraw.Draw(scrim2)
    for yy in range(bh):
        sd2.line([(0, yy), (W, yy)], fill=(*BARK_DEEP, int(235 * (yy / bh) ** 1.4)))
    card.alpha_composite(scrim2, (0, H - bh))
    draw = ImageDraw.Draw(card)

    colors = {"white": WHITE, "gold": GOLD_HI, "red": (255, 92, 74)}
    cap_max = W - 130
    cy = H - 158
    for segs in reversed(caption_lines):
        full = "".join(t for t, _ in segs)
        cap_f = _fit(draw, full, "seguibl.ttf", cap_max, 30, 20)
        total = sum(_tw(draw, t, cap_f) for t, _ in segs)
        x = (W - total) / 2
        for t, ck in segs:
            _glow_text(card, (x, cy), t, cap_f, colors.get(ck, WHITE), (0, 0, 0),
                       glow_radius=6, glow_alpha=210, stroke_width=2, stroke_fill=(8, 5, 2))
            draw = ImageDraw.Draw(card)
            x += _tw(draw, t, cap_f)
        cy -= cap_f.size + 16

    # ---- bottom strip: leaf + wordmark left, rank flag right ----
    _leaf_mark(card, 46, H - 164, 78)
    draw = ImageDraw.Draw(card)
    if rank_flag:
        rf = F("ariblk.ttf", 42)
        rw = _tw(draw, rank_flag.upper(), rf)
        _glow_text(card, (W - rw - 50, H - 104), rank_flag.upper(), rf, GOLD_HI, (0, 0, 0),
                   glow_radius=7, glow_alpha=200, stroke_width=2, stroke_fill=(10, 6, 3))
        draw = ImageDraw.Draw(card)

    # ---- sparkles near gem + caption ----
    _sparkles(card, [(W - 240, 30, W - 30, 220), (60, H - 250, W - 60, H - 90)],
              46, seed=sparkle_seed or hash(player) % 99999)
    draw = ImageDraw.Draw(card)

    # ---- gold-dust pinstripe frame ----
    rnd = random.Random(7)
    dust = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    dd = ImageDraw.Draw(dust)
    for _ in range(900):
        edge = rnd.randrange(4)
        if edge == 0:
            x, y = rnd.uniform(0, W), rnd.uniform(0, 26)
        elif edge == 1:
            x, y = rnd.uniform(0, W), rnd.uniform(H - 26, H)
        elif edge == 2:
            x, y = rnd.uniform(0, 26), rnd.uniform(0, H)
        else:
            x, y = rnd.uniform(W - 26, W), rnd.uniform(0, H)
        s = rnd.uniform(0.8, 2.6)
        dd.ellipse([x, y, x + s, y + s], fill=(*GOLD_HI, rnd.randint(60, 190)))
    card.alpha_composite(dust)
    draw = ImageDraw.Draw(card)
    draw.rounded_rectangle([8, 8, W - 8, H - 8], 30, outline=BARK_DEEP, width=16)
    draw.rounded_rectangle([18, 18, W - 18, H - 18], 24, outline=GOLD, width=4)
    draw.rounded_rectangle([26, 26, W - 26, H - 26], 20, outline=(*GOLD_HI, 150), width=2)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    card.convert("RGB").save(out_path, quality=93)
    return out_path


if __name__ == "__main__":
    import sys
    if "--demo" in sys.argv:
        art, out = sys.argv[2], sys.argv[3]
        compose_card(
            art, out, player="Tristan Atkins", nickname='"The Franchise"', gem=99,
            series="MILESTONE",
            caption_lines=[
                [("FIRST PLAYER IN FRANCHISE HISTORY ", "white"), ("TO 50 HOME RUNS", "gold")],
                [("SWEEP-CLINCHING WIN ", "white"), ("VS ZERO TO HIRO", "red")],
            ],
            rank_flag="1st ever",
        )
        print(f"demo card -> {out}")
