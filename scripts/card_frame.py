"""Card frame compositor — the one-time template for the weekly card pipeline.

Higgsfield renders ART ONLY (never text — AI text comes out mangled). This module
lays the club's frame over that art and composites every word and number from real
data: season banner, series, gem, player nameplate, milestone banner, stat
scoreboard, footer. Art stays unique per card; the frame makes the set a set.

    from card_frame import compose_card
    compose_card(art, out, player="Tristan", gem="50", banner="50 CAREER HOME RUNS",
                 sub_banner="1st in franchise history · July 15, 2026",
                 stats=[("AVG", ".606"), ("HR", "50"), ("RBI", "156"), ("R", "125")])

CLI smoke test:  python scripts/card_frame.py --demo <art.png> <out.png>
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

W, H = 1024, 1434  # 2.5x3.5 trading-card ratio
BARK = (44, 26, 13)
BARK_DEEP = (31, 17, 7)
GOLD = (217, 169, 90)
GOLD_HI = (242, 212, 136)
CREAM = (244, 232, 204)
MAPLE = (194, 65, 12)
MAROON = (124, 33, 48)
SILVER = (200, 202, 208)
SILVER_HI = (236, 238, 242)

FONTS = Path("C:/Windows/Fonts")


def F(name: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(FONTS / name), size)


def _tw(draw, text, font):
    l, t, r, b = draw.textbbox((0, 0), text, font=font)
    return r - l


def _fit(draw, text, font_name, max_width, start, floor=30):
    size = start
    while size > floor:
        f = F(font_name, size)
        if _tw(draw, text, f) <= max_width:
            return f
        size -= 4
    return F(font_name, floor)


def _vgrad(width, height, top_rgba, bottom_rgba):
    grad = Image.new("RGBA", (1, height))
    for y in range(height):
        t = y / max(height - 1, 1)
        px = tuple(int(top_rgba[i] + (bottom_rgba[i] - top_rgba[i]) * t) for i in range(4))
        grad.putpixel((0, y), px)
    return grad.resize((width, height))


def compose_card(art_path, out_path, *, player, gem, banner, sub_banner=None,
                 stats=(), series="MILESTONE SERIES", season_label="2026 SUMMER REC LEAGUE",
                 nickname=None, variant="milestone",
                 footer="MAPLE TREE TAPPERS  ·  THE MAPLE TREE TAP  ·  CARY, ILLINOIS"):
    """Composite one finished card. Returns out_path."""
    accent, accent_hi = (GOLD, GOLD_HI) if variant == "milestone" else (SILVER, SILVER_HI)

    card = Image.new("RGBA", (W, H), (*BARK_DEEP, 255))
    draw = ImageDraw.Draw(card)

    # ---- full-bleed art, cover-fit ----
    art = Image.open(art_path).convert("RGB")
    scale = max(W / art.width, H / art.height)
    art = art.resize((round(art.width * scale), round(art.height * scale)), Image.LANCZOS)
    ax, ay = (art.width - W) // 2, max((art.height - H) // 3, 0)  # bias crop upward
    card.paste(art.crop((ax, ay, ax + W, ay + H)), (0, 0))
    draw = ImageDraw.Draw(card)

    # ---- bottom info panel gradient (art fades into bark) ----
    panel_h = 560
    grad = _vgrad(W, panel_h, (*BARK_DEEP, 0), (*BARK_DEEP, 252))
    card.alpha_composite(grad, (0, H - panel_h))
    solid_h = 210  # fully solid strip at the very bottom for scoreboard+footer
    draw.rectangle([0, H - solid_h, W, H], fill=(*BARK_DEEP, 255))

    # ---- top banner chip (season + series) ----
    chip_w, chip_h, m = 470, 96, 34
    draw.rounded_rectangle([m, m, m + chip_w, m + chip_h], 14, fill=(*BARK_DEEP, 235),
                           outline=accent, width=3)
    draw.text((m + 20, m + 16), season_label, font=F("seguibl.ttf", 27), fill=CREAM)
    draw.text((m + 20, m + 53), series, font=F("seguibl.ttf", 24), fill=accent_hi)

    # ---- gem (top-right diamond with the milestone number) ----
    gx, gy, gr = W - 108, 116, 78
    diamond = [(gx, gy - gr), (gx + gr, gy), (gx, gy + gr), (gx - gr, gy)]
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.polygon(diamond, fill=(*accent, 120))
    card.alpha_composite(glow.filter(ImageFilter.GaussianBlur(18)))
    draw = ImageDraw.Draw(card)
    draw.polygon(diamond, fill=MAROON, outline=accent)
    inner = [(gx, gy - gr + 10), (gx + gr - 10, gy), (gx, gy + gr - 10), (gx - gr + 10, gy)]
    draw.polygon(inner, outline=accent_hi)
    gem_f = _fit(draw, str(gem), "impact.ttf", gr * 1.15, 74, 34)
    gw = _tw(draw, str(gem), gem_f)
    draw.text((gx - gw / 2, gy - gem_f.size / 2 - 6), str(gem), font=gem_f, fill=CREAM)

    # ---- bottom block, laid out bottom-up so nothing ever collides ----
    footer_y = H - 64
    sb_h = 92 if stats else 0
    sb_y0 = footer_y - 12 - sb_h
    sub_f = F("seguibl.ttf", 23)
    sub_y = sb_y0 - 36 if sub_banner else sb_y0
    ban_f = _fit(draw, banner.upper(), "seguibl.ttf", W - 240, 40, 26)
    pill_h = ban_f.size + 30
    pill_y0 = sub_y - 12 - pill_h
    rule_y = pill_y0 - 26
    nick_h = 50 if nickname else 0
    name_f = _fit(draw, player.upper(), "ROCKB.TTF", W - 150, 96, 52)
    name_y = rule_y - 20 - nick_h - name_f.size

    # nameplate
    name = player.upper()
    nw = _tw(draw, name, name_f)
    sh = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(sh).text(((W - nw) / 2 + 4, name_y + 5), name, font=name_f, fill=(0, 0, 0, 210))
    card.alpha_composite(sh.filter(ImageFilter.GaussianBlur(5)))
    draw = ImageDraw.Draw(card)
    draw.text(((W - nw) / 2, name_y), name, font=name_f, fill=CREAM,
              stroke_width=2, stroke_fill=BARK_DEEP)
    if nickname:
        nick_f = F("georgiab.ttf", 33)
        nkw = _tw(draw, nickname, nick_f)
        draw.text(((W - nkw) / 2, name_y + name_f.size + 12), nickname, font=nick_f, fill=accent_hi)

    # gold rules flanking a maple dot
    draw.line([W / 2 - 210, rule_y, W / 2 - 26, rule_y], fill=accent, width=3)
    draw.line([W / 2 + 26, rule_y, W / 2 + 210, rule_y], fill=accent, width=3)
    draw.ellipse([W / 2 - 9, rule_y - 9, W / 2 + 9, rule_y + 9], fill=MAPLE, outline=accent_hi)

    # milestone banner pill
    bw = _tw(draw, banner.upper(), ban_f)
    pw = bw + 76
    px0 = (W - pw) / 2
    draw.rounded_rectangle([px0, pill_y0, px0 + pw, pill_y0 + pill_h], pill_h / 2,
                           fill=MAPLE, outline=accent_hi, width=3)
    draw.text(((W - bw) / 2, pill_y0 + 13), banner.upper(), font=ban_f, fill=(255, 250, 240))

    # sub-banner line
    if sub_banner:
        sw = _tw(draw, sub_banner.upper(), sub_f)
        draw.text(((W - sw) / 2, sub_y), sub_banner.upper(), font=sub_f, fill=accent_hi)

    # stat scoreboard
    if stats:
        n = len(stats)
        sb_w = min(185 * n + 30, W - 140)
        sx0 = (W - sb_w) / 2
        draw.rounded_rectangle([sx0, sb_y0, sx0 + sb_w, sb_y0 + sb_h], 12,
                               fill=(16, 9, 4, 255), outline=accent, width=3)
        cell = sb_w / n
        val_f, lab_f = F("impact.ttf", 44), F("seguibl.ttf", 19)
        for i, (label, value) in enumerate(stats):
            cx = sx0 + cell * i + cell / 2
            if i:
                draw.line([sx0 + cell * i, sb_y0 + 14, sx0 + cell * i, sb_y0 + sb_h - 14],
                          fill=(*accent, 130), width=2)
            vw = _tw(draw, str(value), val_f)
            draw.text((cx - vw / 2, sb_y0 + 8), str(value), font=val_f, fill=CREAM)
            lw = _tw(draw, label.upper(), lab_f)
            draw.text((cx - lw / 2, sb_y0 + sb_h - 30), label.upper(), font=lab_f, fill=accent_hi)

    # footer
    foot_f = F("seguibl.ttf", 20)
    fw = _tw(draw, footer, foot_f)
    draw.text(((W - fw) / 2, footer_y), footer, font=foot_f, fill=(*GOLD, 255))

    # ---- frame: bark border + double pinstripe ----
    draw.rounded_rectangle([10, 10, W - 10, H - 10], 34, outline=BARK_DEEP, width=20)
    draw.rounded_rectangle([22, 22, W - 22, H - 22], 26, outline=accent, width=5)
    draw.rounded_rectangle([34, 34, W - 34, H - 34], 20, outline=(*accent_hi, 160), width=2)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    card.convert("RGB").save(out_path, quality=92)
    return out_path


if __name__ == "__main__":
    import sys
    if "--demo" in sys.argv:
        art, out = sys.argv[2], sys.argv[3]
        compose_card(art, out, player="Tristan", nickname='"The Franchise"',
                     gem="50", banner="50 career home runs",
                     sub_banner="1st in franchise history · July 15, 2026",
                     stats=[("AVG", ".606"), ("HR", "50"), ("RBI", "156"), ("R", "125")])
        print(f"demo card -> {out}")
