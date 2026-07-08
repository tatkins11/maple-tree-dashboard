"""Convert Brian's trading-card PNGs into web-ready assets.

Reads data/processed/trading_cards.json, pulls each source image from
C:/Slowpitch/Player Trading Cards, and writes a 640px-wide webp to
site/public/cards/<asset>.webp. Idempotent — skips assets that are already
newer than their source. Run before export_site_data.py when cards change.
"""
from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

REPO = Path(__file__).resolve().parents[1]
SOURCE_DIR = Path("C:/Slowpitch/Player Trading Cards")
MANIFEST = REPO / "data" / "processed" / "trading_cards.json"
OUT_DIR = REPO / "site" / "public" / "cards"
WIDTH = 640


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    done = skipped = 0
    for card in manifest["cards"]:
        if card.get("generated"):  # art built by its own script (e.g. the Joey card)
            skipped += 1
            continue
        src = SOURCE_DIR / card["file"]
        dst = OUT_DIR / f"{card['asset']}.webp"
        if not src.exists():
            print(f"  ! missing source: {src}")
            continue
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
            skipped += 1
            continue
        img = Image.open(src).convert("RGB")
        ratio = WIDTH / img.width
        img = img.resize((WIDTH, round(img.height * ratio)), Image.LANCZOS)
        img.save(dst, quality=84, method=6)
        done += 1
        print(f"  {card['asset']}.webp <- {card['file']} ({dst.stat().st_size // 1024} KB)")
    print(f"cards processed: {done} new, {skipped} up to date")


if __name__ == "__main__":
    main()
