"""Turn Kling card-animation clips into deployed 'living cards' — one command.

The only manual step left after generating in Kling is naming each output for its
card (the Kling filenames are random). Two ways to feed this:

  A) Name each clip <card-asset>.mp4  (e.g. glove-125-hits.mp4) and drop into the
     --src folder, then:  python scripts/process_living_cards.py --src <folder>
  B) Don't want to rename? Run with --identify to build a numbered contact sheet of
     first-frames so you (or Claude) can map them, then pass --map "0=glove-125-hits,1=..."

For each recognized clip it: seamless-loops the seam (no visible jump), scales to
540px wide, strips audio, writes site/public/cards/living/<asset>.mp4. Then it runs
the export so cards.json flags them living. Finish with the usual build + deploy
(or pass --deploy to do it all).

    python scripts/process_living_cards.py --src "C:/Slowpitch/Card Art/LIVING-CARDS-KIT/living-cards"
    python scripts/process_living_cards.py --src <folder> --deploy
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import imageio_ffmpeg

REPO = Path(__file__).resolve().parents[1]
LIVING_OUT = REPO / "site" / "public" / "cards" / "living"
CARDS_WEBP = REPO / "site" / "public" / "cards"
FF = imageio_ffmpeg.get_ffmpeg_exe()


def valid_assets() -> set[str]:
    m = json.loads((REPO / "data" / "processed" / "trading_cards.json").read_text(encoding="utf-8"))
    return {c["asset"] for c in m["cards"]}


def duration(f: Path) -> float:
    r = subprocess.run([FF, "-i", str(f)], capture_output=True, text=True)
    mt = re.search(r"Duration: (\d+):(\d+):([\d.]+)", r.stderr)
    return int(mt.group(2)) * 60 + float(mt.group(3)) if mt else 5.0


def loop_clip(src: Path, asset: str) -> bool:
    D = duration(src)
    X = min(0.7, D * 0.16)
    fc = (f"[0:v]scale=540:-2,setsar=1,fps=24[s];[s]split[main][head];"
          f"[head]trim=0:{X:.3f},setpts=PTS+{D - X:.3f}/TB,format=yuva420p,"
          f"fade=t=in:st={D - X:.3f}:d={X:.3f}:alpha=1[h2];[main][h2]overlay[v]")
    LIVING_OUT.mkdir(parents=True, exist_ok=True)
    out = LIVING_OUT / f"{asset}.mp4"
    r = subprocess.run([FF, "-y", "-i", str(src), "-filter_complex", fc, "-map", "[v]", "-an",
                        "-c:v", "libx264", "-crf", "25", "-pix_fmt", "yuv420p",
                        "-movflags", "+faststart", "-t", f"{D:.3f}", str(out)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  ! {asset}: ffmpeg failed — {r.stderr[-160:]}")
        return False
    print(f"  ✓ {asset}  ({out.stat().st_size // 1024} KB seamless loop)")
    return True


def main():
    ap = argparse.ArgumentParser(description="Process Kling clips into living cards")
    ap.add_argument("--src", required=True, help="folder of Kling .mp4 outputs")
    ap.add_argument("--map", help='index->asset map when files are not named, e.g. "0=glove-125-hits,1=kives-150-tb"')
    ap.add_argument("--identify", action="store_true", help="build a numbered first-frame contact sheet and exit")
    ap.add_argument("--deploy", action="store_true", help="export + build + deploy after processing")
    args = ap.parse_args()

    src = Path(args.src)
    clips = sorted(src.glob("*.mp4"))
    if not clips:
        raise SystemExit(f"No .mp4 files in {src}")
    assets = valid_assets()

    if args.identify:
        from PIL import Image, ImageDraw, ImageFont
        (src / "_id").mkdir(exist_ok=True)
        thumbs = []
        for i, c in enumerate(clips):
            fp = src / "_id" / f"{i:02d}.jpg"
            subprocess.run([FF, "-y", "-ss", "1", "-i", str(c), "-frames:v", "1", "-q:v", "3", str(fp)],
                           capture_output=True)
            thumbs.append((i, fp, c.name))
        TH, COLS = 300, 6
        rows = (len(thumbs) + COLS - 1) // COLS
        cw = int(TH * 0.66)
        sheet = Image.new("RGB", (COLS * (cw + 10) + 10, rows * (TH + 30) + 10), (24, 13, 6))
        d = ImageDraw.Draw(sheet)
        fnt = ImageFont.truetype("C:/Windows/Fonts/seguibl.ttf", 18)
        for i, fp, _ in thumbs:
            im = Image.open(fp); im.thumbnail((cw, TH))
            x, y = 10 + (i % COLS) * (cw + 10), 10 + (i // COLS) * (TH + 30)
            sheet.paste(im, (x, y))
            d.text((x + 4, y + TH + 4), f"#{i}", font=fnt, fill=(250, 224, 150))
        out = src / "IDENTIFY.jpg"
        sheet.save(out, quality=85)
        print(f"contact sheet -> {out}\nmap with: --map \"0=<asset>,1=<asset>,...\" (indexes match #labels)")
        for i, _, name in thumbs:
            print(f"  #{i}: {name}")
        return

    # build the file->asset resolution
    todo: list[tuple[Path, str]] = []
    if args.map:
        idx = {int(k): v.strip() for k, v in (p.split("=") for p in args.map.split(","))}
        for i, c in enumerate(clips):
            if i in idx:
                todo.append((c, idx[i]))
    else:
        for c in clips:
            if c.stem in assets:
                todo.append((c, c.stem))
            else:
                print(f"  ? {c.name}: stem not a known card asset — rename to <asset>.mp4 or use --map")

    if not todo:
        raise SystemExit("Nothing to process. Name files <asset>.mp4 or pass --map (see --identify).")
    unknown = [a for _, a in todo if a not in assets]
    if unknown:
        raise SystemExit(f"Unknown card assets: {unknown}")

    print(f"processing {len(todo)} living cards:")
    ok = sum(loop_clip(c, a) for c, a in todo)
    print(f"\n{ok}/{len(todo)} looped -> {LIVING_OUT}")

    print("re-exporting cards.json (flags living)…")
    subprocess.run([sys.executable, str(REPO / "scripts" / "export_site_data.py")], check=True)

    if args.deploy:
        print("building + deploying…")
        subprocess.run(["npm", "run", "build"], cwd=REPO / "site", shell=os.name == "nt", check=True)
        subprocess.run([sys.executable, str(REPO / "scripts" / "deploy_site.py")], check=True)
        print("LIVE.")
    else:
        print("next: cd site && npm run build  ·  python scripts/deploy_site.py")


if __name__ == "__main__":
    main()
