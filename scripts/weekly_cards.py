"""Weekly card pipeline — milestone hits in, finished unique cards out.

Flow (run after the weekly sync + export, when milestones.json is fresh):

    python scripts/weekly_cards.py --date 2026-07-15          # plan (dry run)
    python scripts/weekly_cards.py --date 2026-07-15 --make   # generate + composite

For each milestone reached that week it:
  1. builds a ONE-OF-A-KIND art concept — a visual angle never used on any prior
     card (tracked in data/processed/card_concepts.json) mixed with the player's
     lore and the milestone's motif;
  2. generates the ENTIRE CARD in one shot with gpt_image_2 (locked by Brian
     2026-07-21 after the Tristan 50-HR keeper): three references — the player's
     photo (likeness), a club original card (series style), the Tap badge (kit) —
     with the data-true text (name, number, stat, franchise rank, rating) spelled
     out in the prompt. No compositor, no Souls: unified design beats overlays.
     (Souls remain the tool for VIDEO, where photoreal motion needs them.)
  3. drops the finished card into site/public/cards/ and appends the manifest
     entry to data/processed/trading_cards.json (kind=milestone, generated).
  Retake rule: if any word renders smudged/wrong, regenerate that card (~7 cr).

Cost discipline: prints the per-card estimate and total before generating;
art is one gpt_image_2/soul render per card (~7 cr at 2k).

After running: export -> build -> deploy as usual so backs generate on the site.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

HIGGS = shutil.which("higgsfield") or str(Path.home() / "AppData/Roaming/npm/higgsfield.cmd")
BADGE_REF = Path("C:/Slowpitch/Logo/Maple Tree Logo - restored transparent.png")
PHOTO_DIR = Path("C:/Slowpitch/player pics/souls-training")
STYLE_REF = Path("C:/Slowpitch/Player Trading Cards/138f1190-05b1-45bb-b638-950715beda72.png")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from card_frame import gem_tier_for  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "site" / "src" / "data"
MANIFEST = REPO / "data" / "processed" / "trading_cards.json"
CONCEPT_LOG = REPO / "data" / "processed" / "card_concepts.json"
SOULS = REPO / "data" / "players_souls.json"
ART_DIR = Path("C:/Slowpitch/Card Art")
CARDS_OUT = REPO / "site" / "public" / "cards"

# ---------------------------------------------------------------- creative pools
# Visual angles — each used AT MOST ONCE across the whole set (logged), so every
# card is its own thing. Add more angles any time; the pipeline refuses to reuse.
ANGLES = [
    ("storm-monument", "at dusk under breaking storm clouds with god rays, embers and autumn maple leaves swirling, legendary monument mood"),
    ("night-floodlights", "at night under blazing rec-league floodlights, moths in the beams, long dramatic shadows, electric playoff atmosphere"),
    ("golden-hour", "at golden hour with warm low sun flaring across the infield dirt, long soft shadows, nostalgic summer evening"),
    ("neon-retro", "as a 1980s neon retro sports poster, chrome glow, gridlines on the horizon, synthwave dusk sky in warm maple tones"),
    ("comic-pop", "as bold comic-book pop art with halftone dots, dramatic action lines, thick inked outlines, vintage comic cover energy"),
    ("oil-classic", "as a classical oil painting in the style of old sports heroes portraits, rich brushwork, museum lighting, ornate mood"),
    ("35mm-film", "shot on grainy 35mm film in the 1970s, faded warm colors, light leaks, authentic vintage sports photography"),
    ("winter-frost", "in unexpected winter frost, breath visible, frozen infield sparkling, low white sun, quiet epic stillness"),
    ("county-fair", "at a bustling county fair at dusk, ferris wheel bokeh, string lights, popcorn stands, Americana warmth"),
    ("moon-shot", "on the surface of the moon with Earth rising over the outfield fence, surreal epic space Americana"),
    ("thunderstorm", "mid-thunderstorm with a lightning bolt splitting the sky behind the backstop, rain frozen in the flash"),
    ("harvest", "in a maple orchard at peak autumn color, sap buckets on the trees, golden leaves carpeting the base paths"),
    ("blueprint", "as a vintage engineer's blueprint come to life, cyan paper and chalk-white schematic lines glowing warm at the edges"),
    ("stadium-confetti", "amid a championship confetti storm in a packed little stadium, flashbulbs popping everywhere"),
    ("misty-dawn", "in thick morning mist at dawn, first light cutting through, dew on the grass, mythic quiet"),
    ("desert-highway", "on a lonely desert highway diamond at sunset, heat shimmer, a water tower on the horizon, cinematic western mood"),
]

# Milestone motifs — what the scene is ABOUT, keyed by stat.
MOTIFS = {
    "HR": "a mighty home-run moment: a ball rocketing over a distant outfield fence, outfielders watching helplessly",
    "Hits": "a rain of scuffed softballs frozen mid-air over the infield, each one a hit that counted",
    "Singles": "a worn first-base bag glowing like a trophy, spike marks all around it",
    "Doubles": "second base standing alone under a spotlight, a dust cloud from a slide just settling",
    "Triples": "a long dust trail carving from home around first and second toward third base",
    "RBI": "runners crossing home plate in a triumphant parade of dust and high-fives",
    "Runs": "home plate glowing at the end of a scorched base path, cleat prints stamped deep",
    "Walks": "a bat resting calmly against the dugout fence while a base waits patiently",
    "Total Bases": "all four bases lifting off the dirt like ascending monuments in a row",
    "PA": "a batter's box worn deep with a thousand footprints, bathed in light",
    "AB": "a batter's box worn deep with a thousand footprints, bathed in light",
    "Games": "a wall of tally marks carved into the dugout wood, a glove and cap hung with honor",
}

# Player lore — flavor objects/motifs per player (from the club's card canon).
LORE = {
    "glove": "a legendary well-worn fielding glove, bags of bats, a hint of ice and frost",
    "tristan": "crackling lightning energy, a captain's presence",
    "tim": "a tray of 99-cent beers on the dugout bench, a wise calm",
    "kives": "beach vibes, a red solo cup, an outfield fence with a dent in it",
    "porter": "warehouse pallets and streetlights, delivery-run hustle",
    "jj": "a full-extension diving catch silhouette, frequent-flyer wings",
    "corey": "a faint UFO in the sky, a splash of mud, a mysteriously empty bottle",
    "walsh": "carnival tickets and a strongman bell, a big grin energy",
    "joel": "fireworks bursting overhead, roman candles in a bucket",
    "snaxx": "a cooler overflowing with snacks, a hot dog, party vibes",
    "jason": "arcade neon, a game controller resting on the bench, pixel confetti",
    "duff": "a trusty duffel bag of gear, blue-collar grit",
    "slomka": "a fresh jersey with tags just ripped off, new-guy spark",
}

STAT_WORD = {"Hits": "hits", "HR": "home runs", "RBI": "RBI", "Runs": "runs",
             "Doubles": "doubles", "Triples": "triples", "Singles": "singles",
             "Walks": "walks", "Total Bases": "total bases", "PA": "plate appearances",
             "AB": "at-bats", "Games": "games played"}

MS_FIELD = {"Hits": "hits", "HR": "hr", "RBI": "rbi", "Runs": "r", "Doubles": "2b",
            "Triples": "3b", "Singles": "1b", "Walks": "bb", "Total Bases": "tb",
            "PA": "pa", "AB": "ab", "Games": "games"}


def load(path, default=None):
    p = Path(path)
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def ordinal(n):
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


VOLUME_STATS = {"PA", "AB", "Games"}


def rating_for(rank: int, stat: str) -> int:
    """MLB-The-Show-style rating from achievement rareness. 99 is RESERVED for
    franchise firsts (rank 1); each earlier club member costs 3 points; pure
    volume milestones (PA/AB/Games) take a 4-point haircut; floor 65."""
    r = 99 - 3 * (rank - 1) - (4 if stat in VOLUME_STATS else 0)
    return max(r, 65)


def pick_angle(used):
    for key, desc in ANGLES:
        if key not in used:
            return key, desc
    raise SystemExit("All visual angles used! Add new ones to ANGLES in weekly_cards.py.")


# What the COLOSSAL milestone number is built from, per visual angle — the
# number is always a physical monument in the scene (the set's signature move).
NUMBER_MATERIAL = {
    "storm-monument": "molten gold wrapped in crackling lightning",
    "night-floodlights": "blazing stadium light-tubes showering sparks",
    "golden-hour": "three-story 3D gold glitter, sun-flared",
    "neon-retro": "electric neon tubing, buzzing pink-gold",
    "comic-pop": "chromed steel with halftone shading and action lines",
    "oil-classic": "gilded carved museum-frame wood",
    "35mm-film": "vintage theater marquee bulbs, half flickering",
    "winter-frost": "carved glacial ice, glowing from within",
    "county-fair": "carnival marquee bulbs and painted fairground wood",
    "moon-shot": "carved moon rock rim-lit by earthlight",
    "thunderstorm": "pure plasma lightning frozen mid-strike",
    "harvest": "ten thousand autumn maple leaves and flowing maple syrup gold",
    "blueprint": "glowing chalk-white schematic lines lifting off the paper",
    "stadium-confetti": "compressed championship confetti and fireworks",
    "misty-dawn": "solid shafts of dawn god-ray light",
    "desert-highway": "rusted highway-sign steel and buzzing motel neon",
}


def build_prompt(angle_key, angle_desc, motif, lore, player, nickname, value, stat_word,
                 rank_ord, rating):
    """One-shot FULL-CARD design (locked by Brian 2026-07-21 after the Tristan 50 HR
    keeper): the model composes the entire card — art, border, typography — in one
    unified piece, matching the club's original card series via a style reference."""
    material = NUMBER_MATERIAL.get(angle_key, "molten gold")
    nick = f' His nickname "{nickname}" may appear as a small script accent.' if nickname else ""
    return (
        "Design a complete premium sports trading card, matching the exact art style, layout "
        "language, ornate foil border and finish of the reference trading card (second image): "
        "dramatic stylized painted-photo look with bold display typography integrated into the "
        "design. The player is the person from the first reference photo — faithful likeness, "
        f"heroic rendering — mid-action as the central figure. Concept: {motif}, rendered "
        f"{angle_desc}, with a giant glowing number '{value}' built from {material} as the "
        f"design centerpiece behind or beside him. Personal touches woven in: {lore}. "
        "He wears a navy jersey carrying the circular wood-badge bar logo from the third "
        "reference image printed on the chest, faithfully reproduced at realistic size. "
        f"Card text, large and clean, EXACTLY these words and no others: 'MILESTONE MOMENT' "
        f"banner at top, '{player.upper()}' as the name, the giant '{value}', "
        f"'{stat_word.upper()}' beneath it, '{rank_ord.upper()} IN FRANCHISE HISTORY' along "
        f"the bottom, and a red faceted gem in the top corner containing '{rating}'.{nick} "
        "Maple leaf motifs in the border. Warm espresso brown, maple orange and gold palette. "
        "Rich, flashy, collectible — every word spelled exactly as given."
    )


def main():
    ap = argparse.ArgumentParser(description="Weekly milestone-card pipeline")
    ap.add_argument("--date", help="YYYY-MM-DD of the game night (default: latest in milestones.json)")
    ap.add_argument("--make", action="store_true", help="actually generate (default: dry-run plan)")
    ap.add_argument("--only", help="comma-separated slugs to limit to")
    args = ap.parse_args()

    milestones = load(DATA / "milestones.json")
    career = {p["slug"]: p for p in load(DATA / "career_stats.json")["standard"]}
    souls = {p["slug"]: p for p in load(SOULS, {"players": []})["players"]}
    manifest = load(MANIFEST)
    concepts = load(CONCEPT_LOG, {"used_angles": {}, "cards": []})
    used_angles = set(concepts["used_angles"].values())

    recent = milestones["recent"]
    date = args.date or max(e["date"] for e in recent)
    events = [e for e in recent if e["date"] == date]
    if args.only:
        # "tristan" keeps all of a player's cards; "tristan:HR" narrows to one stat
        keep = {s.strip().lower() for s in args.only.split(",")}
        events = [e for e in events
                  if e["slug"] in keep or f"{e['slug']}:{e['stat'].lower()}" in keep]
    if not events:
        raise SystemExit(f"No milestones reached on {date}.")

    existing_assets = {c["asset"] for c in manifest["cards"]}
    plan = []
    for e in events:
        asset = f"{e['slug']}-{e['milestone']}-{MS_FIELD.get(e['stat'], e['stat'].lower())}".replace(" ", "")
        if asset in existing_assets:
            print(f"  skip (card exists): {asset}")
            continue
        angle_key, angle_desc = pick_angle(used_angles)
        used_angles.add(angle_key)
        c = career[e["slug"]]
        rank = sum(1 for p in career.values()
                   if float(p.get(MS_FIELD[e["stat"]]) or 0) >= e["milestone"])
        rating = rating_for(rank, e["stat"])
        # display name + nickname from the roster registry
        full_name = souls.get(e["slug"], {}).get("player", e["player"])
        nick = None
        if '"' in full_name:
            nick = full_name.split('"')[1]
        photo = PHOTO_DIR / e["slug"] / "01.jpg"
        plan.append({
            "event": e, "asset": asset, "angle": angle_key,
            "prompt": build_prompt(angle_key, angle_desc, MOTIFS.get(e["stat"], MOTIFS["Hits"]),
                                   LORE.get(e["slug"], "team spirit"), e["player"], nick,
                                   e["milestone"], STAT_WORD.get(e["stat"], e["stat"]),
                                   ordinal(rank), rating),
            "photo": photo if photo.exists() else None, "rank": rank, "rating": rating,
            "stats": [("AVG", f"{c['avg']:.3f}".lstrip('0')), ("H", int(c["hits"])),
                      ("HR", int(c["hr"])), ("RBI", int(c["rbi"]))],
        })

    from card_frame import gem_tier_for
    print(f"\n{len(plan)} card(s) planned for {date}:")
    for p in plan:
        e = p["event"]
        mode = "photo-ref one-shot" if p["photo"] else "NO PHOTO — style refs only"
        print(f"  {e['player']:9} {e['milestone']} {e['stat']:12} "
              f"OVR {p['rating']} {gem_tier_for(p['rating']):6} ({ordinal(p['rank'])} ever)  "
              f"angle={p['angle']:18} {mode}")
    est = len(plan) * 7
    print(f"\nestimated cost: ~{est} credits ({len(plan)} x ~7 cr one-shot full-card 2k)")
    if not args.make:
        print("\n[dry run] re-run with --make to generate.")
        return

    made = 0
    for p in plan:
        e = p["event"]
        print(f"\n=== {e['player']} — {e['milestone']} {e['stat']} ({p['angle']}) ===")
        art_path = ART_DIR / f"{p['asset']}-card.png"
        # one-shot full-card: photo ref (likeness) + style ref (series) + badge ref (kit)
        cmd = [HIGGS, "generate", "create", "gpt_image_2", "--prompt", p["prompt"]]
        for ref in (p["photo"], STYLE_REF, BADGE_REF):
            if ref and Path(ref).exists():
                cmd += ["--image", str(ref)]
        cmd += ["--aspect_ratio", "2:3", "--resolution", "2k", "--quality", "high",
                "--wait", "--json"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=420, shell=HIGGS.endswith(".cmd"))
        try:
            jobs = json.loads(r.stdout)
            j = jobs[0] if isinstance(jobs, list) else jobs
            url = j["result_url"]
        except Exception:
            print(f"  FAILED to parse job result: {r.stdout[:300]} {r.stderr[:300]}")
            continue
        ART_DIR.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, art_path)

        word = STAT_WORD.get(e["stat"], e["stat"].lower())
        out = CARDS_OUT / f"{p['asset']}.webp"
        from PIL import Image
        img = Image.open(art_path).convert("RGB")
        img.thumbnail((640, 960), Image.LANCZOS)
        img.save(out, quality=88, method=6)

        manifest["cards"].append({
            "file": "", "asset": p["asset"], "slug": e["slug"], "kind": "milestone",
            "generated": True, "rating": p["rating"], "stat": e["stat"], "value": e["milestone"],
            "caption": f"{ordinal(p['rank'])} player in Tappers history to reach "
                       f"{e['milestone']} {word}",
            "flavor": "",
        })
        concepts["used_angles"][p["asset"]] = p["angle"]
        concepts["cards"].append({"asset": p["asset"], "date": date, "angle": p["angle"],
                                  "recipe": "one-shot"})
        made += 1
        print(f"  card -> {out.name}  (angle: {p['angle']})")

    MANIFEST.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    CONCEPT_LOG.write_text(json.dumps(concepts, indent=2), encoding="utf-8")
    print(f"\n{made} card(s) made. Manifest + concept log updated.")
    print("NOTE: write each new card's 'flavor' text in trading_cards.json (author's voice),")
    print("then: export_site_data.py -> npm build -> deploy. Backs generate on the site.")


if __name__ == "__main__":
    main()
