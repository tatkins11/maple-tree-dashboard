"""Seed-race projection — exhaustive enumeration of every remaining outcome.

Sixteen league games remain in Summer 2026, so the whole season tree is 2^16 = 65,536
branches. That is small enough to walk ALL of them rather than simulate, which means
the numbers here are exact given a per-game win probability, not sampled.

Two readings are produced for every team:
  * `possible`   — does ANY branch end with them on the top seed? Pure combinatorics,
                   no probability involved. This is what "mathematically alive" means.
  * `p_top_seed` — the probability mass of the branches where they do, using a log5
                   matchup probability built from each club's Pythagorean win
                   expectation. Assumes independence between games, which is the usual
                   simplification and worth stating out loud.

Seeding follows the site: win percentage, then run differential.

Writes site/src/data/seed_race.json for the Astro page.

    python scripts/build_seed_race.py
"""
from __future__ import annotations

import csv
import json
from itertools import product
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LEAGUE = REPO / "data" / "processed" / "league_schedule_games.csv"
OUT = REPO / "site" / "src" / "data" / "seed_race.json"
SEASON = "Maple Tree Summer 2026"
US = "Maple Tree"

# Pythagorean exponent for slowpitch. Baseball's ~1.83 is tuned to a 4-5 run
# environment; this league averages ~12 a side, where run differential carries less
# information per run, so a flatter exponent fits. 1.5 is the common choice for
# high-scoring formats and is used only to seed matchup odds, never to rank anyone.
PYTHAG_EXP = 1.5


def load():
    played, remaining = [], []
    for r in csv.DictReader(open(LEAGUE, encoding="utf-8-sig")):
        if r["season"] != SEASON:
            continue
        h, a = (r["home_team"] or "").strip(), (r["away_team"] or "").strip()
        if not h or not a:
            continue
        if r["completed_flag"] == "1" and r["home_runs"] != "":
            played.append((h, a, int(r["home_runs"]), int(r["away_runs"])))
        else:
            remaining.append((r["week_label"], h, a))
    return played, remaining


def standings(played):
    t = {}
    for h, a, hr, ar in played:
        for name in (h, a):
            t.setdefault(name, {"w": 0, "l": 0, "rf": 0, "ra": 0})
        t[h]["rf"] += hr; t[h]["ra"] += ar
        t[a]["rf"] += ar; t[a]["ra"] += hr
        if hr > ar:
            t[h]["w"] += 1; t[a]["l"] += 1
        else:
            t[a]["w"] += 1; t[h]["l"] += 1
    return t


def win_prob(t, home, away):
    """log5 on Pythagorean expectations. Clamped so nothing is a certainty —
    Everything hurts at 0-10 still gets a puncher's chance, which is honest."""
    def pyth(name):
        s = t[name]
        rf, ra = max(s["rf"], 1), max(s["ra"], 1)
        return rf ** PYTHAG_EXP / (rf ** PYTHAG_EXP + ra ** PYTHAG_EXP)
    a_, b_ = pyth(home), pyth(away)
    denom = a_ * (1 - b_) + b_ * (1 - a_)
    p = 0.5 if denom == 0 else (a_ * (1 - b_)) / denom
    return min(max(p, 0.05), 0.95)


def seed_order(final):
    """Site convention: win pct, then run differential."""
    return sorted(final, key=lambda n: (-(final[n]["w"] / max(final[n]["w"] + final[n]["l"], 1)),
                                        -(final[n]["rf"] - final[n]["ra"]), n))


def main() -> None:
    played, remaining = load()
    base = standings(played)
    for _, h, a in remaining:
        for n in (h, a):
            base.setdefault(n, {"w": 0, "l": 0, "rf": 0, "ra": 0})

    n_games = len(remaining)
    print(f"{len(played)} games played, {n_games} remaining -> {2 ** n_games:,} branches")

    probs = [win_prob(base, h, a) for _, h, a in remaining]
    # Average margin per team, used to move run differential in each branch so the
    # tiebreaker is not frozen at today's value.
    margin = {n: (s["rf"] - s["ra"]) / max(s["w"] + s["l"], 1) for n, s in base.items()}

    top_mass = {n: 0.0 for n in base}
    top_possible = {n: False for n in base}
    top3_mass = {n: 0.0 for n in base}
    us_seed_mass = {}

    for outcome in product([1, 0], repeat=n_games):
        p = 1.0
        final = {n: dict(s) for n, s in base.items()}
        for (idx, (_, h, a)), home_won in zip(enumerate(remaining), outcome):
            p *= probs[idx] if home_won else (1 - probs[idx])
            w, lo = (h, a) if home_won else (a, h)
            final[w]["w"] += 1
            final[lo]["l"] += 1
            edge = (abs(margin[w]) + abs(margin[lo])) / 2 or 1.0
            final[w]["rf"] += edge
            final[lo]["ra"] += edge
        order = seed_order(final)
        top_mass[order[0]] += p
        top_possible[order[0]] = True
        for n in order[:3]:
            top3_mass[n] += p
        us_seed_mass[order.index(US) + 1] = us_seed_mass.get(order.index(US) + 1, 0.0) + p

    total = sum(top_mass.values())
    rows = []
    for n, s in base.items():
        g = s["w"] + s["l"]
        left = sum(1 for _, h, a in remaining if n in (h, a))
        rows.append({
            "team": n, "wins": s["w"], "losses": s["l"], "games": g,
            "win_pct": s["w"] / max(g, 1),
            "runs_for": s["rf"], "runs_against": s["ra"],
            "run_diff": s["rf"] - s["ra"],
            "rs_per_game": s["rf"] / max(g, 1), "ra_per_game": s["ra"] / max(g, 1),
            "games_left": left, "max_wins": s["w"] + left,
            "p_top_seed": top_mass[n] / total,
            "p_top_three": top3_mass[n] / total,
            "alive_for_top_seed": top_possible[n],
            "is_team": n == US,
        })
    rows.sort(key=lambda r: (-r["win_pct"], -r["run_diff"]))
    for i, r in enumerate(rows, 1):
        r["seed"] = i

    payload = {
        "season": SEASON,
        "games_played": len(played),
        "games_remaining": n_games,
        "branches": 2 ** n_games,
        "method": ("Every remaining outcome enumerated exactly — 2^%d branches, not a "
                   "sample. Per-game odds come from a log5 matchup on each club's "
                   "Pythagorean expectation (exponent %.1f, flattened for a ~12-run "
                   "environment) and are clamped to [5%%, 95%%] so no result is treated "
                   "as certain. Games are assumed independent. Seeding is win "
                   "percentage, then run differential." % (n_games, PYTHAG_EXP)),
        "teams": rows,
        "our_seed_odds": [{"seed": k, "p": v / total} for k, v in sorted(us_seed_mass.items())],
        "remaining": [{"week": w, "home": h, "away": a,
                       "home_win_prob": probs[i], "involves_us": US in (h, a)}
                      for i, (w, h, a) in enumerate(remaining)],
    }
    OUT.write_text(json.dumps(payload, indent=1), encoding="utf-8")

    print(f"\n{'TEAM':18s} {'W-L':>6s} {'LEFT':>4s} {'MAX':>4s} {'P(#1)':>7s} {'P(top3)':>8s}  alive")
    for r in rows:
        print(f"{r['team']:18s} {r['wins']}-{r['losses']:<4d} {r['games_left']:4d} "
              f"{r['max_wins']:4d} {r['p_top_seed']:6.1%} {r['p_top_three']:7.1%}  "
              f"{'yes' if r['alive_for_top_seed'] else 'ELIMINATED'}"
              + ("   <- us" if r["is_team"] else ""))
    print(f"\nMaple Tree seed distribution:")
    for e in payload["our_seed_odds"]:
        if e["p"] > 0.001:
            print(f"  #{e['seed']}: {e['p']:.1%}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
