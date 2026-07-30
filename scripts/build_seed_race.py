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

    # ---- playoff bracket (Brian's sheet, all games Wed 8/19) ------------------
    # Every club makes it. Seeds 1-5 sit out the 6:30 round.
    #   R1   G1 #8v#9 · G2 #7v#10 · G3 #6v#11
    #   R2   G5 #1 vs W(G1) · G4 #4v#5      |  G6 #2 vs W(G2) · G7 #3 vs W(G3)
    #   SF   G8 W(G5) vs W(G4)              |  G9 W(G6) vs W(G7)
    #   F    G10 W(G8) vs W(G9)
    # So #1 and #2 are in opposite halves and can only meet in the final, while #3
    # rides with #2 and #4/#5 ride with #1. Strength for bracket matchups is the
    # club's CURRENT Pythagorean — a team's ability does not change based on which
    # branch of the regular season it took.
    pw = {(x, y): win_prob(base, x, y) for x in base for y in base if x != y}

    def advance(a: dict, b: dict) -> dict:
        """Winner distribution of a matchup between two entrant distributions."""
        out: dict = {}
        for t, pt in a.items():
            for o, po in b.items():
                if t == o:
                    continue
                m = pt * po
                out[t] = out.get(t, 0.0) + m * pw[(t, o)]
                out[o] = out.get(o, 0.0) + m * pw[(o, t)]
        return out

    def run_bracket(order: list) -> tuple[dict, dict]:
        S = {i + 1: {order[i]: 1.0} for i in range(len(order))}
        g1, g2, g3 = advance(S[8], S[9]), advance(S[7], S[10]), advance(S[6], S[11])
        g5, g4 = advance(S[1], g1), advance(S[4], S[5])
        g6, g7 = advance(S[2], g2), advance(S[3], g3)
        g8, g9 = advance(g5, g4), advance(g6, g7)
        final_entrants: dict = {}
        for d in (g8, g9):
            for t, v in d.items():
                final_entrants[t] = final_entrants.get(t, 0.0) + v
        return advance(g8, g9), final_entrants

    champ_mass = {n: 0.0 for n in base}
    final_mass = {n: 0.0 for n in base}
    bye_mass = {n: 0.0 for n in base}

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
        for n in order[:5]:
            bye_mass[n] += p          # seeds 1-5 skip the play-in round
        us_seed_mass[order.index(US) + 1] = us_seed_mass.get(order.index(US) + 1, 0.0) + p
        champs, finalists = run_bracket(order)
        for n, v in champs.items():
            champ_mass[n] += p * v
        for n, v in finalists.items():
            final_mass[n] += p * v

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
            "p_first_round_bye": bye_mass[n] / total,
            "p_reach_final": final_mass[n] / total,
            "p_champion": champ_mass[n] / total,
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
        "bracket_note": ("Every club makes the playoffs — all ten games are Wednesday 8/19. "
                         "Seeds 1-5 skip the 6:30 play-in round. The bracket puts #1 and #2 "
                         "in opposite halves, so they can only meet in the final; #3 rides "
                         "with #2, and #4 and #5 open against each other with the winner "
                         "drawing #1. That makes the drop from #3 to #4 the most expensive "
                         "step on the board, and the drop from #5 to #6 the one that costs "
                         "a bye."),
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
