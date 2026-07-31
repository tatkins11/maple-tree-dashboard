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

# Brian's call (7/30): treat our own two games as coin flips rather than trusting the
# Pythagorean read, which had us at 30% a game against Wasted Talent. We split with the
# 8-2 club last week, we have beaten them before, and a two-game sample against one
# opponent is not worth modelling to a decimal. Everyone ELSE's games keep their
# modelled odds — this override applies only where Maple Tree are involved.
OUR_GAME_WIN_PROB = 0.50

# Explicit matchup overrides, {(favourite, underdog): favourite's win probability}.
# The generic clamp floors every underdog at 5%, which is too kind to a club that is
# 0-10 with a -136 run differential. Brian's call (7/30): Sandlot Vibes should be 99%
# a game against Everything hurts. It matters more than it looks — those two games are
# the only thing standing between Sandlot and a clean sweep, and a Sandlot sweep is
# what closes off our route to the second seed.
MATCHUP_OVERRIDES: dict[tuple[str, str], float] = {
    ("Sandlot Vibes", "Everything hurts"): 0.99,
}


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


def head_to_head(played):
    """Head-to-head wins, runs ALLOWED, and games played, all keyed (a, b).

    The league sheet breaks ties on runs ALLOWED against the tied club, not on
    run differential. For a two-team tie over a fixed set of games the two order
    identically (a's runs allowed IS b's runs scored), so the earlier version
    reached the right answers; for three-way ties they diverge, so this tracks
    the metric the rule actually names."""
    w, ra, gp = {}, {}, {}
    for h, a, hr, ar in played:
        w[(h, a)] = w.get((h, a), 0) + (1 if hr > ar else 0)
        w[(a, h)] = w.get((a, h), 0) + (1 if ar > hr else 0)
        ra[(h, a)] = ra.get((h, a), 0) + ar          # h allowed ar
        ra[(a, h)] = ra.get((a, h), 0) + hr          # a allowed hr
        gp[(h, a)] = gp.get((h, a), 0) + 1
        gp[(a, h)] = gp.get((a, h), 0) + 1
    return w, ra, gp


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


def seed_order(final, h2h_w, h2h_ra, h2h_gp):
    """Official league tiebreakers (Brian supplied the rule sheet 7/30).

    TWO teams tied, in order:
      1. best head-to-head record against the other team
      2. fewest runs allowed against that team
      3. fewest runs allowed against all teams

    THREE OR MORE tied, in order:
      1. best head-to-head win percentage against the others in the tie — but a
         club MUST have at least one win over EVERY other tied club to be ranked
         on this leg at all
      2. fewest AVERAGE runs allowed against the others in the tie, usable only
         if every pair in the group has met at least once
      3. fewest runs allowed against all teams
    """
    pct = lambda n: final[n]["w"] / max(final[n]["w"] + final[n]["l"], 1)  # noqa: E731
    teams = sorted(final, key=lambda n: (-pct(n), n))
    out, i = [], 0
    while i < len(teams):
        j = i
        while j + 1 < len(teams) and pct(teams[j + 1]) == pct(teams[i]):
            j += 1
        # Snapshot before sorting: list.sort() empties the list for the duration of
        # the sort, so a key closing over the live list sees nothing and the whole
        # chain silently collapses to the last leg.
        grp = tuple(teams[i:j + 1])

        if len(grp) == 2:
            a, b = grp

            def key2(t, other={grp[0]: grp[1], grp[1]: grp[0]}):
                o = other[t]
                return (-h2h_w.get((t, o), 0),          # 1. head-to-head record
                        h2h_ra.get((t, o), 0),          # 2. runs allowed to them
                        final[t]["ra"], t)              # 3. runs allowed overall
            ordered = sorted(grp, key=key2)

        elif len(grp) > 2:
            # leg 2 needs every pair to have met
            all_met = all(h2h_gp.get((x, y), 0) > 0 for x in grp for y in grp if x != y)

            def key3(t):
                others = [o for o in grp if o != t]
                gw = sum(h2h_w.get((t, o), 0) for o in others)
                gl = sum(h2h_w.get((o, t), 0) for o in others)
                # "MUST have at least one win against all the other teams tied"
                eligible = all(h2h_w.get((t, o), 0) >= 1 for o in others)
                hpct = gw / (gw + gl) if (gw + gl) else 0.0
                gp_ = sum(h2h_gp.get((t, o), 0) for o in others)
                avg_ra = (sum(h2h_ra.get((t, o), 0) for o in others) / gp_
                          if (all_met and gp_) else float("inf"))
                return (0 if eligible else 1, -hpct if eligible else 0.0,
                        avg_ra, final[t]["ra"], t)
            ordered = sorted(grp, key=key3)
        else:
            ordered = list(grp)

        out.extend(ordered)
        i = j + 1
    return out


def massey_ratings(played, cap: float = 15.0):
    """Least-squares margin rating: solve for per-club strengths that best explain
    every result, so schedule is adjusted for by construction rather than bolted on.

    Margins are capped because a 25-run win says little more than a 15-run win, and
    this league produces plenty of both. The ordering is stable across every cap from
    8 to uncapped, so the choice is not doing hidden work. Ratings read as runs per
    game better than an average club and are constrained to sum to zero.
    """
    import numpy as np

    teams = sorted({t for h, a, _, _ in played for t in (h, a)})
    idx = {t: i for i, t in enumerate(teams)}
    rows, y = [], []
    for h, a, hr, ar in played:
        r = np.zeros(len(teams) + 1)
        r[idx[h]], r[idx[a]], r[-1] = 1, -1, 1      # last column = home edge
        rows.append(r)
        y.append(max(-cap, min(cap, hr - ar)))
    A = np.array(rows)
    y = np.array(y, dtype=float)
    con = np.zeros((1, len(teams) + 1))
    con[0, :len(teams)] = 1
    A = np.vstack([A, con * 50])
    y = np.append(y, 0.0)
    sol, *_ = np.linalg.lstsq(A, y, rcond=None)
    rating = {t: float(sol[idx[t]]) for t in teams}
    pred = np.array([rating[h] - rating[a] + sol[-1] for h, a, _, _ in played])
    act = np.array([max(-cap, min(cap, hr - ar)) for _, _, hr, ar in played])
    r2 = 1 - ((act - pred) ** 2).sum() / ((act - act.mean()) ** 2).sum()
    return rating, float(sol[-1]), float(r2)


def strength_of_schedule(played, remaining, base, probs):
    """Opponents' projected final win pct, each opponent measured EXCLUDING their
    games against the club being rated — otherwise a strong team inflates its own
    schedule rating through its own results."""
    exp_w = {n: float(s["w"]) for n, s in base.items()}
    games = {n: s["w"] + s["l"] for n, s in base.items()}
    for i, (_, h, a) in enumerate(remaining):
        exp_w[h] += probs[i]
        exp_w[a] += 1 - probs[i]
        games[h] += 1
        games[a] += 1

    sched, rem_sched, pair = {}, {}, {}
    for h, a, _, _ in played:
        sched.setdefault(h, []).append(a)
        sched.setdefault(a, []).append(h)
        pair[(h, a)] = pair.get((h, a), 0) + 1
        pair[(a, h)] = pair.get((a, h), 0) + 1
    for _, h, a in remaining:
        sched.setdefault(h, []).append(a)
        sched.setdefault(a, []).append(h)
        rem_sched.setdefault(h, []).append(a)
        rem_sched.setdefault(a, []).append(h)
        pair[(h, a)] = pair.get((h, a), 0) + 1
        pair[(a, h)] = pair.get((a, h), 0) + 1

    def wins_against(o, t):
        w = 0.0
        for h, a, hr, ar in played:
            if {h, a} == {o, t}:
                w += 1.0 if ((h == o and hr > ar) or (a == o and ar > hr)) else 0.0
        for i, (_, h, a) in enumerate(remaining):
            if {h, a} == {o, t}:
                w += probs[i] if h == o else 1 - probs[i]
        return w

    def rate(t, opps):
        vals = []
        for o in opps:
            g = games[o] - pair.get((o, t), 0)
            vals.append((exp_w[o] - wins_against(o, t)) / g if g else 0.5)
        return sum(vals) / len(vals) if vals else 0.0

    return ({t: rate(t, opps) for t, opps in sched.items()},
            {t: rate(t, opps) for t, opps in rem_sched.items()},
            {t: exp_w[t] / games[t] for t in exp_w})


def main() -> None:
    played, remaining = load()
    base = standings(played)
    base_h2h_w, base_h2h_ra, base_h2h_gp = head_to_head(played)
    for _, h, a in remaining:
        for n in (h, a):
            base.setdefault(n, {"w": 0, "l": 0, "rf": 0, "ra": 0})

    n_games = len(remaining)
    print(f"{len(played)} games played, {n_games} remaining -> {2 ** n_games:,} branches")

    probs = []
    for _, h, a in remaining:
        if US in (h, a):
            probs.append(OUR_GAME_WIN_PROB if h == US else 1.0 - OUR_GAME_WIN_PROB)
        elif (h, a) in MATCHUP_OVERRIDES:
            probs.append(MATCHUP_OVERRIDES[(h, a)])
        elif (a, h) in MATCHUP_OVERRIDES:
            probs.append(1.0 - MATCHUP_OVERRIDES[(a, h)])
        else:
            probs.append(win_prob(base, h, a))
    our_idx = [i for i, (_, h, a) in enumerate(remaining) if US in (h, a)]
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
    # Conditional splits on how our own doubleheader goes: 0, 1 or 2 wins.
    scen = {k: {"p": 0.0, "seeds": {}, "bye": 0.0, "final": 0.0, "champ": 0.0}
            for k in (0, 1, 2)}
    # Joint grid against the club one game ahead of us. The seed race is really a
    # two-body problem: our result and theirs. Reporting only our own conditional
    # buries that — "split gives us a 0.08% shot at #2" is true but reads as dead,
    # when the honest statement is that a split gives us a 32% shot at #2 IF they
    # drop both, and the long odds live entirely in that if.
    rival = next((n for n in base if n != US), None)
    stand = sorted(base, key=lambda n: -(base[n]["w"] / max(base[n]["w"] + base[n]["l"], 1)))
    ui = stand.index(US)
    rival = stand[ui - 1] if ui > 0 else stand[1]
    rival_idx = [i for i, (_, h, a) in enumerate(remaining) if rival in (h, a)]
    joint = {}

    seed_dist = {n: {} for n in base}       # club -> seed -> mass
    rec_dist = {n: {} for n in base}        # club -> final record -> mass
    # Rooting leverage. For each game we are NOT playing, track our expected seed
    # split by who won AND by how our own doubleheader went — several games change
    # which side we want depending on our own result, so a single marginal number
    # would give the wrong instruction half the time.
    other_idx = [i for i in range(n_games) if i not in our_idx]
    seen_pair, root_games = set(), []
    for i in other_idx:
        _, h, a = remaining[i]
        if frozenset((h, a)) not in seen_pair:
            seen_pair.add(frozenset((h, a)))
            root_games.append(i)
    root_acc = {}

    top_mass = {n: 0.0 for n in base}
    top_possible = {n: False for n in base}
    top3_mass = {n: 0.0 for n in base}
    us_seed_mass = {}

    for outcome in product([1, 0], repeat=n_games):
        p = 1.0
        final = {n: dict(s) for n, s in base.items()}
        h2h_w = dict(base_h2h_w)
        h2h_ra = dict(base_h2h_ra)
        h2h_gp = dict(base_h2h_gp)
        for (idx, (_, h, a)), home_won in zip(enumerate(remaining), outcome):
            p *= probs[idx] if home_won else (1 - probs[idx])
            w, lo = (h, a) if home_won else (a, h)
            final[w]["w"] += 1
            final[lo]["l"] += 1
            edge = (abs(margin[w]) + abs(margin[lo])) / 2 or 1.0
            final[w]["rf"] += edge
            final[lo]["ra"] += edge
            h2h_w[(w, lo)] = h2h_w.get((w, lo), 0) + 1
            # Projected score for an unplayed game: the loser is charged the winner's
            # typical margin as runs allowed, the winner charged nothing extra.
            h2h_ra[(lo, w)] = h2h_ra.get((lo, w), 0) + edge
            h2h_ra[(w, lo)] = h2h_ra.get((w, lo), 0)
            h2h_gp[(w, lo)] = h2h_gp.get((w, lo), 0) + 1
            h2h_gp[(lo, w)] = h2h_gp.get((lo, w), 0) + 1
        order = seed_order(final, h2h_w, h2h_ra, h2h_gp)
        top_mass[order[0]] += p
        top_possible[order[0]] = True
        for n in order[:3]:
            top3_mass[n] += p
        for n in order[:5]:
            bye_mass[n] += p          # seeds 1-5 skip the play-in round
        our_seat = order.index(US) + 1
        us_seed_mass[our_seat] = us_seed_mass.get(our_seat, 0.0) + p
        for n in base:
            seed_dist[n][order.index(n) + 1] = seed_dist[n].get(order.index(n) + 1, 0.0) + p
            rk = f"{final[n]['w']}-{final[n]['l']}"
            rec_dist[n][rk] = rec_dist[n].get(rk, 0.0) + p
        ow_now = sum(1 for i in our_idx
                     if (remaining[i][1] == US) == bool(outcome[i]))
        for i in root_games:
            _, h, a = remaining[i]
            cell = root_acc.setdefault((i, h if outcome[i] else a, ow_now), [0.0, 0.0])
            cell[0] += p
            cell[1] += p * our_seat
        champs, finalists = run_bracket(order)
        for n, v in champs.items():
            champ_mass[n] += p * v
        for n, v in finalists.items():
            final_mass[n] += p * v

        our_wins = sum(1 for i in our_idx
                       if (remaining[i][1] == US) == bool(outcome[i]))
        sc = scen[our_wins]
        seat = order.index(US) + 1
        sc["p"] += p
        sc["seeds"][seat] = sc["seeds"].get(seat, 0.0) + p
        if seat <= 5:
            sc["bye"] += p
        sc["final"] += p * finalists.get(US, 0.0)
        sc["champ"] += p * champs.get(US, 0.0)

        rival_wins = sum(1 for i in rival_idx
                         if (remaining[i][1] == rival) == bool(outcome[i]))
        cell = joint.setdefault((our_wins, rival_wins), {"p": 0.0, "seeds": {}})
        cell["p"] += p
        cell["seeds"][seat] = cell["seeds"].get(seat, 0.0) + p

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
    rows.sort(key=lambda r: seed_order(base, base_h2h_w, base_h2h_ra,
                                       base_h2h_gp).index(r["team"]))
    for i, r in enumerate(rows, 1):
        r["seed"] = i

    # ---- schedule-adjusted power ranking + strength of schedule ---------------
    rating, hfa, r2 = massey_ratings(played)
    sos_full, sos_rem, proj_pct = strength_of_schedule(played, remaining, base, probs)
    by_record = sorted(base, key=lambda n: (-(base[n]["w"] / max(base[n]["w"] + base[n]["l"], 1)),
                                            -(base[n]["rf"] - base[n]["ra"])))
    rec_rank = {n: i + 1 for i, n in enumerate(by_record)}
    power_rows = []
    for i, n in enumerate(sorted(rating, key=lambda x: -rating[x]), 1):
        s_ = base[n]
        power_rows.append({
            "rank": i, "team": n, "wins": s_["w"], "losses": s_["l"],
            "run_diff": s_["rf"] - s_["ra"], "rating": rating[n],
            "sos_played": sos_full.get(n, 0.0),
            "sos_remaining": sos_rem.get(n),
            "projected_win_pct": proj_pct.get(n, 0.0),
            "record_rank": rec_rank[n], "move": rec_rank[n] - i,
            "is_team": n == US,
        })

    # ---- projected final table: each club's modal seed and record --------------
    projected_rows = []
    for n in base:
        tot_n = sum(seed_dist[n].values()) or 1.0
        modal_seed = max(seed_dist[n], key=seed_dist[n].get)
        modal_rec = max(rec_dist[n], key=rec_dist[n].get)
        projected_rows.append({
            "team": n, "seed": modal_seed, "seed_confidence": seed_dist[n][modal_seed] / tot_n,
            "record": modal_rec,
            "expected_seed": sum(k * v for k, v in seed_dist[n].items()) / tot_n,
            "spread": [{"seed": k, "p": v / tot_n}
                       for k, v in sorted(seed_dist[n].items()) if v / tot_n > 0.04],
            "is_team": n == US,
        })
    projected_rows.sort(key=lambda r: r["expected_seed"])

    # ---- who to root for, per game, per scenario -------------------------------
    LBL = {2: "win_both", 1: "split", 0: "lose_both"}
    rooting_rows = []
    for i in root_games:
        _, h, a = remaining[i]
        row = {"home": h, "away": a, "week": remaining[i][0], "by_scenario": {}}
        dirs = []
        for ow in (2, 1, 0):
            mh, sh = root_acc.get((i, h, ow), [0.0, 0.0])
            ma, sa = root_acc.get((i, a, ow), [0.0, 0.0])
            if not mh or not ma:
                row["by_scenario"][LBL[ow]] = {"root_for": None, "swing": 0.0}
                continue
            eh, ea = sh / mh, sa / ma
            pick = h if eh < ea else a
            swing = abs(eh - ea)
            row["by_scenario"][LBL[ow]] = {
                "root_for": pick if swing > 0.004 else None, "swing": swing}
            if swing > 0.004:
                dirs.append(pick)
        row["unconditional"] = len(set(dirs)) == 1 and len(dirs) > 1
        row["max_swing"] = max(v["swing"] for v in row["by_scenario"].values())
        rooting_rows.append(row)
    rooting_rows.sort(key=lambda r: -r["max_swing"])

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
                   "percentage, then the league tiebreaker chain: head-to-head record, "
                   "runs allowed against the tied club, then runs allowed overall. "
                   "Three-way ties additionally require a win over every other tied "
                   "club to be ranked on head-to-head." % (n_games, PYTHAG_EXP)),
        "teams": rows,
        "bracket_note": ("Every club makes the playoffs — all ten games are Wednesday 8/19. "
                         "Seeds 1-5 skip the 6:30 play-in round. The bracket puts #1 and #2 "
                         "in opposite halves, so they can only meet in the final; #3 rides "
                         "with #2, and #4 and #5 open against each other with the winner "
                         "drawing #1. That makes the drop from #3 to #4 the most expensive "
                         "step on the board, and the drop from #5 to #6 the one that costs "
                         "a bye."),
        "our_seed_odds": [{"seed": k, "p": v / total} for k, v in sorted(us_seed_mass.items())],
        "our_game_win_prob": OUR_GAME_WIN_PROB,
        "power_ranking": power_rows,
        "projected_table": projected_rows,
        "rooting": rooting_rows,
        "massey": {"home_edge": hfa, "r2": r2, "cap": 15},
        "rival": rival,
        "rival_games_left": len(rival_idx),
        "joint": [
            {"our_wins": ow, "rival_wins": rw,
             "p": cell["p"] / total,
             "seeds": [{"seed": sd, "p": v / cell["p"]}
                       for sd, v in sorted(cell["seeds"].items()) if v / cell["p"] > 0.005],
             "p_seed_2_or_better": sum(v for sd, v in cell["seeds"].items() if sd <= 2) / cell["p"]}
            for (ow, rw), cell in sorted(joint.items(), reverse=True) if cell["p"] / total > 1e-6],
        "scenarios": [
            {"wins": k,
             "label": {0: "Lose both", 1: "Split", 2: "Win both"}[k],
             "final_record": f"{base[US]['w'] + k}-{base[US]['l'] + (2 - k)}",
             "p_scenario": scen[k]["p"] / total,
             "p_bye": scen[k]["bye"] / scen[k]["p"] if scen[k]["p"] else 0.0,
             "p_final": scen[k]["final"] / scen[k]["p"] if scen[k]["p"] else 0.0,
             "p_champion": scen[k]["champ"] / scen[k]["p"] if scen[k]["p"] else 0.0,
             "seeds": [{"seed": sd, "p": v / scen[k]["p"]}
                       for sd, v in sorted(scen[k]["seeds"].items()) if v / scen[k]["p"] > 0.00002],
             "likeliest_seed": max(scen[k]["seeds"], key=scen[k]["seeds"].get) if scen[k]["seeds"] else None}
            for k in (0, 1, 2)],
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
