"""Week 6 lineup analysis — is tonight's order the best we have ever put out?

Runs the live Monte Carlo engine (src/models/simulator.py) over the projected
rates for tonight's 12, then benchmarks that number against every lineup this
franchise has actually fielded, the optimal ordering of the same 12, and the
league run environment.

Writes a JSON blob for scripts/build_lineup_analysis_pdf.py to render.

    python scripts/_lineup_analysis_2026_07_29.py
"""
from __future__ import annotations

import json
import sqlite3
import statistics as st
import sys
from collections import defaultdict
from dataclasses import replace
from itertools import permutations
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.models.lineup import SimulationLineupRow  # noqa: E402
from src.models.optimizer import load_optimizer_rules  # noqa: E402
from src.models.roster import select_game_day_projections  # noqa: E402
from src.models.simulator import simulate_lineup_runs  # noqa: E402

DB = REPO / "db" / "all_seasons_identity.sqlite"
SEASON = "Maple Tree Summer 2026"
OUT = REPO / "data" / "processed" / "lineup_analysis_2026_07_29.json"

# Tonight's order (Joey scratched — 12 deep).
ORDER = ["Glove", "Tristan", "Harm", "Tim", "JJ", "Kives",
         "Porter", "Walsh", "Joel", "Slomka", "Duff", "Corey"]
SIMS = 50_000
SEED = 20260729

# Players the projection engine has too little history on to speak about.
# Harm: 11 career PA total, 5 of them an 0-for-4 in 2021. Any point estimate
# for him is noise, so he gets an explicit sensitivity band instead of a number
# we quietly pretend to believe.
THIN_PA = 40


def norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())


def to_row(p, spot: int) -> SimulationLineupRow:
    return SimulationLineupRow(
        player_id=p.player_id, player_name=p.preferred_display_name,
        projection_source=p.projection_source, lineup_spot=spot,
        is_fixed_dhh=bool(p.is_fixed_dhh),
        baserunning_adjustment=p.baserunning_adjustment,
        p_single=p.p_single, p_double=p.p_double, p_triple=p.p_triple,
        p_home_run=p.p_home_run, p_walk=p.p_walk, p_hit_by_pitch=p.p_hit_by_pitch,
        p_reached_on_error=p.p_reached_on_error, p_fielder_choice=p.p_fielder_choice,
        p_grounded_into_double_play=p.p_grounded_into_double_play,
        projected_strikeout_rate=p.projected_strikeout_rate, p_out=p.p_out,
        projected_on_base_rate=p.projected_on_base_rate,
        projected_total_base_rate=p.projected_total_base_rate,
        projected_run_rate=p.projected_run_rate, projected_rbi_rate=p.projected_rbi_rate,
    )


def sim(rows, rules, sims=SIMS, seed=SEED):
    runs = simulate_lineup_runs(rows, rules, simulations=sims, seed=seed)
    runs_sorted = sorted(runs)
    n = len(runs_sorted)
    return {
        "mean": sum(runs) / n,
        "median": st.median(runs),
        "sd": st.pstdev(runs),
        "p10": runs_sorted[int(0.10 * n)],
        "p25": runs_sorted[int(0.25 * n)],
        "p75": runs_sorted[int(0.75 * n)],
        "p90": runs_sorted[int(0.90 * n)],
        "dist": {str(k): runs.count(k) / n for k in sorted(set(runs))},
    }


def team_average_row(rows) -> dict:
    """Rate profile of an average hitter in THIS lineup — the honest stand-in
    for a player the model has no history on."""
    keys = ["p_single", "p_double", "p_triple", "p_home_run", "p_walk",
            "p_hit_by_pitch", "p_reached_on_error", "p_fielder_choice",
            "p_grounded_into_double_play", "projected_strikeout_rate", "p_out"]
    return {k: sum(getattr(r, k) for r in rows) / len(rows) for k in keys}


def main() -> None:
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    rules = load_optimizer_rules()

    projs = select_game_day_projections(
        connection=con, projection_season=SEASON, available_player_names=ORDER)
    by = {norm(p.preferred_display_name): p for p in projs}
    missing = [n for n in ORDER if norm(n) not in by]
    if missing:
        raise SystemExit(f"unresolved: {missing}")

    rows = [to_row(by[norm(n)], i) for i, n in enumerate(ORDER, 1)]
    base = sim(rows, rules)

    # ---- sensitivity on the thin-sample bats -------------------------------
    thin = [r.player_name for r in rows
            if by[norm(r.player_name)].career_plate_appearances < THIN_PA]
    avg = team_average_row(rows)
    lifted = [replace(r, **avg) if r.player_name in thin else r for r in rows]
    lifted_sim = sim(lifted, rules)

    # ---- is the ORDER right, or just the personnel? ------------------------
    # Full 12! is 479M orderings; sample the space instead and keep the best.
    import random
    rng = random.Random(SEED)
    best, worst = None, None
    for _ in range(400):
        perm = rng.sample(range(12), 12)
        cand = [replace(rows[j], lineup_spot=i + 1) for i, j in enumerate(perm)]
        m = sum(simulate_lineup_runs(cand, rules, simulations=2000, seed=SEED)) / 2000
        if best is None or m > best[0]:
            best = (m, [rows[j].player_name for j in perm])
        if worst is None or m < worst[0]:
            worst = (m, [rows[j].player_name for j in perm])
    # re-run the winners at full precision
    def full(names):
        idx = {r.player_name: r for r in rows}
        cand = [replace(idx[n], lineup_spot=i + 1) for i, n in enumerate(names)]
        return sim(cand, rules)["mean"]
    best_mean, worst_mean = full(best[1]), full(worst[1])

    # ---- benchmark: every lineup this franchise has actually fielded -------
    fielded = defaultdict(list)
    q = """select g.game_id, g.season, g.game_date, g.opponent_name,
                  b.lineup_spot, pi.player_name
           from games g
           join player_game_batting b on b.game_id = g.game_id
           join player_identity pi on pi.player_id = b.player_id
           where b.lineup_spot is not null
           order by g.game_date, b.lineup_spot"""
    for r in con.execute(q):
        fielded[(r["game_id"], r["season"], r["game_date"], r["opponent_name"])].append(
            (r["lineup_spot"], r["player_name"]))

    hist = []
    for (gid, season, date, opp), spots in fielded.items():
        names = [n for _, n in sorted(spots)]
        if len(names) < 8:
            continue
        rs = []
        ok = True
        for i, n in enumerate(names, 1):
            p = by.get(norm(n))
            if p is None:  # player not on tonight's projection set — fetch
                extra = select_game_day_projections(
                    connection=con, projection_season=SEASON, available_player_names=[n])
                if not extra:
                    ok = False
                    break
                by[norm(n)] = extra[0]
                p = extra[0]
            rs.append(to_row(p, i))
        if not ok:
            continue
        m = sum(simulate_lineup_runs(rs, rules, simulations=4000, seed=SEED)) / 4000
        hist.append({"season": season, "date": date, "opponent": opp,
                     "n": len(names), "xr": m, "names": names})
    hist.sort(key=lambda h: -h["xr"])

    out = {
        "generated_for": "2026-07-29",
        "opponent": "Sandlot Vibes",
        "order": ORDER,
        "sims": SIMS,
        "rules": {"innings": rules.innings_per_game,
                  "max_hr_non_dhh": rules.max_home_runs_non_dhh},
        "base": base,
        "thin_players": thin,
        "sensitivity_team_avg": lifted_sim,
        "order_best": {"mean": best_mean, "order": best[1]},
        "order_worst": {"mean": worst_mean, "order": worst[1]},
        "historical": hist,
        "players": [{
            "name": ORDER[i],
            "display": rows[i].player_name,
            "spot": i + 1,
            "obp": rows[i].projected_on_base_rate,
            "tb_rate": rows[i].projected_total_base_rate,
            "hr": rows[i].p_home_run,
            "bb": rows[i].p_walk,
            "so": rows[i].projected_strikeout_rate,
            "dhh": bool(rows[i].is_fixed_dhh),
            "cur_pa": by[norm(ORDER[i])].current_plate_appearances,
            "car_pa": by[norm(ORDER[i])].career_plate_appearances,
            "weight": by[norm(ORDER[i])].current_season_weight,
            "source": rows[i].projection_source,
        } for i in range(len(ORDER))],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1), encoding="utf-8")

    print(f"tonight  xR = {base['mean']:.2f}  (median {base['median']:.0f}, "
          f"p10 {base['p10']}, p90 {base['p90']}, sd {base['sd']:.1f})")
    print(f"thin-sample bats: {thin}")
    print(f"  if they hit like a lineup-average bat: xR = {lifted_sim['mean']:.2f}")
    print(f"best sampled order  {best_mean:.2f}   worst sampled order {worst_mean:.2f}")
    print(f"\nhistorical lineups scored: {len(hist)}")
    for h in hist[:6]:
        print(f"  {h['xr']:6.2f}  {h['date']}  {h['season'][:28]:28s} vs {h['opponent']}")
    ranks = [i for i, h in enumerate(hist, 1) if h["date"] == "2026-07-22"]
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
