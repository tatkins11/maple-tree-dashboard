from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from itertools import permutations
from typing import Iterable

from src.models.lineup import SimulationLineupRow, build_simulation_lineup_from_order
from src.models.records import LeagueRulesRecord
from src.models.roster import (
    DEFAULT_ACTIVE_ROSTER_SEASON,
    DEFAULT_AVAILABILITY_PATH,
    DEFAULT_LEAGUE_RULES_PATH,
    GameDayProjectionRow,
    load_available_player_names_with_active_roster_defaults,
    load_league_rules,
    select_game_day_projections,
)
from src.models.simulator import SimulationSummary, simulate_lineup

DEFAULT_PREFERRED_LINEUP = (
    "Jj",
    "Glove",
    "Tristan",
    "Tim",
    "Kives",
    "Porter",
    "Walsh",
    "Duff",
    "Joey",
    "Corey",
    "Joel",
    "Jason",
)
DEFAULT_CORE_TOP_POOL = DEFAULT_PREFERRED_LINEUP[:5]
DEFAULT_PREFERRED_BOTTOM_GROUP = DEFAULT_PREFERRED_LINEUP[6:]
DEFAULT_LEADOFF_POOL = ("Jj", "Glove", "Tim", "Kives")
DEFAULT_ADVISORY_LINEUP_TYPE = "projection-optimized lineup"


@dataclass
class RankedLineup:
    ordered_player_names: list[str]
    summary: SimulationSummary
    dhh_slot: int
    reason: str
    lineup_type: str = DEFAULT_ADVISORY_LINEUP_TYPE


@dataclass
class OptimizationResult:
    best_lineup: RankedLineup
    alternate_lineups: list[RankedLineup]
    evaluated_lineups: int
    available_player_names: list[str]
    near_tie_lineups: list[RankedLineup]


def optimize_lineup(
    connection: sqlite3.Connection,
    projection_season: str,
    game_date: str,
    league_rules: LeagueRulesRecord,
    simulations: int = 1000,
    seed: int | None = None,
    availability_path=DEFAULT_AVAILABILITY_PATH,
    roster_season: str = DEFAULT_ACTIVE_ROSTER_SEASON,
    dhh_slots: Iterable[int] = (2, 3, 4, 5),
    beam_width: int = 8,
    final_candidate_count: int = 5,
    local_search_rounds: int = 1,
    near_tie_delta: float = 0.05,
    mode: str = "unconstrained",
    available_player_names_override: list[str] | None = None,
    core_top_pool: Iterable[str] = DEFAULT_CORE_TOP_POOL,
    leadoff_pool: Iterable[str] = DEFAULT_LEADOFF_POOL,
    preferred_lineup: Iterable[str] = DEFAULT_PREFERRED_LINEUP,
) -> OptimizationResult:
    available_names = (
        list(available_player_names_override)
        if available_player_names_override is not None
        else load_available_player_names_with_active_roster_defaults(
            connection=connection,
            csv_path=availability_path,
            game_date=game_date,
            season_name=roster_season,
        )
    )
    projection_rows = select_game_day_projections(
        connection=connection,
        projection_season=projection_season,
        available_player_names=available_names,
    )
    if not projection_rows:
        raise ValueError("No usable projection rows found for optimizer search.")

    by_name = {row.preferred_display_name: row for row in projection_rows}
    projected_available_names = [row.preferred_display_name for row in projection_rows]
    dhh_row = _find_fixed_dhh(projection_rows)
    preferred_available_order = _resolve_preferred_available_order(
        available_names=projected_available_names,
        preferred_lineup=preferred_lineup,
    )
    slot_candidates = _resolve_dhh_slots(len(projection_rows), dhh_slots, dhh_row is not None)
    candidate_orders: dict[tuple[str, ...], int] = {}
    if mode == "team_aware":
        for order in _build_preferred_lineup_candidates(
            projection_rows=projection_rows,
            available_names=available_names,
            preferred_available_order=preferred_available_order,
            preferred_lineup=preferred_lineup,
            dhh_slots=dhh_slots,
        ):
            candidate_orders.setdefault(tuple(order), _infer_dhh_slot(order, dhh_row))
    else:
        for dhh_slot in slot_candidates:
            for fixed_slots in _select_top_order_anchor_orders(
                projection_rows=projection_rows,
                dhh_slot=dhh_slot,
            ):
                for order in _build_beam_candidates(
                    projection_rows=projection_rows,
                    dhh_row=dhh_row,
                    dhh_slot=dhh_slot,
                    beam_width=beam_width,
                    fixed_slots=fixed_slots,
                ):
                    candidate_orders.setdefault(tuple(order), dhh_slot)

    heuristic_ranked = sorted(
        candidate_orders.items(),
        key=lambda item: _heuristic_lineup_score(
            list(item[0]),
            by_name,
            preferred_available_order=preferred_available_order,
            mode=mode,
        ),
        reverse=True,
    )
    refined_orders: dict[tuple[str, ...], int] = {}
    quick_simulations = max(100, simulations // 5)
    order_items = heuristic_ranked[: min(len(heuristic_ranked), max(6, final_candidate_count * 2))]
    for index, (order, dhh_slot) in enumerate(order_items):
        movable_pairs = (
            _preferred_lineup_movable_pairs(list(order), preferred_available_order)
            if mode == "team_aware"
            else None
        )
        improved_order = _improve_lineup_order(
            connection=connection,
            projection_season=projection_season,
            ordered_player_names=list(order),
            available_player_names=available_names,
            league_rules=league_rules,
            simulations=quick_simulations,
            seed=None if seed is None else seed + index,
            rounds=local_search_rounds,
            movable_pairs=movable_pairs,
        )
        refined_orders.setdefault(tuple(improved_order), _infer_dhh_slot(improved_order, dhh_row))

    scored_quick: list[tuple[list[str], int, float]] = []
    for index, (order, dhh_slot) in enumerate(refined_orders.items()):
        lineup = build_simulation_lineup_from_order(
            connection=connection,
            projection_season=projection_season,
            ordered_player_names=list(order),
            available_player_names=available_names,
        )
        summary = simulate_lineup(
            lineup=lineup,
            league_rules=league_rules,
            simulations=quick_simulations,
            seed=None if seed is None else seed + 1000 + index,
        )
        scored_quick.append((list(order), dhh_slot, summary.average_runs))

    scored_quick.sort(
        key=lambda item: (
            item[2],
            _advanced_analytics_tiebreak_score(list(item[0]), by_name),
            _preferred_order_bonus(list(item[0]), preferred_available_order),
        ),
        reverse=True,
    )
    finalists = scored_quick[: max(3, final_candidate_count)]

    ranked_results: list[RankedLineup] = []
    for index, (order, dhh_slot, _) in enumerate(finalists):
        lineup = build_simulation_lineup_from_order(
            connection=connection,
            projection_season=projection_season,
            ordered_player_names=order,
            available_player_names=available_names,
        )
        summary = simulate_lineup(
            lineup=lineup,
            league_rules=league_rules,
            simulations=simulations,
            seed=None if seed is None else seed + 2000 + index,
        )
        ranked_results.append(
            RankedLineup(
                ordered_player_names=order,
                summary=summary,
                dhh_slot=dhh_slot,
                reason=_describe_lineup_reason(
                    order,
                    by_name,
                    dhh_row,
                    dhh_slot,
                    mode=mode,
                    preferred_available_order=preferred_available_order,
                ),
                lineup_type=_describe_lineup_type(
                    order,
                    preferred_available_order,
                    mode=mode,
                ),
            )
        )

    ranked_results.sort(
        key=lambda item: (
            item.summary.average_runs,
            _advanced_analytics_tiebreak_score(item.ordered_player_names, by_name),
            _preferred_order_bonus(item.ordered_player_names, preferred_available_order),
        ),
        reverse=True,
    )
    best_average = ranked_results[0].summary.average_runs
    near_ties = [
        lineup
        for lineup in ranked_results[1:]
        if best_average - lineup.summary.average_runs <= near_tie_delta
    ]
    return OptimizationResult(
        best_lineup=ranked_results[0],
        alternate_lineups=ranked_results[1:5],
        evaluated_lineups=len(refined_orders),
        available_player_names=available_names,
        near_tie_lineups=near_ties,
    )


def load_optimizer_rules(config_path=DEFAULT_LEAGUE_RULES_PATH) -> LeagueRulesRecord:
    return load_league_rules(config_path)


def _find_fixed_dhh(projection_rows: list[GameDayProjectionRow]) -> GameDayProjectionRow | None:
    dhh_rows = [row for row in projection_rows if row.is_fixed_dhh]
    if not dhh_rows:
        return None
    return dhh_rows[0]


def _resolve_dhh_slots(
    lineup_size: int,
    dhh_slots: Iterable[int],
    has_fixed_dhh: bool,
) -> list[int]:
    valid_slots = sorted({slot for slot in dhh_slots if 1 <= slot <= lineup_size})
    if has_fixed_dhh and valid_slots:
        return valid_slots
    return [0]


def _build_beam_candidates(
    projection_rows: list[GameDayProjectionRow],
    dhh_row: GameDayProjectionRow | None,
    dhh_slot: int,
    beam_width: int,
    fixed_slots: dict[int, str] | None = None,
) -> list[list[str]]:
    lineup_size = len(projection_rows)
    open_slots = [slot for slot in range(1, lineup_size + 1) if slot != dhh_slot]
    available_rows = [
        row for row in projection_rows if dhh_row is None or row.player_id != dhh_row.player_id
    ]

    partials: list[tuple[list[str | None], float, set[int]]] = [([None] * lineup_size, 0.0, set())]
    if fixed_slots:
        seeded_fixed: list[tuple[list[str | None], float, set[int]]] = []
        resolved_rows = {
            slot: next(
                row for row in projection_rows if row.preferred_display_name == player_name
            )
            for slot, player_name in fixed_slots.items()
        }
        for slot, row in resolved_rows.items():
            if dhh_row is not None and dhh_slot > 0 and row.player_id == dhh_row.player_id:
                return []
            if slot == dhh_slot:
                return []
        for order, score, used in partials:
            new_order = list(order)
            new_used = set(used)
            new_score = score
            for slot, row in resolved_rows.items():
                if row.player_id in new_used:
                    return []
                new_order[slot - 1] = row.preferred_display_name
                new_used.add(row.player_id)
                new_score += _slot_fit_score(row, slot)
            seeded_fixed.append((new_order, new_score, new_used))
        partials = seeded_fixed

    if dhh_row is not None and dhh_slot > 0:
        seeded: list[tuple[list[str | None], float, set[int]]] = []
        for order, score, used in partials:
            order[dhh_slot - 1] = dhh_row.preferred_display_name
            seeded.append((order, score + _slot_fit_score(dhh_row, dhh_slot), used | {dhh_row.player_id}))
        partials = seeded

    if fixed_slots:
        open_slots = [slot for slot in open_slots if slot not in fixed_slots]

    for slot in open_slots:
        next_partials: list[tuple[list[str | None], float, set[int]]] = []
        for order, score, used in partials:
            for row in available_rows:
                if row.player_id in used:
                    continue
                new_order = list(order)
                new_order[slot - 1] = row.preferred_display_name
                new_score = score + _slot_fit_score(row, slot)
                next_partials.append((new_order, new_score, used | {row.player_id}))
        next_partials.sort(key=lambda item: item[1], reverse=True)
        partials = next_partials[:beam_width]

    return [[name for name in order if name is not None] for order, _, _ in partials]


def _build_team_aware_candidates(
    projection_rows: list[GameDayProjectionRow],
    dhh_row: GameDayProjectionRow | None,
    dhh_slot: int,
    core_top_pool: Iterable[str],
    leadoff_pool: Iterable[str],
    max_top_orders: int,
) -> list[list[str]]:
    by_name = {row.preferred_display_name: row for row in projection_rows}
    available_names = list(by_name.keys())
    top_group_names = _select_team_aware_top_group(
        projection_rows=projection_rows,
        core_top_pool=core_top_pool,
    )
    top_group_size = min(5, len(available_names))
    top_slots = list(range(1, top_group_size + 1))
    allowed_leadoff = [name for name in leadoff_pool if name in top_group_names]
    if not allowed_leadoff:
        allowed_leadoff = [top_group_names[0]]

    scored_top_orders: list[tuple[float, tuple[str, ...]]] = []
    for top_order in permutations(top_group_names, len(top_group_names)):
        if top_order[0] not in allowed_leadoff:
            continue
        if dhh_row is not None and dhh_row.preferred_display_name in top_order:
            dhh_index = top_order.index(dhh_row.preferred_display_name) + 1
            if dhh_index != dhh_slot:
                continue
        score = sum(
            _slot_fit_score(by_name[name], slot)
            for slot, name in zip(top_slots, top_order)
        )
        scored_top_orders.append((score, top_order))
    scored_top_orders.sort(key=lambda item: item[0], reverse=True)

    candidates: list[list[str]] = []
    for _, top_order in scored_top_orders[:max_top_orders]:
        used_names = set(top_order)
        bottom_names = [name for name in available_names if name not in used_names]
        ordered_bottom = sorted(
            bottom_names,
            key=lambda name: _slot_fit_score(by_name[name], top_group_size + 1),
            reverse=True,
        )
        candidates.append(list(top_order) + ordered_bottom)
    return candidates


def _resolve_preferred_available_order(
    available_names: list[str],
    preferred_lineup: Iterable[str],
) -> list[str]:
    preferred_available = [name for name in preferred_lineup if name in available_names]
    extras = [name for name in available_names if name not in preferred_available]
    return preferred_available + extras


def _build_preferred_lineup_candidates(
    projection_rows: list[GameDayProjectionRow],
    available_names: list[str],
    preferred_available_order: list[str],
    preferred_lineup: Iterable[str],
    dhh_slots: Iterable[int],
) -> list[list[str]]:
    valid_orders: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    preferred_lineup_list = list(preferred_lineup)
    core_top_pool = set(preferred_lineup_list[:5])
    missing_top = [name for name in preferred_lineup_list[:5] if name not in available_names]
    top_segment_size = min(5, len(preferred_available_order))

    def add_candidate(order: list[str]) -> None:
        normalized = _normalize_team_aware_order(
            order=order,
            preferred_available_order=preferred_available_order,
            projection_rows=projection_rows,
            dhh_slots=dhh_slots,
        )
        if not normalized:
            return
        key = tuple(normalized)
        if key in seen:
            return
        seen.add(key)
        valid_orders.append(normalized)

    add_candidate(list(preferred_available_order))
    if not missing_top:
        return valid_orders

    top_indices = [
        index
        for index, name in enumerate(preferred_available_order[:top_segment_size])
        if name in core_top_pool
    ]
    for left, right in zip(top_indices, top_indices[1:]):
        swapped = list(preferred_available_order)
        swapped[left], swapped[right] = swapped[right], swapped[left]
        add_candidate(swapped)

    if len(top_indices) >= 3:
        rotated_left = list(preferred_available_order)
        names = [rotated_left[index] for index in top_indices]
        for index, name in zip(top_indices, names[1:] + names[:1]):
            rotated_left[index] = name
        add_candidate(rotated_left)

        rotated_right = list(preferred_available_order)
        names = [rotated_right[index] for index in top_indices]
        for index, name in zip(top_indices, names[-1:] + names[:-1]):
            rotated_right[index] = name
        add_candidate(rotated_right)

    return valid_orders


def _normalize_team_aware_order(
    order: list[str],
    preferred_available_order: list[str],
    projection_rows: list[GameDayProjectionRow],
    dhh_slots: Iterable[int],
) -> list[str] | None:
    if set(order) != set(preferred_available_order) or len(order) != len(preferred_available_order):
        return None

    dhh_row = _find_fixed_dhh(projection_rows)
    if dhh_row is None or dhh_row.preferred_display_name not in order:
        return list(order)

    valid_slots = _resolve_dhh_slots(len(order), dhh_slots, True)
    current_slot = order.index(dhh_row.preferred_display_name) + 1
    if current_slot in valid_slots:
        return list(order)

    target_slot = min(valid_slots, key=lambda slot: abs(slot - current_slot))
    adjusted = list(order)
    adjusted.pop(current_slot - 1)
    adjusted.insert(target_slot - 1, dhh_row.preferred_display_name)
    return adjusted


def _preferred_lineup_movable_pairs(
    order: list[str],
    preferred_available_order: list[str],
) -> set[tuple[int, int]]:
    if order == preferred_available_order:
        return set()

    top_segment_size = min(5, len(preferred_available_order))
    bottom_start = top_segment_size
    for index in range(bottom_start, len(order)):
        if order[index] != preferred_available_order[index]:
            return set()

    return {
        (left, left + 1)
        for left in range(max(0, top_segment_size - 1))
    }


def _infer_dhh_slot(
    order: list[str],
    dhh_row: GameDayProjectionRow | None,
) -> int:
    if dhh_row is None or dhh_row.preferred_display_name not in order:
        return 0
    return order.index(dhh_row.preferred_display_name) + 1


def _slot_fit_score(row: GameDayProjectionRow, slot: int) -> float:
    obp = row.projected_on_base_rate
    tb = row.projected_total_base_rate
    xbh = row.projected_extra_base_hit_rate
    hr = row.p_home_run
    walk = row.p_walk
    out_avoidance = 1.0 - row.p_out
    on_base_quality = obp + 0.35 * walk
    damage = tb + 1.2 * xbh + 1.4 * hr

    if slot == 1:
        return (
            on_base_quality * 4.6
            + out_avoidance * 2.0
            + tb * 0.8
        )
    if slot == 2:
        return (
            on_base_quality * 3.8
            + out_avoidance * 1.6
            + tb * 1.1
            + xbh * 0.4
        )
    if slot == 3:
        return (
            damage * 3.0
            + obp * 1.8
            + out_avoidance * 1.0
            + walk * 0.3
        )
    if slot == 4:
        return (
            damage * 3.4
            + obp * 1.2
            + out_avoidance * 0.9
            + walk * 0.2
        )
    if slot == 5:
        return (
            damage * 2.5
            + obp * 1.0
            + out_avoidance * 0.8
            + walk * 0.2
        )
    return (
        obp * 1.4
        + out_avoidance * 1.2
        + tb * 0.7
        + walk * 0.2
    )


def _improve_lineup_order(
    connection: sqlite3.Connection,
    projection_season: str,
    ordered_player_names: list[str],
    available_player_names: list[str],
    league_rules: LeagueRulesRecord,
    simulations: int,
    seed: int | None,
    rounds: int,
    movable_pairs: set[tuple[int, int]] | None = None,
) -> list[str]:
    best_order = list(ordered_player_names)
    best_score = _simulate_average_runs(
        connection=connection,
        projection_season=projection_season,
        ordered_player_names=best_order,
        available_player_names=available_player_names,
        league_rules=league_rules,
        simulations=simulations,
        seed=seed,
    )

    for round_index in range(rounds):
        improved = False
        for left in range(len(best_order) - 1):
            for right in range(left + 1, len(best_order)):
                if movable_pairs is not None and (left, right) not in movable_pairs:
                    continue
                candidate = list(best_order)
                candidate[left], candidate[right] = candidate[right], candidate[left]
                score = _simulate_average_runs(
                    connection=connection,
                    projection_season=projection_season,
                    ordered_player_names=candidate,
                    available_player_names=available_player_names,
                    league_rules=league_rules,
                    simulations=simulations,
                    seed=None if seed is None else seed + round_index * 100 + left * 10 + right,
                )
                if score > best_score:
                    best_order = candidate
                    best_score = score
                    improved = True
        if not improved:
            break
    return best_order


def _simulate_average_runs(
    connection: sqlite3.Connection,
    projection_season: str,
    ordered_player_names: list[str],
    available_player_names: list[str],
    league_rules: LeagueRulesRecord,
    simulations: int,
    seed: int | None,
) -> float:
    lineup = build_simulation_lineup_from_order(
        connection=connection,
        projection_season=projection_season,
        ordered_player_names=ordered_player_names,
        available_player_names=available_player_names,
    )
    return simulate_lineup(
        lineup=lineup,
        league_rules=league_rules,
        simulations=simulations,
        seed=seed,
    ).average_runs


def _describe_lineup_reason(
    order: list[str],
    by_name: dict[str, GameDayProjectionRow],
    dhh_row: GameDayProjectionRow | None,
    dhh_slot: int,
    mode: str,
    preferred_available_order: list[str],
) -> str:
    top_two = order[:2]
    middle = order[2:5]
    leadoff = by_name[top_two[0]]
    setup = by_name[top_two[1]] if len(top_two) > 1 else leadoff
    details = [
        f"Leadoff {leadoff.preferred_display_name} brings projected OBP {leadoff.projected_on_base_rate:.3f}",
        f"top table-setting support includes {setup.preferred_display_name}",
    ]
    if dhh_row is not None and dhh_slot > 0:
        details.append(f"fixed DHH {dhh_row.preferred_display_name} is tested in slot {dhh_slot}")
    middle_power = sorted(
        (by_name[name] for name in middle),
        key=lambda row: row.projected_total_base_rate,
        reverse=True,
    )
    if middle_power:
        details.append(
            f"middle-order power is anchored by {middle_power[0].preferred_display_name} at TB rate {middle_power[0].projected_total_base_rate:.3f}"
        )
    if mode == "team_aware":
        details.append(_preferred_order_reason(order, preferred_available_order))
        advanced_note = _advanced_analytics_advisory(order, by_name)
        if advanced_note:
            details.append(advanced_note)
    return "; ".join(details)


def _heuristic_lineup_score(
    order: list[str],
    by_name: dict[str, GameDayProjectionRow],
    preferred_available_order: list[str] | None = None,
    mode: str = "unconstrained",
) -> float:
    base_score = sum(
        _slot_fit_score(by_name[name], slot)
        for slot, name in enumerate(order, start=1)
    )
    advanced_score = _advanced_analytics_tiebreak_score(order, by_name)
    if mode != "team_aware" or preferred_available_order is None:
        return base_score + advanced_score
    return (
        base_score
        + advanced_score
        + _preferred_order_bonus(order, preferred_available_order)
    )


def _preferred_order_bonus(
    order: list[str],
    preferred_available_order: list[str],
) -> float:
    if not preferred_available_order:
        return 0.0

    top_segment_size = min(5, len(preferred_available_order))
    exact_matches = sum(
        1.0
        for index, name in enumerate(order)
        if index < len(preferred_available_order) and name == preferred_available_order[index]
    )
    bottom_matches = sum(
        1.0
        for index in range(top_segment_size, min(len(order), len(preferred_available_order)))
        if order[index] == preferred_available_order[index]
    )
    return exact_matches * 0.025 + bottom_matches * 0.05


def _advanced_analytics_tiebreak_score(
    order: list[str],
    by_name: dict[str, GameDayProjectionRow],
) -> float:
    score = 0.0
    for slot, name in enumerate(order, start=1):
        row = by_name[name]
        table_setter = row.projected_on_base_rate + 0.5 * row.p_walk + 0.3 * (1.0 - row.p_out)
        damage = (
            row.projected_total_base_rate
            + 1.25 * row.projected_extra_base_hit_rate
            + 1.4 * row.p_home_run
        )
        run_producer = row.projected_rbi_rate + 0.6 * row.projected_run_rate + 0.3 * damage
        if slot <= 2:
            score += table_setter * 0.12
        elif slot <= 5:
            score += run_producer * 0.10
        else:
            score += (row.projected_on_base_rate + 0.5 * row.projected_total_base_rate) * 0.04
    return score


def _preferred_order_reason(
    order: list[str],
    preferred_available_order: list[str],
) -> str:
    lineup_type = _describe_lineup_type(order, preferred_available_order, mode="team_aware")
    if lineup_type == "exact preferred lineup":
        return "matches the preferred full-order baseline exactly"
    if lineup_type == "preferred lineup with bottom-half trims":
        return "keeps the preferred order intact while trimming unavailable bottom-half bats"
    if lineup_type == "preferred lineup with limited top-half reshuffle":
        return "stays close to the preferred baseline with only limited top-half reshuffling"
    return "uses the preferred lineup as a baseline"


def _advanced_analytics_advisory(
    order: list[str],
    by_name: dict[str, GameDayProjectionRow],
) -> str:
    notes: list[str] = []
    if len(order) >= 2:
        leadoff = by_name[order[0]]
        second = by_name[order[1]]
        if leadoff.projected_on_base_rate >= 0.58:
            notes.append(f"{leadoff.preferred_display_name} profiles as a table setter")
        if second.projected_on_base_rate >= 0.56:
            notes.append(f"{second.preferred_display_name} supports the top as a secondary table setter")
    middle_names = order[2:5]
    if middle_names:
        best_damage = max(
            middle_names,
            key=lambda name: (
                by_name[name].projected_total_base_rate
                + by_name[name].projected_extra_base_hit_rate
                + by_name[name].projected_rbi_rate
            ),
        )
        notes.append(f"{best_damage} carries the strongest middle-order damage/run-production signal")
    return "; ".join(notes[:2])


def _describe_lineup_type(
    order: list[str],
    preferred_available_order: list[str],
    mode: str,
) -> str:
    if mode != "team_aware":
        return DEFAULT_ADVISORY_LINEUP_TYPE
    if order == preferred_available_order:
        if len(order) == len(DEFAULT_PREFERRED_LINEUP):
            return "exact preferred lineup"
        missing_top = [name for name in DEFAULT_CORE_TOP_POOL if name not in preferred_available_order]
        if missing_top:
            return "preferred lineup with limited top-half reshuffle"
        return "preferred lineup with bottom-half trims"
    return "preferred lineup with limited top-half reshuffle"


def _select_leadoff_candidates(
    projection_rows: list[GameDayProjectionRow],
    top_n: int = 5,
) -> list[str]:
    eligible_rows = [row for row in projection_rows if not row.is_fixed_dhh]
    ranked = sorted(
        eligible_rows,
        key=lambda row: _slot_fit_score(row, 1),
        reverse=True,
    )
    selected = [row.preferred_display_name for row in ranked[:top_n]]
    # Guarantee strong top-of-order anchors are explored when present.
    for preferred_name in ("Glove", "Jj", "Kives"):
        if any(row.preferred_display_name == preferred_name for row in eligible_rows):
            if preferred_name not in selected:
                selected.append(preferred_name)
    return selected


def _select_top_order_anchor_orders(
    projection_rows: list[GameDayProjectionRow],
    dhh_slot: int,
    top_pool_size: int = 5,
    max_orders: int = 12,
) -> list[dict[int, str]]:
    anchor_slots = [slot for slot in (1, 2, 3) if slot != dhh_slot]
    if not anchor_slots:
        return [{}]

    leadoff_candidates = _select_leadoff_candidates(projection_rows, top_n=top_pool_size)
    eligible_names = list(dict.fromkeys(leadoff_candidates))
    # Include one extra top-of-order bat by slot-2 quality when top-3 search needs more depth.
    eligible_rows = [row for row in projection_rows if not row.is_fixed_dhh]
    slot2_ranked = sorted(eligible_rows, key=lambda row: _slot_fit_score(row, 2), reverse=True)
    for row in slot2_ranked[:top_pool_size]:
        if row.preferred_display_name not in eligible_names:
            eligible_names.append(row.preferred_display_name)

    scored_orders: list[tuple[float, dict[int, str]]] = []
    for combo in permutations(eligible_names, len(anchor_slots)):
        fixed_slots = {slot: name for slot, name in zip(anchor_slots, combo)}
        score = sum(
            _slot_fit_score(
                next(row for row in projection_rows if row.preferred_display_name == name),
                slot,
            )
            for slot, name in fixed_slots.items()
        )
        scored_orders.append((score, fixed_slots))

    scored_orders.sort(key=lambda item: item[0], reverse=True)
    selected = [fixed_slots for _, fixed_slots in scored_orders[:max_orders]]
    return selected or [{}]


def _select_team_aware_top_group(
    projection_rows: list[GameDayProjectionRow],
    core_top_pool: Iterable[str],
) -> list[str]:
    by_name = {row.preferred_display_name: row for row in projection_rows}
    available_names = set(by_name.keys())
    core_available = [name for name in core_top_pool if name in available_names]
    top_group_size = min(5, len(projection_rows))
    if len(core_available) >= top_group_size:
        return core_available[:top_group_size]

    remaining = [
        row.preferred_display_name
        for row in sorted(
            projection_rows,
            key=lambda row: (
                _slot_fit_score(row, 2)
                + _slot_fit_score(row, 3)
                + _slot_fit_score(row, 4)
            ),
            reverse=True,
        )
        if row.preferred_display_name not in core_available
    ]
    fill_names = remaining[: max(0, top_group_size - len(core_available))]
    return core_available + fill_names
