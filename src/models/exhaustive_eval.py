from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from itertools import permutations
from pathlib import Path
from statistics import median, pstdev
from time import strftime
from typing import Iterable

from src.models.lineup import build_simulation_lineup_from_order
from src.models.records import LeagueRulesRecord
from src.models.simulator import simulate_lineup_runs


@dataclass(frozen=True)
class EvaluationStage:
    name: str
    total_simulations: int
    delta_threshold: float | None = None
    min_keep: int | None = None
    keep_top_n: int | None = None


@dataclass
class LineupEvaluationResult:
    ordered_player_names: list[str]
    total_simulations: int
    average_runs: float
    median_runs: float
    stddev_runs: float
    standard_error: float
    ci_lower: float
    ci_upper: float
    stage_name: str


@dataclass
class PhaseEvaluationResult:
    phase_name: str
    winner: LineupEvaluationResult
    ranked_results: list[LineupEvaluationResult]
    practical_near_tie: bool


@dataclass
class DefinitiveLineupEvaluationResult:
    phase_1: PhaseEvaluationResult
    phase_2: PhaseEvaluationResult
    full_best_lineup: LineupEvaluationResult
    runner_up_lineup: LineupEvaluationResult | None
    effectively_tied: bool


PHASE_1_STAGES = (
    EvaluationStage("A", total_simulations=5_000, delta_threshold=0.15, min_keep=10),
    EvaluationStage("B", total_simulations=25_000, delta_threshold=0.08, min_keep=5),
    EvaluationStage("C", total_simulations=100_000),
)

PHASE_2_STAGES = (
    EvaluationStage("A", total_simulations=2_000, delta_threshold=0.20, min_keep=20),
    EvaluationStage("B", total_simulations=10_000, delta_threshold=0.08, min_keep=8),
    EvaluationStage("C", total_simulations=50_000, keep_top_n=3),
    EvaluationStage("D", total_simulations=100_000),
)


def generate_top_five_lineups(
    top_pool: Iterable[str],
    fixed_bottom_suffix: Iterable[str],
) -> list[list[str]]:
    return [list(order) + list(fixed_bottom_suffix) for order in permutations(top_pool)]


def generate_bottom_six_lineups(
    fixed_top_prefix: Iterable[str],
    middle_pool: Iterable[str],
    fixed_last: str,
) -> list[list[str]]:
    return [
        list(fixed_top_prefix) + list(order) + [fixed_last]
        for order in permutations(middle_pool)
    ]


def run_definitive_lineup_evaluation(
    connection: sqlite3.Connection,
    projection_season: str,
    game_date: str,
    league_rules: LeagueRulesRecord,
    fixed_bottom_suffix: list[str],
    top_pool: list[str],
    middle_pool: list[str],
    fixed_last: str = "Jason",
    block_size: int = 1_000,
    base_seed: int = 42,
    near_tie_delta: float = 0.03,
    available_player_names_override: list[str] | None = None,
    phase_1_stages: tuple[EvaluationStage, ...] = PHASE_1_STAGES,
    phase_2_stages: tuple[EvaluationStage, ...] = PHASE_2_STAGES,
) -> DefinitiveLineupEvaluationResult:
    available_names = (
        list(available_player_names_override)
        if available_player_names_override is not None
        else list(dict.fromkeys(top_pool + fixed_bottom_suffix))
    )

    phase_1_orders = generate_top_five_lineups(top_pool, fixed_bottom_suffix)
    phase_1 = _run_phase(
        connection=connection,
        phase_name="Phase 1",
        candidate_orders=phase_1_orders,
        projection_season=projection_season,
        available_player_names=available_names,
        league_rules=league_rules,
        stages=phase_1_stages,
        block_size=block_size,
        base_seed=base_seed,
        near_tie_delta=near_tie_delta,
    )

    winning_top_five = phase_1.winner.ordered_player_names[:5]
    phase_2_orders = generate_bottom_six_lineups(
        fixed_top_prefix=winning_top_five,
        middle_pool=middle_pool,
        fixed_last=fixed_last,
    )
    phase_2 = _run_phase(
        connection=connection,
        phase_name="Phase 2",
        candidate_orders=phase_2_orders,
        projection_season=projection_season,
        available_player_names=available_names,
        league_rules=league_rules,
        stages=phase_2_stages,
        block_size=block_size,
        base_seed=base_seed + 1_000_000,
        near_tie_delta=near_tie_delta,
    )

    runner_up = phase_2.ranked_results[1] if len(phase_2.ranked_results) > 1 else None
    return DefinitiveLineupEvaluationResult(
        phase_1=phase_1,
        phase_2=phase_2,
        full_best_lineup=phase_2.winner,
        runner_up_lineup=runner_up,
        effectively_tied=phase_2.practical_near_tie,
    )


def write_definitive_lineup_report(
    result: DefinitiveLineupEvaluationResult,
    report_path: Path,
    projection_season: str,
    game_date: str,
    fixed_bottom_suffix: list[str],
    fixed_last: str,
    near_tie_delta: float,
) -> None:
    lines: list[str] = []
    lines.append("Definitive Lineup Evaluation")
    lines.append(f"projection_season={projection_season}")
    lines.append(f"game_date={game_date}")
    lines.append(f"fixed_phase_1_bottom_suffix={' / '.join(fixed_bottom_suffix)}")
    lines.append(f"fixed_phase_2_last={fixed_last}")
    lines.append(f"practical_near_tie_delta={near_tie_delta:.3f}")
    lines.append("")
    for phase in (result.phase_1, result.phase_2):
        lines.extend(_format_phase_report(phase))
        lines.append("")
    lines.append("Final Summary")
    lines.append("best_full_lineup=" + " / ".join(result.full_best_lineup.ordered_player_names))
    lines.append(
        f"best_expected_runs={result.full_best_lineup.average_runs:.3f} | "
        f"best_median_runs={result.full_best_lineup.median_runs:.3f} | "
        f"best_ci=[{result.full_best_lineup.ci_lower:.3f}, {result.full_best_lineup.ci_upper:.3f}]"
    )
    if result.runner_up_lineup is not None:
        delta = result.runner_up_lineup.average_runs - result.full_best_lineup.average_runs
        lines.append("runner_up=" + " / ".join(result.runner_up_lineup.ordered_player_names))
        lines.append(
            f"runner_up_expected_runs={result.runner_up_lineup.average_runs:.3f} | "
            f"delta_from_winner={delta:.3f}"
        )
    lines.append(
        "decision="
        + ("effectively tied" if result.effectively_tied else "decisive winner")
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def default_report_path() -> Path:
    return Path("data/audits") / f"definitive_lineup_eval_{strftime('%Y%m%d_%H%M%S')}.txt"


def _run_phase(
    connection: sqlite3.Connection,
    phase_name: str,
    candidate_orders: list[list[str]],
    projection_season: str,
    available_player_names: list[str],
    league_rules: LeagueRulesRecord,
    stages: tuple[EvaluationStage, ...],
    block_size: int,
    base_seed: int,
    near_tie_delta: float,
) -> PhaseEvaluationResult:
    aggregates = {
        tuple(order): _LineupAggregate(ordered_player_names=list(order))
        for order in candidate_orders
    }
    surviving_orders = list(aggregates.keys())

    for stage_index, stage in enumerate(stages):
        for lineup_index, order_key in enumerate(surviving_orders):
            aggregate = aggregates[order_key]
            additional_simulations = stage.total_simulations - aggregate.total_simulations
            if additional_simulations <= 0:
                continue
            runs = _simulate_additional_runs(
                connection=connection,
                projection_season=projection_season,
                ordered_player_names=aggregate.ordered_player_names,
                available_player_names=available_player_names,
                league_rules=league_rules,
                simulations=additional_simulations,
                block_size=block_size,
                stage_index=stage_index,
                lineup_index=lineup_index,
                base_seed=base_seed,
            )
            aggregate.runs_by_game.extend(runs)

        ranked = rank_aggregates(
            aggregates=[aggregates[key] for key in surviving_orders],
            stage_name=stage.name,
        )
        if stage.delta_threshold is None and stage.min_keep is None and stage.keep_top_n is None:
            surviving_orders = [tuple(result.ordered_player_names) for result in ranked]
            continue
        surviving_orders = filter_survivor_orders(
            ranked_results=ranked,
            delta_threshold=stage.delta_threshold,
            min_keep=stage.min_keep or 0,
            keep_top_n=stage.keep_top_n,
        )

    final_ranked = rank_aggregates(
        aggregates=[aggregates[key] for key in surviving_orders],
        stage_name=stages[-1].name,
    )
    practical_near_tie = False
    if len(final_ranked) > 1:
        practical_near_tie = (
            final_ranked[0].average_runs - final_ranked[1].average_runs <= near_tie_delta
        )
    return PhaseEvaluationResult(
        phase_name=phase_name,
        winner=final_ranked[0],
        ranked_results=final_ranked[:5],
        practical_near_tie=practical_near_tie,
    )


def rank_aggregates(
    aggregates: Iterable["_LineupAggregate"],
    stage_name: str,
) -> list[LineupEvaluationResult]:
    ranked = [
        _to_evaluation_result(aggregate, stage_name)
        for aggregate in aggregates
    ]
    ranked.sort(key=lambda item: item.average_runs, reverse=True)
    return ranked


def filter_survivor_orders(
    ranked_results: list[LineupEvaluationResult],
    delta_threshold: float | None,
    min_keep: int,
    keep_top_n: int | None = None,
) -> list[tuple[str, ...]]:
    if not ranked_results:
        return []
    winner = ranked_results[0]
    survivors = [
        tuple(result.ordered_player_names)
        for result in ranked_results
        if delta_threshold is None
        or winner.average_runs - result.average_runs <= delta_threshold
    ]
    if keep_top_n is not None:
        survivors = [tuple(result.ordered_player_names) for result in ranked_results[:keep_top_n]]
    elif len(survivors) < min_keep:
        survivors = [tuple(result.ordered_player_names) for result in ranked_results[:min_keep]]
    return survivors


def _simulate_additional_runs(
    connection: sqlite3.Connection,
    projection_season: str,
    ordered_player_names: list[str],
    available_player_names: list[str],
    league_rules: LeagueRulesRecord,
    simulations: int,
    block_size: int,
    stage_index: int,
    lineup_index: int,
    base_seed: int,
) -> list[int]:
    lineup = build_simulation_lineup_from_order(
        connection=connection,
        projection_season=projection_season,
        ordered_player_names=ordered_player_names,
        available_player_names=available_player_names,
    )
    runs: list[int] = []
    blocks = math.ceil(simulations / block_size)
    for block_index in range(blocks):
        sims_this_block = min(block_size, simulations - len(runs))
        seed = base_seed + stage_index * 1_000_000 + lineup_index * 10_000 + block_index
        runs.extend(
            simulate_lineup_runs(
                lineup=lineup,
                league_rules=league_rules,
                simulations=sims_this_block,
                seed=seed,
            )
        )
    return runs


@dataclass
class _LineupAggregate:
    ordered_player_names: list[str]
    runs_by_game: list[int] | None = None

    def __post_init__(self) -> None:
        if self.runs_by_game is None:
            self.runs_by_game = []

    @property
    def total_simulations(self) -> int:
        return len(self.runs_by_game)


def _to_evaluation_result(
    aggregate: _LineupAggregate,
    stage_name: str,
) -> LineupEvaluationResult:
    runs = aggregate.runs_by_game
    if not runs:
        return LineupEvaluationResult(
            ordered_player_names=list(aggregate.ordered_player_names),
            total_simulations=0,
            average_runs=0.0,
            median_runs=0.0,
            stddev_runs=0.0,
            standard_error=0.0,
            ci_lower=0.0,
            ci_upper=0.0,
            stage_name=stage_name,
        )
    average_runs = sum(runs) / len(runs)
    stddev_runs = pstdev(runs) if len(runs) > 1 else 0.0
    standard_error = stddev_runs / math.sqrt(len(runs)) if runs else 0.0
    ci_margin = 1.96 * standard_error
    return LineupEvaluationResult(
        ordered_player_names=list(aggregate.ordered_player_names),
        total_simulations=len(runs),
        average_runs=average_runs,
        median_runs=float(median(runs)),
        stddev_runs=stddev_runs,
        standard_error=standard_error,
        ci_lower=average_runs - ci_margin,
        ci_upper=average_runs + ci_margin,
        stage_name=stage_name,
    )


def _format_phase_report(phase: PhaseEvaluationResult) -> list[str]:
    lines = [phase.phase_name]
    lines.append("winner=" + " / ".join(phase.winner.ordered_player_names))
    lines.append(
        f"winner_expected_runs={phase.winner.average_runs:.3f} | "
        f"winner_median_runs={phase.winner.median_runs:.3f} | "
        f"winner_ci=[{phase.winner.ci_lower:.3f}, {phase.winner.ci_upper:.3f}]"
    )
    lines.append(
        "practical_near_tie="
        + ("yes" if phase.practical_near_tie else "no")
    )
    lines.append("top_5:")
    winner_average = phase.winner.average_runs
    for rank, result in enumerate(phase.ranked_results, start=1):
        delta = result.average_runs - winner_average
        tie_note = " practical_near_tie" if rank > 1 and winner_average - result.average_runs <= 0.03 else ""
        lines.append(
            f"{rank}. {' / '.join(result.ordered_player_names)} | "
            f"avg={result.average_runs:.3f} | median={result.median_runs:.3f} | "
            f"ci=[{result.ci_lower:.3f}, {result.ci_upper:.3f}] | "
            f"delta={delta:.3f}{tie_note}"
        )
    return lines
