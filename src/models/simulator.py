from __future__ import annotations

import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from statistics import median

from src.models.lineup import SimulationLineupRow
from src.models.records import LeagueRulesRecord


EVENT_ORDER = [
    "single",
    "double",
    "triple",
    "home_run",
    "walk",
    "reached_on_error",
    "fielder_choice",
    "grounded_into_double_play",
    "strikeout",
    "other_out",
]


@dataclass
class PlayerGameStats:
    player_id: int
    player_name: str
    lineup_spot: int
    projection_source: str
    plate_appearances: int = 0
    at_bats: int = 0
    singles: int = 0
    doubles: int = 0
    triples: int = 0
    home_runs: int = 0
    walks: int = 0
    hit_by_pitch: int = 0
    reached_on_error: int = 0
    fielder_choice: int = 0
    grounded_into_double_play: int = 0
    strikeouts: int = 0
    other_outs: int = 0
    runs: int = 0
    rbi: int = 0
    total_bases: int = 0

    @property
    def hits(self) -> int:
        return self.singles + self.doubles + self.triples + self.home_runs

    @property
    def outs(self) -> int:
        return self.grounded_into_double_play + self.strikeouts + self.other_outs


@dataclass
class GameSimulationResult:
    runs: int
    event_counts_by_player: dict[str, Counter[str]]
    total_events: Counter[str]
    player_stats: dict[int, PlayerGameStats]
    team_non_dhh_home_runs: int
    dhh_exemption_used: bool


@dataclass
class SimulationSummary:
    simulations: int
    average_runs: float
    median_runs: float
    expected_runs_per_game: float
    run_distribution: dict[int, int]
    player_event_averages: dict[str, dict[str, float]]
    total_event_averages: dict[str, float]
    average_team_non_dhh_home_runs: float
    dhh_exemption_usage_rate: float


@dataclass
class _GameState:
    lineup_index: int = 0
    team_non_dhh_home_runs: int = 0
    dhh_exemption_available: bool = False
    dhh_exemption_used: bool = False


def _empty_player_stats(lineup: list[SimulationLineupRow]) -> dict[int, PlayerGameStats]:
    return {
        row.player_id: PlayerGameStats(
            player_id=row.player_id,
            player_name=row.player_name,
            lineup_spot=row.lineup_spot,
            projection_source=row.projection_source,
        )
        for row in lineup
    }


def simulate_lineup(
    lineup: list[SimulationLineupRow],
    league_rules: LeagueRulesRecord,
    simulations: int = 1000,
    seed: int | None = None,
) -> SimulationSummary:
    rng = random.Random(seed)
    runs_by_game: list[int] = []
    player_events: dict[str, Counter[str]] = defaultdict(Counter)
    total_events: Counter[str] = Counter()
    team_non_dhh_home_runs_by_game: list[int] = []
    dhh_exemption_used_count = 0

    for _ in range(simulations):
        game_result = simulate_game(lineup, league_rules, rng)
        runs_by_game.append(game_result.runs)
        team_non_dhh_home_runs_by_game.append(game_result.team_non_dhh_home_runs)
        if game_result.dhh_exemption_used:
            dhh_exemption_used_count += 1
        for player_name, events in game_result.event_counts_by_player.items():
            player_events[player_name].update(events)
        total_events.update(game_result.total_events)

    return SimulationSummary(
        simulations=simulations,
        average_runs=sum(runs_by_game) / simulations if simulations else 0.0,
        median_runs=median(runs_by_game) if runs_by_game else 0.0,
        expected_runs_per_game=sum(runs_by_game) / simulations if simulations else 0.0,
        run_distribution=dict(sorted(Counter(runs_by_game).items())),
        player_event_averages={
            player: {event: count / simulations for event, count in sorted(events.items())}
            for player, events in sorted(player_events.items())
        },
        total_event_averages={
            event: count / simulations for event, count in sorted(total_events.items())
        },
        average_team_non_dhh_home_runs=_safe_divide(
            sum(team_non_dhh_home_runs_by_game), simulations
        ),
        dhh_exemption_usage_rate=_safe_divide(dhh_exemption_used_count, simulations),
    )


def simulate_lineup_runs(
    lineup: list[SimulationLineupRow],
    league_rules: LeagueRulesRecord,
    simulations: int = 1000,
    seed: int | None = None,
) -> list[int]:
    rng = random.Random(seed)
    runs_by_game: list[int] = []

    for _ in range(simulations):
        game_result = simulate_game(lineup, league_rules, rng)
        runs_by_game.append(game_result.runs)
    return runs_by_game


def simulate_game(
    lineup: list[SimulationLineupRow],
    league_rules: LeagueRulesRecord,
    rng: random.Random | None = None,
) -> GameSimulationResult:
    game_rng = rng or random.Random()
    game_state = _GameState()
    runs = 0
    player_stats = _empty_player_stats(lineup)
    event_counts_by_player: dict[str, Counter[str]] = defaultdict(Counter)
    total_events: Counter[str] = Counter()

    for _inning in range(league_rules.innings_per_game):
        inning_runs, inning_events = _simulate_inning(
            lineup=lineup,
            league_rules=league_rules,
            game_state=game_state,
            rng=game_rng,
            player_stats=player_stats,
        )
        runs += inning_runs
        for player_name, events in inning_events.items():
            event_counts_by_player[player_name].update(events)
            total_events.update(events)

    return GameSimulationResult(
        runs=runs,
        event_counts_by_player=event_counts_by_player,
        total_events=total_events,
        player_stats=player_stats,
        team_non_dhh_home_runs=game_state.team_non_dhh_home_runs,
        dhh_exemption_used=game_state.dhh_exemption_used,
    )


def _simulate_inning(
    lineup: list[SimulationLineupRow],
    league_rules: LeagueRulesRecord,
    game_state: _GameState,
    rng: random.Random,
    player_stats: dict[int, PlayerGameStats],
) -> tuple[int, dict[str, Counter[str]]]:
    outs = 0
    runs = 0
    bases = [None, None, None]
    events_by_player: dict[str, Counter[str]] = defaultdict(Counter)

    while outs < 3:
        batter = lineup[game_state.lineup_index]
        batter_stats = player_stats[batter.player_id]
        game_state.lineup_index = (game_state.lineup_index + 1) % len(lineup)
        event = _draw_event(batter, rng)
        if batter.is_fixed_dhh and game_state.dhh_exemption_available:
            game_state.dhh_exemption_available = False
        event = _resolve_event_with_hr_rules(
            batter=batter,
            event=event,
            game_state=game_state,
            league_rules=league_rules,
            rng=rng,
        )

        batter_stats.plate_appearances += 1
        batter_rbi, scored_runner_ids, outs_delta, bases = _apply_event(
            event=event,
            bases=bases,
            batter_id=batter.player_id,
        )
        _apply_batter_box_score_event(batter_stats, event)
        batter_stats.rbi += batter_rbi
        for runner_id in scored_runner_ids:
            if runner_id is None:
                continue
            player_stats[runner_id].runs += 1

        if batter.is_fixed_dhh and event == "walk":
            game_state.dhh_exemption_available = True

        scored = len(scored_runner_ids)
        runs += scored
        outs += outs_delta
        events_by_player[batter.player_name][event] += 1

    return runs, events_by_player


def _draw_event(batter: SimulationLineupRow, rng: random.Random) -> str:
    weights = [
        max(0.0, batter.p_single),
        max(0.0, batter.p_double),
        max(0.0, batter.p_triple),
        max(0.0, batter.p_home_run),
        max(0.0, batter.p_walk),
        max(0.0, batter.p_reached_on_error),
        max(0.0, batter.p_fielder_choice),
        max(0.0, batter.p_grounded_into_double_play),
        max(0.0, batter.projected_strikeout_rate),
        max(0.0, batter.p_out),
    ]
    total = sum(weights)
    if total <= 0:
        return "other_out"
    threshold = rng.random() * total
    cumulative = 0.0
    for event, weight in zip(EVENT_ORDER, weights):
        cumulative += weight
        if threshold <= cumulative:
            return event
    return "other_out"


def _resolve_event_with_hr_rules(
    batter: SimulationLineupRow,
    event: str,
    game_state: _GameState,
    league_rules: LeagueRulesRecord,
    rng: random.Random,
) -> str:
    if event != "home_run":
        return event
    if not league_rules.fixed_dhh_enabled:
        return event
    if batter.is_fixed_dhh:
        return event
    if game_state.team_non_dhh_home_runs < league_rules.max_home_runs_non_dhh:
        game_state.team_non_dhh_home_runs += 1
        return event
    if game_state.dhh_exemption_available:
        game_state.dhh_exemption_available = False
        game_state.dhh_exemption_used = True
        return event
    return _apply_post_cap_home_run_behavior(batter, rng)


def _apply_post_cap_home_run_behavior(
    batter: SimulationLineupRow,
    rng: random.Random,
) -> str:
    if batter.p_home_run < 0.05:
        return "other_out"
    threshold = rng.random()
    if threshold < 0.50:
        return "single"
    if threshold < 0.70:
        return "double"
    return "other_out"


def _apply_event(
    event: str,
    bases: list[int | None],
    batter_id: int,
) -> tuple[int, list[int | None], int, list[int | None]]:
    first, second, third = bases
    outs = 0

    if event == "single":
        scored_runner_ids = [third] if third else []
        third, second, first = second, first, batter_id
        return len(scored_runner_ids), scored_runner_ids, outs, [first, second, third]

    if event == "double":
        scored_runner_ids = [runner_id for runner_id in [third, second] if runner_id]
        third, second, first = first, batter_id, None
        return len(scored_runner_ids), scored_runner_ids, outs, [first, second, third]

    if event == "triple":
        scored_runner_ids = [runner_id for runner_id in [first, second, third] if runner_id]
        return len(scored_runner_ids), scored_runner_ids, outs, [None, None, batter_id]

    if event == "home_run":
        scored_runner_ids = [runner_id for runner_id in [first, second, third, batter_id] if runner_id]
        return len(scored_runner_ids), scored_runner_ids, outs, [None, None, None]

    if event == "walk":
        return _apply_walk_like_advancement(first, second, third, batter_id)

    if event == "reached_on_error":
        # Treat ROE as a weaker reach event than a clean single:
        # the batter reaches first, but only forced advances are guaranteed.
        return _apply_walk_like_advancement(first, second, third, batter_id)

    if event == "fielder_choice":
        outs += 1
        if first and second and third:
            # Conservative baseline: force at home, batter reaches first, bases remain loaded.
            return 0, [], outs, [batter_id, second, third]
        if first and second:
            # Force the lead runner at third; existing runner on first advances to second.
            return 0, [], outs, [batter_id, first, third]
        if first:
            # Standard force at second; batter replaces the retired runner at first.
            return 0, [], outs, [batter_id, second, third]
        return 0, [], outs, [batter_id, second, third]

    if event == "grounded_into_double_play":
        if first:
            return 0, [], 2, [None, second, third]
        return 0, [], 1, [first, second, third]

    if event == "strikeout":
        return 0, [], 1, [first, second, third]

    return 0, [], 1, [first, second, third]


def _apply_walk_like_advancement(
    first: int | None,
    second: int | None,
    third: int | None,
    batter_id: int,
) -> tuple[int, list[int | None], int, list[int | None]]:
    scored_runner_ids = [third] if first and second and third else []
    new_third = second if first and second else third
    new_second = first if first else second
    new_first = batter_id
    return len(scored_runner_ids), scored_runner_ids, 0, [new_first, new_second, new_third]


def _apply_batter_box_score_event(stats: PlayerGameStats, event: str) -> None:
    if event == "single":
        stats.at_bats += 1
        stats.singles += 1
        stats.total_bases += 1
        return
    if event == "double":
        stats.at_bats += 1
        stats.doubles += 1
        stats.total_bases += 2
        return
    if event == "triple":
        stats.at_bats += 1
        stats.triples += 1
        stats.total_bases += 3
        return
    if event == "home_run":
        stats.at_bats += 1
        stats.home_runs += 1
        stats.total_bases += 4
        return
    if event == "walk":
        stats.walks += 1
        return
    if event == "reached_on_error":
        stats.at_bats += 1
        stats.reached_on_error += 1
        return
    if event == "fielder_choice":
        stats.at_bats += 1
        stats.fielder_choice += 1
        return
    if event == "grounded_into_double_play":
        stats.at_bats += 1
        stats.grounded_into_double_play += 1
        return
    if event == "strikeout":
        stats.at_bats += 1
        stats.strikeouts += 1
        return
    stats.at_bats += 1
    stats.other_outs += 1


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
