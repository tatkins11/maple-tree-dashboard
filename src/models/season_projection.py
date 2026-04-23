from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass
from pathlib import Path
from statistics import median

from src.models.lineup import SimulationLineupRow
from src.models.records import LeagueRulesRecord
from src.models.simulator import PlayerGameStats, simulate_game


@dataclass
class PlayerSeasonStats:
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
    def on_base_events(self) -> int:
        return self.hits + self.walks + self.reached_on_error + self.fielder_choice


@dataclass
class PlayerSeasonProjectionSummary:
    player_id: int
    player_name: str
    lineup_spot: int
    projection_source: str
    mean_plate_appearances: float
    mean_at_bats: float
    mean_singles: float
    mean_doubles: float
    mean_triples: float
    mean_home_runs: float
    mean_walks: float
    mean_runs: float
    mean_rbi: float
    mean_avg: float
    mean_obp: float
    mean_slg: float
    mean_ops: float
    median_plate_appearances: float
    median_at_bats: float
    median_singles: float
    median_doubles: float
    median_triples: float
    median_home_runs: float
    median_walks: float
    median_runs: float
    median_rbi: float
    p10_runs: float
    p90_runs: float
    p10_rbi: float
    p90_rbi: float
    p10_home_runs: float
    p90_home_runs: float


@dataclass
class TeamSeasonProjectionSummary:
    average_runs_per_game: float
    median_runs_per_game: float
    average_runs_per_season: float
    median_runs_per_season: float
    p10_runs_per_season: float
    p90_runs_per_season: float


@dataclass
class SeasonProjectionResult:
    season_games: int
    simulated_seasons: int
    seed: int | None
    lineup: list[SimulationLineupRow]
    team_summary: TeamSeasonProjectionSummary
    player_summaries: list[PlayerSeasonProjectionSummary]


def simulate_season_projection(
    lineup: list[SimulationLineupRow],
    league_rules: LeagueRulesRecord,
    season_games: int = 12,
    simulated_seasons: int = 5000,
    seed: int | None = None,
) -> SeasonProjectionResult:
    rng = random.Random(seed)
    player_season_totals: dict[int, list[PlayerSeasonStats]] = {
        row.player_id: [] for row in lineup
    }
    season_run_totals: list[int] = []
    game_run_totals: list[int] = []

    for _ in range(simulated_seasons):
        season_totals = _empty_season_totals(lineup)
        season_runs = 0
        for _game_number in range(season_games):
            game_result = simulate_game(lineup=lineup, league_rules=league_rules, rng=rng)
            season_runs += game_result.runs
            game_run_totals.append(game_result.runs)
            for player_id, game_stats in game_result.player_stats.items():
                _accumulate_game_into_season(season_totals[player_id], game_stats)
        season_run_totals.append(season_runs)
        for player_id, season_stats in season_totals.items():
            player_season_totals[player_id].append(season_stats)

    player_summaries = [
        _summarize_player(player_id=row.player_id, seasons=player_season_totals[row.player_id])
        for row in sorted(lineup, key=lambda item: item.lineup_spot)
    ]
    team_summary = TeamSeasonProjectionSummary(
        average_runs_per_game=_safe_divide(sum(game_run_totals), len(game_run_totals)),
        median_runs_per_game=median(game_run_totals) if game_run_totals else 0.0,
        average_runs_per_season=_safe_divide(sum(season_run_totals), len(season_run_totals)),
        median_runs_per_season=median(season_run_totals) if season_run_totals else 0.0,
        p10_runs_per_season=_percentile(season_run_totals, 0.10),
        p90_runs_per_season=_percentile(season_run_totals, 0.90),
    )
    return SeasonProjectionResult(
        season_games=season_games,
        simulated_seasons=simulated_seasons,
        seed=seed,
        lineup=lineup,
        team_summary=team_summary,
        player_summaries=player_summaries,
    )


def write_season_projection_csv(
    result: SeasonProjectionResult,
    csv_path: Path,
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "player_name",
        "lineup_spot",
        "projection_source",
        "mean_pa",
        "mean_ab",
        "mean_1b",
        "mean_2b",
        "mean_3b",
        "mean_hr",
        "mean_bb",
        "mean_r",
        "mean_rbi",
        "mean_avg",
        "mean_obp",
        "mean_slg",
        "mean_ops",
        "median_pa",
        "median_ab",
        "median_1b",
        "median_2b",
        "median_3b",
        "median_hr",
        "median_bb",
        "median_r",
        "median_rbi",
        "p10_runs",
        "p90_runs",
        "p10_rbi",
        "p90_rbi",
        "p10_hr",
        "p90_hr",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for summary in result.player_summaries:
            writer.writerow(
                {
                    "player_name": summary.player_name,
                    "lineup_spot": summary.lineup_spot,
                    "projection_source": summary.projection_source,
                    "mean_pa": f"{summary.mean_plate_appearances:.3f}",
                    "mean_ab": f"{summary.mean_at_bats:.3f}",
                    "mean_1b": f"{summary.mean_singles:.3f}",
                    "mean_2b": f"{summary.mean_doubles:.3f}",
                    "mean_3b": f"{summary.mean_triples:.3f}",
                    "mean_hr": f"{summary.mean_home_runs:.3f}",
                    "mean_bb": f"{summary.mean_walks:.3f}",
                    "mean_r": f"{summary.mean_runs:.3f}",
                    "mean_rbi": f"{summary.mean_rbi:.3f}",
                    "mean_avg": f"{summary.mean_avg:.3f}",
                    "mean_obp": f"{summary.mean_obp:.3f}",
                    "mean_slg": f"{summary.mean_slg:.3f}",
                    "mean_ops": f"{summary.mean_ops:.3f}",
                    "median_pa": f"{summary.median_plate_appearances:.3f}",
                    "median_ab": f"{summary.median_at_bats:.3f}",
                    "median_1b": f"{summary.median_singles:.3f}",
                    "median_2b": f"{summary.median_doubles:.3f}",
                    "median_3b": f"{summary.median_triples:.3f}",
                    "median_hr": f"{summary.median_home_runs:.3f}",
                    "median_bb": f"{summary.median_walks:.3f}",
                    "median_r": f"{summary.median_runs:.3f}",
                    "median_rbi": f"{summary.median_rbi:.3f}",
                    "p10_runs": f"{summary.p10_runs:.3f}",
                    "p90_runs": f"{summary.p90_runs:.3f}",
                    "p10_rbi": f"{summary.p10_rbi:.3f}",
                    "p90_rbi": f"{summary.p90_rbi:.3f}",
                    "p10_hr": f"{summary.p10_home_runs:.3f}",
                    "p90_hr": f"{summary.p90_home_runs:.3f}",
                }
            )


def write_season_projection_report(
    result: SeasonProjectionResult,
    report_path: Path,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("Simulated season projection report\n")
        handle.write("\n")
        handle.write(f"season_games: {result.season_games}\n")
        handle.write(f"simulated_seasons: {result.simulated_seasons}\n")
        handle.write(f"seed: {result.seed}\n")
        handle.write("\n")
        handle.write("Team summary\n")
        handle.write(
            f"average_runs_per_game: {result.team_summary.average_runs_per_game:.3f}\n"
        )
        handle.write(
            f"median_runs_per_game: {result.team_summary.median_runs_per_game:.3f}\n"
        )
        handle.write(
            f"average_runs_per_season: {result.team_summary.average_runs_per_season:.3f}\n"
        )
        handle.write(
            f"median_runs_per_season: {result.team_summary.median_runs_per_season:.3f}\n"
        )
        handle.write(
            f"p10_runs_per_season: {result.team_summary.p10_runs_per_season:.3f}\n"
        )
        handle.write(
            f"p90_runs_per_season: {result.team_summary.p90_runs_per_season:.3f}\n"
        )
        handle.write("\n")
        handle.write("Assumptions\n")
        handle.write("- Existing hitter event probabilities are used unchanged.\n")
        handle.write("- Existing runner advancement logic is used unchanged.\n")
        handle.write("- Runs and RBI are tracked directly from simulated scoring events.\n")
        handle.write("- OPS is computed from simulated OBP + SLG.\n")
        handle.write("\n")
        handle.write("Player summaries\n")
        for summary in result.player_summaries:
            handle.write(
                f"{summary.lineup_spot}. {summary.player_name} | source={summary.projection_source} | "
                f"mean_pa={summary.mean_plate_appearances:.3f} | mean_ab={summary.mean_at_bats:.3f} | "
                f"mean_1b={summary.mean_singles:.3f} | mean_2b={summary.mean_doubles:.3f} | "
                f"mean_3b={summary.mean_triples:.3f} | mean_hr={summary.mean_home_runs:.3f} | mean_bb={summary.mean_walks:.3f} | "
                f"mean_r={summary.mean_runs:.3f} | mean_rbi={summary.mean_rbi:.3f} | "
                f"mean_avg={summary.mean_avg:.3f} | mean_obp={summary.mean_obp:.3f} | mean_slg={summary.mean_slg:.3f} | mean_ops={summary.mean_ops:.3f} | "
                f"median_r={summary.median_runs:.3f} | p10_r={summary.p10_runs:.3f} | p90_r={summary.p90_runs:.3f}\n"
            )


def _empty_season_totals(
    lineup: list[SimulationLineupRow],
) -> dict[int, PlayerSeasonStats]:
    return {
        row.player_id: PlayerSeasonStats(
            player_id=row.player_id,
            player_name=row.player_name,
            lineup_spot=row.lineup_spot,
            projection_source=row.projection_source,
        )
        for row in lineup
    }


def _accumulate_game_into_season(
    season_stats: PlayerSeasonStats,
    game_stats: PlayerGameStats,
) -> None:
    season_stats.plate_appearances += game_stats.plate_appearances
    season_stats.at_bats += game_stats.at_bats
    season_stats.singles += game_stats.singles
    season_stats.doubles += game_stats.doubles
    season_stats.triples += game_stats.triples
    season_stats.home_runs += game_stats.home_runs
    season_stats.walks += game_stats.walks
    season_stats.hit_by_pitch += game_stats.hit_by_pitch
    season_stats.reached_on_error += game_stats.reached_on_error
    season_stats.fielder_choice += game_stats.fielder_choice
    season_stats.grounded_into_double_play += game_stats.grounded_into_double_play
    season_stats.strikeouts += game_stats.strikeouts
    season_stats.other_outs += game_stats.other_outs
    season_stats.runs += game_stats.runs
    season_stats.rbi += game_stats.rbi
    season_stats.total_bases += game_stats.total_bases


def _summarize_player(
    player_id: int,
    seasons: list[PlayerSeasonStats],
) -> PlayerSeasonProjectionSummary:
    first = seasons[0]
    pa_values = [season.plate_appearances for season in seasons]
    ab_values = [season.at_bats for season in seasons]
    single_values = [season.singles for season in seasons]
    double_values = [season.doubles for season in seasons]
    triple_values = [season.triples for season in seasons]
    hr_values = [season.home_runs for season in seasons]
    walk_values = [season.walks for season in seasons]
    run_values = [season.runs for season in seasons]
    rbi_values = [season.rbi for season in seasons]
    mean_hits = _safe_divide(sum(season.hits for season in seasons), len(seasons))
    mean_at_bats = _safe_divide(sum(ab_values), len(ab_values))
    mean_total_bases = _safe_divide(sum(season.total_bases for season in seasons), len(seasons))
    mean_on_base_events = _safe_divide(sum(season.on_base_events for season in seasons), len(seasons))
    mean_pa = _safe_divide(sum(pa_values), len(pa_values))
    mean_avg = _safe_divide(mean_hits, mean_at_bats)
    mean_slg = _safe_divide(mean_total_bases, mean_at_bats)
    mean_obp = _safe_divide(mean_on_base_events, mean_pa)
    return PlayerSeasonProjectionSummary(
        player_id=player_id,
        player_name=first.player_name,
        lineup_spot=first.lineup_spot,
        projection_source=first.projection_source,
        mean_plate_appearances=mean_pa,
        mean_at_bats=mean_at_bats,
        mean_singles=_safe_divide(sum(single_values), len(single_values)),
        mean_doubles=_safe_divide(sum(double_values), len(double_values)),
        mean_triples=_safe_divide(sum(triple_values), len(triple_values)),
        mean_home_runs=_safe_divide(sum(hr_values), len(hr_values)),
        mean_walks=_safe_divide(sum(walk_values), len(walk_values)),
        mean_runs=_safe_divide(sum(run_values), len(run_values)),
        mean_rbi=_safe_divide(sum(rbi_values), len(rbi_values)),
        mean_avg=mean_avg,
        mean_obp=mean_obp,
        mean_slg=mean_slg,
        mean_ops=mean_obp + mean_slg,
        median_plate_appearances=median(pa_values),
        median_at_bats=median(ab_values),
        median_singles=median(single_values),
        median_doubles=median(double_values),
        median_triples=median(triple_values),
        median_home_runs=median(hr_values),
        median_walks=median(walk_values),
        median_runs=median(run_values),
        median_rbi=median(rbi_values),
        p10_runs=_percentile(run_values, 0.10),
        p90_runs=_percentile(run_values, 0.90),
        p10_rbi=_percentile(rbi_values, 0.10),
        p90_rbi=_percentile(rbi_values, 0.90),
        p10_home_runs=_percentile(hr_values, 0.10),
        p90_home_runs=_percentile(hr_values, 0.90),
    )


def _percentile(values: list[int], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    weight = position - lower
    return float(lower_value + (upper_value - lower_value) * weight)


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
