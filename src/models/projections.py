from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from src.models.records import HitterProjectionRecord


CURRENT_SEASON_PRIOR_PA = 120.0
DEFAULT_INJURY_MULTIPLIER = 0.35
RECENCY_WEIGHTS = [1.0, 0.7, 0.5]
RECENCY_FLOOR = 0.35


@dataclass
class _RateInputs:
    season: str
    player_name: str
    canonical_name: str
    plate_appearances: int
    singles: int
    doubles: int
    triples: int
    home_runs: int
    walks: int
    strikeouts: int
    hit_by_pitch: int
    reached_on_error: int
    fielder_choice: int
    grounded_into_double_play: int
    runs: int
    rbi: int
    total_bases: int
    batting_average_risp_weighted: float
    two_out_rbi: int
    left_on_base: int


@dataclass
class _SeasonMetadata:
    injury_flag: bool = False
    manual_weight_multiplier: float | None = None


@dataclass
class _WeightedSummary:
    weighted: _RateInputs
    weighted_prior_plate_appearances: float
    season_count_used: int
    consistency_score: float
    volatility_score: float
    trend_score: float


PRIMARY_PROJECTION_FIELDS = [
    "projection_season",
    "player_id",
    "player_name",
    "canonical_name",
    "projection_source",
    "current_plate_appearances",
    "career_plate_appearances",
    "baseline_plate_appearances",
    "weighted_prior_plate_appearances",
    "season_count_used",
    "current_season_weight",
    "consistency_score",
    "volatility_score",
    "trend_score",
    "p_single",
    "p_double",
    "p_triple",
    "p_home_run",
    "p_walk",
    "projected_strikeout_rate",
    "p_reached_on_error",
    "p_fielder_choice",
    "p_grounded_into_double_play",
    "p_out",
    "projected_on_base_rate",
    "projected_total_base_rate",
    "projected_run_rate",
    "projected_rbi_rate",
    "projected_extra_base_hit_rate",
    "fixed_dhh_flag",
    "baserunning_adjustment",
]

SECONDARY_PROJECTION_FIELDS = [
    "secondary_batting_average_risp",
    "secondary_two_out_rbi_rate",
    "secondary_left_on_base_rate",
]


def build_hitter_projections(
    connection: sqlite3.Connection,
    projection_season: str,
    current_season_prior_pa: float = CURRENT_SEASON_PRIOR_PA,
) -> list[HitterProjectionRecord]:
    current_rows = _fetch_aggregated_rows(connection, season_filter=projection_season)
    season_rows = _fetch_season_rows(connection)
    metadata_by_player_season = _fetch_player_season_metadata(connection)
    projections: list[HitterProjectionRecord] = []

    for player_id, player_seasons in season_rows.items():
        career = _sum_rows(player_seasons)
        current = current_rows.get(player_id)
        if current is None:
            current = _empty_inputs(career)
            weighted_summary = _build_weighted_prior_summary(
                player_id=player_id,
                player_seasons=player_seasons,
                current_season=None,
                metadata_by_player_season=metadata_by_player_season,
            )
            baseline = weighted_summary.weighted if weighted_summary.weighted.plate_appearances > 0 else career
            current_weight = 0.0
            projection_source = "career_fallback"
        else:
            weighted_summary = _build_weighted_prior_summary(
                player_id=player_id,
                player_seasons=player_seasons,
                current_season=projection_season,
                metadata_by_player_season=metadata_by_player_season,
            )
            baseline = weighted_summary.weighted if weighted_summary.weighted.plate_appearances > 0 else career
            base_weight = _safe_divide(current.plate_appearances, current.plate_appearances + current_season_prior_pa)
            current_weight = _apply_consistency_and_trend_adjustments(
                base_weight=base_weight,
                consistency_score=weighted_summary.consistency_score,
                trend_score=weighted_summary.trend_score,
            )
            projection_source = "season_blended"

        p_single = _blend_rate(current.singles, current.plate_appearances, baseline.singles, baseline.plate_appearances, current_weight)
        p_double = _blend_rate(current.doubles, current.plate_appearances, baseline.doubles, baseline.plate_appearances, current_weight)
        p_triple = _blend_rate(current.triples, current.plate_appearances, baseline.triples, baseline.plate_appearances, current_weight)
        p_home_run = _blend_rate(current.home_runs, current.plate_appearances, baseline.home_runs, baseline.plate_appearances, current_weight)
        p_walk = _blend_rate(current.walks, current.plate_appearances, baseline.walks, baseline.plate_appearances, current_weight)
        projected_strikeout_rate = _blend_rate(current.strikeouts, current.plate_appearances, baseline.strikeouts, baseline.plate_appearances, current_weight)
        p_hit_by_pitch = 0.0
        p_reached_on_error = _blend_rate(current.reached_on_error, current.plate_appearances, baseline.reached_on_error, baseline.plate_appearances, current_weight)
        p_fielder_choice = _blend_rate(current.fielder_choice, current.plate_appearances, baseline.fielder_choice, baseline.plate_appearances, current_weight)
        p_grounded_into_double_play = _blend_rate(current.grounded_into_double_play, current.plate_appearances, baseline.grounded_into_double_play, baseline.plate_appearances, current_weight)

        modeled_non_out_rate = sum([
            p_single,
            p_double,
            p_triple,
            p_home_run,
            p_walk,
            projected_strikeout_rate,
            p_reached_on_error,
            p_fielder_choice,
            p_grounded_into_double_play,
        ])
        p_out = max(0.0, 1.0 - modeled_non_out_rate)
        projected_on_base_rate = (
            p_single + p_double + p_triple + p_home_run + p_walk + p_reached_on_error + p_fielder_choice
        )

        projections.append(
            HitterProjectionRecord(
                projection_season=projection_season,
                player_id=player_id,
                player_name=current.player_name,
                canonical_name=current.canonical_name,
                projection_source=projection_source,
                current_plate_appearances=current.plate_appearances,
                career_plate_appearances=career.plate_appearances,
                baseline_plate_appearances=baseline.plate_appearances,
                weighted_prior_plate_appearances=weighted_summary.weighted_prior_plate_appearances,
                season_count_used=weighted_summary.season_count_used,
                current_season_weight=current_weight,
                consistency_score=weighted_summary.consistency_score,
                volatility_score=weighted_summary.volatility_score,
                trend_score=weighted_summary.trend_score,
                p_single=p_single,
                p_double=p_double,
                p_triple=p_triple,
                p_home_run=p_home_run,
                p_walk=p_walk,
                projected_strikeout_rate=projected_strikeout_rate,
                p_hit_by_pitch=0.0,
                p_reached_on_error=p_reached_on_error,
                p_fielder_choice=p_fielder_choice,
                p_grounded_into_double_play=p_grounded_into_double_play,
                p_out=p_out,
                projected_on_base_rate=projected_on_base_rate,
                projected_total_base_rate=(p_single + (2 * p_double) + (3 * p_triple) + (4 * p_home_run)),
                projected_run_rate=_blend_rate(current.runs, current.plate_appearances, baseline.runs, baseline.plate_appearances, current_weight),
                projected_rbi_rate=_blend_rate(current.rbi, current.plate_appearances, baseline.rbi, baseline.plate_appearances, current_weight),
                projected_extra_base_hit_rate=(p_double + p_triple + p_home_run),
                fixed_dhh_flag=0,
                baserunning_adjustment=0.0,
                secondary_batting_average_risp=_blend_scalar(
                    _safe_divide(current.batting_average_risp_weighted, current.plate_appearances),
                    _safe_divide(baseline.batting_average_risp_weighted, baseline.plate_appearances),
                    current_weight,
                ),
                secondary_two_out_rbi_rate=_blend_rate(current.two_out_rbi, current.plate_appearances, baseline.two_out_rbi, baseline.plate_appearances, current_weight),
                secondary_left_on_base_rate=_blend_rate(current.left_on_base, current.plate_appearances, baseline.left_on_base, baseline.plate_appearances, current_weight),
            )
        )

    return sorted(projections, key=lambda item: item.player_name.lower())


def build_hitter_projection_table(projections: Iterable[HitterProjectionRecord]) -> pd.DataFrame:
    rows = [projection.model_dump() for projection in projections]
    columns = PRIMARY_PROJECTION_FIELDS + SECONDARY_PROJECTION_FIELDS
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows)[columns].copy()


def _fetch_aggregated_rows(connection: sqlite3.Connection, season_filter: str | None) -> dict[int, _RateInputs]:
    where_clause = ""
    parameters: tuple[object, ...] = ()
    if season_filter is not None:
        where_clause = "WHERE s.season = ?"
        parameters = (season_filter,)
    rows = connection.execute(
        f"""
        SELECT
            i.player_id,
            COALESCE(MAX(s.season), '') AS season,
            i.player_name,
            i.canonical_name,
            SUM(s.plate_appearances) AS plate_appearances,
            SUM(s.singles) AS singles,
            SUM(s.doubles) AS doubles,
            SUM(s.triples) AS triples,
            SUM(s.home_runs) AS home_runs,
            SUM(s.walks) AS walks,
            SUM(s.strikeouts) AS strikeouts,
            SUM(s.hit_by_pitch) AS hit_by_pitch,
            SUM(s.reached_on_error) AS reached_on_error,
            SUM(s.fielder_choice) AS fielder_choice,
            SUM(s.grounded_into_double_play) AS grounded_into_double_play,
            SUM(s.runs) AS runs,
            SUM(s.rbi) AS rbi,
            SUM(s.total_bases) AS total_bases,
            SUM(s.two_out_rbi) AS two_out_rbi,
            SUM(s.left_on_base) AS left_on_base,
            SUM(s.batting_average_risp * s.plate_appearances) AS batting_average_risp_weighted
        FROM season_batting_stats s
        JOIN player_identity i ON i.player_id = s.player_id
        {where_clause}
        GROUP BY i.player_id, i.player_name, i.canonical_name
        """,
        parameters,
    ).fetchall()
    return {
        int(row["player_id"]): _RateInputs(
            season=str(row["season"]),
            player_name=str(row["player_name"]),
            canonical_name=str(row["canonical_name"]),
            plate_appearances=int(row["plate_appearances"] or 0),
            singles=int(row["singles"] or 0),
            doubles=int(row["doubles"] or 0),
            triples=int(row["triples"] or 0),
            home_runs=int(row["home_runs"] or 0),
            walks=int(row["walks"] or 0),
            strikeouts=int(row["strikeouts"] or 0),
            hit_by_pitch=int(row["hit_by_pitch"] or 0),
            reached_on_error=int(row["reached_on_error"] or 0),
            fielder_choice=int(row["fielder_choice"] or 0),
            grounded_into_double_play=int(row["grounded_into_double_play"] or 0),
            runs=int(row["runs"] or 0),
            rbi=int(row["rbi"] or 0),
            total_bases=int(row["total_bases"] or 0),
            batting_average_risp_weighted=float(row["batting_average_risp_weighted"] or 0.0),
            two_out_rbi=int(row["two_out_rbi"] or 0),
            left_on_base=int(row["left_on_base"] or 0),
        )
        for row in rows
    }


def _fetch_season_rows(connection: sqlite3.Connection) -> dict[int, list[_RateInputs]]:
    rows = connection.execute(
        """
        SELECT
            i.player_id,
            s.season,
            i.player_name,
            i.canonical_name,
            s.plate_appearances,
            s.singles,
            s.doubles,
            s.triples,
            s.home_runs,
            s.walks,
            s.strikeouts,
            s.hit_by_pitch,
            s.reached_on_error,
            s.fielder_choice,
            s.grounded_into_double_play,
            s.runs,
            s.rbi,
            s.total_bases,
            (s.batting_average_risp * s.plate_appearances) AS batting_average_risp_weighted,
            s.two_out_rbi,
            s.left_on_base
        FROM season_batting_stats s
        JOIN player_identity i ON i.player_id = s.player_id
        ORDER BY i.player_id, s.season DESC
        """
    ).fetchall()
    by_player: dict[int, list[_RateInputs]] = {}
    for row in rows:
        by_player.setdefault(int(row["player_id"]), []).append(
            _RateInputs(
                season=str(row["season"]),
                player_name=str(row["player_name"]),
                canonical_name=str(row["canonical_name"]),
                plate_appearances=int(row["plate_appearances"] or 0),
                singles=int(row["singles"] or 0),
                doubles=int(row["doubles"] or 0),
                triples=int(row["triples"] or 0),
                home_runs=int(row["home_runs"] or 0),
                walks=int(row["walks"] or 0),
                strikeouts=int(row["strikeouts"] or 0),
                hit_by_pitch=int(row["hit_by_pitch"] or 0),
                reached_on_error=int(row["reached_on_error"] or 0),
                fielder_choice=int(row["fielder_choice"] or 0),
                grounded_into_double_play=int(row["grounded_into_double_play"] or 0),
                runs=int(row["runs"] or 0),
                rbi=int(row["rbi"] or 0),
                total_bases=int(row["total_bases"] or 0),
                batting_average_risp_weighted=float(row["batting_average_risp_weighted"] or 0.0),
                two_out_rbi=int(row["two_out_rbi"] or 0),
                left_on_base=int(row["left_on_base"] or 0),
            )
        )
    return by_player


def _fetch_player_season_metadata(connection: sqlite3.Connection) -> dict[tuple[int, str], _SeasonMetadata]:
    rows = connection.execute(
        """
        SELECT player_id, season, injury_flag, manual_weight_multiplier
        FROM player_season_metadata
        """
    ).fetchall()
    return {
        (int(row["player_id"]), str(row["season"])): _SeasonMetadata(
            injury_flag=bool(row["injury_flag"]),
            manual_weight_multiplier=float(row["manual_weight_multiplier"]) if row["manual_weight_multiplier"] is not None else None,
        )
        for row in rows
    }


def _build_weighted_prior_summary(
    player_id: int,
    player_seasons: list[_RateInputs],
    current_season: str | None,
    metadata_by_player_season: dict[tuple[int, str], _SeasonMetadata],
) -> _WeightedSummary:
    prior_rows = [row for row in player_seasons if current_season is None or row.season != current_season]
    if not prior_rows:
        empty = _empty_inputs(player_seasons[0])
        return _WeightedSummary(empty, 0.0, 0, 0.0, 1.0, 0.0)

    weighted_row = _empty_inputs(prior_rows[0])
    weighted_pa_total = 0.0
    obp_rates: list[float] = []
    tb_rates: list[float] = []
    hr_rates: list[float] = []

    for index, row in enumerate(prior_rows):
        recency_weight = RECENCY_WEIGHTS[index] if index < len(RECENCY_WEIGHTS) else RECENCY_FLOOR
        metadata = metadata_by_player_season.get((player_id, row.season), _SeasonMetadata())
        season_weight = recency_weight * _season_weight_multiplier(metadata)
        weighted_pa_total += row.plate_appearances * season_weight
        weighted_row.plate_appearances += int(round(row.plate_appearances * season_weight))
        weighted_row.singles += int(round(row.singles * season_weight))
        weighted_row.doubles += int(round(row.doubles * season_weight))
        weighted_row.triples += int(round(row.triples * season_weight))
        weighted_row.home_runs += int(round(row.home_runs * season_weight))
        weighted_row.walks += int(round(row.walks * season_weight))
        weighted_row.strikeouts += int(round(row.strikeouts * season_weight))
        weighted_row.hit_by_pitch += int(round(row.hit_by_pitch * season_weight))
        weighted_row.reached_on_error += int(round(row.reached_on_error * season_weight))
        weighted_row.fielder_choice += int(round(row.fielder_choice * season_weight))
        weighted_row.grounded_into_double_play += int(round(row.grounded_into_double_play * season_weight))
        weighted_row.runs += int(round(row.runs * season_weight))
        weighted_row.rbi += int(round(row.rbi * season_weight))
        weighted_row.total_bases += int(round(row.total_bases * season_weight))
        weighted_row.batting_average_risp_weighted += row.batting_average_risp_weighted * season_weight
        weighted_row.two_out_rbi += int(round(row.two_out_rbi * season_weight))
        weighted_row.left_on_base += int(round(row.left_on_base * season_weight))

        pa = max(1, row.plate_appearances)
        obp_rates.append((row.singles + row.doubles + row.triples + row.home_runs + row.walks + row.reached_on_error + row.fielder_choice) / pa)
        tb_rates.append(row.total_bases / pa)
        hr_rates.append(row.home_runs / pa)

    weighted_row.player_name = prior_rows[0].player_name
    weighted_row.canonical_name = prior_rows[0].canonical_name
    consistency_score, volatility_score = _calculate_consistency_and_volatility(obp_rates, tb_rates, hr_rates)
    trend_score = _compute_trend_score(obp_rates, tb_rates, hr_rates)
    return _WeightedSummary(
        weighted=weighted_row,
        weighted_prior_plate_appearances=weighted_pa_total,
        season_count_used=len(prior_rows),
        consistency_score=consistency_score,
        volatility_score=volatility_score,
        trend_score=trend_score,
    )


def _sum_rows(rows: list[_RateInputs]) -> _RateInputs:
    total = _empty_inputs(rows[0])
    for row in rows:
        total.plate_appearances += row.plate_appearances
        total.singles += row.singles
        total.doubles += row.doubles
        total.triples += row.triples
        total.home_runs += row.home_runs
        total.walks += row.walks
        total.strikeouts += row.strikeouts
        total.hit_by_pitch += row.hit_by_pitch
        total.reached_on_error += row.reached_on_error
        total.fielder_choice += row.fielder_choice
        total.grounded_into_double_play += row.grounded_into_double_play
        total.runs += row.runs
        total.rbi += row.rbi
        total.total_bases += row.total_bases
        total.batting_average_risp_weighted += row.batting_average_risp_weighted
        total.two_out_rbi += row.two_out_rbi
        total.left_on_base += row.left_on_base
    return total


def _empty_inputs(template: _RateInputs) -> _RateInputs:
    return _RateInputs(
        season=template.season,
        player_name=template.player_name,
        canonical_name=template.canonical_name,
        plate_appearances=0,
        singles=0,
        doubles=0,
        triples=0,
        home_runs=0,
        walks=0,
        strikeouts=0,
        hit_by_pitch=0,
        reached_on_error=0,
        fielder_choice=0,
        grounded_into_double_play=0,
        runs=0,
        rbi=0,
        total_bases=0,
        batting_average_risp_weighted=0.0,
        two_out_rbi=0,
        left_on_base=0,
    )


def _season_weight_multiplier(metadata: _SeasonMetadata) -> float:
    if metadata.manual_weight_multiplier is not None:
        return metadata.manual_weight_multiplier
    if metadata.injury_flag:
        return DEFAULT_INJURY_MULTIPLIER
    return 1.0


def _calculate_consistency_and_volatility(obp_rates: list[float], tb_rates: list[float], hr_rates: list[float]) -> tuple[float, float]:
    stddevs = [_stddev(values) for values in (obp_rates, tb_rates, hr_rates) if len(values) >= 2]
    if not stddevs:
        return 0.5, 0.0
    volatility = min(1.0, (sum(stddevs) / len(stddevs)) * 4.0)
    consistency = max(0.0, 1.0 - volatility)
    return consistency, volatility


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _compute_trend_score(obp_rates: list[float], tb_rates: list[float], hr_rates: list[float]) -> float:
    if len(obp_rates) < 2:
        return 0.0
    components = []
    for values in (obp_rates, tb_rates, hr_rates):
        components.append(values[0] - values[-1])
    return max(-0.15, min(0.15, sum(components) / len(components)))


def _apply_consistency_and_trend_adjustments(base_weight: float, consistency_score: float, trend_score: float) -> float:
    adjusted = base_weight + (0.08 * consistency_score) + (0.50 * trend_score)
    return max(0.05, min(0.95, adjusted))


def _blend_rate(current_count: int, current_pa: int, baseline_count: int, baseline_pa: int, current_weight: float) -> float:
    current_rate = _safe_divide(current_count, current_pa)
    baseline_rate = _safe_divide(baseline_count, baseline_pa)
    return _blend_scalar(current_rate, baseline_rate, current_weight)


def _blend_scalar(current_value: float, baseline_value: float, current_weight: float) -> float:
    return (current_weight * current_value) + ((1.0 - current_weight) * baseline_value)


def _safe_divide(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)
