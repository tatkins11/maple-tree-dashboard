from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.models.records import SeasonBattingStatRecord


PRIMARY_FEATURE_FIELDS = [
    "season",
    "player_name",
    "canonical_name",
    "games",
    "plate_appearances",
    "at_bats",
    "hits",
    "singles",
    "doubles",
    "triples",
    "home_runs",
    "walks",
    "strikeouts",
    "sacrifice_hits",
    "sacrifice_flies",
    "reached_on_error",
    "fielder_choice",
    "grounded_into_double_play",
    "runs",
    "rbi",
    "total_bases",
    "hit_rate_per_ab",
    "single_rate_per_pa",
    "double_rate_per_pa",
    "triple_rate_per_pa",
    "home_run_rate_per_pa",
    "walk_rate_per_pa",
    "strikeout_rate_per_pa",
    "roe_rate_per_pa",
    "fc_rate_per_pa",
    "gidp_rate_per_pa",
    "run_rate_per_pa",
    "rbi_rate_per_pa",
    "total_bases_per_ab",
    "on_base_events",
    "on_base_events_per_pa",
    "extra_base_hits",
    "extra_base_hit_rate_per_pa",
    "batting_average",
    "on_base_percentage",
    "slugging_percentage",
    "ops",
]

SECONDARY_FEATURE_FIELDS = [
    "batting_average_risp",
    "two_out_rbi",
    "two_out_rbi_per_pa",
    "left_on_base",
    "left_on_base_per_pa",
]

EXCLUDED_FEATURE_FIELDS = [
    "SB",
    "SB%",
    "CS",
    "PIK",
    "QAB",
    "QAB%",
    "PA/BB",
    "BB/K",
    "C%",
    "HHB",
    "LD%",
    "FB%",
    "GB%",
    "BABIP",
    "PS",
    "PS/PA",
    "2S+3",
    "2S+3%",
    "6+",
    "6+%",
    "AB/HR",
]


def build_hitter_feature_table(
    records: Iterable[SeasonBattingStatRecord],
) -> pd.DataFrame:
    rows = []
    for record in records:
        on_base_events = (
            record.hits
            + record.walks
            + record.reached_on_error
            + record.fielder_choice
        )
        extra_base_hits = record.doubles + record.triples + record.home_runs
        rows.append(
            {
                "season": record.season,
                "player_name": record.player_name,
                "canonical_name": record.canonical_name,
                "games": record.games,
                "plate_appearances": record.plate_appearances,
                "at_bats": record.at_bats,
                "hits": record.hits,
                "singles": record.singles,
                "doubles": record.doubles,
                "triples": record.triples,
                "home_runs": record.home_runs,
                "walks": record.walks,
                "strikeouts": record.strikeouts,
                "sacrifice_hits": record.sacrifice_hits,
                "sacrifice_flies": record.sacrifice_flies,
                "reached_on_error": record.reached_on_error,
                "fielder_choice": record.fielder_choice,
                "grounded_into_double_play": record.grounded_into_double_play,
                "runs": record.runs,
                "rbi": record.rbi,
                "total_bases": record.total_bases,
                "hit_rate_per_ab": _safe_divide(record.hits, record.at_bats),
                "single_rate_per_pa": _safe_divide(record.singles, record.plate_appearances),
                "double_rate_per_pa": _safe_divide(record.doubles, record.plate_appearances),
                "triple_rate_per_pa": _safe_divide(record.triples, record.plate_appearances),
                "home_run_rate_per_pa": _safe_divide(record.home_runs, record.plate_appearances),
                "walk_rate_per_pa": _safe_divide(record.walks, record.plate_appearances),
                "strikeout_rate_per_pa": _safe_divide(record.strikeouts, record.plate_appearances),
                "roe_rate_per_pa": _safe_divide(record.reached_on_error, record.plate_appearances),
                "fc_rate_per_pa": _safe_divide(record.fielder_choice, record.plate_appearances),
                "gidp_rate_per_pa": _safe_divide(
                    record.grounded_into_double_play, record.plate_appearances
                ),
                "run_rate_per_pa": _safe_divide(record.runs, record.plate_appearances),
                "rbi_rate_per_pa": _safe_divide(record.rbi, record.plate_appearances),
                "total_bases_per_ab": _safe_divide(record.total_bases, record.at_bats),
                "on_base_events": on_base_events,
                "on_base_events_per_pa": _safe_divide(on_base_events, record.plate_appearances),
                "extra_base_hits": extra_base_hits,
                "extra_base_hit_rate_per_pa": _safe_divide(
                    extra_base_hits, record.plate_appearances
                ),
                "batting_average": record.batting_average,
                "on_base_percentage": record.on_base_percentage,
                "slugging_percentage": record.slugging_percentage,
                "ops": record.ops,
                "batting_average_risp": record.batting_average_risp,
                "two_out_rbi": record.two_out_rbi,
                "two_out_rbi_per_pa": _safe_divide(record.two_out_rbi, record.plate_appearances),
                "left_on_base": record.left_on_base,
                "left_on_base_per_pa": _safe_divide(record.left_on_base, record.plate_appearances),
            }
        )

    ordered_columns = PRIMARY_FEATURE_FIELDS + SECONDARY_FEATURE_FIELDS
    if not rows:
        return pd.DataFrame(columns=ordered_columns)
    return pd.DataFrame(rows)[ordered_columns].copy()


def _safe_divide(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)
