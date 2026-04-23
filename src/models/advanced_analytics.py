from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


ADVANCED_RUNS_PER_WIN = 10.0
REPLACEMENT_LEVEL_PERCENTILE = 0.20
REPLACEMENT_LEVEL_FALLBACK_FACTOR = 0.80
REPLACEMENT_LEVEL_MIN_PA = 20
TEAM_RELATIVE_SCALE = 100.0

ARCHETYPE_THRESHOLDS = {
    "table_setter_obp_plus": 106.0,
    "table_setter_non_out_multiplier": 1.00,
    "balanced_obp_floor": 96.0,
    "balanced_slg_floor": 96.0,
    "balanced_obp_ceiling": 118.0,
    "balanced_slg_ceiling": 118.0,
    "gap_power_slg_plus": 112.0,
    "gap_power_xbh_rate": 0.10,
    "hr_threat_hr_plus": 140.0,
    "hr_threat_hr_rate": 0.08,
    "run_producer_index": 118.0,
    "low_damage_obp_plus": 110.0,
    "low_damage_slg_plus": 92.0,
    "bottom_order_obp_floor": 90.0,
    "bottom_order_slg_floor": 88.0,
}

ARCHETYPE_DISPLAY_ORDER = [
    "HR Threat",
    "Gap Power",
    "Run Producer",
    "Balanced Bat",
    "Table Setter",
    "Low-Damage OBP Bat",
    "Bottom-Order Bat",
]


@dataclass(frozen=True)
class AdvancedAnalyticsMetadata:
    mode: str
    comparison_group_label: str
    baseline_player_count: int
    average_offensive_run_rate: float
    replacement_offensive_run_rate: float
    replacement_percentile: float
    replacement_min_pa: int
    runs_per_win: float


def calculate_advanced_analytics(
    dataframe: pd.DataFrame,
    *,
    comparison_dataframe: pd.DataFrame | None = None,
    mode: str,
    comparison_group_label: str,
    replacement_percentile: float = REPLACEMENT_LEVEL_PERCENTILE,
    replacement_min_pa: int = REPLACEMENT_LEVEL_MIN_PA,
    replacement_fallback_factor: float = REPLACEMENT_LEVEL_FALLBACK_FACTOR,
    runs_per_win: float = ADVANCED_RUNS_PER_WIN,
) -> tuple[pd.DataFrame, AdvancedAnalyticsMetadata]:
    metrics = _compute_base_metrics(dataframe)
    comparison_metrics = _compute_base_metrics(comparison_dataframe if comparison_dataframe is not None else dataframe)
    baselines = _calculate_baselines(
        comparison_metrics,
        replacement_percentile=replacement_percentile,
        replacement_min_pa=replacement_min_pa,
        replacement_fallback_factor=replacement_fallback_factor,
    )

    metrics = metrics.assign(
        team_relative_obp=_plus_metric(metrics["obp"], baselines["obp"]),
        team_relative_slg=_plus_metric(metrics["slg"], baselines["slg"]),
        team_relative_ops=_plus_metric(metrics["ops"], baselines["ops"]),
        team_relative_tb_per_pa=_plus_metric(metrics["tb_per_pa"], baselines["tb_per_pa"]),
        team_relative_hr_rate=_plus_metric(metrics["hr_rate"], baselines["hr_rate"]),
        run_production_index=TEAM_RELATIVE_SCALE
        * (
            _safe_ratio_series(metrics["rbi_per_pa"], baselines["rbi_per_pa"])
            + _safe_ratio_series(metrics["runs_per_pa"], baselines["runs_per_pa"])
        )
        / 2.0,
        run_conversion_index=_plus_metric(
            metrics["runs_per_on_base_event"],
            baselines["runs_per_on_base_event"],
        ),
    )

    metrics = metrics.assign(
        offensive_runs_above_average=(metrics["offensive_run_rate"] - baselines["offensive_run_rate"]) * metrics["pa"],
        runs_above_replacement=(metrics["offensive_run_rate"] - baselines["replacement_offensive_run_rate"])
        * metrics["pa"],
    )
    metrics = metrics.assign(
        raa=metrics["offensive_runs_above_average"],
        rar=metrics["runs_above_replacement"],
        owar=_safe_ratio_series(metrics["runs_above_replacement"], runs_per_win),
    )

    metrics = metrics.assign(
        archetype=metrics.apply(lambda row: _classify_archetype(row, baselines), axis=1)
    )

    metadata = AdvancedAnalyticsMetadata(
        mode=mode,
        comparison_group_label=comparison_group_label,
        baseline_player_count=int(len(comparison_metrics)),
        average_offensive_run_rate=float(baselines["offensive_run_rate"]),
        replacement_offensive_run_rate=float(baselines["replacement_offensive_run_rate"]),
        replacement_percentile=float(replacement_percentile),
        replacement_min_pa=int(replacement_min_pa),
        runs_per_win=float(runs_per_win),
    )
    return metrics, metadata


def build_advanced_leaderboards(dataframe: pd.DataFrame, limit: int = 5) -> dict[str, pd.DataFrame]:
    if dataframe.empty:
        return {}

    return {
        "Best On-Base": dataframe.sort_values(
            ["team_relative_obp", "obp", "pa"], ascending=[False, False, False]
        ).head(limit)[["player", "obp", "team_relative_obp", "pa"]],
        "Best Power": dataframe.sort_values(
            ["iso", "xbh_rate", "hr_rate", "pa"], ascending=[False, False, False, False]
        ).head(limit)[["player", "iso", "hr_rate", "tb_per_pa"]],
        "Best Run Producer": dataframe.sort_values(
            ["run_production_index", "rbi_per_pa", "pa"], ascending=[False, False, False]
        ).head(limit)[["player", "rbi_per_pa", "runs_per_on_base_event", "pa"]],
        "Best Team-Relative Bat": dataframe.sort_values(
            ["team_relative_ops", "ops", "pa"], ascending=[False, False, False]
        ).head(limit)[["player", "team_relative_ops", "ops", "pa"]],
        "Highest RAR / oWAR": dataframe.sort_values(
            ["rar", "owar", "pa"], ascending=[False, False, False]
        ).head(limit)[["player", "rar", "owar", "pa"]],
    }


def build_archetype_summary(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    grouped = (
        dataframe.groupby("archetype", dropna=False)
        .agg(
            hitters=("player", "count"),
            avg_obp=("obp", "mean"),
            avg_slg=("slg", "mean"),
            avg_owar=("owar", "mean"),
        )
        .reset_index()
    )
    grouped = grouped.assign(
        archetype_rank=grouped["archetype"].apply(
            lambda value: ARCHETYPE_DISPLAY_ORDER.index(value)
            if value in ARCHETYPE_DISPLAY_ORDER
            else len(ARCHETYPE_DISPLAY_ORDER)
        )
    )
    return grouped.sort_values(["archetype_rank", "hitters", "avg_owar"], ascending=[True, False, False]).drop(columns=["archetype_rank"])


def build_player_comparison(dataframe: pd.DataFrame, players: list[str]) -> pd.DataFrame:
    if dataframe.empty or not players:
        return pd.DataFrame()

    comparison_columns = [
        "player",
        "pa",
        "obp",
        "slg",
        "ops",
        "iso",
        "xbh_rate",
        "hr_rate",
        "tb_per_pa",
        "non_out_rate",
        "walk_rate",
        "rbi_per_pa",
        "runs_per_on_base_event",
        "team_relative_obp",
        "team_relative_slg",
        "team_relative_ops",
        "raa",
        "rar",
        "owar",
        "archetype",
    ]
    selected = dataframe[dataframe["player"].isin(players)][comparison_columns].copy()
    if selected.empty:
        return pd.DataFrame()
    return selected.set_index("player").transpose()


def _compute_base_metrics(dataframe: pd.DataFrame | None) -> pd.DataFrame:
    if dataframe is None or dataframe.empty:
        return pd.DataFrame()

    metrics = dataframe.copy()
    for column in [
        "games",
        "pa",
        "ab",
        "hits",
        "1b",
        "2b",
        "3b",
        "hr",
        "bb",
        "so",
        "hbp",
        "sac",
        "sf",
        "roe",
        "fc",
        "gidp",
        "r",
        "rbi",
        "tb",
        "two_out_rbi",
        "lob",
        "ba_risp",
    ]:
        if column not in metrics.columns:
            metrics[column] = 0

    extra_base_hits = metrics["2b"] + metrics["3b"] + metrics["hr"]
    on_base_events = metrics["hits"] + metrics["bb"] + metrics["hbp"] + metrics["roe"] + metrics["fc"]
    non_out_events = metrics["hits"] + metrics["bb"] + metrics["hbp"] + metrics["roe"]
    non_hr_on_base_events = metrics["1b"] + metrics["2b"] + metrics["3b"] + metrics["bb"] + metrics["hbp"] + metrics["roe"] + metrics["fc"]

    avg = _safe_ratio_series(metrics["hits"], metrics["ab"])
    obp = _safe_ratio_series(metrics["hits"] + metrics["bb"] + metrics["hbp"], metrics["ab"] + metrics["bb"] + metrics["hbp"] + metrics["sf"])
    slg = _safe_ratio_series(metrics["tb"], metrics["ab"])
    ops = obp + slg
    iso = _safe_ratio_series(metrics["tb"] - metrics["hits"], metrics["ab"])
    xbh_rate = _safe_ratio_series(extra_base_hits, metrics["pa"])
    hr_rate = _safe_ratio_series(metrics["hr"], metrics["pa"])
    tb_per_pa = _safe_ratio_series(metrics["tb"], metrics["pa"])
    tb_per_ab = _safe_ratio_series(metrics["tb"], metrics["ab"])
    extra_base_hit_share_of_hits = _safe_ratio_series(extra_base_hits, metrics["hits"])

    on_base_rate = _safe_ratio_series(on_base_events, metrics["pa"])
    non_out_rate = _safe_ratio_series(non_out_events, metrics["pa"])
    walk_rate = _safe_ratio_series(metrics["bb"], metrics["pa"])
    hbp_rate = _safe_ratio_series(metrics["hbp"], metrics["pa"])
    roe_rate = _safe_ratio_series(metrics["roe"], metrics["pa"])
    fc_rate = _safe_ratio_series(metrics["fc"], metrics["pa"])

    rbi_per_pa = _safe_ratio_series(metrics["rbi"], metrics["pa"])
    rbi_per_hit = _safe_ratio_series(metrics["rbi"], metrics["hits"])
    runs_per_pa = _safe_ratio_series(metrics["r"], metrics["pa"])
    runs_per_on_base_event = _safe_ratio_series(metrics["r"], on_base_events)
    runs_per_non_hr_on_base_event = _safe_ratio_series(metrics["r"], non_hr_on_base_events)
    two_out_rbi_rate = _safe_ratio_series(metrics["two_out_rbi"], metrics["pa"])
    lob_per_pa = _safe_ratio_series(metrics["lob"], metrics["pa"])

    offensive_run_rate = non_out_rate * tb_per_pa
    offensive_runs_created = offensive_run_rate * metrics["pa"]

    return metrics.assign(
        extra_base_hits=extra_base_hits,
        on_base_events=on_base_events,
        non_out_events=non_out_events,
        non_hr_on_base_events=non_hr_on_base_events,
        avg=avg,
        obp=obp,
        slg=slg,
        ops=ops,
        iso=iso,
        xbh_rate=xbh_rate,
        hr_rate=hr_rate,
        tb_per_pa=tb_per_pa,
        tb_per_ab=tb_per_ab,
        extra_base_hit_share_of_hits=extra_base_hit_share_of_hits,
        on_base_rate=on_base_rate,
        non_out_rate=non_out_rate,
        walk_rate=walk_rate,
        hbp_rate=hbp_rate,
        roe_rate=roe_rate,
        fc_rate=fc_rate,
        rbi_per_pa=rbi_per_pa,
        rbi_per_hit=rbi_per_hit,
        runs_per_pa=runs_per_pa,
        runs_per_on_base_event=runs_per_on_base_event,
        runs_per_non_hr_on_base_event=runs_per_non_hr_on_base_event,
        two_out_rbi_rate=two_out_rbi_rate,
        lob_per_pa=lob_per_pa,
        offensive_run_rate=offensive_run_rate,
        offensive_runs_created=offensive_runs_created,
    )


def _calculate_baselines(
    comparison_metrics: pd.DataFrame,
    *,
    replacement_percentile: float,
    replacement_min_pa: int,
    replacement_fallback_factor: float,
) -> dict[str, float]:
    if comparison_metrics.empty:
        return {
            "obp": 0.0,
            "slg": 0.0,
            "ops": 0.0,
            "tb_per_pa": 0.0,
            "hr_rate": 0.0,
            "rbi_per_pa": 0.0,
            "runs_per_pa": 0.0,
            "runs_per_on_base_event": 0.0,
            "non_out_rate": 0.0,
            "offensive_run_rate": 0.0,
            "xbh_rate": 0.0,
            "replacement_offensive_run_rate": 0.0,
        }

    total_pa = float(comparison_metrics["pa"].sum())
    total_ab = float(comparison_metrics["ab"].sum())
    total_hits = float(comparison_metrics["hits"].sum())
    total_walks = float(comparison_metrics["bb"].sum())
    total_hbp = float(comparison_metrics["hbp"].sum())
    total_sf = float(comparison_metrics["sf"].sum())
    total_tb = float(comparison_metrics["tb"].sum())
    total_hr = float(comparison_metrics["hr"].sum())
    total_rbi = float(comparison_metrics["rbi"].sum())
    total_runs = float(comparison_metrics["r"].sum())
    total_on_base = float(comparison_metrics["on_base_events"].sum())
    total_non_out = float(comparison_metrics["non_out_events"].sum())
    total_runs_created = float(comparison_metrics["offensive_runs_created"].sum())
    total_xbh = float(comparison_metrics["extra_base_hits"].sum())

    offensive_run_rate = _safe_ratio(total_runs_created, total_pa)
    replacement_candidates = comparison_metrics[comparison_metrics["pa"] >= replacement_min_pa]["offensive_run_rate"]
    if len(replacement_candidates) >= 3:
        replacement_rate = float(replacement_candidates.quantile(replacement_percentile))
    elif not replacement_candidates.empty:
        replacement_rate = float(replacement_candidates.min())
    else:
        replacement_rate = offensive_run_rate * replacement_fallback_factor

    return {
        "obp": _safe_ratio(total_hits + total_walks + total_hbp, total_ab + total_walks + total_hbp + total_sf),
        "slg": _safe_ratio(total_tb, total_ab),
        "ops": _safe_ratio(total_hits + total_walks + total_hbp, total_ab + total_walks + total_hbp + total_sf)
        + _safe_ratio(total_tb, total_ab),
        "tb_per_pa": _safe_ratio(total_tb, total_pa),
        "hr_rate": _safe_ratio(total_hr, total_pa),
        "rbi_per_pa": _safe_ratio(total_rbi, total_pa),
        "runs_per_pa": _safe_ratio(total_runs, total_pa),
        "runs_per_on_base_event": _safe_ratio(total_runs, total_on_base),
        "non_out_rate": _safe_ratio(total_non_out, total_pa),
        "offensive_run_rate": offensive_run_rate,
        "xbh_rate": _safe_ratio(total_xbh, total_pa),
        "replacement_offensive_run_rate": replacement_rate,
    }


def _plus_metric(series: pd.Series, baseline: float) -> pd.Series:
    return _safe_ratio_series(series, baseline) * TEAM_RELATIVE_SCALE


def _classify_archetype(row: pd.Series, baselines: dict[str, float]) -> str:
    hr_plus = float(row.get("team_relative_hr_rate", 0.0))
    slg_plus = float(row.get("team_relative_slg", 0.0))
    obp_plus = float(row.get("team_relative_obp", 0.0))
    ops_plus = float(row.get("team_relative_ops", 0.0))
    hr_rate = float(row.get("hr_rate", 0.0))
    xbh_rate = float(row.get("xbh_rate", 0.0))
    non_out_rate = float(row.get("non_out_rate", 0.0))
    run_production_index = float(row.get("run_production_index", 0.0))

    if hr_plus >= ARCHETYPE_THRESHOLDS["hr_threat_hr_plus"] or hr_rate >= ARCHETYPE_THRESHOLDS["hr_threat_hr_rate"]:
        return "HR Threat"
    if slg_plus >= ARCHETYPE_THRESHOLDS["gap_power_slg_plus"] and xbh_rate >= max(
        ARCHETYPE_THRESHOLDS["gap_power_xbh_rate"], baselines["xbh_rate"] * 1.10
    ):
        return "Gap Power"
    if run_production_index >= ARCHETYPE_THRESHOLDS["run_producer_index"] and slg_plus >= TEAM_RELATIVE_SCALE:
        return "Run Producer"
    if obp_plus >= ARCHETYPE_THRESHOLDS["low_damage_obp_plus"] and slg_plus < ARCHETYPE_THRESHOLDS["low_damage_slg_plus"]:
        return "Low-Damage OBP Bat"
    if obp_plus >= ARCHETYPE_THRESHOLDS["table_setter_obp_plus"] and non_out_rate >= baselines["non_out_rate"] * ARCHETYPE_THRESHOLDS["table_setter_non_out_multiplier"]:
        return "Table Setter"
    if (
        ARCHETYPE_THRESHOLDS["balanced_obp_floor"] <= obp_plus <= ARCHETYPE_THRESHOLDS["balanced_obp_ceiling"]
        and ARCHETYPE_THRESHOLDS["balanced_slg_floor"] <= slg_plus <= ARCHETYPE_THRESHOLDS["balanced_slg_ceiling"]
    ):
        return "Balanced Bat"
    if ops_plus >= TEAM_RELATIVE_SCALE:
        return "Balanced Bat"
    if obp_plus < ARCHETYPE_THRESHOLDS["bottom_order_obp_floor"] and slg_plus < ARCHETYPE_THRESHOLDS["bottom_order_slg_floor"]:
        return "Bottom-Order Bat"
    if obp_plus >= TEAM_RELATIVE_SCALE or slg_plus >= TEAM_RELATIVE_SCALE:
        return "Balanced Bat"
    return "Low-Damage OBP Bat"


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _safe_ratio_series(numerator: pd.Series | float, denominator: pd.Series | float) -> pd.Series:
    numerator_series = numerator.astype(float) if isinstance(numerator, pd.Series) else pd.Series([float(numerator)])
    if isinstance(denominator, pd.Series):
        denominator_series = denominator.astype(float)
    else:
        denominator_series = pd.Series(float(denominator), index=numerator_series.index, dtype=float)
    result = numerator_series.divide(denominator_series.replace(0, float("nan")))
    return result.fillna(0.0).astype(float)
