from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from src.models.records import SeasonBattingStatRecord
from src.utils.names import display_player_name, normalize_player_name
from src.utils.validation import (
    validate_season_record,
    validate_season_records_dataset,
)


_COLUMN_ALIASES = {
    "player": "player_name",
    "name": "player_name",
    "player_name": "player_name",
    "gp": "games",
    "g": "games",
    "games": "games",
    "pa": "plate_appearances",
    "plate appearances": "plate_appearances",
    "ab": "at_bats",
    "at bats": "at_bats",
    "h": "hits",
    "1b": "singles",
    "2b": "doubles",
    "3b": "triples",
    "hr": "home_runs",
    "bb": "walks",
    "so": "strikeouts",
    "k": "strikeouts",
    "hbp": "hit_by_pitch",
    "sac": "sacrifice_hits",
    "sf": "sacrifice_flies",
    "roe": "reached_on_error",
    "fc": "fielder_choice",
    "r": "runs",
    "rbi": "rbi",
    "tb": "total_bases",
    "avg": "batting_average",
    "obp": "on_base_percentage",
    "slg": "slugging_percentage",
    "ops": "ops",
    "gidp": "grounded_into_double_play",
    "ba/risp": "batting_average_risp",
    "2outrbi": "two_out_rbi",
    "lob": "left_on_base",
}

_INTEGER_FIELDS = [
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
    "hit_by_pitch",
    "sacrifice_hits",
    "sacrifice_flies",
    "reached_on_error",
    "fielder_choice",
    "runs",
    "rbi",
    "total_bases",
    "two_out_rbi",
    "left_on_base",
]

_FLOAT_FIELDS = [
    "batting_average",
    "on_base_percentage",
    "slugging_percentage",
    "ops",
]

_OPTIONAL_FLOAT_FIELDS = [
    "batting_average_risp",
]


def import_season_stats_csv(
    csv_path: Path,
    season: str | None = None,
) -> tuple[List[SeasonBattingStatRecord], List[str]]:
    dataframe, issues = _load_gamechanger_season_dataframe(csv_path)
    required = {"player_name", *(_INTEGER_FIELDS + _FLOAT_FIELDS + _OPTIONAL_FLOAT_FIELDS)}
    missing = sorted(required.difference(dataframe.columns))
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")

    inferred_season = season or infer_season_from_filename(csv_path.name)
    records: List[SeasonBattingStatRecord] = []

    for _, row in dataframe.iterrows():
        player_name = _coerce_player_name(row["player_name"])
        if not player_name:
            issues.append(f"missing player name in row sourced from {csv_path.name}")
            continue

        record = SeasonBattingStatRecord(
            season=inferred_season,
            player_name=player_name,
            canonical_name=normalize_player_name(player_name),
            games=_coerce_int(row["games"], "games", player_name, issues),
            plate_appearances=_coerce_int(
                row["plate_appearances"], "plate_appearances", player_name, issues
            ),
            at_bats=_coerce_int(row["at_bats"], "at_bats", player_name, issues),
            hits=_coerce_int(row["hits"], "hits", player_name, issues),
            singles=_coerce_int(row["singles"], "singles", player_name, issues),
            doubles=_coerce_int(row["doubles"], "doubles", player_name, issues),
            triples=_coerce_int(row["triples"], "triples", player_name, issues),
            home_runs=_coerce_int(row["home_runs"], "home_runs", player_name, issues),
            walks=_coerce_int(row["walks"], "walks", player_name, issues),
            strikeouts=_coerce_int(
                row["strikeouts"], "strikeouts", player_name, issues
            ),
            hit_by_pitch=0,
            sacrifice_hits=_coerce_int(
                row["sacrifice_hits"], "sacrifice_hits", player_name, issues
            ),
            sacrifice_flies=_coerce_int(
                row["sacrifice_flies"], "sacrifice_flies", player_name, issues
            ),
            reached_on_error=_coerce_int(
                row["reached_on_error"], "reached_on_error", player_name, issues
            ),
            fielder_choice=_coerce_int(
                row["fielder_choice"], "fielder_choice", player_name, issues
            ),
            grounded_into_double_play=_coerce_int(
                row["grounded_into_double_play"],
                "grounded_into_double_play",
                player_name,
                issues,
            ),
            runs=_coerce_int(row["runs"], "runs", player_name, issues),
            rbi=_coerce_int(row["rbi"], "rbi", player_name, issues),
            total_bases=_coerce_int(
                row["total_bases"], "total_bases", player_name, issues
            ),
            batting_average=_coerce_float(
                row["batting_average"], "batting_average", player_name, issues
            ),
            on_base_percentage=_coerce_float(
                row["on_base_percentage"], "on_base_percentage", player_name, issues
            ),
            slugging_percentage=_coerce_float(
                row["slugging_percentage"], "slugging_percentage", player_name, issues
            ),
            ops=_coerce_float(row["ops"], "ops", player_name, issues),
            batting_average_risp=_coerce_float(
                row["batting_average_risp"],
                "batting_average_risp",
                player_name,
                issues,
                allow_missing=True,
            ),
            two_out_rbi=_coerce_int(
                row["two_out_rbi"], "two_out_rbi", player_name, issues
            ),
            left_on_base=_coerce_int(
                row["left_on_base"], "left_on_base", player_name, issues
            ),
            raw_source_file=csv_path.name,
        )
        issues.extend(validate_season_record(record))
        records.append(record)

    issues.extend(validate_season_records_dataset(records))
    return records, issues


def infer_season_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    for suffix in (" Stats", "_Stats", "-Stats"):
        stem = stem.replace(suffix, "")
    return stem


def _normalized_columns(columns: pd.Index) -> dict[str, str]:
    normalized = {}
    for column in columns:
        key = str(column).strip().lower().replace("_", " ")
        normalized[column] = _COLUMN_ALIASES.get(key, key.replace(" ", "_"))
    return normalized


def _load_gamechanger_season_dataframe(csv_path: Path) -> tuple[pd.DataFrame, List[str]]:
    dataframe = pd.read_csv(csv_path, header=1, dtype=str)
    dataframe = dataframe.rename(columns=_normalized_columns(dataframe.columns))
    dataframe = dataframe.iloc[:, :54].copy()
    dataframe = _ensure_optional_feature_columns(dataframe)
    dataframe["player_name"] = dataframe.apply(_combine_player_name, axis=1)
    issues = _collect_non_player_row_issues(dataframe, csv_path.name)
    dataframe = dataframe[~dataframe.apply(_is_non_player_row, axis=1)].copy()
    dataframe = dataframe.reset_index(drop=True)
    return dataframe, issues


def _combine_player_name(row: pd.Series) -> str:
    first = _text_or_empty(row.get("first", ""))
    last = _text_or_empty(row.get("last", ""))
    full_name = " ".join(part for part in (first, last) if part)
    return display_player_name(full_name)


def _is_non_player_row(row: pd.Series) -> bool:
    number = str(row.get("number", "") or "").strip().lower()
    player_name = str(row.get("player_name", "") or "").strip().lower()
    if not player_name:
        return True
    if number in {"totals", "glossary"}:
        return True
    return False


def _collect_non_player_row_issues(
    dataframe: pd.DataFrame, source_name: str
) -> List[str]:
    issues: List[str] = []
    for _, row in dataframe.iterrows():
        number = _text_or_empty(row.get("number", "")).lower()
        player_name = _text_or_empty(row.get("player_name", ""))
        if number in {"totals", "glossary"}:
            continue
        if player_name:
            continue
        if any(_text_or_empty(row.get(field, "")) for field in ("games", "plate_appearances", "at_bats", "hits")):
            issues.append(f"missing player name in row sourced from {source_name}")
    return issues


def _coerce_player_name(value: object) -> str:
    return display_player_name(_text_or_empty(value))


def _coerce_int(
    value: object, field_name: str, player_name: str, issues: list[str]
) -> int:
    normalized = _normalize_numeric_token(value)
    if normalized is None:
        issues.append(f"missing numeric field '{field_name}' for {player_name}")
        return 0
    try:
        return int(float(normalized))
    except ValueError:
        issues.append(
            f"malformed numeric field '{field_name}' for {player_name}: {value}"
        )
        return 0


def _coerce_float(
    value: object,
    field_name: str,
    player_name: str,
    issues: list[str],
    allow_missing: bool = False,
) -> float:
    normalized = _normalize_numeric_token(value)
    if normalized is None:
        if not allow_missing:
            issues.append(f"missing numeric field '{field_name}' for {player_name}")
        return 0.0
    try:
        return float(normalized)
    except ValueError:
        issues.append(
            f"malformed numeric field '{field_name}' for {player_name}: {value}"
        )
        return 0.0


def _normalize_numeric_token(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text in {"-", "N/A"}:
        return None
    return text


def _text_or_empty(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _ensure_optional_feature_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    for column in ("grounded_into_double_play",):
        if column not in dataframe.columns:
            dataframe[column] = "0"
    return dataframe
