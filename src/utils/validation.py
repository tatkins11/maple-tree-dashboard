from __future__ import annotations

from typing import List

from src.models.records import PlayerGameBattingRecord, SeasonBattingStatRecord


def validate_season_record(record: SeasonBattingStatRecord) -> List[str]:
    issues: List[str] = []
    calculated_hits = (
        record.singles + record.doubles + record.triples + record.home_runs
    )
    if calculated_hits != record.hits:
        issues.append(
            f"hits mismatch for {record.player_name}: expected {calculated_hits}, found {record.hits}"
        )

    calculated_total_bases = (
        record.singles
        + (2 * record.doubles)
        + (3 * record.triples)
        + (4 * record.home_runs)
    )
    if calculated_total_bases != record.total_bases:
        issues.append(
            f"total_bases mismatch for {record.player_name}: expected {calculated_total_bases}, found {record.total_bases}"
        )

    minimum_known_pa = (
        record.at_bats
        + record.walks
        + record.sacrifice_hits
        + record.sacrifice_flies
    )
    if minimum_known_pa > record.plate_appearances:
        issues.append(
            "plate_appearances mismatch for "
            f"{record.player_name}: minimum expected {minimum_known_pa}, found {record.plate_appearances}"
        )

    return issues


def validate_player_game_record(record: PlayerGameBattingRecord) -> List[str]:
    issues: List[str] = []
    calculated_at_bats = (
        record.singles
        + record.doubles
        + record.triples
        + record.home_runs
        + record.strikeouts
        + record.fielder_choice
        + record.double_plays
        + record.outs
    )
    if calculated_at_bats != record.at_bats:
        issues.append(
            f"at_bats mismatch for {record.player_name}: expected {calculated_at_bats}, found {record.at_bats}"
        )

    calculated_pa = record.at_bats + record.walks + record.sacrifice_flies
    if calculated_pa != record.plate_appearances:
        issues.append(
            f"plate_appearances mismatch for {record.player_name}: expected {calculated_pa}, found {record.plate_appearances}"
        )

    if record.unclassified_symbols:
        issues.append(
            f"unclassified scorebook symbols for {record.player_name}: {', '.join(record.unclassified_symbols)}"
        )

    return issues


def validate_season_records_dataset(
    records: list[SeasonBattingStatRecord],
) -> List[str]:
    issues: List[str] = []
    canonical_to_names: dict[str, set[str]] = {}

    for record in records:
        if not record.player_name.strip():
            issues.append("missing player name after normalization")
        canonical_to_names.setdefault(record.canonical_name, set()).add(record.player_name)

    for canonical_name, names in canonical_to_names.items():
        if len(names) > 1:
            issues.append(
                "duplicate players caused by naming mismatch for "
                f"{canonical_name}: {', '.join(sorted(names))}"
            )

    return issues
