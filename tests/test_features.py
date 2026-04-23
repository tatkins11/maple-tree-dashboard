from src.models.features import (
    EXCLUDED_FEATURE_FIELDS,
    PRIMARY_FEATURE_FIELDS,
    SECONDARY_FEATURE_FIELDS,
    build_hitter_feature_table,
)
from src.models.records import SeasonBattingStatRecord


def test_build_hitter_feature_table_separates_primary_and_secondary_fields() -> None:
    record = SeasonBattingStatRecord(
        season="Maple Tree Fall 2025",
        player_name="Jane Smith",
        canonical_name="jane smith",
        games=10,
        plate_appearances=20,
        at_bats=18,
        hits=10,
        singles=6,
        doubles=2,
        triples=1,
        home_runs=1,
        walks=1,
        strikeouts=2,
        hit_by_pitch=1,
        sacrifice_hits=1,
        sacrifice_flies=0,
        reached_on_error=1,
        fielder_choice=1,
        grounded_into_double_play=1,
        runs=8,
        rbi=9,
        total_bases=17,
        batting_average=0.556,
        on_base_percentage=0.600,
        slugging_percentage=0.944,
        ops=1.544,
        batting_average_risp=0.500,
        two_out_rbi=3,
        left_on_base=4,
        raw_source_file="Maple Tree Fall 2025 Stats.csv",
    )

    feature_table = build_hitter_feature_table([record])

    assert list(feature_table.columns) == PRIMARY_FEATURE_FIELDS + SECONDARY_FEATURE_FIELDS
    row = feature_table.iloc[0]
    assert row["on_base_events"] == 13
    assert row["on_base_events_per_pa"] == 13 / 20
    assert row["home_run_rate_per_pa"] == 1 / 20
    assert row["two_out_rbi"] == 3
    assert row["left_on_base_per_pa"] == 4 / 20


def test_excluded_feature_list_blocks_steal_and_subjective_metrics() -> None:
    assert "SB" in EXCLUDED_FEATURE_FIELDS
    assert "SB%" in EXCLUDED_FEATURE_FIELDS
    assert "CS" in EXCLUDED_FEATURE_FIELDS
    assert "PIK" in EXCLUDED_FEATURE_FIELDS
    assert "HHB" in EXCLUDED_FEATURE_FIELDS
    assert "PS/PA" in EXCLUDED_FEATURE_FIELDS
