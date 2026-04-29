from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class SeasonBattingStatRecord(BaseModel):
    season: str
    player_name: str
    canonical_name: str
    games: int = 0
    plate_appearances: int = 0
    at_bats: int = 0
    hits: int = 0
    singles: int = 0
    doubles: int = 0
    triples: int = 0
    home_runs: int = 0
    walks: int = 0
    strikeouts: int = 0
    hit_by_pitch: int = 0
    sacrifice_hits: int = 0
    sacrifice_flies: int = 0
    reached_on_error: int = 0
    fielder_choice: int = 0
    grounded_into_double_play: int = 0
    runs: int = 0
    rbi: int = 0
    total_bases: int = 0
    batting_average: float = 0.0
    on_base_percentage: float = 0.0
    slugging_percentage: float = 0.0
    ops: float = 0.0
    batting_average_risp: float = 0.0
    two_out_rbi: int = 0
    left_on_base: int = 0
    raw_source_file: str


class PlayerGameBattingRecord(BaseModel):
    lineup_spot: int
    player_name: str
    canonical_name: str
    plate_appearances: int = 0
    at_bats: int = 0
    singles: int = 0
    doubles: int = 0
    triples: int = 0
    home_runs: int = 0
    walks: int = 0
    strikeouts: int = 0
    sacrifice_flies: int = 0
    fielder_choice: int = 0
    double_plays: int = 0
    outs: int = 0
    runs: int = 0
    rbi: int = 0
    raw_scorebook_file: str
    unclassified_symbols: List[str] = Field(default_factory=list)


class ParsedGame(BaseModel):
    team_name: str | None = None
    game_date: str
    game_time: str | None = None
    opponent_name: str
    team_score: int | None = None
    opponent_score: int | None = None
    source_file: str
    season: str
    notes: Optional[str] = None
    player_rows: List[PlayerGameBattingRecord]


class HitterProjectionRecord(BaseModel):
    projection_season: str
    player_id: int
    player_name: str
    canonical_name: str
    projection_source: str = "season_blended"
    current_plate_appearances: int = 0
    career_plate_appearances: int = 0
    baseline_plate_appearances: int = 0
    weighted_prior_plate_appearances: float = 0.0
    season_count_used: int = 0
    current_season_weight: float = 0.0
    consistency_score: float = 0.0
    volatility_score: float = 0.0
    trend_score: float = 0.0
    p_single: float = 0.0
    p_double: float = 0.0
    p_triple: float = 0.0
    p_home_run: float = 0.0
    p_walk: float = 0.0
    projected_strikeout_rate: float = 0.0
    p_hit_by_pitch: float = 0.0
    p_reached_on_error: float = 0.0
    p_fielder_choice: float = 0.0
    p_grounded_into_double_play: float = 0.0
    p_out: float = 0.0
    projected_on_base_rate: float = 0.0
    projected_total_base_rate: float = 0.0
    projected_run_rate: float = 0.0
    projected_rbi_rate: float = 0.0
    projected_extra_base_hit_rate: float = 0.0
    fixed_dhh_flag: int = 0
    baserunning_adjustment: float = 0.0
    secondary_batting_average_risp: float = 0.0
    secondary_two_out_rbi_rate: float = 0.0
    secondary_left_on_base_rate: float = 0.0


class PlayerMetadataRecord(BaseModel):
    player_id: int
    preferred_display_name: str
    is_fixed_dhh: bool = False
    baserunning_grade: str = "C"
    consistency_grade: str = "C"
    speed_flag: bool = False
    active_flag: bool = True
    notes: str | None = None


class LeagueRulesRecord(BaseModel):
    innings_per_game: int = 7
    steals_allowed: bool = False
    fixed_dhh_enabled: bool = True
    max_home_runs_non_dhh: int = 3
    ignore_slaughter_rule: bool = True
