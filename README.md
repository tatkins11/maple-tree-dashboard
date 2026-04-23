# slowpitch_optimizer

`slowpitch_optimizer` is a batting-only slowpitch softball analytics project. This sprint focuses on ingesting GameChanger season batting CSV exports into normalized SQLite tables.

## Modeling Rules

- Stolen bases are not allowed in this league, so steal-related stats are never included in the model
- Primary model inputs must come from directly recorded batting-event outcomes
- Situational and context stats are preserved as secondary features, not core lineup-driving features
- Pitch-count-dependent and subjective contact-quality stats are excluded from the first model
- Hit-by-pitch is not modeled as a slowpitch batting event and is ignored in projections and simulation

## Source Files

- Raw season CSV exports go in `data/raw/season_csv/`
- Approved alias overrides live in `data/processed/player_alias_overrides.csv`
- Player-season projection metadata lives in `data/processed/player_season_metadata.csv`
- Active season roster lives in `data/processed/current_spring_roster.csv`
- Game-day availability lives in `data/processed/game_day_availability.csv`
- Game-day lineup order lives in `data/processed/game_day_lineup.csv`
- League rules live in `data/processed/league_rules.json`
- Team schedule source lives in `data/processed/team_schedule.csv`
- League-wide schedule/results source lives in `data/processed/league_schedule_games.csv`
- Optional standings snapshots live in `data/processed/standings_snapshot.csv`
- The SQLite database lives in `db/`
- `sync_sources.py` is the CLI entry point for ingesting source files

## Setup

```bash
python -m pip install -r requirements.txt
```

## Local Dashboard

The local Streamlit dashboard wraps the existing SQLite database, season stats, projections, roster state, simulator, and optimizer into a day-to-day internal tool.

Launch it locally with:

```bash
streamlit run streamlit_app.py
```

Dashboard pages:

- `Overview`
  team summary, quick season totals, top hitters, and navigation links
- `Current Season Stats`
  sortable hitter table with optional projection columns
- `All-Time / Career Stats`
  career totals, leaders, season filters, and minimum PA filters
- `Advanced Analytics`
  offense-only advanced batting metrics, team-relative analytics, archetypes, and custom RAA / RAR / oWAR
- `Records`
  career and single-season hitter record leaderboards with adjustable minimum PA for rate stats
- `Milestone Tracker`
  career batting milestones showing who is closest to the next notable career mark, with active-roster and category views
- `Schedule`
  Maple Tree schedule plus league-wide scouting, weekly scoreboard views, and optional standings snapshots loaded from local CSV-backed schedule data
- `Lineup Optimizer`
  choose game date, available players, optimizer mode, and run the real optimizer from the dashboard
- `Admin / Data`
  inspect identities, aliases, metadata, active roster, and projection source types

The dashboard is intentionally thin:

- it reads directly from `db/all_seasons_identity.sqlite`
- it uses the existing optimizer and roster-selection logic
- it does not rebuild simulator or projection logic in the UI layer

## Schedule

The `Schedule` dashboard page is a local data-driven team schedule view. It is intentionally built from local CSV imports instead of a live scraper so the dashboard stays reliable and easy to update.

Implementation files:

- schedule import/model logic lives in [src/models/schedule.py](C:\Slowpitch\slowpitch_optimizer\src\models\schedule.py)
- dashboard schedule helpers live in [src/dashboard/data.py](C:\Slowpitch\slowpitch_optimizer\src\dashboard\data.py)
- the Streamlit page lives in [pages/8_Schedule.py](C:\Slowpitch\slowpitch_optimizer\pages\8_Schedule.py)
- the import helper CLI lives in [manage_schedule.py](C:\Slowpitch\slowpitch_optimizer\manage_schedule.py)

The page includes:

- `Team Schedule` mode:
  `Next Game`, `Upcoming Games`, `Full Schedule`, optional standings snapshot display, recorded game results with score and `W/L/T`, and season summary widgets for current record, runs for, runs against, games completed, and games remaining
- `League Scouting` mode:
  `Week Scoreboard`, `Team Scout Card`, opponent schedule/results lookup, and full league schedule filtering for scouting

The schedule schema is future-proofed for:

- results
- runs for / runs against
- win/loss display
- notes
- later links to game stats, lineup, or recap pages
- league-wide scouting across all teams in the division

### Schedule Import Workflow

Import the default local schedule CSV into the main database:

```bash
python manage_schedule.py --db-path "db/all_seasons_identity.sqlite" --mode import
```

Import from explicit custom CSV paths:

```bash
python manage_schedule.py --db-path "db/all_seasons_identity.sqlite" --mode import --schedule-csv "data/processed/team_schedule.csv" --standings-csv "data/processed/standings_snapshot.csv" --league-schedule-csv "data/processed/league_schedule_games.csv"
```

Inspect loaded schedule rows:

```bash
python manage_schedule.py --db-path "db/all_seasons_identity.sqlite" --mode inspect --season "Spring 2026"
```

Record or update a completed game result:

```bash
python manage_schedule.py --db-path "db/all_seasons_identity.sqlite" --mode record-result --game-id "spring-2026-week-1-g1" --runs-for 22 --runs-against 18 --notes "Walk-off in the 7th"
```

### Expected Schedule CSV Columns

- `game_id`
- `season`
- `league_name`
- `division_name`
- `week_label`
- `game_date`
- `game_time`
- `team_name`
- `opponent_name`
- `home_away`
- `location_or_field`
- `status`
- `completed_flag`
- `is_bye`
- `result`
- `runs_for`
- `runs_against`
- `notes`
- `source`

### Expected League Schedule CSV Columns

- `league_game_id`
- `season`
- `league_name`
- `division_name`
- `week_label`
- `game_date`
- `game_time`
- `location_or_field`
- `home_team`
- `away_team`
- `status`
- `completed_flag`
- `home_runs`
- `away_runs`
- `result_summary`
- `notes`
- `source`

### Expected Standings CSV Columns

- `season`
- `league_name`
- `division_name`
- `snapshot_date`
- `team_name`
- `wins`
- `losses`
- `ties`
- `win_pct`
- `games_back`
- `notes`
- `source`

### Schedule Notes

- `schedule_games` is team-centric in v1, so each row represents one game for one team
- `league_schedule_games` is league-wide and stores one row per actual league game for scouting and scoreboard views
- byes are stored explicitly with `is_bye = 1`
- completed games are stored on the same schedule rows with:
  `completed_flag`, `runs_for`, `runs_against`, and derived `result`
- `Next Game` skips bye rows
- completed games now display final scores and `W/L/T` status without changing the page structure
- league scouting does not use fake bye rows; it focuses on real division games only

### Recommended Weekly Schedule Workflow

1. update `data/processed/team_schedule.csv` if Maple Tree-specific notes or manually entered results changed
2. update `data/processed/league_schedule_games.csv` with the latest league scores and scheduled games
3. optionally update `data/processed/standings_snapshot.csv`
4. run:

```bash
python manage_schedule.py --db-path "db/all_seasons_identity.sqlite" --mode import --schedule-csv "data/processed/team_schedule.csv" --standings-csv "data/processed/standings_snapshot.csv" --league-schedule-csv "data/processed/league_schedule_games.csv"
```

## Milestone Tracker

The `Milestone Tracker` dashboard page shows career batting milestones using canonical player identities and verified career totals from `season_batting_stats`.

Milestone logic:

- career totals are aggregated by canonical player identity
- aliases are never counted as separate players
- the page finds the next unpassed milestone for each player and stat
- if a player is exactly on a milestone, the next higher milestone becomes the target
- if a player has cleared every configured milestone for a stat, the page shows `All listed milestones cleared`

Included milestone categories:

- `Games`
- `PA`
- `AB`
- `Hits`
- `Singles`
- `Doubles`
- `Triples`
- `HR`
- `RBI`
- `Runs`
- `Walks`
- `Total Bases`

Milestone ladders are defined in one place in [src/dashboard/data.py](C:\Slowpitch\slowpitch_optimizer\src\dashboard\data.py) under `MILESTONE_LADDERS`.

## Advanced Analytics

The `Advanced Analytics` dashboard page adds a team-specific offense-only analytics layer built from trusted batting outcomes in `season_batting_stats`.

Implementation files:

- analytics formulas and archetype logic live in [src/models/advanced_analytics.py](C:\Slowpitch\slowpitch_optimizer\src\models\advanced_analytics.py)
- dashboard data access for the page lives in [src/dashboard/data.py](C:\Slowpitch\slowpitch_optimizer\src\dashboard\data.py)
- the Streamlit page lives in [pages/7_Advanced_Analytics.py](C:\Slowpitch\slowpitch_optimizer\pages\7_Advanced_Analytics.py)

Included advanced metrics:

- Power / damage:
  `ISO`, `XBH_rate`, `HR_rate`, `TB_per_PA`, `TB_per_AB`, `extra_base_hits`, `extra_base_hit_share_of_hits`
- On-base / survival:
  `on_base_rate`, `non_out_rate`, `walk_rate`, `hbp_rate`, `roe_rate`, `fc_rate`
- Run production / conversion:
  `RBI_per_PA`, `RBI_per_hit`, `runs_per_PA`, `runs_per_on_base_event`, `runs_per_non_hr_on_base_event`, `run_production_index`, `run_conversion_index`
- Context / secondary:
  `BA/RISP`, `2OUTRBI`, `2OUTRBI_rate`, `LOB`, `LOB_per_PA`
- Team-relative:
  `team_relative_OBP`, `team_relative_SLG`, `team_relative_OPS`, `team_relative_TB_per_PA`, `team_relative_HR_rate`
- Value:
  `RAA`, `RAR`, `oWAR`

Excluded metrics and why:

- steal stats and steal-derived value are excluded because this league does not allow steals
- pitch-count, pitch-sequence, and count-leverage metrics are excluded because this is a batting-only project without trusted pitch-by-pitch inputs
- hard-hit, spray, batted-ball-quality, and subjective contact metrics are excluded because they are not trusted enough for this project
- defensive, pitching, and non-batting value components are excluded because this sprint is offense-only

Core formulas:

- `ISO = (TB - H) / AB`
- `XBH_rate = (2B + 3B + HR) / PA`
- `HR_rate = HR / PA`
- `TB_per_PA = TB / PA`
- `TB_per_AB = TB / AB`
- `on_base_rate = (H + BB + HBP + ROE + FC) / PA`
- `non_out_rate = (H + BB + HBP + ROE) / PA`
- `walk_rate = BB / PA`
- `roe_rate = ROE / PA`
- `fc_rate = FC / PA`
- `RBI_per_PA = RBI / PA`
- `runs_per_PA = R / PA`
- `runs_per_on_base_event = R / (H + BB + HBP + ROE + FC)`
- `runs_per_non_hr_on_base_event = R / (1B + 2B + 3B + BB + HBP + ROE + FC)`

Team-relative metrics:

- team-relative metrics are 100-based internal “plus” stats
- `100` means team-average for the selected comparison group
- values above `100` are better than team average
- values below `100` are below team average
- example:
  `team_relative_OBP = 100 * player_OBP / comparison_group_OBP`

Comparison-group definition:

- `Season` mode compares hitters within the selected season
- `Career` mode compares hitters within the selected season filter set
- active-roster filtering is applied before the comparison baseline is built if `Active roster only` is enabled
- minimum-PA filtering affects the displayed table and the comparison baseline for advanced analytics so the page stays internally consistent

Custom value metrics:

- this project uses a simplified internal offense-only run-value model
- internal offensive run rate is:
  `offensive_run_rate = non_out_rate * TB_per_PA`
- estimated offensive runs created are:
  `offensive_runs_created = offensive_run_rate * PA`

Average baseline:

- the average baseline is the weighted comparison-group offensive run rate from the currently selected analytics view

Replacement level:

- replacement level is defined as the `20th percentile` offensive run rate among hitters in the comparison group with at least `20 PA`
- if too few hitters clear that threshold, replacement falls back to `80%` of the comparison-group average offensive run rate
- this is intentionally team-specific and offense-only

Runs-to-wins conversion:

- `runs_per_win = 10.0`
- this is a configurable internal constant, not a league-calibrated universal value

Value formulas:

- `RAA = (player_offensive_run_rate - average_offensive_run_rate) * PA`
- `RAR = (player_offensive_run_rate - replacement_offensive_run_rate) * PA`
- `oWAR = RAR / runs_per_win`

Important value-metric caveat:

- `oWAR` on this page is a team-specific offense-only WAR-style estimate
- it is not full baseball WAR
- it does not include defense, baserunning value beyond batting-event outcomes, pitching, positional adjustment, or league-wide park adjustments

Archetype logic summary:

- archetypes are rule-based and transparent, not model-generated
- current v1 archetypes:
  `Table Setter`, `Balanced Bat`, `Gap Power`, `HR Threat`, `Run Producer`, `Low-Damage OBP Bat`, `Bottom-Order Bat`
- classifications are driven by combinations of:
  `team_relative_OBP`, `team_relative_SLG`, `team_relative_HR_rate`, `XBH_rate`, `non_out_rate`, and `run_production_index`

HBP note:

- `hbp_rate` is included for formula completeness, but HBP is ignored by this project’s slowpitch ingest/modeling rules and therefore will normally display as `0`

## Usage

```bash
python sync_sources.py --season-csv "data/raw/season_csv/Maple Tree Fall 2025 Stats.csv"
```

To use an explicit alias override file:

```bash
python sync_sources.py --season-csv "data/raw/season_csv/Maple Tree Fall 2025 Stats.csv" --alias-overrides "data/processed/player_alias_overrides.csv"
```

Each run writes or updates:

- `players`
- `player_identity`
- `player_aliases`
- `player_metadata`
- `season_rosters`
- `season_batting_stats`
- `hitter_projections` when projection build is run

Each run also writes a simple audit report to `data/audits/` showing players loaded, aliases loaded, season batting rows loaded, validation or parsing uncertainties, and identity items that need review.

## Career Name Matching Workflow

- `player_identity` stores one durable canonical player record per real person
- `player_aliases` stores source-name variants that map back to a single `player_id`
- Approved manual mappings go in [player_alias_overrides.csv](C:\Slowpitch\slowpitch_optimizer\data\processed\player_alias_overrides.csv)
- Matching order is:
  `exact alias match` -> `exact canonical name match` -> `safe normalized match` -> `new identity or manual review`
- Uncertain names are never silently auto-merged
- Ambiguous names are flagged for manual review in the audit report instead of being merged

Use the helper script to inspect the current identity and alias state:

```bash
python review_player_aliases.py --db-path "db/slowpitch_optimizer.sqlite"
```

## Hitter Projections

Projection building lives in [src/models/projections.py](C:\Slowpitch\slowpitch_optimizer\src\models\projections.py) and persists rows to `hitter_projections`.

Build projections for a target current season:

```bash
python build_hitter_projections.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025"
```

Sync or seed player-season metadata:

```bash
python manage_player_season_metadata.py --db-path "db/all_seasons_identity.sqlite"
```

Each projection row contains primary projected per-PA outcomes:

- `p_single`
- `p_double`
- `p_triple`
- `p_home_run`
- `p_walk`
- `projected_strikeout_rate`
- `p_reached_on_error`
- `p_fielder_choice`
- `p_grounded_into_double_play`
- `p_out`

And supporting rates:

- `weighted_prior_plate_appearances`
- `season_count_used`
- `consistency_score`
- `volatility_score`
- `trend_score`
- `projected_on_base_rate`
- `projected_total_base_rate`
- `projected_run_rate`
- `projected_rbi_rate`
- `projected_extra_base_hit_rate`
- `fixed_dhh_flag`
- `baserunning_adjustment`

Secondary context fields are attached but not intended as primary model drivers:

- `secondary_batting_average_risp`
- `secondary_two_out_rbi_rate`
- `secondary_left_on_base_rate`

Projection formulas and assumptions:

- Current season is the selected `projection_season`
- Career baseline is rebuilt season by season instead of pooling all older seasons equally
- If prior seasons exist, the baseline uses weighted prior seasons excluding the selected current season
- If no prior seasons exist, the baseline falls back to the all-time row for that player
- Current season weight uses plate appearances:
  `current_weight = current_pa / (current_pa + 120)`
- Prior seasons use moderate recency weighting:
  most recent prior season `1.00`, next `0.70`, next `0.50`, then floor at `0.35`
- Injury-flagged seasons are downweighted before contributing to the prior baseline
- `player_season_metadata.csv` can override a player-season weight directly with `manual_weight_multiplier`
- Player consistency and season-to-season volatility are computed from core event-rate variation
- More consistent players keep more of their recent form; more volatile players shrink more toward the weighted prior baseline
- A small trend score nudges the projection toward improving or declining recent form without replacing the baseline
- Event probabilities are blended per plate appearance:
  `projected_rate = current_weight * current_rate + (1 - current_weight) * baseline_rate`
- `p_out` is the residual after the modeled event probabilities are summed
- `projected_total_base_rate = p_single + 2*p_double + 3*p_triple + 4*p_home_run`
- `projected_on_base_rate = p_single + p_double + p_triple + p_home_run + p_walk + p_reached_on_error + p_fielder_choice`
- `projected_extra_base_hit_rate = p_double + p_triple + p_home_run`
- `projected_run_rate` and `projected_rbi_rate` are blended from runs per PA and RBI per PA
- Divide-by-zero and missing values resolve safely to `0`

For season stat-line reporting:

- `projected_run_rate` and `projected_rbi_rate` should be treated as projection-layer support fields, not final player season totals
- Preferred player season stat output now comes from repeated simulated full seasons
- Simulated season output tracks `R` and `RBI` directly from simulated scoring events instead of multiplying proxy rates by estimated plate appearances

## Roster And Rules Layer

Season roster management:

- The active spring roster source file is [current_spring_roster.csv](C:\Slowpitch\slowpitch_optimizer\data\processed\current_spring_roster.csv)
- Import or inspect the active roster with:

```bash
python manage_season_roster.py --db-path "db/all_seasons_identity.sqlite" --mode import
python manage_season_roster.py --db-path "db/all_seasons_identity.sqlite" --mode inspect
```

- Roster names must match a known canonical identity or alias
- Unknown names are flagged for review instead of guessed
- The current active roster defaults to season name `Current Spring`

Player metadata is stored in `player_metadata` with:

- `player_id`
- `preferred_display_name`
- `is_fixed_dhh`
- `baserunning_grade`
- `consistency_grade`
- `speed_flag`
- `active_flag`
- `notes`

Project-specific fixed DHH rule:

- The canonical `Tristan` / `Teo` player identity is intentionally the same player
- That canonical identity defaults to `is_fixed_dhh = true`
- The simulator treats that identity as exempt from the non-DHH team 3-HR cap

Export or import editable player metadata with:

```bash
python manage_player_metadata.py --db-path "db/all_seasons_identity.sqlite" --mode export
python manage_player_metadata.py --db-path "db/all_seasons_identity.sqlite" --mode import
```

Game-day availability workflow:

- Edit [game_day_availability.csv](C:\Slowpitch\slowpitch_optimizer\data\processed\game_day_availability.csv)
- Add rows with `game_date`, `player_name`, `available_flag`, and optional notes
- `available_flag` accepts values like `yes`, `true`, or `1`
- If a game date has no explicit availability rows yet, the preview and lineup builder default to the active roster for the selected roster season

League rules workflow:

- Edit [league_rules.json](C:\Slowpitch\slowpitch_optimizer\data\processed\league_rules.json)
- Current rules are:
  `innings_per_game = 7`
  `steals_allowed = false`
  `fixed_dhh_enabled = true`
  `max_home_runs_non_dhh = 3`
  `ignore_slaughter_rule = true`

Home run cap behavior:

- The non-DHH `3 HR` limit is modeled as a team-wide cap, not a per-player cap
- The fixed DHH is fully exempt and DHH home runs do not count toward that team limit
- If the DHH reaches via `walk`, the team gains one temporary non-DHH HR exemption
- That exemption lasts until the first qualifying non-DHH HR uses it or until the DHH bats again, whichever happens first
- After the non-DHH team is at the cap, typical HR hitters are softly suppressed instead of every extra HR becoming a pure out
- Low-HR hitters are effectively unchanged by that post-cap adjustment

Preview a game-day input set:

```bash
python preview_game_day.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-20"
```

Projection selection helper behavior:

- Takes a selected season and a list of available players
- Matches by normalized player name
- Returns the active projection rows for that day
- Includes fixed DHH flag and player metadata fields with the projection output
- Every available player is part of the offensive lineup pool for that day; there is no bench-selection or best-9 logic

## Lineup Simulation

Simulation lineup input building lives in [src/models/lineup.py](C:\Slowpitch\slowpitch_optimizer\src\models\lineup.py) and the inning/game simulator lives in [src/models/simulator.py](C:\Slowpitch\slowpitch_optimizer\src\models\simulator.py).

Manual lineup workflow:

- Edit [game_day_lineup.csv](C:\Slowpitch\slowpitch_optimizer\data\processed\game_day_lineup.csv)
- Add rows with `game_date`, `lineup_spot`, `player_name`, and optional notes
- Lineup spots must be consecutive starting at `1`
- Players must also be marked available in the game-day availability file
- The lineup length must exactly equal the number of available players for that game date
- If `12` players are available, the lineup must contain `12` hitters
- If `10` players are available, the lineup must contain `10` hitters
- There is no bench optimization and no "best 9" selection logic in this project

Simulate one lineup:

```bash
python simulate_lineup.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-20" --simulations 5000 --seed 42
```

The lineup simulation summary now also surfaces:

- `average_team_non_dhh_home_runs`
- `dhh_exemption_usage_rate`

Compare multiple manual lineups:

```bash
python compare_manual_lineups.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-20" --simulations 3000 --seed 42
```

Manual comparison scenarios live in [manual_lineup_scenarios.csv](C:\Slowpitch\slowpitch_optimizer\data\processed\manual_lineup_scenarios.csv).

Optimize a full batting order:

```bash
python optimize_lineup.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-23" --simulations 1500 --seed 42
python optimize_lineup.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-23" --simulations 1500 --seed 42 --mode team_aware
```

Run the definitive exhaustive evaluation workflow:

```bash
python definitive_lineup_eval.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-23"
```

This workflow is separate from the heuristic optimizer and is meant for high-confidence lineup decisions. It:

- exhaustively tests all `120` top-5 permutations with the agreed `6-12` fixed suffix
- takes the winning `1-5` from that phase
- exhaustively tests all `720` arrangements of `6-11` with `Jason` fixed `12th`
- uses repeated simulation blocks, deterministic seeds, survivor filtering, and final high-simulation reruns
- reports expected runs, median runs, standard deviation, standard error, and 95% confidence intervals
- marks the result as a practical near-tie if the final top 2 lineups are within `0.03` expected runs

Project player season stats from repeated simulated full seasons:

```bash
python project_season_stats.py --db-path "db/all_seasons_identity.sqlite" --projection-season "Maple Tree Fall 2025" --game-date "2026-04-23" --season-games 12 --simulated-seasons 5000 --seed 42
```

Simulated season projection behavior:

- Uses the current fixed lineup from the game-day lineup workflow
- Uses the existing hitter projection probabilities unchanged as the talent input model
- Simulates full seasons instead of multiplying rate proxies by guessed playing time
- Tracks player `PA`, `AB`, `1B`, `2B`, `3B`, `HR`, `R`, `RBI`, and `TB` directly from simulated game outcomes
- Derives `AVG`, `SLG`, and `OPS` from simulated totals
- Writes a player season projection CSV to `data/processed/`
- Writes an audit-style report to `data/audits/`
- Reports uncertainty bands with mean, median, 10th percentile, and 90th percentile summaries

Optimizer behavior:

- Uses the existing game-day availability, hitter projections, metadata, league rules, and simulator
- Every available player must appear exactly once in the batting order
- There is no bench selection and no best-9 logic
- Fixed DHH handling is preserved, and the Tristan/Teo canonical identity remains exempt from the non-DHH HR cap
- The search does not brute-force every permutation for larger lineups
- It uses a practical heuristic search:
  `beam-style candidate generation -> DHH slot tests (2, 3, 4, 5) -> local swap improvement -> final Monte Carlo ranking`
- Unconstrained search now explicitly explores multiple plausible leadoff anchors so strong top-of-order constructions are less likely to be missed early
- In the current spring roster, the search guarantees coverage for key leadoff candidates such as `Glove`, `Jj`, and `Kives` when they are available
- Heuristic slot scoring favors underlying bat quality rather than context-dependent run totals:
  leadoff and top-of-order favor `projected_on_base_rate`, walk skill, and out avoidance;
  middle-order favors `projected_total_base_rate`, `projected_extra_base_hit_rate`, and `p_home_run`;
  weaker low-damage bats naturally fall toward the bottom half before the simulator does final ranking
- Strikeout rate is not used to meaningfully shape lineup order in the optimizer heuristic because strikeouts are relatively rare in this slowpitch environment
- Candidate lineups are ranked by simulated expected runs
- Output includes the best lineup, expected runs, median runs, and several alternate full batting orders

Team-aware mode:

- `unconstrained` keeps the general heuristic search
- `team_aware` treats the top of the order as an anchor-group problem
- When the core top-5 pool is available, the optimizer keeps the top 5 within:
  `Glove`, `Tim`, `Tristan`, `Kives`, `Jj`
- It optimizes the order within that top-5 group, then arranges the remaining hitters in the bottom half
- When one or more core hitters are unavailable, the optimizer uses the available subset of the core pool at the top and fills the remaining top-half spots with the best next-tier available bats
- Leadoff candidates in team-aware mode are limited to:
  `Glove`, `Tim`, `Kives`, `Jj`

Simulation-ready lineup row fields:

- `player_id`
- `player_name`
- `lineup_spot`
- `is_fixed_dhh`
- `baserunning_adjustment`
- `p_single`
- `p_double`
- `p_triple`
- `p_home_run`
- `p_walk`
- `p_reached_on_error`
- `p_fielder_choice`
- `p_grounded_into_double_play`
- `projected_strikeout_rate`
- `p_out`
- `projected_on_base_rate`
- `projected_total_base_rate`
- `projected_run_rate`
- `projected_rbi_rate`

Simulation event assumptions:

- Event draw uses:
  `single`, `double`, `triple`, `home_run`, `walk`, `reached_on_error`, `fielder_choice`, `grounded_into_double_play`, `strikeout`, `other_out`
- `projected_strikeout_rate` is its own event bucket
- `p_out` is the residual non-strikeout, non-GIDP out bucket
- `fielder_choice` remains distinct from other outs
- `grounded_into_double_play` remains distinct from other outs
- Non-DHH hitters are capped at `3` home runs per game
- If a non-DHH hitter exceeds the HR cap, extra HR events are treated as `other_out`
- Fixed DHH hitters are exempt from that HR cap

Runner advancement baseline:

- Start each inning with empty bases and `0` outs
- Single: runner on 3rd scores, other runners advance one base
- Double: runners on 2nd and 3rd score, runner on 1st moves to 3rd
- Triple: all runners score, batter to 3rd
- Home run: batter and all runners score
- Walk: forced one-base advance only
- Reached on error: weaker than a clean single; batter reaches 1st and only forced one-base advancement is guaranteed
- Fielder's choice: one out is recorded, batter reaches safely at 1st, and forced-runner advancement is kept conservative
- Grounded into double play: if 1st base is occupied, record two outs and remove the runner on 1st plus the batter; otherwise treat it as a single out
- Strikeout and other_out: one out, runners hold

Known simplifications:

- No steals
- No baserunning aggression model yet
- No park effects, handedness, weather, or opponent defense
- No pitch-by-pitch logic
- No sacrifice-specific runner advancement beyond the imported event rates
- FC and ROE advancement are intentionally conservative placeholders that can be refined later

Future optimizer constraint:

- Any future lineup search must permute the full available-player set for that game day
- Optimizer search must never drop available hitters to create a shorter batting order

## Supported CSV Format

The current importer supports the real GameChanger season export structure found in `data/raw/season_csv/`:

- Row 1 is a section banner row containing labels like `Batting`, `Pitching`, and `Fielding`
- Row 2 contains the actual column headers
- Batting fields are read from the batting block at the front of the file
- Player names are built from the `First` and `Last` columns
- Summary rows like `Totals`, blank rows, and `Glossary` are ignored

The batting columns currently mapped into SQLite are:

- Primary trusted event columns:
  `GP`, `PA`, `AB`, `H`, `1B`, `2B`, `3B`, `HR`, `RBI`, `R`, `BB`, `SO`, `SAC`, `SF`, `ROE`, `FC`, `TB`, `GIDP` when present
- Secondary situational columns:
  `BA/RISP`, `2OUTRBI`, `LOB`

Current assumptions:

- A player may appear with only `First` or only `Last`; whichever exists is treated as the player display name
- Missing or malformed numeric batting fields are flagged in the audit report and loaded as `0`
- Reconciliation checks validate hits, total bases, and plate appearances where applicable
- Canonical player matching uses normalized lowercase names with punctuation removed

## Hitter Feature Table

Feature engineering lives in [src/models/features.py](C:\Slowpitch\slowpitch_optimizer\src\models\features.py).

Primary features:

- Raw event counts: `H`, `1B`, `2B`, `3B`, `HR`, `BB`, `SO`, `SAC`, `SF`, `ROE`, `FC`, `GIDP`, `R`, `RBI`, `TB`
- Derived event rates: hit rate per AB, hit-type rates per PA, walk rate per PA, strikeout rate per PA, ROE rate per PA, FC rate per PA, GIDP rate per PA, run rate per PA, RBI rate per PA, total bases per AB
- On-base and run-production proxies: on-base events, on-base events per PA, extra-base hits, extra-base-hit rate per PA, `AVG`, `OBP`, `SLG`, `OPS`

Secondary features:

- `BA/RISP`
- `2OUTRBI`
- `LOB`

Excluded features:

- Steal-related: `SB`, `SB%`, `CS`, `PIK`
- Pitch-count-dependent: `PS`, `PS/PA`, `2S+3`, `2S+3%`, `6+`, `6+%`, `AB/HR`
- Subjective or non-core contact-quality fields: `QAB`, `QAB%`, `BB/K`, `C%`, `HHB`, `LD%`, `FB%`, `GB%`, `BABIP`
