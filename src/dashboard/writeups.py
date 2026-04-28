from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd


EMPTY_WHO_PLAYED_WELL = "Add standout notes from the box score review once both games are logged."
EMPTY_IMPROVEMENT_NOTE = "Add the biggest adjustment point for next week after reviewing both games."
EMPTY_MILESTONE_RECORD_CONTEXT = "Current milestone and record context will populate here as more verified data is loaded."
LINEUP_STRENGTH_METRICS = (
    ("proj_obp", "Proj OBP"),
    ("proj_run_rate", "Proj run rate"),
    ("proj_rbi_rate", "Proj RBI rate"),
    ("proj_xbh_rate", "Proj XBH rate"),
)
LINEUP_ARCHETYPE_POOLS = {
    "proj_obp": (
        "Ignition switch",
        "Traffic starter",
        "Count-control artist",
        "Inning extender",
        "Line-turn technician",
        "Reset-button bat",
        "Top-card compass",
        "Bottom-card connector",
    ),
    "proj_run_rate": (
        "Run-pressure engine",
        "Basepath agitator",
        "Chaos courier",
        "Scoreboard stirrer",
        "Second-wave spark",
        "Pressure recycler",
        "Dugout irritant",
        "Closing-speed nuisance",
    ),
    "proj_rbi_rate": (
        "Traffic finisher",
        "Crooked-number broker",
        "RBI collector",
        "Damage accountant",
        "Run-cashing foreman",
        "Gap-to-RBI translator",
        "Rally closer",
        "Order-weight carrier",
    ),
    "proj_xbh_rate": (
        "Extra-base menace",
        "Gap-shot dealer",
        "Damage bat",
        "Doubles pressure point",
        "Fence-line problem",
        "Loud-contact specialist",
        "Alley finder",
        "Barrel authority",
    ),
}
LINEUP_SELECTION_THRESHOLD = 0.22


@dataclass(frozen=True)
class ResolvedPostgameGame:
    label: str
    game_id: str
    opponent_name: str
    date_display: str
    time_display: str
    home_away_display: str
    location_or_field: str
    team_score: int
    opponent_score: int
    headline: str
    standout_notes: tuple[str, ...]
    improvement_note: str

    @property
    def result_label(self) -> str:
        if self.team_score > self.opponent_score:
            return "W"
        if self.team_score < self.opponent_score:
            return "L"
        return "T"


def build_pregame_key_lines(
    lineup_rows: list[dict[str, object]],
    milestone_lines: list[str],
    opponent_lines: list[str],
    *,
    week_bundle: dict[str, object] | None = None,
    season_summary: dict[str, object] | None = None,
) -> list[str]:
    players = [str(row.get("player") or "").strip() for row in lineup_rows if str(row.get("player") or "").strip()]
    top_group = ", ".join(players[:3])
    middle_group = ", ".join(players[3:6])
    scouting = _parse_opponent_scouting_snapshot(opponent_lines)
    summary = season_summary or {}
    week_label = str((week_bundle or {}).get("week_label") or "this week")
    wins = int(summary.get("wins", 0) or 0)
    losses = int(summary.get("losses", 0) or 0)
    games_completed = int(summary.get("games_completed", 0) or 0)
    runs_for = int(summary.get("runs_for", 0) or 0)
    runs_against = int(summary.get("runs_against", 0) or 0)

    keys: list[str] = []
    if games_completed == 0:
        keys.append(
            f"{week_label} starts the real scouting file, so Game 1 has to establish the pace Maple Tree wants and Game 2 has to cash in whatever the first opener teaches."
        )
    elif runs_against > runs_for:
        keys.append(
            f"{week_label} needs a cleaner defensive tone than the opener showed: after allowing {runs_against} runs in {games_completed} games, the priority is cutting off free extra bases and forcing Bullseyes to string hits together honestly."
        )
    else:
        keys.append(
            f"Maple Tree is {wins}-{losses} right now, so {week_label} is about stacking another composed night instead of letting the doubleheader drift into scramble-ball."
        )

    if scouting.get("opponent_name"):
        opponent_name = str(scouting["opponent_name"])
        opponent_scored = float(scouting.get("opponent_scored_per_game", 0.0) or 0.0)
        opponent_allowed = float(scouting.get("opponent_allowed_per_game", 0.0) or 0.0)
        maple_tree_scored = float(scouting.get("maple_tree_scored_per_game", 0.0) or 0.0)
        if opponent_scored >= maple_tree_scored + 2.0:
            keys.append(
                f"{opponent_name} is averaging {opponent_scored:.1f} runs a game, so the first job is winning the free-90 battle and keeping one messy inning from becoming their whole night."
            )
        else:
            keys.append(
                f"{opponent_name} has only been allowing {opponent_allowed:.1f} runs a game, so Maple Tree has to build offense through repeated traffic instead of waiting around for one heroic swing."
            )
    elif opponent_lines and opponent_lines[0].startswith("No completed opponent results"):
        keys.append(
            "Treat Game 1 like live reconnaissance: track outfield depth, relay discipline, and who starts donating extra sixty feet, then press those same seams harder in Game 2."
        )

    if top_group and middle_group:
        keys.append(
            f"Let {top_group} light the first fuse, then ask {middle_group} to turn that traffic into full crooked innings so the other dugout has to solve pressure twice, not just once."
        )
    elif top_group:
        keys.append(
            f"Let {top_group} set the tone early and keep Maple Tree hitting with people on base instead of trying to survive on solo noise."
        )
    return keys[:4]


def annotate_pregame_lineup(lineup_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    metric_rank_scores = _build_lineup_metric_rank_scores(lineup_rows)
    usage_counts: dict[str, int] = {}
    annotated_rows: list[dict[str, object]] = []

    for row in lineup_rows:
        annotated_row = dict(row)
        strength = _select_lineup_strength(row, metric_rank_scores, usage_counts)
        usage_counts[strength["strength_key"]] = usage_counts.get(strength["strength_key"], 0) + 1
        annotated_row.update(strength)
        annotated_rows.append(annotated_row)
    return annotated_rows


def build_pregame_overview_insight_lines(
    lineup_rows: list[dict[str, object]],
    *,
    projected_runs_per_game: float | None = None,
    lineup_season_summary: dict[str, object] | None = None,
    lineup_descriptor: str = "recommended order",
) -> list[str]:
    ordered_rows = sorted(
        [dict(row) for row in lineup_rows if str(row.get("player") or "").strip()],
        key=lambda row: int(row.get("spot", 0) or 0),
    )
    lines: list[str] = []

    if projected_runs_per_game is not None and projected_runs_per_game > 0:
        lines.append(
            f"Projection snapshot: the {lineup_descriptor} simulates to {float(projected_runs_per_game):.1f} runs per game."
        )

    if lineup_season_summary and int(lineup_season_summary.get("pa", 0) or 0) > 0:
        lines.append(
            "Current season lineup snapshot: tonight's available group is slashing "
            f"{float(lineup_season_summary.get('avg', 0.0) or 0.0):.3f}/"
            f"{float(lineup_season_summary.get('obp', 0.0) or 0.0):.3f}/"
            f"{float(lineup_season_summary.get('slg', 0.0) or 0.0):.3f} "
            f"({float(lineup_season_summary.get('ops', 0.0) or 0.0):.3f} OPS) with "
            f"{int(lineup_season_summary.get('runs', 0) or 0)} runs, "
            f"{int(lineup_season_summary.get('home_runs', 0) or 0)} homers, and "
            f"{int(lineup_season_summary.get('rbi', 0) or 0)} RBI across "
            f"{int(lineup_season_summary.get('pa', 0) or 0)} PA."
        )

    pressure_pocket = _build_pressure_pocket_line(ordered_rows)
    if pressure_pocket:
        lines.append(pressure_pocket)

    return lines


def build_pregame_markdown(
    *,
    season: str,
    week_bundle: dict[str, object],
    season_summary: dict[str, object],
    lineup_rows: list[dict[str, object]],
    milestone_lines: list[str],
    opponent_lines: list[str],
    key_lines: list[str],
    overview_insight_lines: list[str] | None = None,
) -> str:
    week_label = str(week_bundle.get("week_label") or "Selected Week")
    games = week_bundle.get("non_bye_games")
    if not isinstance(games, pd.DataFrame) or games.empty:
        return f"# {week_label} Pregame Write-Up\n\nNo games are loaded for the selected week."
    annotated_lineup_rows = annotate_pregame_lineup(lineup_rows)

    opponents = [str(name) for name in week_bundle.get("opponent_names", []) if str(name).strip()]
    opponent_text = opponents[0] if len(opponents) == 1 else ", ".join(opponents)
    first_game = games.iloc[0]
    time_values = [str(value).strip() for value in games["time_display"].fillna("").tolist() if str(value).strip()]
    field_values = sorted(
        {
            str(value).strip()
            for value in games["location_or_field"].fillna("").tolist()
            if str(value).strip()
        }
    )

    lines = [
        f"# {week_label} Pregame Write-Up",
        "",
        "## Week Overview",
        f"- Maple Tree heads into {week_label} for a doubleheader vs {opponent_text}.",
        f"- Date: {first_game['date_display']}. Game times: {', '.join(time_values) if time_values else 'TBD'}.",
        f"- Field: {', '.join(field_values) if field_values else 'TBD'}.",
        *[f"- {line}" for line in (overview_insight_lines or []) if str(line).strip()],
        f"- {_build_pregame_context_line(season_summary)}",
        "",
        "## Tonight's Lineup",
    ]

    if annotated_lineup_rows:
        for row in annotated_lineup_rows:
            player_name = str(row.get("player", "")).strip()
            lineup_note = str(row.get("lineup_note") or "").strip()
            note_suffix = f" ({lineup_note})" if lineup_note else ""
            strength_note = str(row.get("strength_note") or "Projection snapshot pending").strip()
            season_note = _build_lineup_season_metrics_note(row)
            line = f"{int(row.get('spot', 0))}. {player_name}{note_suffix} - {strength_note}"
            if season_note:
                line += f" Current season: {season_note}"
            lines.append(line)
    else:
        lines.append("No optimizer lineup is available yet for the selected player pool.")

    lines.extend(["", "## Milestone Watch"])
    if milestone_lines:
        lines.extend(f"- {line}" for line in milestone_lines)
    else:
        lines.append("- No active-roster milestones are inside the immediate watch window right now.")

    lines.extend(["", "## Opponent Scouting"])
    if opponent_lines:
        lines.extend(f"- {line}" for line in opponent_lines)
    else:
        lines.append("- No opponent scouting notes are loaded yet.")

    lines.extend(["", "## Keys for the Night"])
    if key_lines:
        lines.extend(f"- {line}" for line in key_lines)
    else:
        lines.append("- Stay clean defensively, pressure the bases early, and make the opponent play uphill.")

    manager_note = _build_manager_corner(
        lineup_rows=lineup_rows,
        opponent_lines=opponent_lines,
        opponent_text=opponent_text,
        week_bundle=week_bundle,
        season_summary=season_summary,
    )
    lines.extend(["", "## Manager's Corner", manager_note])

    return "\n".join(lines).strip() + "\n"


def resolve_postgame_games(
    games: pd.DataFrame,
    manual_inputs: dict[str, dict[str, str]],
) -> tuple[list[ResolvedPostgameGame], list[str]]:
    if games.empty:
        return [], ["No games are loaded for the selected week."]

    ordered_games = games.sort_values(["game_datetime", "week_label", "game_id"]).reset_index(drop=True)
    resolved_games: list[ResolvedPostgameGame] = []
    errors: list[str] = []

    for index, (_, row) in enumerate(ordered_games.iterrows(), start=1):
        game_id = str(row.get("game_id") or f"game-{index}")
        manual = manual_inputs.get(game_id, {})
        team_score_text = str(manual.get("team_score") or "").strip()
        opponent_score_text = str(manual.get("opponent_score") or "").strip()

        if not team_score_text and not pd.isna(row.get("runs_for")):
            team_score_text = str(int(row["runs_for"]))
        if not opponent_score_text and not pd.isna(row.get("runs_against")):
            opponent_score_text = str(int(row["runs_against"]))

        if not team_score_text or not opponent_score_text:
            errors.append(f"Game {index} needs both final score fields before the recap can be generated.")
            continue

        try:
            team_score = _parse_score(team_score_text)
            opponent_score = _parse_score(opponent_score_text)
        except ValueError:
            errors.append(f"Game {index} has an invalid score entry. Use whole numbers only.")
            continue

        standout_notes = tuple(
            note
            for note in (
                str(manual.get("standout_1") or "").strip(),
                str(manual.get("standout_2") or "").strip(),
            )
            if note
        )
        resolved_games.append(
            ResolvedPostgameGame(
                label=f"Game {index}",
                game_id=game_id,
                opponent_name=str(row.get("opponent_display") or row.get("opponent_name") or "Opponent"),
                date_display=str(row.get("date_display") or row.get("game_date") or ""),
                time_display=str(row.get("time_display") or row.get("game_time") or ""),
                home_away_display=str(row.get("home_away_display") or ""),
                location_or_field=str(row.get("location_or_field") or ""),
                team_score=team_score,
                opponent_score=opponent_score,
                headline=str(manual.get("headline") or "").strip(),
                standout_notes=standout_notes,
                improvement_note=str(manual.get("improvement") or "").strip(),
            )
        )

    return resolved_games, errors


def build_postgame_markdown(
    *,
    season: str,
    week_bundle: dict[str, object],
    resolved_games: list[ResolvedPostgameGame],
    weekly_summary_note: str,
    week_mvp: str,
    context_lines: list[str],
) -> str:
    week_label = str(week_bundle.get("week_label") or "Selected Week")
    title = f"# {week_label} Postgame Recap"

    weekly_summary_line = _build_weekly_summary_line(week_label, resolved_games)
    who_played_well = _collect_standout_lines(resolved_games)
    improvement_lines = _collect_improvement_lines(resolved_games)

    lines = [
        title,
        "",
        "## Weekly Result Summary",
        f"- {weekly_summary_line}",
    ]
    if weekly_summary_note.strip():
        lines.append(f"- Overall note: {weekly_summary_note.strip()}")

    for game in resolved_games:
        lines.extend(
            [
                "",
                f"## {game.label} Recap",
                (
                    f"- Result: Maple Tree {game.team_score}, {game.opponent_name} {game.opponent_score} "
                    f"({game.result_label})."
                ),
                (
                    f"- Setting: {game.date_display}"
                    f"{f' at {game.time_display}' if game.time_display else ''}"
                    f"{f' | {game.home_away_display}' if game.home_away_display else ''}"
                    f"{f' | {game.location_or_field}' if game.location_or_field else ''}."
                ),
            ]
        )
        if game.headline:
            lines.append(f"- Turning point: {game.headline}")
        if game.standout_notes:
            lines.append(f"- Standout notes: {'; '.join(game.standout_notes)}")
        if game.improvement_note:
            lines.append(f"- Next adjustment: {game.improvement_note}")

    lines.extend(["", "## Who Played Well"])
    if who_played_well:
        lines.extend(f"- {line}" for line in who_played_well)
    else:
        lines.append(f"- {EMPTY_WHO_PLAYED_WELL}")

    lines.extend(["", "## Week MVP"])
    lines.append(f"- {week_mvp.strip() if week_mvp.strip() else 'Select the weekly MVP once the box scores are reviewed.'}")

    lines.extend(["", "## Where to Improve Next Week"])
    if improvement_lines:
        lines.extend(f"- {line}" for line in improvement_lines)
    else:
        lines.append(f"- {EMPTY_IMPROVEMENT_NOTE}")

    lines.extend(["", "## Milestone/Record Context"])
    if context_lines:
        lines.extend(f"- {line}" for line in context_lines)
    else:
        lines.append(f"- {EMPTY_MILESTONE_RECORD_CONTEXT}")

    return "\n".join(lines).strip() + "\n"


def suggest_markdown_filename(*, season: str, week_label: str, phase: str) -> str:
    season_slug = _slugify(season)
    week_slug = _slugify(week_label)
    phase_slug = _slugify(phase)
    return f"{season_slug}-{week_slug}-{phase_slug}.md"


def _parse_score(value: str) -> int:
    score = int(value)
    if score < 0:
        raise ValueError("score must be non-negative")
    return score


def _build_weekly_summary_line(week_label: str, resolved_games: list[ResolvedPostgameGame]) -> str:
    wins = sum(1 for game in resolved_games if game.result_label == "W")
    losses = sum(1 for game in resolved_games if game.result_label == "L")
    total_for = sum(game.team_score for game in resolved_games)
    total_against = sum(game.opponent_score for game in resolved_games)
    return (
        f"Maple Tree went {wins}-{losses} in {week_label}, scoring {total_for} runs "
        f"and allowing {total_against} across the doubleheader."
    )


def _collect_standout_lines(resolved_games: list[ResolvedPostgameGame]) -> list[str]:
    standout_lines: list[str] = []
    for game in resolved_games:
        if game.standout_notes:
            standout_lines.append(f"{game.label}: {'; '.join(game.standout_notes)}")
    return standout_lines


def _collect_improvement_lines(resolved_games: list[ResolvedPostgameGame]) -> list[str]:
    lines: list[str] = []
    for game in resolved_games:
        if game.improvement_note:
            lines.append(f"{game.label}: {game.improvement_note}")
    return lines


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower())
    return normalized.strip("-") or "writeup"


def _build_pregame_context_line(season_summary: dict[str, object]) -> str:
    record = str(season_summary.get("record") or "0-0")
    games_completed = int(season_summary.get("games_completed", 0) or 0)
    runs_for = int(season_summary.get("runs_for", 0) or 0)
    runs_against = int(season_summary.get("runs_against", 0) or 0)
    if games_completed == 0 and runs_for == 0 and runs_against == 0:
        return "Opening night means the standings are clean, the scouting report is thin, and every slowpitch lineup card still looks unbeatable on paper."
    return f"Current team context: {record} with {runs_for} runs scored and {runs_against} allowed."


def _build_lineup_season_metrics_note(row: dict[str, object]) -> str:
    season_pa = int(row.get("season_pa", 0) or 0)
    if season_pa <= 0:
        return ""
    return (
        f"{float(row.get('season_avg', 0.0) or 0.0):.3f}/"
        f"{float(row.get('season_obp', 0.0) or 0.0):.3f}/"
        f"{float(row.get('season_slg', 0.0) or 0.0):.3f} "
        f"({float(row.get('season_ops', 0.0) or 0.0):.3f} OPS) in {season_pa} PA, "
        f"{int(row.get('season_r', 0) or 0)} R, {int(row.get('season_rbi', 0) or 0)} RBI."
    )


def _build_manager_corner(
    *,
    lineup_rows: list[dict[str, object]],
    opponent_lines: list[str],
    opponent_text: str,
    week_bundle: dict[str, object] | None = None,
    season_summary: dict[str, object] | None = None,
) -> str:
    players = [str(row.get("player") or "").strip() for row in lineup_rows if str(row.get("player") or "").strip()]
    top_group = ", ".join(players[:3]) if players else "the top of the order"
    middle_group = ", ".join(players[3:6]) if len(players) >= 6 else "the middle of the order"
    bottom_group = ", ".join(players[6:9]) if len(players) >= 9 else "the rest of the lineup"
    week_label = str((week_bundle or {}).get("week_label") or "this week")
    summary = season_summary or {}
    wins = int(summary.get("wins", 0) or 0)
    losses = int(summary.get("losses", 0) or 0)
    games_completed = int(summary.get("games_completed", 0) or 0)
    runs_for = int(summary.get("runs_for", 0) or 0)
    runs_against = int(summary.get("runs_against", 0) or 0)
    scouting = _parse_opponent_scouting_snapshot(opponent_lines)

    if games_completed == 0:
        lead_sentence = (
            f"{week_label} is the clean slate version of slowpitch: no standings scar tissue yet, just the job of setting Maple Tree's pace before the night gets loud."
        )
    elif wins < losses:
        lead_sentence = (
            f"{week_label} is the first real answer-back night for Maple Tree. A {wins}-{losses} start and {runs_against} runs allowed through {games_completed} games mean the group does not need panic; it needs sharper edges and fewer free gifts."
        )
    elif wins > losses:
        lead_sentence = (
            f"{week_label} is a chance to press the standings advantage. Maple Tree has opened {wins}-{losses}, and the next step is making the opponent feel that pressure from the first inning instead of waiting for the game to ask permission."
        )
    else:
        lead_sentence = (
            f"{week_label} is a pivot point night. Maple Tree's {wins}-{losses} start has been noisy enough to show the upside and messy enough to show exactly where the next level of clean softball has to come from."
        )

    if scouting.get("opponent_name"):
        opponent_name = str(scouting["opponent_name"])
        opponent_scored = float(scouting.get("opponent_scored_per_game", 0.0) or 0.0)
        opponent_allowed = float(scouting.get("opponent_allowed_per_game", 0.0) or 0.0)
        maple_tree_scored = float(scouting.get("maple_tree_scored_per_game", 0.0) or 0.0)
        maple_tree_allowed = float(scouting.get("maple_tree_allowed_per_game", 0.0) or 0.0)
        scouting_sentence = (
            f"{opponent_name} is scoring {opponent_scored:.1f} a game and allowing {opponent_allowed:.1f}, while Maple Tree is sitting at {maple_tree_scored:.1f} scored and {maple_tree_allowed:.1f} allowed. That says the doubleheader will be decided less by raw talent than by who controls the free runners, the extra ninety feet, and the one inning where routine play starts to wobble."
        )
    elif opponent_lines and opponent_lines[0].startswith("No completed opponent results"):
        scouting_sentence = (
            f"{opponent_text} is still more rumor than report, so Game 1 has to function as live reconnaissance and Game 2 has to look like the staff already knew the answers."
        )
    else:
        scouting_sentence = (
            "The opponent has enough recent data loaded that Maple Tree should know where the pressure points are before the first pitch of the night."
        )

    return (
        f"{lead_sentence} "
        f"The formal assignment is to let {top_group} establish order at the top, let {middle_group} cash in traffic before the defense can recover, and have {bottom_group} keep the line moving long enough for the machine to circle back around. "
        f"Maple Tree does not need heroic improvisation. It needs disciplined swings at hittable pitches, aggressive but adult baserunning, and routine defensive plays handled with the kind of boring competence that keeps a scorebook from becoming modern art.\n\n"
        f"{scouting_sentence} That means watching who panics on throws, which outfielders turn singles into doubles by accident, and whether the opponent treats a little pressure like a nuisance or a constitutional crisis. "
        f"If Maple Tree avoids giveaway innings and forces every out to be earned honestly, the order has enough thump to turn the doubleheader into a very serious administrative problem for the other dugout."
    )


def _parse_opponent_scouting_snapshot(opponent_lines: list[str]) -> dict[str, object]:
    if not opponent_lines:
        return {}
    first_line = str(opponent_lines[0] or "").strip()
    if not first_line or first_line.startswith("No completed opponent results"):
        return {}

    result: dict[str, object] = {}
    name_match = re.match(r"^(?P<name>[^:]+):", first_line)
    if name_match:
        result["opponent_name"] = name_match.group("name").strip()

    opponent_rate_match = re.search(
        r"scores (?P<scored>\d+(?:\.\d+)?)/game and allows (?P<allowed>\d+(?:\.\d+)?)/game",
        first_line,
    )
    if opponent_rate_match:
        result["opponent_scored_per_game"] = float(opponent_rate_match.group("scored"))
        result["opponent_allowed_per_game"] = float(opponent_rate_match.group("allowed"))

    maple_tree_rate_match = re.search(
        r"Maple Tree scores (?P<scored>\d+(?:\.\d+)?)/game and allows (?P<allowed>\d+(?:\.\d+)?)/game",
        first_line,
    )
    if maple_tree_rate_match:
        result["maple_tree_scored_per_game"] = float(maple_tree_rate_match.group("scored"))
        result["maple_tree_allowed_per_game"] = float(maple_tree_rate_match.group("allowed"))
    return result


def _select_lineup_strength(
    row: dict[str, object],
    metric_rank_scores: dict[str, dict[str, float]],
    usage_counts: dict[str, int],
) -> dict[str, object]:
    candidates: list[dict[str, object]] = []
    player_name = str(row.get("player") or "").strip()
    spot = int(row.get("spot", 0) or 0)

    for metric_key, stat_label in LINEUP_STRENGTH_METRICS:
        value = _coerce_rate(row.get(metric_key))
        rank_score = metric_rank_scores.get(metric_key, {}).get(player_name, 0.0)
        variety_penalty = usage_counts.get(metric_key, 0) * 0.02
        candidates.append(
            {
                "strength_key": metric_key,
                "strength_stat_label": stat_label,
                "strength_value": value,
                "score": rank_score - variety_penalty,
            }
        )

    shortlisted = _shortlist_lineup_metric_candidates(candidates)
    chosen = _choose_preferred_metric_for_spot(shortlisted, spot)
    archetype_label = _next_archetype_label(str(chosen["strength_key"]), usage_counts)
    strength_value = float(chosen["strength_value"])
    strength_note = (
        "Projection snapshot pending"
        if strength_value <= 0
        else f"{archetype_label} ({chosen['strength_stat_label']} {strength_value:.3f})"
    )
    return {
        "strength_key": str(chosen["strength_key"]),
        "strength_label": archetype_label,
        "strength_stat_label": str(chosen["strength_stat_label"]),
        "strength_value": strength_value,
        "strength_note": strength_note,
    }


def _coerce_rate(value: object) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _build_pressure_pocket_line(lineup_rows: list[dict[str, object]]) -> str:
    if len(lineup_rows) < 3:
        return ""

    best_window: list[dict[str, object]] | None = None
    best_score = float("-inf")
    for start in range(len(lineup_rows) - 2):
        window = lineup_rows[start : start + 3]
        average_rbi = sum(_coerce_rate(row.get("proj_rbi_rate")) for row in window) / len(window)
        average_xbh = sum(_coerce_rate(row.get("proj_xbh_rate")) for row in window) / len(window)
        score = average_rbi + average_xbh
        if score > best_score:
            best_window = window
            best_score = score

    if not best_window:
        return ""

    names = ", ".join(str(row.get("player") or "").strip() for row in best_window if str(row.get("player") or "").strip())
    if not names:
        return ""

    average_rbi = sum(_coerce_rate(row.get("proj_rbi_rate")) for row in best_window) / len(best_window)
    average_xbh = sum(_coerce_rate(row.get("proj_xbh_rate")) for row in best_window) / len(best_window)
    return (
        f"Biggest lineup edge: {names} form the heaviest pressure pocket with average projected RBI rate {average_rbi:.3f} "
        f"and XBH rate {average_xbh:.3f}."
    )


def _build_lineup_metric_rank_scores(lineup_rows: list[dict[str, object]]) -> dict[str, dict[str, float]]:
    if not lineup_rows:
        return {}

    dataframe = pd.DataFrame(lineup_rows)
    if dataframe.empty or "player" not in dataframe.columns:
        return {}

    rank_scores: dict[str, dict[str, float]] = {}
    for metric_key, _ in LINEUP_STRENGTH_METRICS:
        if metric_key not in dataframe.columns:
            rank_scores[metric_key] = {}
            continue
        metric_values = pd.to_numeric(dataframe[metric_key], errors="coerce").fillna(0.0)
        ranked = metric_values.rank(method="average", pct=True)
        rank_scores[metric_key] = {
            str(player_name): float(rank_score)
            for player_name, rank_score in zip(dataframe["player"].tolist(), ranked.tolist(), strict=False)
        }
    return rank_scores


def _shortlist_lineup_metric_candidates(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    if not candidates:
        return []
    best_score = max(float(candidate["score"]) for candidate in candidates)
    return [
        candidate
        for candidate in candidates
        if float(candidate["score"]) >= best_score - LINEUP_SELECTION_THRESHOLD
    ]


def _choose_preferred_metric_for_spot(
    candidates: list[dict[str, object]],
    spot: int,
) -> dict[str, object]:
    if not candidates:
        return {
            "strength_key": "proj_obp",
            "strength_stat_label": "Proj OBP",
            "strength_value": 0.0,
            "score": 0.0,
        }

    candidates_by_key = {
        str(candidate["strength_key"]): candidate
        for candidate in candidates
    }
    for metric_key in _metric_preference_order_for_spot(spot):
        if metric_key in candidates_by_key:
            return candidates_by_key[metric_key]
    return max(candidates, key=lambda candidate: (float(candidate["score"]), float(candidate["strength_value"])))


def _metric_preference_order_for_spot(spot: int) -> tuple[str, ...]:
    if spot <= 1:
        return ("proj_obp", "proj_run_rate", "proj_xbh_rate", "proj_rbi_rate")
    if spot == 2:
        return ("proj_run_rate", "proj_obp", "proj_xbh_rate", "proj_rbi_rate")
    if spot == 3:
        return ("proj_rbi_rate", "proj_xbh_rate", "proj_obp", "proj_run_rate")
    if spot == 4:
        return ("proj_xbh_rate", "proj_rbi_rate", "proj_obp", "proj_run_rate")
    if spot == 5:
        return ("proj_rbi_rate", "proj_obp", "proj_xbh_rate", "proj_run_rate")
    if spot == 6:
        return ("proj_xbh_rate", "proj_obp", "proj_rbi_rate", "proj_run_rate")
    if spot == 7:
        return ("proj_rbi_rate", "proj_xbh_rate", "proj_obp", "proj_run_rate")
    if spot == 8:
        return ("proj_obp", "proj_run_rate", "proj_rbi_rate", "proj_xbh_rate")
    if spot == 9:
        return ("proj_obp", "proj_run_rate", "proj_xbh_rate", "proj_rbi_rate")
    if spot == 10:
        return ("proj_run_rate", "proj_obp", "proj_rbi_rate", "proj_xbh_rate")
    if spot == 11:
        return ("proj_run_rate", "proj_rbi_rate", "proj_obp", "proj_xbh_rate")
    return ("proj_obp", "proj_run_rate", "proj_rbi_rate", "proj_xbh_rate")


def _next_archetype_label(metric_key: str, usage_counts: dict[str, int]) -> str:
    label_pool = LINEUP_ARCHETYPE_POOLS.get(metric_key, ("Projection note",))
    label_index = usage_counts.get(metric_key, 0)
    if label_index < len(label_pool):
        return str(label_pool[label_index])
    return f"{label_pool[-1]} {label_index + 1}"
