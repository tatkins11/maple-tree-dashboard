from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple

import fitz

from src.models.records import ParsedGame, PlayerGameBattingRecord
from src.utils.names import display_player_name, normalize_player_name
from src.utils.validation import validate_player_game_record


_LINEUP_RE = re.compile(r"^(?P<spot>\d+)\.\s*(?P<name>[^|]+?)\s*\|\s*(?P<outcomes>.+)$")
_DATE_RE = re.compile(r"Game Date:\s*(?P<date>\d{4}-\d{2}-\d{2})")
_OPPONENT_RE = re.compile(r"Opponent:\s*(?P<opponent>.+)")
_SEASON_RE = re.compile(r"Season:\s*(?P<season>.+)")
_NOTES_RE = re.compile(r"Notes:\s*(?P<notes>.+)")

_OUTCOME_MAP = {
    "1b": "singles",
    "single": "singles",
    "2b": "doubles",
    "double": "doubles",
    "3b": "triples",
    "triple": "triples",
    "hr": "home_runs",
    "home run": "home_runs",
    "bb": "walks",
    "walk": "walks",
    "k": "strikeouts",
    "so": "strikeouts",
    "sf": "sacrifice_flies",
    "fc": "fielder_choice",
    "dp": "double_plays",
    "out": "outs",
    "go": "outs",
    "fo": "outs",
    "lo": "outs",
}

_AT_BAT_FIELDS = {
    "singles",
    "doubles",
    "triples",
    "home_runs",
    "strikeouts",
    "fielder_choice",
    "double_plays",
    "outs",
}


def parse_scorebook_pdf(pdf_path: Path) -> tuple[ParsedGame, List[str]]:
    text = extract_pdf_text(pdf_path)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    game_date = _extract_value(lines, _DATE_RE, "date")
    opponent_name = _extract_value(lines, _OPPONENT_RE, "opponent")
    season = _extract_value(lines, _SEASON_RE, "season")
    notes = _extract_optional_value(lines, _NOTES_RE, "notes")

    player_rows: List[PlayerGameBattingRecord] = []
    issues: List[str] = []

    for line in lines:
        match = _LINEUP_RE.match(line)
        if not match:
            continue
        row, row_issues = _parse_lineup_row(
            lineup_spot=int(match.group("spot")),
            player_name=match.group("name").strip(),
            outcomes_text=match.group("outcomes"),
            source_file=pdf_path.name,
        )
        player_rows.append(row)
        issues.extend(row_issues)

    if not player_rows:
        raise ValueError(
            "No supported batting rows were found in the scorebook PDF text."
        )

    parsed_game = ParsedGame(
        game_date=game_date,
        opponent_name=opponent_name,
        source_file=pdf_path.name,
        season=season,
        notes=notes,
        player_rows=player_rows,
    )
    return parsed_game, issues


def extract_pdf_text(pdf_path: Path) -> str:
    document = fitz.open(pdf_path)
    try:
        return "\n".join(page.get_text("text") for page in document)
    finally:
        document.close()


def _parse_lineup_row(
    lineup_spot: int,
    player_name: str,
    outcomes_text: str,
    source_file: str,
) -> Tuple[PlayerGameBattingRecord, List[str]]:
    normalized_name = display_player_name(player_name)
    row = PlayerGameBattingRecord(
        lineup_spot=lineup_spot,
        player_name=normalized_name,
        canonical_name=normalize_player_name(normalized_name),
        raw_scorebook_file=source_file,
    )

    for token in _tokenize_outcomes(outcomes_text):
        mapped_field = _OUTCOME_MAP.get(token.lower())
        if mapped_field is None:
            row.unclassified_symbols.append(token)
            continue
        setattr(row, mapped_field, getattr(row, mapped_field) + 1)

    row.at_bats = sum(getattr(row, field) for field in _AT_BAT_FIELDS)
    row.plate_appearances = row.at_bats + row.walks + row.sacrifice_flies

    issues = validate_player_game_record(row)
    return row, issues


def _tokenize_outcomes(outcomes_text: str) -> List[str]:
    tokens = [part.strip() for part in outcomes_text.split(",")]
    return [token for token in tokens if token]


def _extract_value(lines: List[str], pattern: re.Pattern[str], group: str) -> str:
    value = _extract_optional_value(lines, pattern, group)
    if value is None:
        raise ValueError(f"Expected '{group}' metadata in scorebook PDF text.")
    return value


def _extract_optional_value(
    lines: List[str], pattern: re.Pattern[str], group: str
) -> str | None:
    for line in lines:
        match = pattern.search(line)
        if match:
            return match.group(group).strip()
    return None
