from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable


def write_audit_report(
    audit_dir: Path,
    counts: dict[str, int],
    uncertainties: Iterable[str],
    identity_notes: Iterable[str],
) -> Path:
    audit_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = audit_dir / f"audit_{timestamp}.txt"
    uncertainty_list = list(uncertainties)
    identity_note_list = list(identity_notes)
    lines = [
        "slowpitch_optimizer audit report",
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        f"players_loaded: {counts['players_loaded']}",
        f"player_aliases_loaded: {counts['player_aliases_loaded']}",
        f"season_batting_rows_loaded: {counts['season_batting_rows_loaded']}",
        f"games_loaded: {counts['games_loaded']}",
        f"player_game_rows_loaded: {counts['player_game_rows_loaded']}",
        f"rows_with_parsing_uncertainty: {len(uncertainty_list)}",
        f"identity_review_items: {len(identity_note_list)}",
        "",
        "uncertainties:",
    ]
    if uncertainty_list:
        lines.extend(f"- {item}" for item in uncertainty_list)
    else:
        lines.append("- none")

    lines.extend(["", "identity_review:"])
    if identity_note_list:
        lines.extend(f"- {item}" for item in identity_note_list)
    else:
        lines.append("- none")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path
