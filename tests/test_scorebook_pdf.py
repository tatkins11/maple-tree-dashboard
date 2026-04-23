from pathlib import Path

import fitz

from src.ingest.scorebook_pdf import parse_scorebook_pdf


def test_parse_scorebook_pdf_supported_layout(tmp_path: Path) -> None:
    pdf_path = tmp_path / "MapleTree_vs_Bullseyes_Oct_15_2025.pdf"
    document = fitz.open()
    page = document.new_page()
    lines = [
        "Game Date: 2025-10-15",
        "Opponent: Bullseyes",
        "Season: Maple Tree Fall 2025",
        "Notes: synthetic test fixture",
        "1. Jane Smith | 1B, BB, HR",
        "2. Bob Jones | K, FC, 2B, X9",
    ]
    page.insert_text((72, 72), "\n".join(lines))
    document.save(pdf_path)
    document.close()

    parsed_game, issues = parse_scorebook_pdf(pdf_path)

    assert parsed_game.game_date == "2025-10-15"
    assert parsed_game.opponent_name == "Bullseyes"
    assert parsed_game.season == "Maple Tree Fall 2025"
    assert len(parsed_game.player_rows) == 2

    jane = parsed_game.player_rows[0]
    assert jane.lineup_spot == 1
    assert jane.player_name == "Jane Smith"
    assert jane.singles == 1
    assert jane.walks == 1
    assert jane.home_runs == 1
    assert jane.plate_appearances == 3

    bob = parsed_game.player_rows[1]
    assert bob.lineup_spot == 2
    assert bob.strikeouts == 1
    assert bob.fielder_choice == 1
    assert bob.doubles == 1
    assert bob.unclassified_symbols == ["X9"]
    assert any("X9" in issue for issue in issues)
