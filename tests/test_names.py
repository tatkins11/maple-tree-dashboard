from src.utils.names import normalize_player_name


def test_normalize_player_name_collapses_case_spacing_and_punctuation() -> None:
    assert normalize_player_name("  JANE   O'Neil-Smith  ") == "jane oneilsmith"
