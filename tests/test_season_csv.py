from pathlib import Path

from src.ingest.season_csv import import_season_stats_csv


def test_import_season_stats_csv_reads_real_gamechanger_shape(tmp_path: Path) -> None:
    csv_path = tmp_path / "Maple Tree Fall 2025 Stats.csv"
    csv_path.write_text(
        "\n".join(
            [
                ",,,Batting,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,Pitching",
                "Number,Last,First,GP,PA,AB,AVG,OBP,OPS,SLG,H,1B,2B,3B,HR,RBI,R,BB,SO,K-L,HBP,SAC,SF,ROE,FC,SB,SB%,CS,PIK,QAB,QAB%,PA/BB,BB/K,C%,HHB,LD%,FB%,GB%,BABIP,BA/RISP,LOB,2OUTRBI,XBH,TB,PS,PS/PA,2S+3,2S+3%,6+,6+%,AB/HR,GIDP,GITP,CI,IP",
                ",Smith,Jane,10,20,18,.556,.600,1.544,.944,10,6,2,1,1,9,8,1,2,2,0,0,1,0,0,0,-,0,0,0,0.00,20.0,0.500,85.00,0,0.00,0.00,0.00,.000,.000,0,0,4,17,0,0.00,0,0.00,0,0.00,18.0,0,0,0,0.0",
                "Totals,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,",
                "Glossary,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,",
            ]
        ),
        encoding="utf-8",
    )

    records, issues = import_season_stats_csv(csv_path)

    assert len(records) == 1
    assert issues == []
    assert records[0].season == "Maple Tree Fall 2025"
    assert records[0].player_name == "Jane Smith"
    assert records[0].canonical_name == "jane smith"
    assert records[0].hits == 10
    assert records[0].home_runs == 1
    assert records[0].hit_by_pitch == 0
    assert records[0].sacrifice_hits == 0
    assert records[0].reached_on_error == 0
    assert records[0].fielder_choice == 0
    assert records[0].grounded_into_double_play == 0
    assert records[0].batting_average_risp == 0.0
    assert records[0].two_out_rbi == 0
    assert records[0].left_on_base == 0
    assert records[0].ops == 1.544


def test_import_season_stats_csv_flags_missing_names_and_bad_numbers(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "Broken Team Stats.csv"
    csv_path.write_text(
        "\n".join(
            [
                ",,,Batting,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,Pitching",
                "Number,Last,First,GP,PA,AB,AVG,OBP,OPS,SLG,H,1B,2B,3B,HR,RBI,R,BB,SO,K-L,HBP,SAC,SF,ROE,FC,SB,SB%,CS,PIK,QAB,QAB%,PA/BB,BB/K,C%,HHB,LD%,FB%,GB%,BABIP,BA/RISP,LOB,2OUTRBI,XBH,TB,PS,PS/PA,2S+3,2S+3%,6+,6+%,AB/HR,GIDP,GITP,CI,IP",
                ",,,10,20,18,.556,.600,1.544,.944,10,6,2,1,1,9,8,1,2,2,0,0,1,0,0,0,-,0,0,0,0.00,20.0,0.500,85.00,0,0.00,0.00,0.00,.000,.000,0,0,4,17,0,0.00,0,0.00,0,0.00,18.0,0,0,0,0.0",
                ",Smith,Jane,ten,20,18,.556,.600,1.544,.944,10,6,2,1,1,9,8,1,2,2,0,0,1,0,0,0,-,0,0,0,0.00,20.0,0.500,85.00,0,0.00,0.00,0.00,.000,.000,0,0,4,17,0,0.00,0,0.00,0,0.00,18.0,0,0,0,0.0",
            ]
        ),
        encoding="utf-8",
    )

    records, issues = import_season_stats_csv(csv_path, season="Broken Team")

    assert len(records) == 1
    assert any("missing player name" in issue for issue in issues)
    assert any("malformed numeric field 'games'" in issue for issue in issues)
