from __future__ import annotations

import argparse
from pathlib import Path

from src.ingest.manual_boxscore import (
    DEFAULT_GAME_BOXSCORE_BATTING_PATH,
    DEFAULT_GAME_BOXSCORE_GAMES_PATH,
    import_manual_boxscore_bundle,
)
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import or inspect manually transcribed game box scores.")
    parser.add_argument(
        "--db-path",
        default="db/all_seasons_identity.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--games-csv",
        default=str(DEFAULT_GAME_BOXSCORE_GAMES_PATH),
        help="CSV file containing one row per game screenshot.",
    )
    parser.add_argument(
        "--batting-csv",
        default=str(DEFAULT_GAME_BOXSCORE_BATTING_PATH),
        help="CSV file containing batting rows keyed to game_key.",
    )
    parser.add_argument(
        "--mode",
        choices=["import", "inspect"],
        required=True,
        help="Import manual box score CSVs or inspect loaded game rows.",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Optional season filter when inspecting loaded games.",
    )
    parser.add_argument(
        "--team-name",
        default=None,
        help="Optional team name filter when inspecting loaded games.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        if args.mode == "import":
            result = import_manual_boxscore_bundle(
                connection,
                games_csv_path=Path(args.games_csv),
                batting_csv_path=Path(args.batting_csv),
            )
            print(f"Imported game rows: {result.games_imported}")
            print(f"Imported batting rows: {result.batting_rows_imported}")
            print(f"Imported schedule rows: {result.schedule_rows_imported}")
            if result.identity_notes:
                print("Identity notes:")
                for note in result.identity_notes:
                    print(f"- {note}")
            if result.uncertainties:
                print("Import issues:")
                for issue in result.uncertainties:
                    print(f"- {issue}")
        else:
            params: list[object] = []
            where_parts: list[str] = []
            if args.season:
                where_parts.append("season = ?")
                params.append(args.season)
            if args.team_name:
                where_parts.append("COALESCE(team_name, '') = ?")
                params.append(args.team_name)
            where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
            rows = connection.execute(
                f"""
                SELECT
                    game_id,
                    season,
                    team_name,
                    game_date,
                    game_time,
                    opponent_name,
                    team_score,
                    opponent_score,
                    source_file
                FROM games
                {where_clause}
                ORDER BY season DESC, game_date, COALESCE(game_time, ''), team_name, opponent_name
                """
                ,
                params,
            ).fetchall()
            print(f"Loaded games: {len(rows)}")
            for row in rows:
                score_text = ""
                if row["team_score"] is not None and row["opponent_score"] is not None:
                    score_text = f" | score={row['team_score']}-{row['opponent_score']}"
                print(
                    f"{row['game_id']} | {row['season']} | {row['team_name'] or ''} vs {row['opponent_name']} | "
                    f"{row['game_date']} {row['game_time'] or ''}{score_text} | source={row['source_file']}"
                )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
