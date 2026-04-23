from __future__ import annotations

import argparse
from pathlib import Path

from src.models.schedule import (
    DEFAULT_LEAGUE_SCHEDULE_PATH,
    DEFAULT_SCHEDULE_PATH,
    DEFAULT_SCHEDULE_TEAM_NAME,
    DEFAULT_STANDINGS_PATH,
    import_schedule_bundle,
    update_game_result,
)
from src.utils.db import connect_db, initialize_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import or inspect team schedule data.")
    parser.add_argument(
        "--db-path",
        default="db/all_seasons_identity.sqlite",
        help="SQLite database path.",
    )
    parser.add_argument(
        "--schedule-csv",
        default=str(DEFAULT_SCHEDULE_PATH),
        help="Local CSV file containing schedule rows.",
    )
    parser.add_argument(
        "--standings-csv",
        default=str(DEFAULT_STANDINGS_PATH),
        help="Optional local CSV file containing standings snapshot rows.",
    )
    parser.add_argument(
        "--league-schedule-csv",
        default=str(DEFAULT_LEAGUE_SCHEDULE_PATH),
        help="Optional local CSV file containing full league schedule/results rows.",
    )
    parser.add_argument(
        "--mode",
        choices=["import", "inspect", "record-result"],
        required=True,
        help="Import local schedule CSVs or inspect the current schedule rows.",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Optional season filter when inspecting schedule rows.",
    )
    parser.add_argument(
        "--team-name",
        default=DEFAULT_SCHEDULE_TEAM_NAME,
        help="Team name filter when inspecting schedule rows.",
    )
    parser.add_argument("--game-id", default=None, help="Schedule game_id to update when recording a result.")
    parser.add_argument("--runs-for", type=int, default=None, help="Maple Tree runs scored.")
    parser.add_argument("--runs-against", type=int, default=None, help="Opponent runs allowed.")
    parser.add_argument("--result", default=None, help="Optional explicit result override: W, L, or T.")
    parser.add_argument("--notes", default=None, help="Optional notes to attach to the game result.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    connection = connect_db(Path(args.db_path))
    try:
        initialize_database(connection)
        if args.mode == "import":
            result = import_schedule_bundle(
                connection=connection,
                schedule_csv_path=Path(args.schedule_csv),
                standings_csv_path=Path(args.standings_csv),
                league_schedule_csv_path=Path(args.league_schedule_csv),
            )
            print(f"Imported schedule rows: {result.games_imported}")
            print(f"Imported standings rows: {result.standings_rows_imported}")
            print(f"Imported league schedule rows: {result.league_games_imported}")
        elif args.mode == "record-result":
            if not args.game_id or args.runs_for is None or args.runs_against is None:
                raise SystemExit("--game-id, --runs-for, and --runs-against are required for --mode record-result")
            updated = update_game_result(
                connection,
                game_id=args.game_id,
                runs_for=args.runs_for,
                runs_against=args.runs_against,
                result=args.result,
                notes=args.notes,
            )
            print(f"Updated games: {updated}")
        else:
            params: list[object] = []
            where_parts = []
            if args.season:
                where_parts.append("season = ?")
                params.append(args.season)
            if args.team_name:
                where_parts.append("team_name = ?")
                params.append(args.team_name)

            where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
            rows = connection.execute(
                f"""
                SELECT
                    game_id,
                    season,
                    week_label,
                    game_date,
                    game_time,
                    team_name,
                    opponent_name,
                    home_away,
                    location_or_field,
                    status,
                    completed_flag,
                    result,
                    runs_for,
                    runs_against,
                    is_bye
                FROM schedule_games
                {where_clause}
                ORDER BY season DESC, game_date, COALESCE(game_time, ''), week_label
                """,
                params,
            ).fetchall()
            print(f"Schedule rows: {len(rows)}")
            for row in rows:
                opponent = row["opponent_name"] or "BYE"
                venue = row["location_or_field"] or ""
                score_text = (
                    ""
                    if row["runs_for"] is None or row["runs_against"] is None
                    else f"{row['runs_for']}-{row['runs_against']}"
                )
                print(
                    f"{row['game_id']} | {row['season']} | {row['week_label'] or ''} | {row['game_date']} {row['game_time'] or ''} | "
                    f"{row['team_name']} vs {opponent} | {row['home_away'] or ''} | {venue} | {row['status']}"
                    f" | completed={row['completed_flag']} | result={row['result'] or ''}"
                    f" | score={score_text}"
                )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
