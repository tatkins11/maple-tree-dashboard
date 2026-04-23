from __future__ import annotations

import sqlite3
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from typing import Iterable, Optional

from src.dashboard.config import get_database_url, should_use_hosted_database
from src.models.records import (
    HitterProjectionRecord,
    ParsedGame,
    PlayerGameBattingRecord,
    SeasonBattingStatRecord,
)


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def connect_app_db(db_path: Path):
    if should_use_hosted_database():
        return connect_postgres_db(get_database_url())
    connection = connect_db(db_path)
    initialize_database(connection)
    return connection


def connect_postgres_db(database_url: str):
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "Hosted database mode requires psycopg. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    connection = psycopg.connect(_normalize_postgres_url(database_url))
    initialize_postgres_database(connection)
    return PostgresConnectionAdapter(connection, dict_row)


class PostgresConnectionAdapter:
    def __init__(self, connection, dict_row_factory):
        self._connection = connection
        self._dict_row_factory = dict_row_factory

    def execute(self, query: str, params: Iterable[object] | None = None):
        cursor = self._connection.cursor(row_factory=self._dict_row_factory)
        cursor.execute(_translate_sql_placeholders(query), tuple(params or ()))
        return cursor

    def executemany(self, query: str, params_seq: Iterable[Iterable[object]]):
        cursor = self._connection.cursor()
        cursor.executemany(_translate_sql_placeholders(query), [tuple(params) for params in params_seq])
        return cursor

    def executescript(self, script: str) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(script)

    def cursor(self):
        return PostgresCursorAdapter(self._connection.cursor())

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class PostgresCursorAdapter:
    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def description(self):
        return self._cursor.description

    def execute(self, query: str, params: Iterable[object] | None = None):
        self._cursor.execute(_translate_sql_placeholders(query), tuple(params or ()))
        return self

    def executemany(self, query: str, params_seq: Iterable[Iterable[object]]):
        self._cursor.executemany(_translate_sql_placeholders(query), [tuple(params) for params in params_seq])
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self) -> None:
        self._cursor.close()


def _translate_sql_placeholders(query: str) -> str:
    return query.replace("?", "%s")


def _normalize_postgres_url(database_url: str) -> str:
    parts = urlsplit(database_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    if "sslmode" not in query:
        query["sslmode"] = "require"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            canonical_name TEXT NOT NULL UNIQUE,
            active_flag INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS player_identity (
            player_id INTEGER PRIMARY KEY,
            player_name TEXT NOT NULL,
            canonical_name TEXT NOT NULL UNIQUE,
            active_flag INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );

        CREATE TABLE IF NOT EXISTS player_aliases (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            source_name TEXT NOT NULL,
            normalized_source_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_file TEXT,
            match_method TEXT NOT NULL,
            approved_flag INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (player_id) REFERENCES player_identity(player_id),
            UNIQUE (source_name, source_type)
        );

        CREATE TABLE IF NOT EXISTS player_metadata (
            player_id INTEGER PRIMARY KEY,
            preferred_display_name TEXT NOT NULL,
            is_fixed_dhh INTEGER NOT NULL DEFAULT 0,
            baserunning_grade TEXT NOT NULL DEFAULT 'C',
            consistency_grade TEXT NOT NULL DEFAULT 'C',
            speed_flag INTEGER NOT NULL DEFAULT 0,
            active_flag INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            FOREIGN KEY (player_id) REFERENCES player_identity(player_id)
        );

        CREATE TABLE IF NOT EXISTS season_rosters (
            season_name TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            source_name TEXT NOT NULL,
            active_flag INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            PRIMARY KEY (season_name, player_id),
            FOREIGN KEY (player_id) REFERENCES player_identity(player_id)
        );

        CREATE TABLE IF NOT EXISTS player_season_metadata (
            player_id INTEGER NOT NULL,
            season TEXT NOT NULL,
            injury_flag INTEGER NOT NULL DEFAULT 0,
            manual_weight_multiplier REAL,
            notes TEXT,
            PRIMARY KEY (player_id, season),
            FOREIGN KEY (player_id) REFERENCES player_identity(player_id)
        );

        CREATE TABLE IF NOT EXISTS games (
            game_id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            opponent_name TEXT NOT NULL,
            source_file TEXT NOT NULL UNIQUE,
            season TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS season_batting_stats (
            season TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            games INTEGER NOT NULL,
            plate_appearances INTEGER NOT NULL,
            at_bats INTEGER NOT NULL,
            hits INTEGER NOT NULL,
            singles INTEGER NOT NULL,
            doubles INTEGER NOT NULL,
            triples INTEGER NOT NULL,
            home_runs INTEGER NOT NULL,
            walks INTEGER NOT NULL,
            strikeouts INTEGER NOT NULL,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_hits INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL,
            reached_on_error INTEGER NOT NULL DEFAULT 0,
            fielder_choice INTEGER NOT NULL DEFAULT 0,
            grounded_into_double_play INTEGER NOT NULL DEFAULT 0,
            runs INTEGER NOT NULL,
            rbi INTEGER NOT NULL,
            total_bases INTEGER NOT NULL,
            batting_average REAL NOT NULL,
            on_base_percentage REAL NOT NULL,
            slugging_percentage REAL NOT NULL,
            ops REAL NOT NULL,
            batting_average_risp REAL NOT NULL DEFAULT 0,
            two_out_rbi INTEGER NOT NULL DEFAULT 0,
            left_on_base INTEGER NOT NULL DEFAULT 0,
            raw_source_file TEXT NOT NULL,
            PRIMARY KEY (season, player_id, raw_source_file),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );

        CREATE TABLE IF NOT EXISTS player_game_batting (
            game_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            lineup_spot INTEGER NOT NULL,
            plate_appearances INTEGER NOT NULL,
            at_bats INTEGER NOT NULL,
            singles INTEGER NOT NULL,
            doubles INTEGER NOT NULL,
            triples INTEGER NOT NULL,
            home_runs INTEGER NOT NULL,
            walks INTEGER NOT NULL,
            strikeouts INTEGER NOT NULL,
            sacrifice_flies INTEGER NOT NULL,
            fielder_choice INTEGER NOT NULL,
            double_plays INTEGER NOT NULL,
            outs INTEGER NOT NULL,
            raw_scorebook_file TEXT NOT NULL,
            PRIMARY KEY (game_id, player_id),
            FOREIGN KEY (game_id) REFERENCES games(game_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );

        CREATE TABLE IF NOT EXISTS hitter_projections (
            projection_season TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            projection_source TEXT NOT NULL DEFAULT 'season_blended',
            current_plate_appearances INTEGER NOT NULL,
            career_plate_appearances INTEGER NOT NULL,
            baseline_plate_appearances INTEGER NOT NULL,
            weighted_prior_plate_appearances REAL NOT NULL DEFAULT 0,
            season_count_used INTEGER NOT NULL DEFAULT 0,
            current_season_weight REAL NOT NULL,
            consistency_score REAL NOT NULL DEFAULT 0,
            volatility_score REAL NOT NULL DEFAULT 0,
            trend_score REAL NOT NULL DEFAULT 0,
            p_single REAL NOT NULL,
            p_double REAL NOT NULL,
            p_triple REAL NOT NULL,
            p_home_run REAL NOT NULL,
            p_walk REAL NOT NULL,
            projected_strikeout_rate REAL NOT NULL DEFAULT 0,
            p_hit_by_pitch REAL NOT NULL,
            p_reached_on_error REAL NOT NULL,
            p_fielder_choice REAL NOT NULL,
            p_grounded_into_double_play REAL NOT NULL,
            p_out REAL NOT NULL,
            projected_on_base_rate REAL NOT NULL,
            projected_total_base_rate REAL NOT NULL,
            projected_run_rate REAL NOT NULL,
            projected_rbi_rate REAL NOT NULL,
            projected_extra_base_hit_rate REAL NOT NULL,
            fixed_dhh_flag INTEGER NOT NULL DEFAULT 0,
            baserunning_adjustment REAL NOT NULL DEFAULT 0,
            secondary_batting_average_risp REAL NOT NULL DEFAULT 0,
            secondary_two_out_rbi_rate REAL NOT NULL DEFAULT 0,
            secondary_left_on_base_rate REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (projection_season, player_id),
            FOREIGN KEY (player_id) REFERENCES player_identity(player_id)
        );

        CREATE TABLE IF NOT EXISTS schedule_games (
            game_id TEXT PRIMARY KEY,
            season TEXT NOT NULL,
            league_name TEXT,
            division_name TEXT,
            week_label TEXT,
            game_date TEXT NOT NULL,
            game_time TEXT,
            team_name TEXT NOT NULL,
            opponent_name TEXT,
            home_away TEXT,
            location_or_field TEXT,
            status TEXT NOT NULL DEFAULT 'scheduled',
            completed_flag INTEGER NOT NULL DEFAULT 0,
            is_bye INTEGER NOT NULL DEFAULT 0,
            result TEXT,
            runs_for INTEGER,
            runs_against INTEGER,
            notes TEXT,
            source TEXT
        );

        CREATE TABLE IF NOT EXISTS standings_snapshot (
            snapshot_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT NOT NULL,
            league_name TEXT,
            division_name TEXT,
            snapshot_date TEXT NOT NULL,
            team_name TEXT NOT NULL,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            ties INTEGER NOT NULL DEFAULT 0,
            win_pct REAL NOT NULL DEFAULT 0,
            games_back REAL,
            notes TEXT,
            source TEXT
        );

        CREATE TABLE IF NOT EXISTS league_schedule_games (
            league_game_id TEXT PRIMARY KEY,
            season TEXT NOT NULL,
            league_name TEXT,
            division_name TEXT,
            week_label TEXT,
            game_date TEXT NOT NULL,
            game_time TEXT,
            location_or_field TEXT,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            completed_flag INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER,
            away_runs INTEGER,
            result_summary TEXT,
            notes TEXT,
            source TEXT
        );

        CREATE TABLE IF NOT EXISTS writeups (
            writeup_id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT NOT NULL,
            week_label TEXT NOT NULL,
            phase TEXT NOT NULL,
            title TEXT NOT NULL,
            markdown TEXT NOT NULL,
            source TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (season, week_label, phase)
        );
        """
    )
    _ensure_column(connection, "season_batting_stats", "hit_by_pitch", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "sacrifice_hits", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "reached_on_error", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "fielder_choice", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "grounded_into_double_play", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "batting_average_risp", "REAL NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "two_out_rbi", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "season_batting_stats", "left_on_base", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "hitter_projections", "projected_strikeout_rate", "REAL NOT NULL DEFAULT 0")
    _ensure_column(connection, "hitter_projections", "projection_source", "TEXT NOT NULL DEFAULT 'season_blended'")
    _ensure_column(connection, "hitter_projections", "weighted_prior_plate_appearances", "REAL NOT NULL DEFAULT 0")
    _ensure_column(connection, "hitter_projections", "season_count_used", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "hitter_projections", "consistency_score", "REAL NOT NULL DEFAULT 0")
    _ensure_column(connection, "hitter_projections", "volatility_score", "REAL NOT NULL DEFAULT 0")
    _ensure_column(connection, "hitter_projections", "trend_score", "REAL NOT NULL DEFAULT 0")
    _ensure_column(connection, "schedule_games", "completed_flag", "INTEGER NOT NULL DEFAULT 0")
    _backfill_player_identity(connection)
    _backfill_player_metadata(connection)
    connection.commit()


def initialize_postgres_database(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                player_id SERIAL PRIMARY KEY,
                player_name TEXT NOT NULL,
                canonical_name TEXT NOT NULL UNIQUE,
                active_flag INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS player_identity (
                player_id INTEGER PRIMARY KEY REFERENCES players(player_id),
                player_name TEXT NOT NULL,
                canonical_name TEXT NOT NULL UNIQUE,
                active_flag INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS player_aliases (
                alias_id SERIAL PRIMARY KEY,
                player_id INTEGER NOT NULL REFERENCES player_identity(player_id),
                source_name TEXT NOT NULL,
                normalized_source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_file TEXT,
                match_method TEXT NOT NULL,
                approved_flag INTEGER NOT NULL DEFAULT 0,
                UNIQUE (source_name, source_type)
            );

            CREATE TABLE IF NOT EXISTS player_metadata (
                player_id INTEGER PRIMARY KEY REFERENCES player_identity(player_id),
                preferred_display_name TEXT NOT NULL,
                is_fixed_dhh INTEGER NOT NULL DEFAULT 0,
                baserunning_grade TEXT NOT NULL DEFAULT 'C',
                consistency_grade TEXT NOT NULL DEFAULT 'C',
                speed_flag INTEGER NOT NULL DEFAULT 0,
                active_flag INTEGER NOT NULL DEFAULT 1,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS season_rosters (
                season_name TEXT NOT NULL,
                player_id INTEGER NOT NULL REFERENCES player_identity(player_id),
                source_name TEXT NOT NULL,
                active_flag INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                PRIMARY KEY (season_name, player_id)
            );

            CREATE TABLE IF NOT EXISTS player_season_metadata (
                player_id INTEGER NOT NULL REFERENCES player_identity(player_id),
                season TEXT NOT NULL,
                injury_flag INTEGER NOT NULL DEFAULT 0,
                manual_weight_multiplier REAL,
                notes TEXT,
                PRIMARY KEY (player_id, season)
            );

            CREATE TABLE IF NOT EXISTS games (
                game_id SERIAL PRIMARY KEY,
                game_date TEXT NOT NULL,
                opponent_name TEXT NOT NULL,
                source_file TEXT NOT NULL UNIQUE,
                season TEXT NOT NULL,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS season_batting_stats (
                season TEXT NOT NULL,
                player_id INTEGER NOT NULL REFERENCES players(player_id),
                games INTEGER NOT NULL,
                plate_appearances INTEGER NOT NULL,
                at_bats INTEGER NOT NULL,
                hits INTEGER NOT NULL,
                singles INTEGER NOT NULL,
                doubles INTEGER NOT NULL,
                triples INTEGER NOT NULL,
                home_runs INTEGER NOT NULL,
                walks INTEGER NOT NULL,
                strikeouts INTEGER NOT NULL,
                hit_by_pitch INTEGER NOT NULL DEFAULT 0,
                sacrifice_hits INTEGER NOT NULL DEFAULT 0,
                sacrifice_flies INTEGER NOT NULL,
                reached_on_error INTEGER NOT NULL DEFAULT 0,
                fielder_choice INTEGER NOT NULL DEFAULT 0,
                grounded_into_double_play INTEGER NOT NULL DEFAULT 0,
                runs INTEGER NOT NULL,
                rbi INTEGER NOT NULL,
                total_bases INTEGER NOT NULL,
                batting_average REAL NOT NULL,
                on_base_percentage REAL NOT NULL,
                slugging_percentage REAL NOT NULL,
                ops REAL NOT NULL,
                batting_average_risp REAL NOT NULL DEFAULT 0,
                two_out_rbi INTEGER NOT NULL DEFAULT 0,
                left_on_base INTEGER NOT NULL DEFAULT 0,
                raw_source_file TEXT NOT NULL,
                PRIMARY KEY (season, player_id, raw_source_file)
            );

            CREATE TABLE IF NOT EXISTS player_game_batting (
                game_id INTEGER NOT NULL REFERENCES games(game_id),
                player_id INTEGER NOT NULL REFERENCES players(player_id),
                lineup_spot INTEGER NOT NULL,
                plate_appearances INTEGER NOT NULL,
                at_bats INTEGER NOT NULL,
                singles INTEGER NOT NULL,
                doubles INTEGER NOT NULL,
                triples INTEGER NOT NULL,
                home_runs INTEGER NOT NULL,
                walks INTEGER NOT NULL,
                strikeouts INTEGER NOT NULL,
                sacrifice_flies INTEGER NOT NULL,
                fielder_choice INTEGER NOT NULL,
                double_plays INTEGER NOT NULL,
                outs INTEGER NOT NULL,
                raw_scorebook_file TEXT NOT NULL,
                PRIMARY KEY (game_id, player_id)
            );

            CREATE TABLE IF NOT EXISTS hitter_projections (
                projection_season TEXT NOT NULL,
                player_id INTEGER NOT NULL REFERENCES player_identity(player_id),
                projection_source TEXT NOT NULL DEFAULT 'season_blended',
                current_plate_appearances INTEGER NOT NULL,
                career_plate_appearances INTEGER NOT NULL,
                baseline_plate_appearances INTEGER NOT NULL,
                weighted_prior_plate_appearances REAL NOT NULL DEFAULT 0,
                season_count_used INTEGER NOT NULL DEFAULT 0,
                current_season_weight REAL NOT NULL,
                consistency_score REAL NOT NULL DEFAULT 0,
                volatility_score REAL NOT NULL DEFAULT 0,
                trend_score REAL NOT NULL DEFAULT 0,
                p_single REAL NOT NULL,
                p_double REAL NOT NULL,
                p_triple REAL NOT NULL,
                p_home_run REAL NOT NULL,
                p_walk REAL NOT NULL,
                projected_strikeout_rate REAL NOT NULL DEFAULT 0,
                p_hit_by_pitch REAL NOT NULL,
                p_reached_on_error REAL NOT NULL,
                p_fielder_choice REAL NOT NULL,
                p_grounded_into_double_play REAL NOT NULL,
                p_out REAL NOT NULL,
                projected_on_base_rate REAL NOT NULL,
                projected_total_base_rate REAL NOT NULL,
                projected_run_rate REAL NOT NULL,
                projected_rbi_rate REAL NOT NULL,
                projected_extra_base_hit_rate REAL NOT NULL,
                fixed_dhh_flag INTEGER NOT NULL DEFAULT 0,
                baserunning_adjustment REAL NOT NULL DEFAULT 0,
                secondary_batting_average_risp REAL NOT NULL DEFAULT 0,
                secondary_two_out_rbi_rate REAL NOT NULL DEFAULT 0,
                secondary_left_on_base_rate REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (projection_season, player_id)
            );

            CREATE TABLE IF NOT EXISTS schedule_games (
                game_id TEXT PRIMARY KEY,
                season TEXT NOT NULL,
                league_name TEXT,
                division_name TEXT,
                week_label TEXT,
                game_date TEXT NOT NULL,
                game_time TEXT,
                team_name TEXT NOT NULL,
                opponent_name TEXT,
                home_away TEXT,
                location_or_field TEXT,
                status TEXT NOT NULL DEFAULT 'scheduled',
                completed_flag INTEGER NOT NULL DEFAULT 0,
                is_bye INTEGER NOT NULL DEFAULT 0,
                result TEXT,
                runs_for INTEGER,
                runs_against INTEGER,
                notes TEXT,
                source TEXT
            );

            CREATE TABLE IF NOT EXISTS standings_snapshot (
                snapshot_row_id SERIAL PRIMARY KEY,
                season TEXT NOT NULL,
                league_name TEXT,
                division_name TEXT,
                snapshot_date TEXT NOT NULL,
                team_name TEXT NOT NULL,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                ties INTEGER NOT NULL DEFAULT 0,
                win_pct REAL NOT NULL DEFAULT 0,
                games_back REAL,
                notes TEXT,
                source TEXT
            );

            CREATE TABLE IF NOT EXISTS league_schedule_games (
                league_game_id TEXT PRIMARY KEY,
                season TEXT NOT NULL,
                league_name TEXT,
                division_name TEXT,
                week_label TEXT,
                game_date TEXT NOT NULL,
                game_time TEXT,
                location_or_field TEXT,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'scheduled',
                completed_flag INTEGER NOT NULL DEFAULT 0,
                home_runs INTEGER,
                away_runs INTEGER,
                result_summary TEXT,
                notes TEXT,
                source TEXT
            );

            CREATE TABLE IF NOT EXISTS writeups (
                writeup_id SERIAL PRIMARY KEY,
                season TEXT NOT NULL,
                week_label TEXT NOT NULL,
                phase TEXT NOT NULL,
                title TEXT NOT NULL,
                markdown TEXT NOT NULL,
                source TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (season, week_label, phase)
            );
            """
        )
    connection.commit()


def get_or_create_player(
    connection: sqlite3.Connection, player_name: str, canonical_name: str
) -> int:
    existing = connection.execute(
        "SELECT player_id FROM player_identity WHERE canonical_name = ?",
        (canonical_name,),
    ).fetchone()
    if existing:
        connection.execute(
            """
            UPDATE players
            SET player_name = ?, active_flag = 1
            WHERE player_id = ?
            """,
            (player_name, existing["player_id"]),
        )
        connection.execute(
            """
            UPDATE player_identity
            SET player_name = ?, active_flag = 1
            WHERE player_id = ?
            """,
            (player_name, existing["player_id"]),
        )
        return int(existing["player_id"])

    cursor = connection.execute(
        """
        INSERT INTO players (player_name, canonical_name, active_flag)
        VALUES (?, ?, 1)
        """,
        (player_name, canonical_name),
    )
    player_id = int(cursor.lastrowid)
    connection.execute(
        """
        INSERT OR REPLACE INTO player_identity (player_id, player_name, canonical_name, active_flag)
        VALUES (?, ?, ?, 1)
        """,
        (player_id, player_name, canonical_name),
    )
    return player_id


def upsert_game(connection: sqlite3.Connection, game: ParsedGame) -> int:
    existing = connection.execute(
        "SELECT game_id FROM games WHERE source_file = ?",
        (game.source_file,),
    ).fetchone()
    if existing:
        connection.execute(
            """
            UPDATE games
            SET game_date = ?, opponent_name = ?, season = ?, notes = ?
            WHERE game_id = ?
            """,
            (
                game.game_date,
                game.opponent_name,
                game.season,
                game.notes,
                existing["game_id"],
            ),
        )
        return int(existing["game_id"])

    cursor = connection.execute(
        """
        INSERT INTO games (game_date, opponent_name, source_file, season, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (game.game_date, game.opponent_name, game.source_file, game.season, game.notes),
    )
    return int(cursor.lastrowid)


def replace_season_batting_stats(
    connection: sqlite3.Connection,
    records: Iterable[tuple[SeasonBattingStatRecord, int]],
) -> int:
    count = 0
    for record, player_id in records:
        connection.execute(
            """
            INSERT OR REPLACE INTO season_batting_stats (
                season,
                player_id,
                games,
                plate_appearances,
                at_bats,
                hits,
                singles,
                doubles,
                triples,
                home_runs,
                walks,
                strikeouts,
                hit_by_pitch,
                sacrifice_hits,
                sacrifice_flies,
                reached_on_error,
                fielder_choice,
                grounded_into_double_play,
                runs,
                rbi,
                total_bases,
                batting_average,
                on_base_percentage,
                slugging_percentage,
                ops,
                batting_average_risp,
                two_out_rbi,
                left_on_base,
                raw_source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.season,
                player_id,
                record.games,
                record.plate_appearances,
                record.at_bats,
                record.hits,
                record.singles,
                record.doubles,
                record.triples,
                record.home_runs,
                record.walks,
                record.strikeouts,
                record.hit_by_pitch,
                record.sacrifice_hits,
                record.sacrifice_flies,
                record.reached_on_error,
                record.fielder_choice,
                record.grounded_into_double_play,
                record.runs,
                record.rbi,
                record.total_bases,
                record.batting_average,
                record.on_base_percentage,
                record.slugging_percentage,
                record.ops,
                record.batting_average_risp,
                record.two_out_rbi,
                record.left_on_base,
                record.raw_source_file,
            ),
        )
        count += 1
    connection.commit()
    return count


def replace_player_game_batting(
    connection: sqlite3.Connection,
    game_id: int,
    player_rows: Iterable[PlayerGameBattingRecord],
) -> int:
    count = 0
    for row in player_rows:
        player_id = get_or_create_player(connection, row.player_name, row.canonical_name)
        connection.execute(
            """
            INSERT OR REPLACE INTO player_game_batting (
                game_id,
                player_id,
                lineup_spot,
                plate_appearances,
                at_bats,
                singles,
                doubles,
                triples,
                home_runs,
                walks,
                strikeouts,
                sacrifice_flies,
                fielder_choice,
                double_plays,
                outs,
                raw_scorebook_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                game_id,
                player_id,
                row.lineup_spot,
                row.plate_appearances,
                row.at_bats,
                row.singles,
                row.doubles,
                row.triples,
                row.home_runs,
                row.walks,
                row.strikeouts,
                row.sacrifice_flies,
                row.fielder_choice,
                row.double_plays,
                row.outs,
                row.raw_scorebook_file,
            ),
        )
        count += 1
    connection.commit()
    return count


def fetch_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "players_loaded": int(
            connection.execute("SELECT COUNT(*) FROM player_identity").fetchone()[0]
        ),
        "player_aliases_loaded": int(
            connection.execute("SELECT COUNT(*) FROM player_aliases").fetchone()[0]
        ),
        "season_roster_rows_loaded": int(
            connection.execute("SELECT COUNT(*) FROM season_rosters").fetchone()[0]
        ),
        "season_batting_rows_loaded": int(
            connection.execute("SELECT COUNT(*) FROM season_batting_stats").fetchone()[0]
        ),
        "games_loaded": int(connection.execute("SELECT COUNT(*) FROM games").fetchone()[0]),
        "player_game_rows_loaded": int(
            connection.execute("SELECT COUNT(*) FROM player_game_batting").fetchone()[0]
        ),
        "hitter_projections_loaded": int(
            connection.execute("SELECT COUNT(*) FROM hitter_projections").fetchone()[0]
        ),
        "schedule_games_loaded": int(
            connection.execute("SELECT COUNT(*) FROM schedule_games").fetchone()[0]
        ),
        "standings_rows_loaded": int(
            connection.execute("SELECT COUNT(*) FROM standings_snapshot").fetchone()[0]
        ),
        "league_schedule_games_loaded": int(
            connection.execute("SELECT COUNT(*) FROM league_schedule_games").fetchone()[0]
        ),
    }


def replace_hitter_projections(
    connection: sqlite3.Connection,
    projection_season: str,
    projections: Iterable[HitterProjectionRecord],
) -> int:
    connection.execute(
        "DELETE FROM hitter_projections WHERE projection_season = ?",
        (projection_season,),
    )
    count = 0
    for projection in projections:
        connection.execute(
            """
            INSERT INTO hitter_projections (
                projection_season,
                player_id,
                projection_source,
                current_plate_appearances,
                career_plate_appearances,
                baseline_plate_appearances,
                weighted_prior_plate_appearances,
                season_count_used,
                current_season_weight,
                consistency_score,
                volatility_score,
                trend_score,
                p_single,
                p_double,
                p_triple,
                p_home_run,
                p_walk,
                projected_strikeout_rate,
                p_hit_by_pitch,
                p_reached_on_error,
                p_fielder_choice,
                p_grounded_into_double_play,
                p_out,
                projected_on_base_rate,
                projected_total_base_rate,
                projected_run_rate,
                projected_rbi_rate,
                projected_extra_base_hit_rate,
                fixed_dhh_flag,
                baserunning_adjustment,
                secondary_batting_average_risp,
                secondary_two_out_rbi_rate,
                secondary_left_on_base_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                projection.projection_season,
                projection.player_id,
                projection.projection_source,
                projection.current_plate_appearances,
                projection.career_plate_appearances,
                projection.baseline_plate_appearances,
                projection.weighted_prior_plate_appearances,
                projection.season_count_used,
                projection.current_season_weight,
                projection.consistency_score,
                projection.volatility_score,
                projection.trend_score,
                projection.p_single,
                projection.p_double,
                projection.p_triple,
                projection.p_home_run,
                projection.p_walk,
                projection.projected_strikeout_rate,
                projection.p_hit_by_pitch,
                projection.p_reached_on_error,
                projection.p_fielder_choice,
                projection.p_grounded_into_double_play,
                projection.p_out,
                projection.projected_on_base_rate,
                projection.projected_total_base_rate,
                projection.projected_run_rate,
                projection.projected_rbi_rate,
                projection.projected_extra_base_hit_rate,
                projection.fixed_dhh_flag,
                projection.baserunning_adjustment,
                projection.secondary_batting_average_risp,
                projection.secondary_two_out_rbi_rate,
                projection.secondary_left_on_base_rate,
            ),
        )
        count += 1
    connection.commit()
    return count


def _ensure_column(
    connection: sqlite3.Connection, table_name: str, column_name: str, column_sql: str
) -> None:
    existing = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in existing:
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def _backfill_player_identity(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO player_identity (player_id, player_name, canonical_name, active_flag)
        SELECT player_id, player_name, canonical_name, active_flag
        FROM players
        """
    )


def _backfill_player_metadata(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO player_metadata (
            player_id,
            preferred_display_name,
            is_fixed_dhh,
            baserunning_grade,
            consistency_grade,
            speed_flag,
            active_flag,
            notes
        )
        SELECT
            player_id,
            player_name,
            0,
            'C',
            'C',
            0,
            active_flag,
            NULL
        FROM player_identity
        """
    )
    connection.execute(
        """
        UPDATE player_metadata
        SET
            preferred_display_name = 'Tristan',
            is_fixed_dhh = 1,
            notes = CASE
                WHEN notes IS NULL OR notes = '' THEN 'Default fixed DHH canonical identity (Tristan/Teo)'
                WHEN instr(notes, 'Default fixed DHH canonical identity') > 0 THEN notes
                ELSE notes || '; Default fixed DHH canonical identity (Tristan/Teo)'
            END
        WHERE player_id IN (
            SELECT player_id
            FROM player_identity
            WHERE canonical_name = 'tristan'
        )
        """
    )
    connection.execute(
        """
        UPDATE player_metadata
        SET
            preferred_display_name = 'Joey',
            notes = CASE
                WHEN notes IS NULL OR notes = '' THEN 'Preferred display name set to Joey for current spring roster (canonical identity Snaxx)'
                WHEN instr(notes, 'Preferred display name set to Joey for current spring roster') > 0 THEN notes
                ELSE notes || '; Preferred display name set to Joey for current spring roster (canonical identity Snaxx)'
            END
        WHERE player_id IN (
            SELECT player_id
            FROM player_identity
            WHERE canonical_name = 'snaxx'
        )
        """
    )
