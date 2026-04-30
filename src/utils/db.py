from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
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
        database_url = get_database_url()
        try:
            _sync_hosted_database_from_repo_sources(database_url)
        except Exception as exc:
            print(f"Hosted source sync skipped: {exc}")
        return connect_postgres_db(database_url)
    connection = connect_db(db_path)
    initialize_database(connection)
    return connection


def connect_postgres_db(database_url: str, *, autocommit: bool = True):
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "Hosted database mode requires psycopg. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    connection = psycopg.connect(_normalize_postgres_url(database_url), autocommit=autocommit)
    initialize_postgres_database(connection)
    if not autocommit:
        connection.commit()
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
            team_name TEXT,
            game_date TEXT NOT NULL,
            game_time TEXT,
            opponent_name TEXT NOT NULL,
            team_score INTEGER,
            opponent_score INTEGER,
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
            runs INTEGER NOT NULL DEFAULT 0,
            rbi INTEGER NOT NULL DEFAULT 0,
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
    _ensure_column(connection, "games", "team_name", "TEXT")
    _ensure_column(connection, "games", "game_time", "TEXT")
    _ensure_column(connection, "games", "team_score", "INTEGER")
    _ensure_column(connection, "games", "opponent_score", "INTEGER")
    _ensure_column(connection, "player_game_batting", "runs", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "player_game_batting", "rbi", "INTEGER NOT NULL DEFAULT 0")
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
            team_name TEXT,
            game_date TEXT NOT NULL,
            game_time TEXT,
            opponent_name TEXT NOT NULL,
            team_score INTEGER,
            opponent_score INTEGER,
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
                runs INTEGER NOT NULL DEFAULT 0,
                rbi INTEGER NOT NULL DEFAULT 0,
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
        cursor.execute("ALTER TABLE player_game_batting ADD COLUMN IF NOT EXISTS runs INTEGER NOT NULL DEFAULT 0;")
        cursor.execute("ALTER TABLE player_game_batting ADD COLUMN IF NOT EXISTS rbi INTEGER NOT NULL DEFAULT 0;")
        cursor.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS team_name TEXT;")
        cursor.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS game_time TEXT;")
        cursor.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS team_score INTEGER;")
        cursor.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS opponent_score INTEGER;")
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
            SET team_name = ?, game_date = ?, game_time = ?, opponent_name = ?, team_score = ?, opponent_score = ?, season = ?, notes = ?
            WHERE game_id = ?
            """,
            (
                game.team_name,
                game.game_date,
                game.game_time,
                game.opponent_name,
                game.team_score,
                game.opponent_score,
                game.season,
                game.notes,
                existing["game_id"],
            ),
        )
        return int(existing["game_id"])

    cursor = connection.execute(
        """
        INSERT INTO games (team_name, game_date, game_time, opponent_name, team_score, opponent_score, source_file, season, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            game.team_name,
            game.game_date,
            game.game_time,
            game.opponent_name,
            game.team_score,
            game.opponent_score,
            game.source_file,
            game.season,
            game.notes,
        ),
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
                runs,
                rbi,
                raw_scorebook_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                row.runs,
                row.rbi,
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


_HOSTED_SOURCE_SYNC_LOCK = threading.Lock()
_HOSTED_SOURCE_SYNC_CACHE: dict[str, str] = {}
_HOSTED_SOURCE_SYNC_KEY = "repo-source-sync-v1"


def _sync_hosted_database_from_repo_sources(database_url: str) -> None:
    if not database_url:
        return

    signature = _build_repo_source_signature()
    if _HOSTED_SOURCE_SYNC_CACHE.get(database_url) == signature:
        return

    with _HOSTED_SOURCE_SYNC_LOCK:
        if _HOSTED_SOURCE_SYNC_CACHE.get(database_url) == signature:
            return

        postgres_connection = connect_postgres_db(database_url)
        try:
            _ensure_source_sync_state_table(postgres_connection)
            current_signature = _read_source_sync_signature(postgres_connection, _HOSTED_SOURCE_SYNC_KEY)
        finally:
            postgres_connection.close()

        if current_signature == signature:
            _HOSTED_SOURCE_SYNC_CACHE[database_url] = signature
            return

        with tempfile.TemporaryDirectory(prefix="hosted-source-sync-") as temp_dir:
            temp_root = Path(temp_dir)
            sqlite_path = temp_root / "dashboard.sqlite"
            _copy_postgres_snapshot_to_sqlite(sqlite_path=sqlite_path, database_url=database_url)
            _apply_repo_source_updates(sqlite_path=sqlite_path, audit_dir=temp_root / "audits")

            from sync_to_supabase import sync_sqlite_to_postgres

            sync_sqlite_to_postgres(
                sqlite_path=sqlite_path,
                database_url=database_url,
                replace=True,
            )

        postgres_connection = connect_postgres_db(database_url)
        try:
            _ensure_source_sync_state_table(postgres_connection)
            postgres_connection.execute(
                """
                INSERT INTO source_sync_state (sync_key, signature)
                VALUES (?, ?)
                ON CONFLICT(sync_key) DO UPDATE SET
                    signature = excluded.signature,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (_HOSTED_SOURCE_SYNC_KEY, signature),
            )
            postgres_connection.commit()
        finally:
            postgres_connection.close()

        _HOSTED_SOURCE_SYNC_CACHE[database_url] = signature


def _repo_root_path() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_repo_source_signature() -> str:
    hasher = hashlib.sha256()
    for path in _iter_repo_source_sync_paths():
        relative = path.relative_to(_repo_root_path()).as_posix()
        hasher.update(relative.encode("utf-8"))
        hasher.update(b"\0")
        if path.exists():
            hasher.update(path.read_bytes())
        else:
            hasher.update(b"<missing>")
        hasher.update(b"\0")
    return hasher.hexdigest()


def _iter_repo_source_sync_paths() -> list[Path]:
    repo_root = _repo_root_path()
    season_csv_dir = repo_root / "data" / "raw" / "season_csv"

    paths = [
        repo_root / "src" / "utils" / "db.py",
        repo_root / "src" / "ingest" / "pipeline.py",
        repo_root / "src" / "ingest" / "manual_boxscore.py",
        repo_root / "src" / "ingest" / "season_csv.py",
        repo_root / "src" / "models" / "schedule.py",
        repo_root / "src" / "models" / "season_metadata.py",
        repo_root / "src" / "models" / "season_roster.py",
        repo_root / "src" / "models" / "projections.py",
        repo_root / "build_hitter_projections.py",
        repo_root / "manage_player_metadata.py",
        repo_root / "data" / "processed" / "player_alias_overrides.csv",
        repo_root / "data" / "processed" / "player_metadata.csv",
        repo_root / "data" / "processed" / "player_season_metadata.csv",
        repo_root / "data" / "processed" / "current_spring_roster.csv",
        repo_root / "data" / "processed" / "team_schedule.csv",
        repo_root / "data" / "processed" / "standings_snapshot.csv",
        repo_root / "data" / "processed" / "league_schedule_games.csv",
        repo_root / "data" / "processed" / "game_boxscore_games.csv",
        repo_root / "data" / "processed" / "game_boxscore_batting.csv",
    ]

    if season_csv_dir.exists():
        paths.extend(sorted(season_csv_dir.glob("*.csv")))

    return sorted({path.resolve() for path in paths}, key=lambda path: path.as_posix().lower())


def _ensure_source_sync_state_table(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS source_sync_state (
            sync_key TEXT PRIMARY KEY,
            signature TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.commit()


def _read_source_sync_signature(connection, sync_key: str) -> str | None:
    row = connection.execute(
        "SELECT signature FROM source_sync_state WHERE sync_key = ?",
        (sync_key,),
    ).fetchone()
    if not row:
        return None
    return str(row["signature"])


def _copy_postgres_snapshot_to_sqlite(*, sqlite_path: Path, database_url: str) -> None:
    from sync_to_supabase import SYNC_TABLES, build_insert_sql

    sqlite_connection = connect_db(sqlite_path)
    postgres_connection = connect_postgres_db(database_url)
    try:
        initialize_database(sqlite_connection)
        for table in SYNC_TABLES:
            columns = [str(row["name"]) for row in sqlite_connection.execute(f"PRAGMA table_info({table})").fetchall()]
            if not columns:
                continue
            rows = postgres_connection.execute(f"SELECT {', '.join(columns)} FROM {table}").fetchall()
            if not rows:
                continue
            sqlite_connection.executemany(
                build_insert_sql(table, columns),
                [tuple(row[column] for column in columns) for row in rows],
            )
        sqlite_connection.commit()
    finally:
        sqlite_connection.close()
        postgres_connection.close()


def _apply_repo_source_updates(*, sqlite_path: Path, audit_dir: Path) -> None:
    from manage_player_metadata import DEFAULT_METADATA_CSV, import_player_metadata
    from src.ingest.manual_boxscore import (
        DEFAULT_GAME_BOXSCORE_BATTING_PATH,
        DEFAULT_GAME_BOXSCORE_GAMES_PATH,
        import_manual_boxscore_bundle,
    )
    from src.ingest.pipeline import sync_sources
    from src.models.projections import build_hitter_projections
    from src.models.schedule import (
        DEFAULT_LEAGUE_SCHEDULE_PATH,
        DEFAULT_SCHEDULE_PATH,
        DEFAULT_STANDINGS_PATH,
        import_schedule_bundle,
    )
    from src.models.season_metadata import DEFAULT_SEASON_METADATA_PATH, sync_player_season_metadata
    from src.models.season_roster import (
        DEFAULT_ACTIVE_ROSTER_SEASON,
        DEFAULT_SEASON_ROSTER_PATH,
        import_season_roster,
    )
    from src.utils.player_identity import DEFAULT_ALIAS_OVERRIDE_PATH

    repo_root = _repo_root_path()
    season_csv_paths = sorted((repo_root / "data" / "raw" / "season_csv").glob("*.csv"))
    if season_csv_paths:
        sync_sources(
            db_path=sqlite_path,
            audit_dir=audit_dir,
            season_csv_paths=season_csv_paths,
            alias_override_path=repo_root / DEFAULT_ALIAS_OVERRIDE_PATH,
        )

    connection = connect_db(sqlite_path)
    try:
        initialize_database(connection)

        metadata_csv = repo_root / DEFAULT_METADATA_CSV
        if metadata_csv.exists():
            import_player_metadata(connection, metadata_csv)

        roster_csv = repo_root / DEFAULT_SEASON_ROSTER_PATH
        if roster_csv.exists():
            import_season_roster(
                connection=connection,
                csv_path=roster_csv,
                season_name=DEFAULT_ACTIVE_ROSTER_SEASON,
            )

        season_metadata_path = repo_root / DEFAULT_SEASON_METADATA_PATH
        if season_metadata_path.exists():
            sync_player_season_metadata(connection, season_metadata_path)

        import_schedule_bundle(
            connection=connection,
            schedule_csv_path=repo_root / DEFAULT_SCHEDULE_PATH,
            standings_csv_path=repo_root / DEFAULT_STANDINGS_PATH,
            league_schedule_csv_path=repo_root / DEFAULT_LEAGUE_SCHEDULE_PATH,
        )

        import_manual_boxscore_bundle(
            connection,
            games_csv_path=repo_root / DEFAULT_GAME_BOXSCORE_GAMES_PATH,
            batting_csv_path=repo_root / DEFAULT_GAME_BOXSCORE_BATTING_PATH,
            alias_override_path=repo_root / DEFAULT_ALIAS_OVERRIDE_PATH,
        )

        projection_seasons = [
            str(row["projection_season"])
            for row in connection.execute(
                "SELECT DISTINCT projection_season FROM hitter_projections WHERE projection_season <> ''"
            ).fetchall()
        ]
        for projection_season in projection_seasons:
            projections = build_hitter_projections(connection=connection, projection_season=projection_season)
            replace_hitter_projections(connection, projection_season, projections)
    finally:
        connection.close()
