from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.utils.names import normalize_player_name


DEFAULT_SEASON_METADATA_PATH = Path("data/processed/player_season_metadata.csv")


@dataclass
class PlayerSeasonMetadataRecord:
    player_id: int
    season: str
    injury_flag: bool = False
    manual_weight_multiplier: float | None = None
    notes: str = ""


def ensure_player_season_metadata_file(csv_path: Path = DEFAULT_SEASON_METADATA_PATH) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "player_name",
                "season",
                "injury_flag",
                "manual_weight_multiplier",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "player_name": "Tristan",
                "season": "Maple Tree Tappers Summer 2025",
                "injury_flag": "true",
                "manual_weight_multiplier": "",
                "notes": "Injury-affected season",
            }
        )


def sync_player_season_metadata(
    connection: sqlite3.Connection,
    csv_path: Path = DEFAULT_SEASON_METADATA_PATH,
) -> int:
    ensure_player_season_metadata_file(csv_path)
    rows_inserted = 0
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            player_name = (row.get("player_name") or "").strip()
            season = (row.get("season") or "").strip()
            if not player_name or not season:
                continue
            normalized = normalize_player_name(player_name)
            player_row = connection.execute(
                """
                SELECT player_id
                FROM player_identity
                WHERE canonical_name = ?
                   OR lower(player_name) = ?
                   OR EXISTS (
                        SELECT 1
                        FROM player_aliases pa
                        WHERE pa.player_id = player_identity.player_id
                          AND pa.normalized_source_name = ?
                   )
                ORDER BY CASE WHEN canonical_name = ? THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (normalized, player_name.lower(), normalized, normalized),
            ).fetchone()
            if player_row is None:
                continue
            injury_flag = (row.get("injury_flag") or "").strip().lower() in {"1", "true", "yes", "y"}
            manual_weight_multiplier = (row.get("manual_weight_multiplier") or "").strip()
            notes = (row.get("notes") or "").strip()
            connection.execute(
                """
                INSERT OR REPLACE INTO player_season_metadata (
                    player_id, season, injury_flag, manual_weight_multiplier, notes
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(player_row["player_id"]),
                    season,
                    1 if injury_flag else 0,
                    float(manual_weight_multiplier) if manual_weight_multiplier else None,
                    notes,
                ),
            )
            rows_inserted += 1
    connection.commit()
    return rows_inserted
