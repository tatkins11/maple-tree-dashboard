# Dispatch brief — game-facing stats export (the data contract)

*From the vault chief of staff, 7/23. Maple Tree: The Game (new repo, `C:\MapleTreeGame`) builds its ratings and modes from this site's stats. It must NEVER read this project's internals — it consumes a published export only. This brief creates that contract. Small job: one export script + a documented shape.*

## What to build
An export step (e.g., `export_game_extract.py`) that runs after the normal weekly stats update and writes a versioned extract to **`C:\MapleTreeGame\data\imports\`** (the game's inbox — this project OWNS the shape; the game owns consumption):

1. `manifest.json` — extract version, generated-at date, season coverage, row counts (the game refuses mismatched versions rather than guessing).
2. `players_<year>.json` — per-player season batting lines for EVERY org year on record (the year-teams + ratings inputs), plus batted-ball/spray profile fields where tracked.
3. `rosters.json` — who was on each year's team.
4. `games_<year>.json` — game-by-game schedule + REAL results (opponent, park, score, our box line) — Season Replay mode re-sims the real schedule and grades against reality.
5. `opponents.json` — accumulated stats on each rival team (their hitters/pitching as we recorded them) — feeds mock-team ratings.
6. `parks.json` — the three fields with dimensions (two ~300 all around, one 350) and which games were played where.

## Rules
- **Shape is a contract:** once v1 ships, changes are versioned in the manifest, never silent. Internal site/DB refactors stay free as long as the export holds.
- Export is regenerable and idempotent; re-running overwrites cleanly.
- Missing data is EXPLICIT (`null` + a manifest note), never zero-filled — the game's ratings must not mistake absent stats for bad players (the missing-data-zeros rule).
- This project never reads from the game repo; the game never reads from this one. The `data/imports/` folder is the only crossing.

## Definition of done
Extract generated for all org years; manifest documents the shape (field list + types); one write-back line to the vault boards; the game session notified the contract is live.
