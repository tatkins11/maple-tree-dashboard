# Maple Tree Weekly Runbook

The full weekly cycle for game weeks. Two halves: **pregame** (Brian posts the lineup)
and **postgame** (Brian posts the season CSV, game-by-game screenshots, and league results).
Every gotcha in here caused a real hiccup once — don't skip the checks.

---

## Part A — Pregame preview (before Wednesday)

**Brian provides:** the batting order, 1 through N.

1. Data should already be current from last week's sync. If anything changed since, run
   `python scripts/export_site_data.py` first so the preview matches the site.
2. Generate:
   ```
   python scripts/build_gameday_preview.py --lineup "Glove,Kives,Tristan,Tim,JJ,Porter,Corey,Joel,Walsh,Duff,Jason"
   ```
   Auto-detects the next unplayed game (opponent, DH times, field, week label) from
   schedule.json; storylines, opponent scouting, and milestone watch generate from data;
   lineup rows pick up each hitter's card art. Handles deep orders (11–12 batters) and
   unknown names show a "CARD TBD" placeholder tile. **3 pages:** ① overview (matchup,
   storylines, opponent's full season slate, reigning POTW, full standings), ② the lineup,
   ③ "The Milestone Board" — every active hitter's nearest career milestones.
   - **Roster / injury news** → add `--story "Injury report.|Body text…"` (repeatable). It
     rides just under the matchup line. Use it whenever Brian flags who's in/out (e.g. a
     hurt regular, a returning player). A fit-guard keeps storylines off the standings.
   - A player who's OUT simply isn't in the `--lineup`; nothing else needed.
3. Deliver the PDF from `data/writeups/maple-tree-<season>/`. **Writeup PDFs are local
   deliverables — never committed.**
4. **Keep this lineup** — it is the batting order for the postgame box scores.

---

## Part B — Postgame sync (after Wednesday)

**Brian provides:** ① the season stats CSV export, ② GameChanger game-stats screenshots
(one per game), ③ the league results for the week (text paste).

### B1. Transcribe + derive the box scores

- **Screenshots are sorted by hits, NOT batting order.** True batting order = the preview
  lineup, minus anyone who didn't play (everyone slides up), plus any subs where Brian says.
  When in doubt, ask Brian — this burned us in Week 2.
- **`lineup_spot` must be the true batting order.** The postgame recap prints box scores
  in that order. (An old note said order didn't matter — that's obsolete.)
- **Transcribe Game 1 by hand** from its screenshot. **Derive Game 2 exactly** as
  `(new season CSV − old season CSV − Game 1)` — no screenshot squinting, and it
  self-checks. Verify:
  - Game 1 line-sum == Game 1 team-totals row from the screenshot
  - Derived Game 2 == Game 2 team-totals row
  - Any player who played only one game: season delta == that game's line exactly
- Template: **copy `scripts/_week3_sync.py` → `scripts/_weekN_sync.py`** and fill in the
  week's constants (game keys, scores, notes, Game-1 lines, batting orders, league scores).
  Run it dry (asserts + printed reconciliation) → review → run with `--write`.

### B2. What the sync script writes

| File | What |
|---|---|
| `data/processed/game_boxscore_games.csv` | one row per game (scores + color notes: ejections, injuries) |
| `data/processed/game_boxscore_batting.csv` | batting lines in true order (`outs = ab − h`) |
| `data/processed/team_schedule.csv` | our 2 rows → `completed`, flag `1`, W/L, RF/RA, notes |
| `data/processed/league_schedule_games.csv` | ALL league games that week → `completed`, flag `1`, home/away runs |
| `data/raw/season_csv/Maple Tree <Season> Stats.csv` | overwritten with Brian's new export |

Season-CSV parsing gotcha: the GameChanger header **repeats column names** across
Batting/Pitching/Fielding — always use the FIRST occurrence (batting block).

### B3. Reload the local DB (three loaders)

```
python sync_sources.py --season-csv "data/raw/season_csv/Maple Tree Summer 2026 Stats.csv" --db-path db/all_seasons_identity.sqlite
python manage_boxscores.py --mode import --games-csv data/processed/game_boxscore_games.csv --batting-csv data/processed/game_boxscore_batting.csv
python manage_schedule.py --mode import --schedule-csv data/processed/team_schedule.csv --league-schedule-csv data/processed/league_schedule_games.csv
```

⚠ `sync_sources.py`'s **default `--db-path` points at the wrong file** — always pass
`db/all_seasons_identity.sqlite` explicitly. The other two default correctly.
(`db/*.sqlite` is a local artifact, never committed.)

### B4. Export + verify

```
python scripts/export_site_data.py
```
- Watch for **"unmatched batted-ball row"** warnings — GameChanger name drift
  (historical: Teo = Tristan '21–'22, Snaxx = Joey '25–'26; aliases live in
  `BB_ALIASES` in the export script).
- Spot-check `site/src/data/meta.json`: record, seed board (W/L/RF/RA), POTW.
- Spot-check `milestones.json` → `recent`: only real crossings (reached-events are
  gated on the official career table, so game-log drift can't mint phantoms — the
  Kives 100-RBI lesson).

### B5. New trading cards (whenever Brian drops them)

1. Files land in `C:\Slowpitch\Player Trading Cards` (odd filenames — **view each
   image** to identify it; diff folder vs manifest to find new ones).
2. Add a manifest entry in `data/processed/trading_cards.json` — for event cards use
   `kind: "special"` with authored `series` / `caption` / `flavor` / `facts`.
   **Caption must include "Week N"** — the postgame recap auto-features week-captioned
   specials on its Card Corner page.
3. `python scripts/process_trading_cards.py` (→ `site/public/cards/*.webp`; commit the webp).

### B6. Build, commit, push

```
cd site && npm run build
```
Commit **data CSVs + `site/src/data/*.json` + new card webps** (JSON is committed —
Netlify never runs Python). Do NOT commit: audits, writeup PDFs, `_weekN` helper
scripts, the sqlite DB.

`git push origin main` also refreshes the **hosted Streamlit back office** — its
`connect_app_db` re-syncs the hosted Postgres from repo CSVs on next boot. No manual
Supabase step.

### B7. Deploy the public site

**Netlify is NOT git-linked** — a push does not publish. Deploy is a ~10-second zip
upload of the built `site/dist`:

```powershell
$env:NETLIFY_AUTH_TOKEN="nfp_…"; python scripts/deploy_site.py
```

Brian runs this (the token is his and is never stored). One-time alternative:
`setx NETLIFY_AUTH_TOKEN "nfp_…"` then relaunch — after that Claude can run the
deploy directly with no token on the command line.

**Verify live** (don't trust the upload alone):
```
curl -s -o /dev/null -w "%{http_code}" https://mapletreesoftball.netlify.app/
```
plus a check that the home record and any new card webps return 200.

### B8. Postgame recap PDF

```
python scripts/build_postgame_recap.py --stories-only \
  --story "Headline.|Deadpan body copy…" \
  --story "…"
```
- 4 pages: recap + POG + ALL milestones · full standings + every league score ·
  box scores (true batting order) · Card Corner (auto from week-captioned specials).
- House voice: **comedic but overly serious** — wire-service deadpan ("The bottle
  declined comment", "The fence, undefeated since 2019, was not penalized on the play").
- Render pages to PNG (pymupdf) and eyeball before delivering. Local deliverable.

---

## Standing conventions

- **Playoff games fold into season stats** (Brian's convention): add the box score AND
  add each player's playoff line into their season-CSV row (counting cols + recompute
  AVG/OBP/SLG/OPS) — the season loader REPLACEs per (season, player), it doesn't sum.
- No CSV/PDF export buttons on the site or dashboard.
- Weekly sync is a **manual conversation** — no cron/automation; Brian reviews in the loop.
- OBP is PA-based everywhere; site numbers must equal dashboard numbers (both read the
  same `src.dashboard.data` functions via the export).
