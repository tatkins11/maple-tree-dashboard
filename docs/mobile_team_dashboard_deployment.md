# Mobile Team Dashboard Deployment

## Recommended Setup
- Host the Streamlit app from this repository.
- Use Supabase Postgres as the shared hosted database.
- Set teammates up with the viewer password.
- Keep the admin password private for manager-only pages and update workflows.

## Streamlit Secrets
Create Streamlit Cloud secrets from `.streamlit/secrets.example.toml`:

```toml
APP_MODE = "hosted"
DATABASE_URL = "postgresql://postgres:[PASSWORD]@[HOST]:5432/postgres"
VIEWER_PASSWORD = "team-password"
ADMIN_PASSWORD = "manager-password"
```

Local development can omit these values. With no passwords configured, local runs default to admin access.

## Supabase Sync
Before syncing, check local readiness:

```powershell
python check_deployment_ready.py --sqlite-path db/all_seasons_identity.sqlite
```

After updating the local SQLite database with Codex or local scripts, push the current local database to Supabase:

```powershell
$env:DATABASE_URL="postgresql://postgres:[PASSWORD]@[HOST]:5432/postgres"
python sync_to_supabase.py --sqlite-path db/all_seasons_identity.sqlite
```

The sync replaces hosted dashboard tables by default so the online app mirrors the local source of truth. Use `--append` only for intentional manual append workflows.

## Streamlit Cloud
- App entrypoint: `streamlit_app.py`
- Python dependencies: `requirements.txt`
- Secrets: paste the hosted values above.
- Share the deployed URL with the team after confirming viewer login works.

## iPhone Install Steps
- Open the hosted dashboard link in Safari.
- Enter the viewer password.
- Tap Share.
- Choose `Add to Home Screen`.
- Launch it from the Home Screen like a team app.

## Weekly Manager Workflow
- Update stats/results locally with Codex or the existing scripts.
- Generate/save postgame write-ups locally or in the admin dashboard.
- Run `python check_deployment_ready.py`.
- Run `sync_to_supabase.py`.
- Rerun or refresh the hosted Streamlit app.
