from __future__ import annotations

import os
from pathlib import Path
from typing import Any


APP_MODE_LOCAL = "local"
APP_MODE_HOSTED = "hosted"


def get_setting(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value not in (None, ""):
        return str(value)
    try:
        import streamlit as st

        value = st.secrets.get(name, "")
        if value not in (None, ""):
            return str(value)
    except Exception:
        return default
    return default


def get_app_mode() -> str:
    mode = get_setting("APP_MODE", APP_MODE_LOCAL).strip().lower()
    return APP_MODE_HOSTED if mode == APP_MODE_HOSTED else APP_MODE_LOCAL


def get_database_url() -> str:
    return get_setting("DATABASE_URL", "").strip()


def get_connection_cache_key() -> str:
    database_url = get_database_url()
    if database_url:
        return f"{APP_MODE_HOSTED}:{database_url}"
    return f"{get_app_mode()}:local"


def should_use_hosted_database() -> bool:
    database_url = get_database_url()
    if database_url:
        return True
    return get_app_mode() == APP_MODE_HOSTED and bool(database_url)


def get_viewer_password() -> str:
    return get_setting("VIEWER_PASSWORD", "").strip()


def get_admin_password() -> str:
    return get_setting("ADMIN_PASSWORD", "").strip()


def get_default_sqlite_path(default_path: Path) -> Path:
    configured = get_setting("SQLITE_DB_PATH", "").strip()
    return Path(configured) if configured else default_path
