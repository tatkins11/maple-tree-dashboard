from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.dashboard.config import should_use_hosted_database


def database_path_control(default_path: Path, *, key: str) -> str:
    if should_use_hosted_database():
        st.sidebar.caption("Database: hosted Supabase/Postgres")
        return str(default_path)
    return st.sidebar.text_input("Database path", value=str(default_path), key=key)


def render_mobile_install_help() -> None:
    st.markdown("### iPhone App Shortcut")
    st.info(
        "On iPhone, open this dashboard in Safari, tap Share, then choose Add to Home Screen. "
        "It will launch from your Home Screen like a team app."
    )

