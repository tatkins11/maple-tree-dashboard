from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from src.dashboard.config import (
    get_admin_password,
    get_app_mode,
    get_viewer_password,
)


ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"
ROLE_NONE = ""
VIEWER_HIDDEN_PAGE_SLUGS = (
    "Lineup_Optimizer",
    "Admin_Data",
)


@dataclass(frozen=True)
class AuthConfig:
    app_mode: str
    viewer_password: str
    admin_password: str

    @property
    def requires_password(self) -> bool:
        return bool(self.viewer_password or self.admin_password)


def get_auth_config() -> AuthConfig:
    return AuthConfig(
        app_mode=get_app_mode(),
        viewer_password=get_viewer_password(),
        admin_password=get_admin_password(),
    )


def resolve_role_for_password(password: str, config: AuthConfig) -> str:
    candidate = password.strip()
    if config.admin_password and candidate == config.admin_password:
        return ROLE_ADMIN
    if config.viewer_password and candidate == config.viewer_password:
        return ROLE_VIEWER
    return ROLE_NONE


def current_role() -> str:
    return str(st.session_state.get("auth_role") or ROLE_NONE)


def is_admin() -> bool:
    return current_role() == ROLE_ADMIN


def ensure_authenticated() -> str:
    config = get_auth_config()
    if current_role():
        _render_session_controls()
        return current_role()

    if not config.requires_password:
        st.session_state["auth_role"] = ROLE_ADMIN
        _render_session_controls()
        return ROLE_ADMIN

    st.title("Maple Tree Dashboard")
    st.caption("Enter the team password to view the dashboard.")
    with st.form("team_access_form"):
        password = st.text_input("Team password", type="password")
        submitted = st.form_submit_button("Enter")
    if submitted:
        role = resolve_role_for_password(password, config)
        if role:
            st.session_state["auth_role"] = role
            st.rerun()
        st.error("That password did not match the viewer or admin password.")
    st.stop()


def require_admin() -> None:
    role = ensure_authenticated()
    if role != ROLE_ADMIN:
        st.warning("This manager page is hidden from team viewer access.")
        st.stop()


def _render_session_controls() -> None:
    role = current_role()
    if not role:
        return
    _render_role_based_sidebar_visibility(role)
    st.sidebar.caption(f"Access: {role.title()}")
    if st.sidebar.button("Log out", key="auth_logout"):
        st.session_state.pop("auth_role", None)
        st.rerun()


def role_based_sidebar_css(role: str) -> str:
    if role != ROLE_VIEWER:
        return ""

    selectors: list[str] = []
    for slug in VIEWER_HIDDEN_PAGE_SLUGS:
        selectors.append(f'section[data-testid="stSidebar"] li:has(a[href*="{slug}"])')
        selectors.append(f'section[data-testid="stSidebar"] a[href*="{slug}"]')

    return (
        "<style>\n"
        + ",\n".join(selectors)
        + "\n{display: none !important;}\n"
        + "</style>"
    )


def _render_role_based_sidebar_visibility(role: str) -> None:
    css = role_based_sidebar_css(role)
    if css:
        st.markdown(css, unsafe_allow_html=True)
