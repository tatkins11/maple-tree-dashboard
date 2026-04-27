from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import streamlit as st

from src.dashboard.config import should_use_hosted_database


RESPONSIVE_AUTO = "Auto"
RESPONSIVE_MOBILE = "Mobile"
RESPONSIVE_FULL = "Full"
RESPONSIVE_LAYOUT_OPTIONS = [RESPONSIVE_AUTO, RESPONSIVE_MOBILE, RESPONSIVE_FULL]
RESPONSIVE_BREAKPOINT = 768


@dataclass(frozen=True)
class ResponsiveLayoutContext:
    mode: str
    detected_width: int | None
    is_mobile_layout: bool


def resolve_responsive_layout_mode(
    mode: str,
    *,
    detected_width: int | None,
    breakpoint: int = RESPONSIVE_BREAKPOINT,
) -> bool:
    if mode == RESPONSIVE_MOBILE:
        return True
    if mode == RESPONSIVE_FULL:
        return False
    if detected_width is None:
        return False
    return int(detected_width) <= int(breakpoint)


def get_responsive_layout_context(*, key: str) -> ResponsiveLayoutContext:
    mode_key = f"{key}_responsive_mode"
    width_key = f"{key}_viewport_width"
    eval_key = f"{key}_viewport_eval"

    mode = st.sidebar.selectbox(
        "Layout",
        options=RESPONSIVE_LAYOUT_OPTIONS,
        index=0,
        key=mode_key,
        help="Auto switches to the mobile layout on narrow screens. Mobile and Full override it manually.",
    )

    detected_width = st.session_state.get(width_key)
    if mode == RESPONSIVE_AUTO:
        try:
            from streamlit_js_eval import streamlit_js_eval  # type: ignore

            latest_width = streamlit_js_eval(
                js_expressions="window.innerWidth",
                key=eval_key,
            )
            if isinstance(latest_width, (int, float)):
                detected_width = int(latest_width)
                st.session_state[width_key] = detected_width
        except Exception:
            detected_width = st.session_state.get(width_key)

    is_mobile_layout = resolve_responsive_layout_mode(
        str(mode),
        detected_width=detected_width if isinstance(detected_width, int) else None,
    )
    return ResponsiveLayoutContext(
        mode=str(mode),
        detected_width=detected_width if isinstance(detected_width, int) else None,
        is_mobile_layout=is_mobile_layout,
    )


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
