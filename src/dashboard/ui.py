from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import pandas as pd
import streamlit as st

from src.dashboard.config import should_use_hosted_database


RESPONSIVE_AUTO = "Auto"
RESPONSIVE_MOBILE = "Mobile"
RESPONSIVE_FULL = "Full"
RESPONSIVE_LAYOUT_OPTIONS = [RESPONSIVE_AUTO, RESPONSIVE_MOBILE, RESPONSIVE_FULL]
RESPONSIVE_BREAKPOINT = 768
PLAYER_CARD_URL_PATH = "player-card"
PLAYER_LINK_DISPLAY_REGEX = r"#(.*)$"


@dataclass(frozen=True)
class ResponsiveLayoutContext:
    mode: str
    detected_width: int | None
    is_mobile_layout: bool


@dataclass(frozen=True)
class MobileStandingsCard:
    team_name: str
    is_selected: bool
    wins: int
    losses: int
    games_back: float
    runs_for: int
    runs_against: int
    run_diff: int


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


def build_mobile_standings_cards(
    standings: pd.DataFrame,
    *,
    selected_team: str | None = None,
) -> list[MobileStandingsCard]:
    if standings.empty:
        return []

    cards: list[MobileStandingsCard] = []
    for _, row in standings.iterrows():
        team_name = str(row.get("team_name") or "").strip()
        cards.append(
            MobileStandingsCard(
                team_name=team_name,
                is_selected=bool(selected_team) and team_name == str(selected_team),
                wins=int(row.get("wins") or 0),
                losses=int(row.get("losses") or 0),
                games_back=float(row.get("games_back") or 0.0),
                runs_for=int(row.get("runs_for") or 0),
                runs_against=int(row.get("runs_against") or 0),
                run_diff=int(row.get("run_diff") or 0),
            )
        )
    return cards


def render_mobile_standings_cards(
    standings: pd.DataFrame,
    *,
    selected_team: str | None = None,
    css_class_prefix: str = "mobile-standings",
) -> None:
    cards = build_mobile_standings_cards(standings, selected_team=selected_team)
    if not cards:
        return

    st.markdown(
        f"""
        <style>
        .{css_class_prefix}-card {{
            border: 1px solid rgba(49, 51, 63, 0.10);
            border-radius: 0.9rem;
            padding: 0.8rem 0.9rem;
            background: #fafafa;
            margin-bottom: 0.55rem;
        }}
        .{css_class_prefix}-header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 0.8rem;
            margin-bottom: 0.22rem;
        }}
        .{css_class_prefix}-team {{
            font-size: 1rem;
            font-weight: 800;
            color: #111827;
        }}
        .{css_class_prefix}-record {{
            font-size: 0.92rem;
            font-weight: 700;
            color: #374151;
        }}
        .{css_class_prefix}-row {{
            font-size: 0.88rem;
            color: #4b5563;
            margin: 0.08rem 0;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    for card in cards:
        selected_marker = " *" if card.is_selected else ""
        st.markdown(
            f"""
            <div class="{css_class_prefix}-card">
              <div class="{css_class_prefix}-header">
                <div class="{css_class_prefix}-team">{escape(card.team_name)}{selected_marker}</div>
                <div class="{css_class_prefix}-record">{card.wins}-{card.losses}</div>
              </div>
              <div class="{css_class_prefix}-row"><strong>RD:</strong> {card.run_diff} &nbsp; <strong>GB:</strong> {card.games_back:.1f}</div>
              <div class="{css_class_prefix}-row"><strong>RF:</strong> {card.runs_for} &nbsp; <strong>RA:</strong> {card.runs_against}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def build_player_page_href(canonical_name: str, display_name: str | None = None) -> str:
    canonical = str(canonical_name).strip()
    label = str(display_name or canonical).strip()
    if not canonical:
        return "#"
    return f"./{PLAYER_CARD_URL_PATH}?player={quote(canonical, safe='')}#{label}"


def build_player_link_html(display_name: str, canonical_name: str) -> str:
    if not str(canonical_name).strip():
        return escape(display_name)
    href = build_player_page_href(canonical_name, display_name)
    return f'<a href="{escape(href, quote=True)}" target="_self">{escape(display_name)}</a>'


def with_player_link_column(
    dataframe: pd.DataFrame,
    *,
    player_column: str = "player",
    canonical_column: str = "canonical_name",
    output_column: str = "player_link",
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    working = dataframe.copy()
    if player_column not in working.columns or canonical_column not in working.columns:
        return working

    working.loc[:, output_column] = [
        build_player_page_href(
            str(canonical_name or ""),
            str(display_name or canonical_name or ""),
        )
        for display_name, canonical_name in zip(working[player_column], working[canonical_column])
    ]
    return working


def player_link_column_config(*, label: str = "Player", width: str = "medium") -> st.column_config.LinkColumn:
    return st.column_config.LinkColumn(label, width=width, display_text=PLAYER_LINK_DISPLAY_REGEX)


def _format_table_cell(value, formatter) -> str:
    if pd.isna(value):
        return ""
    if callable(formatter):
        return str(formatter(value))
    if isinstance(formatter, str):
        return formatter.format(value)
    return str(value)


def _format_link_cell(value) -> str:
    if pd.isna(value):
        return ""
    href = str(value).strip()
    if not href or href == "#":
        return ""
    parsed = urlsplit(href)
    label = unquote(parsed.fragment).strip() or href
    return f'<a href="{escape(href, quote=True)}" target="_self">{escape(label)}</a>'


_HEAT_STOPS = ((0.0, (251, 250, 247)), (0.5, (220, 252, 231)), (1.0, (126, 226, 168)))


def _heat_color(norm: float) -> str:
    """Interpolate the Maple Tree heat scale (paper -> soft green) at norm 0..1."""
    norm = max(0.0, min(1.0, float(norm)))
    for (low, low_rgb), (high, high_rgb) in zip(_HEAT_STOPS, _HEAT_STOPS[1:]):
        if norm <= high:
            span = (high - low) or 1.0
            t = (norm - low) / span
            r, g, b = (round(a + (b_ - a) * t) for a, b_ in zip(low_rgb, high_rgb))
            return f"#{r:02x}{g:02x}{b:02x}"
    return "#7ee2a8"


def sparkline_svg(values, *, width: int = 64, height: int = 20, stroke: str = "#15803d") -> str:
    """Tiny inline-SVG sparkline (polyline + end dot) for static table cells."""
    numbers = [float(v) for v in values if v is not None and not pd.isna(v)]
    if not numbers:
        return ""
    pad = 3
    low, high = min(numbers), max(numbers)
    span = (high - low) or 1.0
    if len(numbers) == 1:
        xs = [width / 2]
    else:
        step = (width - 2 * pad) / (len(numbers) - 1)
        xs = [pad + i * step for i in range(len(numbers))]
    ys = [height - pad - ((v - low) / span) * (height - 2 * pad) for v in numbers]
    points = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;">'
        f'<polyline points="{points}" fill="none" stroke="{stroke}" stroke-width="1.6" '
        f'stroke-linecap="round" stroke-linejoin="round"/>'
        f'<circle cx="{xs[-1]:.1f}" cy="{ys[-1]:.1f}" r="2" fill="{stroke}"/>'
        f"</svg>"
    )


def render_static_table(
    dataframe: pd.DataFrame,
    *,
    column_labels: dict[str, str] | None = None,
    formatters: dict[str, object] | None = None,
    link_columns: list[str] | None = None,
    heat_columns: list[str] | None = None,
    css_class: str = "dashboard-static-table",
    container=None,
) -> None:
    if dataframe.empty:
        return

    target = container or st
    display = dataframe.copy().astype(object)
    formatters = formatters or {}
    for column, formatter in formatters.items():
        if column in display.columns:
            display.loc[:, column] = display[column].map(lambda value: _format_table_cell(value, formatter))

    # Heat pills: color each cell by where its (numeric) value sits in the
    # column's range. Applied after formatting so the pill wraps the display
    # string, but normalized on the original numeric values.
    for column in heat_columns or []:
        if column not in display.columns or column not in dataframe.columns:
            continue
        numeric = pd.to_numeric(dataframe[column], errors="coerce")
        low, high = numeric.min(), numeric.max()
        span = float(high - low) if pd.notna(low) and pd.notna(high) and high != low else 1.0
        pills = []
        for raw, formatted in zip(numeric, display[column]):
            if pd.isna(raw) or formatted in ("", None):
                pills.append(formatted)
                continue
            color = _heat_color((float(raw) - float(low)) / span if pd.notna(low) else 0.0)
            pills.append(f'<span class="mt-heat" style="background:{color};">{formatted}</span>')
        display.loc[:, column] = pills

    if column_labels:
        ordered_columns = [column for column in dataframe.columns if column in display.columns]
        display = display[ordered_columns].rename(columns=column_labels)

    if link_columns:
        resolved_link_columns = [
            column_labels.get(column, column) if column_labels else column
            for column in link_columns
        ]
        # Keep player links inside the HTML table so we preserve borders, striping, and alignment.
        for column in resolved_link_columns:
            if column in display.columns:
                display.loc[:, column] = display[column].map(_format_link_cell)

    target.markdown(
        f"""
        <style>
        .{css_class}-wrap {{
            overflow-x: auto;
            margin-bottom: 0.4rem;
        }}
        table.{css_class} {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
            background: white;
        }}
        table.{css_class} thead th {{
            text-align: left;
            font-weight: 700;
            color: #374151;
            padding: 0.55rem 0.65rem;
            border-bottom: 1px solid rgba(49, 51, 63, 0.12);
            background: #fafafa;
            white-space: nowrap;
        }}
        table.{css_class} tbody td {{
            padding: 0.5rem 0.65rem;
            border-bottom: 1px solid rgba(49, 51, 63, 0.08);
            color: #1f2937;
            white-space: nowrap;
            vertical-align: top;
        }}
        table.{css_class} tbody tr:nth-child(even) {{
            background: rgba(249, 250, 251, 0.7);
        }}
        table.{css_class} a {{
            color: #2563eb;
            text-decoration: none;
        }}
        table.{css_class} a:hover {{
            text-decoration: underline;
        }}
        table.{css_class} .mt-heat {{
            display: inline-block;
            min-width: 3.4em;
            padding: 0.06rem 0.5rem;
            border-radius: 999px;
            text-align: center;
            font-weight: 600;
            color: #14532d;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    target.markdown(
        f'<div class="{css_class}-wrap">{display.to_html(index=False, escape=False, classes=css_class, border=0)}</div>',
        unsafe_allow_html=True,
    )

_PERSISTENT_STATE_PREFIX = "_persistent_"
_PERSISTENT_MULTI_SEPARATOR = "|"


def _persistent_state_key(query_key: str) -> str:
    return f"{_PERSISTENT_STATE_PREFIX}{query_key}"


def _read_query_param(query_key: str) -> str | None:
    try:
        raw = st.query_params.get(query_key)
    except Exception:
        return None
    return None if raw is None else str(raw)


def _write_query_param(query_key: str, encoded: str) -> None:
    try:
        current = st.query_params.get(query_key)
    except Exception:
        current = None
    if encoded == "":
        if current is not None:
            try:
                del st.query_params[query_key]
            except Exception:
                pass
        return
    if str(current) != encoded:
        try:
            st.query_params[query_key] = encoded
        except Exception:
            pass


def persistent_selectbox(
    label: str,
    options: list,
    *,
    query_key: str,
    default=None,
    **kwargs,
):
    state_key = _persistent_state_key(query_key)
    if state_key not in st.session_state:
        raw = _read_query_param(query_key)
        resolved = None
        if raw is not None:
            for option in options:
                if str(option) == raw:
                    resolved = option
                    break
        if resolved is None:
            resolved = default if default is not None else (options[0] if options else None)
        st.session_state[state_key] = resolved
    elif options and st.session_state[state_key] not in options:
        st.session_state[state_key] = default if default in options else options[0]
    value = st.selectbox(label, options=options, key=state_key, **kwargs)
    _write_query_param(query_key, "" if value is None else str(value))
    return value


def persistent_multiselect(
    label: str,
    options: list,
    *,
    query_key: str,
    default: list | None = None,
    separator: str = _PERSISTENT_MULTI_SEPARATOR,
    **kwargs,
) -> list:
    state_key = _persistent_state_key(query_key)
    if state_key not in st.session_state:
        raw = _read_query_param(query_key)
        if raw is not None:
            option_lookup = {str(option): option for option in options}
            resolved = [option_lookup[item] for item in raw.split(separator) if item in option_lookup]
        else:
            resolved = list(default) if default is not None else list(options)
        st.session_state[state_key] = resolved
    else:
        st.session_state[state_key] = [item for item in st.session_state[state_key] if item in options]
    value = st.multiselect(label, options=options, key=state_key, **kwargs)
    _write_query_param(query_key, separator.join(str(item) for item in value))
    return value


def persistent_slider(
    label: str,
    *,
    query_key: str,
    min_value: int,
    max_value: int,
    default: int,
    step: int = 1,
    **kwargs,
) -> int:
    state_key = _persistent_state_key(query_key)
    if state_key not in st.session_state:
        raw = _read_query_param(query_key)
        resolved = default
        if raw is not None:
            try:
                candidate = int(raw)
                if min_value <= candidate <= max_value:
                    resolved = candidate
            except (TypeError, ValueError):
                pass
        st.session_state[state_key] = resolved
    value = st.slider(
        label,
        min_value=min_value,
        max_value=max_value,
        step=step,
        key=state_key,
        **kwargs,
    )
    _write_query_param(query_key, str(value))
    return value


def persistent_segmented_control(
    label: str,
    options: list,
    *,
    query_key: str,
    default,
    **kwargs,
):
    state_key = _persistent_state_key(query_key)
    if state_key not in st.session_state:
        raw = _read_query_param(query_key)
        resolved = default
        if raw is not None:
            for option in options:
                if str(option) == raw:
                    resolved = option
                    break
        st.session_state[state_key] = resolved
    elif options and st.session_state[state_key] not in options:
        st.session_state[state_key] = default
    value = st.segmented_control(label, options=options, key=state_key, **kwargs)
    _write_query_param(query_key, "" if value is None else str(value))
    return value


def persistent_toggle(
    label: str,
    *,
    query_key: str,
    default: bool = False,
    **kwargs,
) -> bool:
    state_key = _persistent_state_key(query_key)
    if state_key not in st.session_state:
        raw = _read_query_param(query_key)
        if raw in ("1", "true", "True"):
            st.session_state[state_key] = True
        elif raw in ("0", "false", "False"):
            st.session_state[state_key] = False
        else:
            st.session_state[state_key] = default
    value = st.toggle(label, key=state_key, **kwargs)
    _write_query_param(query_key, "1" if value else "0")
    return value


def render_page_header(title: str, *, kicker: str = "", subtitle: str = "") -> None:
    """Branded page header: green kicker, bold title, subtitle, accent rule.

    Shared by every page so headers stay visually consistent with the Maple
    Tree theme without each page carrying its own header CSS.
    """
    st.markdown(
        """
        <style>
        .mt-page-header {
            margin: 0.1rem 0 1rem 0;
        }
        .mt-page-kicker {
            color: #15803d;
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            margin-bottom: 0.1rem;
        }
        .mt-page-title {
            color: #14532d;
            font-size: 2.05rem;
            font-weight: 800;
            line-height: 1.1;
        }
        .mt-page-subtitle {
            color: #6b7280;
            font-size: 0.95rem;
            margin-top: 0.3rem;
        }
        .mt-page-rule {
            width: 52px;
            height: 4px;
            border-radius: 999px;
            background: #15803d;
            margin-top: 0.55rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    kicker_html = f'<div class="mt-page-kicker">{escape(kicker)}</div>' if kicker else ""
    subtitle_html = f'<div class="mt-page-subtitle">{escape(subtitle)}</div>' if subtitle else ""
    st.markdown(
        f"""
        <div class="mt-page-header">
          {kicker_html}
          <div class="mt-page-title">{escape(title)}</div>
          {subtitle_html}
          <div class="mt-page-rule"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_data_freshness_caption(
    freshness: dict[str, object] | None,
    *,
    label: str = "Stats current through",
    empty_message: str = "No completed games loaded yet for this season.",
) -> None:
    if not freshness:
        st.caption(empty_message)
        return
    summary = str(freshness.get("summary") or "").strip()
    if not summary:
        st.caption(empty_message)
        return
    st.caption(f"{label} {summary}")


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
