from __future__ import annotations

import re


_MULTISPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")


def normalize_player_name(name: str) -> str:
    cleaned = _MULTISPACE_RE.sub(" ", name.strip().lower())
    cleaned = _NON_ALNUM_RE.sub("", cleaned)
    return _MULTISPACE_RE.sub(" ", cleaned).strip()


def display_player_name(name: str) -> str:
    collapsed = _MULTISPACE_RE.sub(" ", name.strip())
    return " ".join(part.capitalize() for part in collapsed.split(" "))
