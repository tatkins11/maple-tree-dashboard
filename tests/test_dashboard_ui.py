from src.dashboard.ui import (
    RESPONSIVE_AUTO,
    RESPONSIVE_FULL,
    RESPONSIVE_MOBILE,
    resolve_responsive_layout_mode,
)


def test_responsive_auto_uses_mobile_on_narrow_screens() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_AUTO, detected_width=390) is True


def test_responsive_auto_uses_full_on_wider_screens() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_AUTO, detected_width=1024) is False


def test_responsive_auto_falls_back_to_full_when_width_unknown() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_AUTO, detected_width=None) is False


def test_responsive_mobile_override_always_enables_mobile_layout() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_MOBILE, detected_width=None) is True
    assert resolve_responsive_layout_mode(RESPONSIVE_MOBILE, detected_width=1440) is True


def test_responsive_full_override_always_disables_mobile_layout() -> None:
    assert resolve_responsive_layout_mode(RESPONSIVE_FULL, detected_width=None) is False
    assert resolve_responsive_layout_mode(RESPONSIVE_FULL, detected_width=375) is False
