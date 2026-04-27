from src.dashboard.auth import (
    ROLE_ADMIN,
    ROLE_NONE,
    ROLE_VIEWER,
    AuthConfig,
    VIEWER_HIDDEN_PAGE_SLUGS,
    role_based_sidebar_css,
    resolve_role_for_password,
)


def test_resolve_role_for_admin_and_viewer_passwords() -> None:
    config = AuthConfig(
        app_mode="hosted",
        viewer_password="team-pass",
        admin_password="manager-pass",
    )

    assert resolve_role_for_password("manager-pass", config) == ROLE_ADMIN
    assert resolve_role_for_password("team-pass", config) == ROLE_VIEWER
    assert resolve_role_for_password("wrong", config) == ROLE_NONE


def test_admin_password_wins_if_passwords_overlap() -> None:
    config = AuthConfig(
        app_mode="hosted",
        viewer_password="shared-pass",
        admin_password="shared-pass",
    )

    assert resolve_role_for_password("shared-pass", config) == ROLE_ADMIN


def test_role_based_sidebar_css_hides_manager_pages_for_viewers() -> None:
    css = role_based_sidebar_css(ROLE_VIEWER)

    assert "<style>" in css
    for slug in VIEWER_HIDDEN_PAGE_SLUGS:
        assert slug in css
    assert "display: none" in css


def test_role_based_sidebar_css_is_empty_for_admins() -> None:
    assert role_based_sidebar_css(ROLE_ADMIN) == ""
