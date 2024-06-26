"""Sidebar component for the app."""

from performer_searcher import styles

import reflex as rx


def sidebar_header() -> rx.Component:
    """Sidebar header.

    Returns:
        The sidebar header component.
    """
    return rx.hstack(
        # The logo.
        rx.flex(
            rx.color_mode_cond(
                light=rx.image(
                    src="/black_tender_logo.svg", height="4em", align="center"
                ),
                dark=rx.image(
                    src="/light_tender_logo.svg", height="4em", align="center"
                ),
            ),
            rx.flex(
                rx.text("P", weight="medium", size="5", align="center"),
                rx.text("er", weight="medium", size="5", align="center", style = {"color": "red"}),
                rx.text("fo", weight="medium", size="5", align="center"),
                rx.text("ro", weight="medium", size="5", align="center", style={"color": "red"}),
                align="center",
                justify="center",
                direction="row"
            ),
            align="center",
            justify="center",
            direction="column"
        ),
        align="center",
        justify="center",
        width="100%",
        border_bottom=styles.border,
        padding_x="1em",
        padding_y="2em",
    )


def sidebar_footer() -> rx.Component:
    """Sidebar footer.

    Returns:
        The sidebar footer component.
    """
    return rx.hstack(
        rx.text(
            "Developed by PetSko",
            color_scheme="gray",
            align="center"
        ),
        width="100%",
        border_top=styles.border,
        padding="1em",
        align="center",
        justify="center"
    )


def sidebar_item(text: str, url: str) -> rx.Component:
    """Sidebar item.

    Args:
        text: The text of the item.
        url: The URL of the item.

    Returns:
        rx.Component: The sidebar item component.
    """
    # Whether the item is active.
    active = (rx.State.router.page.path == url.lower()) | (
            (rx.State.router.page.path == "/") & text == "Home"
    )

    return rx.link(
        rx.hstack(
            rx.text(
                text,
            ),
            bg=rx.cond(
                active,
                rx.color("accent", 2),
                "transparent",
            ),
            border=rx.cond(
                active,
                f"1px solid {rx.color('accent', 6)}",
                f"1px solid {rx.color('gray', 6)}",
            ),
            color=rx.cond(
                active,
                styles.accent_text_color,
                styles.text_color,
            ),
            align="center",
            border_radius=styles.border_radius,
            width="100%",
            padding="1em",
        ),
        href=url,
        width="100%",
    )


def sidebar() -> rx.Component:
    """The sidebar.

    Returns:
        The sidebar component.
    """
    # Get all the decorated pages and add them to the sidebar.
    from reflex.page import get_decorated_pages

    return rx.box(
        rx.vstack(
            sidebar_header(),
            rx.vstack(
                *[
                    sidebar_item(
                        text=page.get("title", page["route"].strip("/").capitalize()),
                        url=page["route"],
                    )
                    for page in get_decorated_pages()
                ],
                width="100%",
                overflow_y="auto",
                align_items="flex-start",
                padding="1em",
            ),
            rx.spacer(),
            sidebar_footer(),
            height="100dvh",
        ),
        display=["none", "none", "block"],
        min_width=styles.sidebar_width,
        height="100%",
        position="sticky",
        top="0px",
        border_right=styles.border,
        style={"width": "17em"}
    )
