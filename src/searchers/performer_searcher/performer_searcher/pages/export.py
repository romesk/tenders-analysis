"""The dashboard page."""

from performer_searcher.templates import template

import reflex as rx


@template(route="/export", title="Export")
def export() -> rx.Component:
    """The dashboard page.

    Returns:
        The UI for the dashboard page.
    """
    return rx.vstack(
        rx.heading("Export", size="8"),
        rx.text("Welcome to Reflex!"),
        rx.text(
            "You can edit this page in ",
            rx.code("{your_app}/pages/export.py"),
        ),
    )
