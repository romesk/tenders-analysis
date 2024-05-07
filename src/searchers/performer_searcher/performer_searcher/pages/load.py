import json
from typing import List, Dict

from performer_searcher.templates import template

import reflex as rx

@template(route="/load", title="Data Load")
def load() -> rx.Component:
    return rx.flex(
        rx.button("Load to Mongo", size="4"),
        rx.button("Load to Clickhouse", size="4"),
        spacing="5",
        align="center",
        justify="center"
    )


