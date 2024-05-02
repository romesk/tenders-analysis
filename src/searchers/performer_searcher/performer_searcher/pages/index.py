"""Welcome to Reflex! This file outlines the steps to create a basic app."""
from typing import Tuple, List
import sys

sys.path.append("../../../src")
from performer_searcher.templates import ThemeState, template
import pandas as pd

from config import CONFIG
from services.clickhouse import ClickhouseService
from reflex.components.radix.themes.components.card import Card
from reflex.components.radix.themes.layout import Flex

from rxconfig import config
import reflex as rx

clickhouse = ClickhouseService(CONFIG.CLICKHOUSE.HOST, CONFIG.CLICKHOUSE.USER, CONFIG.CLICKHOUSE.PASSWORD)
tender_text_style = {
    "margin-left": "10px",
    "margin-right": "20px",
}
dk_division_text_style = {
    # "margin-left": "20px"
}
dk_group_text_style = {
    # "margin-left": "110px",
}
dk_class_text_style = {
    # "margin-left": "120px"
}
tenders_box_style = {
    "width": "100%"
}
switcher_style = {
    "margin-bottom": "30px"
}


def tender(title, initials: str, genre: str):
    return rx.card(
        rx.flex(
            rx.flex(
                rx.avatar(fallback="TOB", style=avatar_style),
                rx.flex(
                    rx.text(title, size="3", weight="bold"),
                    rx.text(
                        "mail: asdasds", size="1", color_scheme="gray"
                    ),
                    rx.text(
                        "phone: 123123123", size="1", color_scheme="gray"
                    ),
                    direction="column",
                    spacing="2",
                    style=tender_text_style,
                ),
                direction="row",
                align_items="center",
                spacing="2",
                style=tenders_box_style

            ),
            justify="between",
            style=tenders_box_style
        ),
        style=tenders_box_style
    )


class MainController(rx.State):
    tender_id: str = "1"
    df: pd.DataFrame = pd.DataFrame()
    data: List = [

    ]
    columns: List[str] = ["Performer ID"]
    dk: List[str] = [[]]

    def get_dk_hierarhy(self):
        self.dk = clickhouse.query(
            f"select division, group, class_name from default.TenderInfo where tender_id = '{self.tender_id}'").values.tolist()
        print(self.dk)


@template(route="/", title="Home")
def index() -> Flex:
    return (
        rx.flex(
            rx.flex(
                rx.input(
                    placeholder="Input Tender ID...",
                    on_change=MainController.set_tender_id
                ),
                rx.button(
                    "Get DK-s",
                    color_scheme="grass",
                    on_click=MainController.get_dk_hierarhy,
                ),
                rx.flex(
                    rx.flex(
                        rx.text("Division"),
                        rx.input(
                            placeholder="Input Tender ID...",
                            value=MainController.dk[0][0],
                            #on_change=MainController.set_tender_id
                        ),
                        direction="column",
                        align="center",
                        style=tender_text_style,
                    ),
                    rx.flex(
                        rx.text("Group", style=dk_group_text_style),
                        rx.input(
                            placeholder="Input Tender ID...",
                            value=MainController.dk[0][1],
                            #on_change=MainController.set_tender_id
                        ),
                        direction="column",
                        align="center",
                        style=tender_text_style),
                    rx.flex(
                        rx.text("Class", style=dk_class_text_style),
                        rx.input(
                            placeholder="Input Tender ID...",
                            value=MainController.dk[0][2],
                            #on_change=MainController.set_tender_id,
                        ),
                        direction="column",
                        align="center",
                        style=tender_text_style,
                    ),
                    direction="row",
                    spacing="2",
                    style=tender_text_style,
                ),
                direction="column",
                spacing="2",
                style=tender_text_style,
            ),
            rx.card(
                rx.flex(
                    rx.data_table(
                        data=MainController.data,
                        columns=MainController.columns,
                        pagination=True,
                        search=True,
                        sort=True,

                    ),
                    direction="column",
                    spacing="3",
                    align="center",
                    align_items="center"
                ),
                style=tenders_box_style,
                align="center",
                align_items="center",
            ),
            align="center",
            align_items="center",
            direction="column",
            style=tenders_box_style
        )

    )


app = rx.App()
app.add_page(index)
