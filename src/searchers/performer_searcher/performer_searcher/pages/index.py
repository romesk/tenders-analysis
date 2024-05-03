"""Welcome to Reflex! This file outlines the steps to create a basic app."""
from typing import Tuple, List
import sys

sys.path.append("../../../src")
from performer_searcher.templates import ThemeState, template
import pandas as pd

from config import CONFIG
from services.clickhouse import ClickhouseService
from services.mongo import MongoService
from reflex.components.radix.themes.layout import Flex

import reflex as rx

clickhouse = ClickhouseService(CONFIG.CLICKHOUSE.HOST, CONFIG.CLICKHOUSE.USER, CONFIG.CLICKHOUSE.PASSWORD)
mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

# TODO styles namings
dk_params = {
    "margin-left": "10px",
    "margin-right": "20px",
}
width_100p_style = {
    "width": "100%"
}
margin_bottom_30px_style = {
    "margin-bottom": "30px"
}
searcher_style = {
    "margin-bottom": "30px",
    "width": "100%",
    "margin-left": "10px",
    "margin-right": "20px",
}


class MainController(rx.State):
    tender_id: str = "1"
    df: pd.DataFrame = pd.DataFrame()
    performers: List = [

    ]
    performer_columns: List[str] = ["Type", "Name", "Region", "Phone", "Email"]
    division: str = ""
    group: str = ""
    class_name: str = ""

    def get_dk_hierarhy(self):
        dk = clickhouse.query(
            f"select division, group, class_name from default.TenderInfo where tender_id = '{self.tender_id}'").values.tolist()
        self.division = dk[0][0]
        self.group = dk[0][1]
        self.class_name = dk[0][2]
        print(dk)

    def search_performers(self):
        kveds = mongo.find_one(CONFIG.MONGO.DK_TO_KVED_COLLECTION,
                               {'division': self.division, 'group': self.group, 'class_name': self.class_name})['kveds']
        kveds_str = "', '".join(kveds)
        self.performers = clickhouse.query(
            f"select P.organization_type, P.organization_name, SA.region_name, P.organization_phone, P.organization_email from default.Performer as P inner join default.StreetAddress as SA on SA.id = P.location where class_code in ({kveds_str})").values.tolist()


@template(route="/", title="Home")
def index() -> Flex:
    return (
        rx.flex(
            rx.flex(
                rx.flex(
                    rx.input(
                        placeholder="Input Tender ID...",
                        on_change=MainController.set_tender_id,
                    ),
                    rx.button(
                        "Get DK-s",
                        color_scheme="grass",
                        on_click=MainController.get_dk_hierarhy,
                    ),
                    justify="center",
                    spacing="2",

                ),
                rx.flex(
                    rx.flex(
                        rx.text("Division"),
                        rx.input(
                            placeholder="Input Tender ID...",
                            value=MainController.division,
                            on_change=MainController.set_division
                        ),
                        direction="column",
                        align="center",
                        style=dk_params,
                    ),
                    rx.flex(
                        rx.text("Group", style=dk_params),
                        rx.input(
                            placeholder="Input Tender ID...",
                            value=MainController.group,
                            on_change=MainController.set_group
                        ),
                        direction="column",
                        align="center",
                        style=dk_params),
                    rx.flex(
                        rx.text("Class", style=dk_params),
                        rx.input(
                            placeholder="Input Tender ID...",
                            value=MainController.class_name,
                            on_change=MainController.set_class_name,
                        ),
                        direction="column",
                        align="center",
                        style=dk_params,
                    ),
                    rx.flex(
                        rx.button(
                            "Get Performers",
                            color_scheme="grass",
                            on_click=MainController.search_performers
                        ),
                        justify="center",
                        align="end",
                    ),
                    direction="row",
                    spacing="2",
                    justify="center",
                    style=dk_params,
                ),
                direction="column",
                spacing="2",
                justify="center",
                style=searcher_style,
            ),
            rx.card(
                rx.flex(
                    rx.data_table(
                        data=MainController.performers,
                        columns=MainController.performer_columns,
                        pagination=True,
                        search=True,
                        sort=True,
                        align='center',
                        resizable=True,
                    ),
                    direction="column",
                    spacing="3",
                    align="start",
                    align_items="start",
                    justify="start",
                    style={"max-width": "65em", "overflow-x": "auto"},
                ),
                style={"width": "100%"},
                align="center",
                align_items="center",
                justify="center"
            ),
            align="center",
            align_items="center",
            direction="column",
            style={"width": "100%"}
        )

    )


app = rx.App()
app.add_page(index)
