"""Welcome to Reflex! This file outlines the steps to create a basic app."""
import random
import typing
from collections import defaultdict
from typing import Tuple, List
import sys
import re
from reflex.style import Style

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

suggestion_style: Style = Style(
    {'z-index': "10", 'width': '10em', "max-width": "10em", "position": "absolute",
     "margin-top": "3.5em", 'visibility': "visible"})


class MainController(rx.State):
    tender_id: str = ""
    pattern = r'^UA-\d{4}-\d{2}-\d{2}-\d{6}-[a-z]$'
    is_bad_tender = False
    tender_error_style = "hidden"
    performer_error_style = "hidden"
    bar_size = 1
    df: pd.DataFrame = pd.DataFrame()
    performers: List = []
    performer_columns: List[str] = ["Type", "Name", "Region", "Phone", "Email"]
    division: str = ""
    group: str = ""
    class_name: str = ""
    div_suggestions = []
    group_suggestions = []
    class_name_suggestions = []
    graph_data = [{"name": "", "count": 0}]
    processing = False
    def get_dk_hierarhy(self):
        if self.validate_tender_id(self.tender_id):
            dk = clickhouse.query(
                f"select division, group, class_name from default.TenderInfo where tender_id = '{self.tender_id}'").values.tolist()
            self.division = dk[0][0]
            self.group = dk[0][1]
            self.class_name = dk[0][2]
            print(dk)
        else:
            self.tender_error_style = "visible"
            self.is_bad_tender = True

    def get_div_suggestions(self, text):
        dk = [suggestion[0] for suggestion in clickhouse.query(
            f"select distinct (division) from default.TenderInfo where division LIKE '{self.division}%' and division != 'n/a' order by division limit 10").values.tolist()]
        self.div_suggestions = dk
        print(dk)

    def get_group_suggestions(self, text):
        dk = [suggestion[0] for suggestion in clickhouse.query(
            f"select distinct (group) from default.TenderInfo where group LIKE '{self.group}%' and division = '{self.division}' and group != 'n/a' order by group limit 10").values.tolist()]
        self.group_suggestions = dk
        print(dk)

    def get_class_name_suggestions(self, text):
        dk = [suggestion[0] for suggestion in clickhouse.query(
            f"select distinct (class_name) from default.TenderInfo where class_name LIKE '{self.class_name}%' and group = '{self.group}' and division = '{self.division}' and class_name != 'n/a' order by class_name limit 10").values.tolist()]
        self.class_name_suggestions = dk
        print(dk)

    def search_performers(self):
        self.performers = []
        self.processing = True
        yield
        try:
            kveds = mongo.find_one(CONFIG.MONGO.DK_TO_KVED_COLLECTION,
                                   {'division': self.division, 'group': self.group, 'class_name': self.class_name})[
                'kveds']
            kveds_str = "', '".join(kveds)
        except:
            kveds_str = "''"
            self.performer_error_style = "visible"
            self.graph_data = [{"name": "", "count": 0}]

        else:
            self.performers = clickhouse.query(
                f"select P.organization_type, P.organization_name, SA.region_name, P.organization_phone, P.organization_email from default.Performer as P inner join default.StreetAddress as SA on SA.id = P.location where class_code in ({kveds_str})").values.tolist()
            print(self.performers)
            grouped_data = defaultdict(int)
            for entry in self.performers:
                region = entry[2]
                grouped_data[region] += 1
            self.bar_size = int(800 / len(grouped_data))
            unsorted_graph = [{"name": region[:3], "count": count} for region, count in grouped_data.items()]
            self.graph_data = sorted(unsorted_graph, key=lambda x: x["count"], reverse=True)
        self.processing = False

    def div_suggestion_clicked(self, suggestion, type):
        self.division = suggestion

    def delete_div_suggestions(self):
        self.div_suggestions = []

    def delete_group_suggestions(self):
        self.group_suggestions = []

    def delete_class_name_suggestions(self):
        self.class_name_suggestions = []

    def validate_tender_id(self, tender_id):
        return bool(re.match(self.pattern, tender_id))

    def change_tender_error_style(self, value):
        self.tender_error_style = value

    def change_performer_error_style(self, value):
        self.performer_error_style = value


def div_suggestion(title):
    return (
        rx.flex(
            rx.button(title, size="2", weight="bold", on_click=MainController.set_division(title),
                      style={"text-wrap": "pretty", 'width': '30em', "max-width": "30em", "padding": "0.5em",
                             'height': '4em', "max-height": "8em", "text-align": "center",
                             "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                             "border-radius": "0.5em"}
                      ),
            direction="row",
            spacing="2",
            justify="start",
            align="start"
        ))


def group_suggestion(title):
    return (
        rx.flex(
            rx.button(title, size="2", weight="bold", on_click=MainController.set_group(title),
                      style={"text-wrap": "pretty", 'width': '30em', "max-width": "30em", "padding": "0.5em",
                             'height': '4em', "max-height": "8em", "text-align": "center",
                             "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                             "border-radius": "0.5em"}
                      ),
            direction="row",
            spacing="1",
            justify="start",
            align="start"
        ))


def class_name_suggestion(title):
    return (
        rx.flex(
            rx.button(title, size="2", weight="bold", on_click=MainController.set_class_name(title),
                      style={"text-wrap": "pretty", 'width': '30em', "max-width": "30em", "padding": "0.5em",
                             'height': '4em', "max-height": "8em", "text-align": "center",
                             "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                             "border-radius": "0.5em"}
                      ),
            direction="row",
            spacing="1",
            justify="start",
            align="start"
        ))


@template(route="/", title="Home")
def index() -> Flex:
    return (
        rx.flex(
            rx.flex(
                rx.flex(
                    rx.flex(
                        rx.flex(
                            rx.input(
                                placeholder="Input Tender ID...",
                                on_change=MainController.set_tender_id,
                                on_click=[MainController.change_tender_error_style("hidden"),
                                          MainController.set_is_bad_tender(False)],
                                style={"width": "18.5em",
                                       "border-color": rx.cond(MainController.is_bad_tender, "red", "darkgray")},
                                min_length=22,
                            ),
                            rx.flex(
                                rx.flex(
                                    rx.image(src="/error.png", style={"width": "2.5em", "height": "2.5em"}),
                                    rx.text("Incorrect Tender ID entered!", size="2", weight="medium"),
                                    direction="row",
                                    align="center",
                                    spacing="2"
                                ),
                                rx.button("Close", on_click=[MainController.change_tender_error_style("hidden"),
                                                             MainController.set_is_bad_tender(False)], bg="red",
                                          fg="green"),
                                style={"z-index": "25", "position": "absolute", "padding": "0.6em", "margin-top": "2em",
                                       "visibility": MainController.tender_error_style, "border": f"0.13em solid red",
                                       "border-radius": "0.5em"},
                                bg=rx.color("red", 7),
                                direction="column",
                                spacing="2",
                            ),

                            rx.button(
                                "Get DK from Tender",
                                on_click=MainController.get_dk_hierarhy,
                                bg=rx.color(ThemeState.accent_color, 9),
                                style={"color":"black"},
                            ),
                            style={"margin-bottom": "0.5em", "margin-top": "0.5em"},
                            spacing="2",
                        ),
                        justify="center",
                        style={"width": "28em", "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                               "border-radius": "0.5em"},
                        bg=rx.color(ThemeState.accent_color, 4)
                    ),
                    rx.flex(
                        rx.flex(
                            rx.text("Division", style={"margin-top": "0.5em"}, weight="medium"),
                            rx.input(
                                placeholder="Input Division...",
                                value=MainController.division,
                                on_change=[MainController.set_division, MainController.get_div_suggestions,
                                           MainController.set_group(''), MainController.delete_group_suggestions(''),
                                           MainController.set_class_name(''),
                                           MainController.delete_class_name_suggestions('')],
                                on_click=MainController.get_div_suggestions(''),
                                on_double_click=MainController.delete_div_suggestions(),
                                style={"width": "30em", "border-color": "darkgray"},
                            ),
                            rx.flex(
                                rx.foreach(
                                    MainController.div_suggestions,
                                    div_suggestion,
                                ),
                                direction='column',
                                style={"z-index": "20", "position": "absolute", "top": "11.8em"},
                                on_click=MainController.delete_div_suggestions,
                            ),
                            direction="column",
                            justify="center",
                            align="center",
                            style={"width": "30em"},
                        ),
                        rx.flex(
                            rx.text("Group", weight="medium"),
                            rx.input(
                                placeholder="Input Group...",
                                value=MainController.group,
                                on_change=[MainController.set_group, MainController.get_group_suggestions,
                                           MainController.set_class_name(''),
                                           MainController.delete_class_name_suggestions('')],
                                on_click=MainController.get_group_suggestions(''),
                                on_double_click=MainController.delete_group_suggestions(),
                                style={"width": "30em", "border-color": "darkgray"},
                            ),
                            rx.flex(
                                rx.foreach(
                                    MainController.group_suggestions,
                                    group_suggestion,
                                ),
                                direction='column',
                                style={"z-index": "15",  "position": "absolute", "top": "16.1em"},
                                on_click=MainController.delete_group_suggestions
                            ),
                            direction="column",
                            align="center",
                            justify="center",
                            style={"width": "30em"},
                        ),
                        rx.flex(
                            rx.text("Class", weight="medium"),
                            rx.input(
                                placeholder="Input Class...",
                                value=MainController.class_name,
                                on_change=[MainController.set_class_name, MainController.get_class_name_suggestions],
                                on_click=MainController.get_class_name_suggestions(''),
                                on_double_click=MainController.delete_class_name_suggestions(),
                                style={"width": "30em", "border-color": "darkgray"},
                            ),
                            rx.flex(
                                rx.foreach(
                                    MainController.class_name_suggestions,
                                    class_name_suggestion,
                                ),
                                direction='column',
                                style={"z-index": "10",  "position": "absolute", "top": "20.3em"},
                                on_click=MainController.delete_class_name_suggestions,
                            ),
                            rx.flex(
                                rx.flex(
                                    rx.image(src="/info.png", style={"width": "2.5em", "height": "2.5em"}),
                                    rx.text("Zero Performers Found", size="2", weight="medium"),
                                    direction="row",
                                    align="center",
                                    spacing="2"
                                ),
                                rx.button("Close", on_click=[MainController.change_performer_error_style("hidden")],
                                          bg="lightgray",
                                          style={"color": "black", "border": f"0.13em solid gray",
                                                 "border-radius": "0.5em"}),
                                style={"z-index": "25", "position": "absolute", "padding": "0.6em", "margin-top": "0em",
                                       "visibility": MainController.performer_error_style,
                                       "border": f"0.13em solid gray",
                                       "border-radius": "0.5em", "margin-top": "6.5em"},
                                bg=rx.color("gray", 5),
                                direction="column",
                                spacing="2",

                            ),
                            direction="column",
                            align="center",
                            style={"width": "30em"},
                        ),
                        rx.flex(
                            rx.button(
                                "Find Performers",
                                color_scheme="grass",
                                on_click=MainController.search_performers,
                                bg=rx.color(ThemeState.accent_color, 9),
                                style={"margin-bottom": "1em", "color": "black"},
                            ),
                            justify="start",
                            align="center",

                        ),
                        direction="column",
                        spacing="3",
                        justify="center",
                        align="center",
                        style={"width": "28em", "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                               "border-radius": "0.5em"},
                        bg=rx.color(ThemeState.accent_color, 4)
                    ),
                    direction="column",
                    spacing="2",
                    justify="start",
                    align="start",
                    style={"margin-bottom": "0.5em"},
                ),
                rx.flex(
                    rx.text("Performers count in Region", align="right", weight="medium", size="3", style={"padding-right": "1em", "padding-top": "0.5em"}),
                    rx.flex(
                        rx.recharts.bar_chart(
                            rx.recharts.bar(
                                data_key="count", stroke="black", fill="white", stroke_width=1,
                                bar_size=MainController.bar_size
                            ),
                            rx.recharts.x_axis(data_key="name"),
                            rx.recharts.y_axis(allow_decimals=False),
                            data=MainController.graph_data,
                            max_bar_size=26,
                            width=1200,
                            height=280,

                        ),
                        style={"max-height": "20em", "max-width": "40em", "overflow-x": "scroll"}
                    ),
                    style={"max-height": "20em", "max-width": "40em",
                           "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                           "border-radius": "0.5em"},
                    direction="column",
                    bg=rx.color(ThemeState.accent_color, 4),
                ),
                direction="row",
                spacing="2"

            ),

            rx.card(
                rx.flex(
                    rx.cond(
                        MainController.processing,
                        rx.chakra.circular_progress(is_indeterminate=True, color="gray"),
                    ),
                    rx.text("Search by Region", style={"margin-left": "0.5em"}, weight="medium"),
                    rx.data_table(
                        data=MainController.performers,
                        columns=MainController.performer_columns,
                        pagination=True,
                        search=True,
                        sort=True,
                        align='center',
                        resizable=True,
                        key="Region"
                    ),
                    direction="column",
                    align="start",
                    align_items="start",
                    justify="start",
                    style={"max-width": "67em", "overflow-x": "auto"},
                ),
                style={"width": "100%", "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                   "border-radius": "0.5em"},
                align="center",
                align_items="center",
                justify="center",
                bg=rx.color(ThemeState.accent_color, 4)
            ),
            align="center",
            align_items="start",
            justify="start",
            direction="column",
            style={"width": "100%"},
        )
    )


app = rx.App()
app.add_page(index)
