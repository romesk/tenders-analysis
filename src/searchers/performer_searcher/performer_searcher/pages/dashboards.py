"""Welcome to Reflex! This file outlines the steps to create a basic app."""
import datetime
import random
import typing
from collections import defaultdict
from typing import Tuple, List
import sys
import re

from dateutil.relativedelta import relativedelta
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


class DashboardController(rx.State):
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
    start_of_year = datetime.date(2024, 1, 1)
    start_period_tender = start_of_year
    end_period_tender = datetime.date.today()
    days_from_year_start: int = datetime.datetime.now().timetuple().tm_yday

    def set_range(self, ranges):
        self.start_period_tender = self.start_of_year + relativedelta(days=ranges[0])
        self.end_period_tender = self.start_of_year + relativedelta(days=ranges[1] - 1)

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
            rx.button(title, size="2", weight="bold", on_click=DashboardController.set_division(title),
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
            rx.button(title, size="2", weight="bold", on_click=DashboardController.set_group(title),
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
            rx.button(title, size="2", weight="bold", on_click=DashboardController.set_class_name(title),
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


@template(route="/dashboards", title="Dashboards")
def dashboards() -> Flex:
    return (
        rx.flex(
            rx.flex(
                rx.flex(
                    rx.flex(
                        rx.flex(
                            rx.flex(
                                rx.text("Division", style={"margin-top": "0.5em"}, weight="medium"),
                                rx.input(
                                    placeholder="Input Division...",
                                    value=DashboardController.division,
                                    on_change=[DashboardController.set_division,
                                               DashboardController.get_div_suggestions,
                                               DashboardController.set_group(''),
                                               DashboardController.delete_group_suggestions(''),
                                               DashboardController.set_class_name(''),
                                               DashboardController.delete_class_name_suggestions('')],
                                    on_click=DashboardController.get_div_suggestions(''),
                                    on_double_click=DashboardController.delete_div_suggestions(),
                                    style={"width": "30em", "border-color": "darkgray"},
                                ),
                                rx.flex(
                                    rx.foreach(
                                        DashboardController.div_suggestions,
                                        div_suggestion,
                                    ),
                                    direction='column',
                                    style={"z-index": "20", "position": "absolute", "top": "11.8em"},
                                    on_click=DashboardController.delete_div_suggestions,
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
                                    value=DashboardController.group,
                                    on_change=[DashboardController.set_group, DashboardController.get_group_suggestions,
                                               DashboardController.set_class_name(''),
                                               DashboardController.delete_class_name_suggestions('')],
                                    on_click=DashboardController.get_group_suggestions(''),
                                    on_double_click=DashboardController.delete_group_suggestions(),
                                    style={"width": "30em", "border-color": "darkgray"},
                                ),
                                rx.flex(
                                    rx.foreach(
                                        DashboardController.group_suggestions,
                                        group_suggestion,
                                    ),
                                    direction='column',
                                    style={"z-index": "15", "position": "absolute", "top": "16.1em"},
                                    on_click=DashboardController.delete_group_suggestions
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
                                    value=DashboardController.class_name,
                                    on_change=[DashboardController.set_class_name,
                                               DashboardController.get_class_name_suggestions],
                                    on_click=DashboardController.get_class_name_suggestions(''),
                                    on_double_click=DashboardController.delete_class_name_suggestions(),
                                    style={"width": "30em", "border-color": "darkgray"},
                                ),
                                rx.flex(
                                    rx.foreach(
                                        DashboardController.class_name_suggestions,
                                        class_name_suggestion,
                                    ),
                                    direction='column',
                                    style={"z-index": "10", "position": "absolute", "top": "20.3em"},
                                    on_click=DashboardController.delete_class_name_suggestions,
                                ),
                                rx.flex(
                                    rx.flex(
                                        rx.image(src="/info.png", style={"width": "2.5em", "height": "2.5em"}),
                                        rx.text("Zero Closed Tenders Found", size="2", weight="medium"),
                                        direction="row",
                                        align="center",
                                        spacing="2"
                                    ),
                                    rx.button("Close",
                                              on_click=[DashboardController.change_performer_error_style("hidden")],
                                              bg="lightgray",
                                              style={"color": "black", "border": f"0.13em solid gray",
                                                     "border-radius": "0.5em"}),
                                    style={"z-index": "25", "position": "absolute", "padding": "0.6em",
                                           "margin-top": "0em",
                                           "visibility": DashboardController.performer_error_style,
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
                                    "Find Closed Tenders",
                                    color_scheme="grass",
                                    on_click=DashboardController.search_performers,
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
                            style={
                                "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                                "border-radius": "0.5em"},
                            bg=rx.color(ThemeState.accent_color, 4)
                        ),
                        rx.flex(
                            rx.text(DashboardController.start_period_tender, style={"width": "8em"}, weight="medium",
                                    align="center"),
                            rx.slider(default_value=[0, DashboardController.days_from_year_start], min=0,
                                      max=DashboardController.days_from_year_start,
                                      on_change=[DashboardController.set_range], width="15em"),
                            rx.text(DashboardController.end_period_tender, style={"width": "8em"}, weight="medium",
                                    align="center"),
                            direction="row",
                            align="center",
                            justify="center",
                            style={"border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                                   "border-radius": "0.5em", "padding": "0.7em"},
                            bg=rx.color(ThemeState.accent_color, 4),
                            width="30.3em"

                        ),
                        direction="column",
                        spacing="2",
                        justify="start",
                        align="start",
                        style={"margin-bottom": "0.5em"},
                    ),
                    rx.flex(
                        rx.text("Closed tenders count in Region", align="right", weight="medium", size="3",
                                style={"padding-right": "1em", "padding-top": "0.5em"}),
                        rx.flex(
                            rx.recharts.bar_chart(
                                rx.recharts.bar(
                                    data_key="count", stroke="black", fill="white", stroke_width=1,
                                    bar_size=DashboardController.bar_size
                                ),
                                rx.recharts.x_axis(data_key="name"),
                                rx.recharts.y_axis(allow_decimals=False),
                                data=DashboardController.graph_data,
                                max_bar_size=26,
                                width=1200,
                                height=280,

                            ),
                            style={"max-height": "20em", "max-width": "40em", "overflow-x": "scroll"}
                        ),
                        style={"max-height": "20em", "max-width": "40em",
                               "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                               "border-radius": "0.5em"},
                        bg=rx.color(ThemeState.accent_color, 4),
                        direction="column"
                    ),
                    direction="row",
                    spacing="2"
                ),
                rx.recharts.line_chart(rx.recharts.line(
                    data_key="pv",
                    stroke="#8884d8",
                ),

                    rx.recharts.x_axis(data_key="name"),
                    rx.recharts.y_axis(),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3"),
                    rx.recharts.graphing_tooltip(),
                    rx.recharts.legend(),
                    data=data,
                    height=400),
                spacing="1",
                direction="column",
                justify="center",
                align="center"
            ),
        )
    )


data = [
    {"name": "Page A", "uv": 4000, "pv": 2400, "amt": 2400},
    {"name": "Page B", "uv": 3000, "pv": 1398, "amt": 2210},
    {"name": "Page C", "uv": 2000, "pv": 9800, "amt": 2290},
    {"name": "Page D", "uv": 2780, "pv": 3908, "amt": 2000},
    {"name": "Page E", "uv": 1890, "pv": 4800, "amt": 2181},
    {"name": "Page F", "uv": 2390, "pv": 3800, "amt": 2500},
    {"name": "Page G", "uv": 3490, "pv": 4300, "amt": 2100},
]
app = rx.App()
app.add_page(dashboards)
