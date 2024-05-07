"""The dashboard page."""
from datetime import datetime, timedelta
from typing import List

from performer_searcher.templates import template, ThemeState

import reflex as rx
from rich import region

from config import CONFIG
from services import MongoService, ClickhouseService

mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
clickhouse = ClickhouseService(CONFIG.CLICKHOUSE.HOST, CONFIG.CLICKHOUSE.USER, CONFIG.CLICKHOUSE.PASSWORD)


def get_all_regions():
    query = "SELECT distinct (SA.region_name) FROM TenderClosed TC JOIN TenderInfo TI ON TC.tender_id = TI.tender_id INNER JOIN StreetAddress SA ON TI.location = SA.id"
    return [item[0] for item in clickhouse.query(query).values.tolist()]


def generate_month():
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    # Generate months for the current year
    current_year_months = [f"{current_year}-{str(month).zfill(2)}" for month in range(1, current_month + 1)]

    return sorted(current_year_months, reverse=True)


class TransactionController(rx.State):
    region = "All"
    month = "All"
    processing = False
    total_amount = -1
    participant_count = -1
    avg_duration = -1
    table_data = []

    def get_tender_closed_regions_month(self):
        self.processing = True
        yield
        filter = "WHERE "
        region_filter = None
        month_filter = None

        if self.region != "All":
            region_filter = f"region_name = '{self.region}'"

        if self.month != "All":
            month_filter = f"month = '{self.month}'"

        # Construct the WHERE clause
        if region_filter and month_filter:
            filter += f"{region_filter} AND {month_filter}"
        elif region_filter:
            filter += region_filter
        elif month_filter:
            filter += month_filter

        # Construct the query
        query = "SELECT SUM(amount), SUM(participant_count), AVG(duration) FROM TenderClosed TC JOIN TenderInfo TI ON TC.tender_id = TI.tender_id INNER JOIN StreetAddress SA ON TI.location = SA.id INNER JOIN DateDim DD ON DD.day = TC.close_time_id"

        if filter != "WHERE ":
            query += " " + filter

        query_response = clickhouse.query(query).values.tolist()[0]
        print(query_response)
        try:
            self.total_amount = int(query_response[0])
            self.participant_count = query_response[1]
            self.avg_duration = int(query_response[2])
            query = "SELECT title, amount, open_time_id, duration, participant_count, performer_id FROM TenderClosed TC JOIN TenderInfo TI ON TC.tender_id = TI.tender_id INNER JOIN StreetAddress SA ON TI.location = SA.id INNER JOIN DateDim DD ON DD.day = TC.close_time_id"
            if filter != "WHERE ":
                query += " " + filter
            self.table_data = clickhouse.query(query).values.tolist()
            print(self.table_data)
        except:
            self.total_amount = -1
            self.participant_count = -1
            self.avg_duration = -1
            self.table_data = []
        self.processing = False



@template(route="/transactions", title="Transaction Logs")
def transtactions() -> rx.Component:
    """The dashboard page.

    Returns:
        The UI for the dashboard page.
    """
    return rx.flex(
        rx.flex(
            rx.flex(
                rx.text("Region", weight="medium"),
                rx.select(["All"] + get_all_regions(), placeholder="Select region",
                          on_change=TransactionController.set_region,
                          default_value="All", width="8em"),
                direction="row",
                spacing="3",
                align="center"
            ),
            rx.flex(
                rx.text("Month", weight="medium"),
                rx.select(["All"] + generate_month(), placeholder="Select month",
                          on_change=TransactionController.set_month,
                          default_value="All", width="8em"),
                direction="row",
                spacing="3",
                align="center"
            ),
            rx.button("Filter", on_click=TransactionController.get_tender_closed_regions_month,
                      style={"width": "8em"}, ),
            align="center",
            justify="center",
            spacing="5",
            direction="row",
            bg=rx.color(ThemeState.accent_color, 3),
            style={"max-width": "67em", "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                   "border-radius": "0.5em", "margin-bottom": "0.5em", "padding": "0.5em"}
        ),
        rx.flex(
            rx.flex(
                rx.flex(
                    rx.text("Amount of money", style={"width": "10em", "margin-left": "0.5em"}, weight="medium"),
                    rx.cond(TransactionController.total_amount != -1,
                            rx.flex(
                                rx.text(f"{TransactionController.total_amount}"), rx.text("UAH", weight="medium"),
                                spacing="2"
                            ),
                            rx.text("")
                            ),
                    direction="row",
                    spacing="5",
                    align="center",
                    style={"width": "20em", "padding": "1em",
                           "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                           "border-radius": "0.5em", "margin-bottom": "0.5em", "padding": "0.5em"}
                ),
                rx.flex(
                    rx.text("Participant count", style={"width": "10em", "margin-left": "0.5em"}, weight="medium"),
                    rx.cond(TransactionController.participant_count != -1,
                            rx.flex(
                                rx.text(f"{TransactionController.participant_count}"),
                                rx.text("People", weight="medium"),
                                spacing="2"
                            ),
                            rx.text("")),
                    direction="row",
                    spacing="5",
                    align="center",
                    style={"width": "20em", "padding": "1em",
                           "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                           "border-radius": "0.5em", "margin-bottom": "0.5em", "padding": "0.5em"}
                ),
                rx.flex(
                    rx.text("Average duration", style={"width": "10em", "margin-left": "0.5em"}, weight="medium"),
                    rx.cond(TransactionController.avg_duration != -1,
                            rx.flex(
                                rx.text(f"{TransactionController.avg_duration}"), rx.text("Days", weight="medium"),
                                spacing="2"
                            ),
                            rx.text("")),
                    direction="row",
                    spacing="5",
                    align="center",
                    style={"width": "20em", "padding": "1em",
                           "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                           "border-radius": "0.5em", "margin-bottom": "0.5em", "padding": "0.5em"}
                ),
                direction="row",
                spacing="6",
                style={"margin-top": "0.5em"}
            ),
            rx.flex(
                rx.cond(
                    TransactionController.processing,
                    rx.chakra.circular_progress(is_indeterminate=True, color="gray"),
                ),
                rx.data_table(
                    data=TransactionController.table_data,
                    columns=["Tender Desc", "Amount", "Open Time", "Duration", "Participants", "Performer"],
                    pagination=True,
                    search=True,
                    sort=True,
                    align='center',
                    resizable=True,
                    key="Region"
                ),
                direction="column",
                style={"max-width": "67em", "overflow-x": "auto"},
            ),
            style={"max-width": "67em", "overflow-x": "auto", "width": "67em",
                   "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                   "border-radius": "0.5em", "margin-bottom": "0.5em", "padding": "1em"},
            direction="column",
            align="center",
            justify="center",
            spacing="2",

        ),
        style={"max-width": "67em"},
        direction="column",
    )
