"""The dashboard page."""
from typing import List

from performer_searcher.templates import template, ThemeState

import reflex as rx

from config import CONFIG
from services import MongoService

mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)


class TransactionController(rx.State):
    run_logs_data: List[str] = []
    run_logs_data_columns: List[str] = ["coll_id", "collection", "operation", "run_id"]
    run_data: List[str] = []
    run_data_columns: List[str] = ["run_id", "start_dt", "end_dt"]

    def get_run_logs_data(self):
        run_logs = mongo.find(CONFIG.MONGO.RUNS_LOGS_COLLECTION, {})
        for log in run_logs[:200]:
            key_id = ""
            if log["collection"] == "tenders":
                key_id = mongo.find_one(CONFIG.MONGO.TENDERS_COLLECTION, {"_id": log["object_id"]})["tenderID"]
            elif log["collection"] == "entities":
                key_id = mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"_id": log["object_id"]})["edrpou"]
            self.run_logs_data.append([str(key_id), str(log["collection"]), str(log["operation"]), str(log["run_id"])])

    def get_runs(self):
        runs = mongo.find(CONFIG.MONGO.RUNS_COLLECTION, {})
        for run in runs[:200]:
            self.run_data.append([str(run["run_id"]), str(run["start"]), str(run["end"])])


@template(route="/transactions", title="Transaction Logs")
def transtactions() -> rx.Component:
    """The dashboard page.

    Returns:
        The UI for the dashboard page.
    """
    return rx.flex(
        rx.button("Get Logs", on_click=[TransactionController.get_run_logs_data, TransactionController.get_runs],
                  style={"max-width": "8em", "margin-bottom": "0.5em"}),
        rx.flex(
            rx.card(
                rx.text("Run Logs", align="right", weight="medium", size="5"),
                rx.flex(
                    rx.data_table(
                        data=TransactionController.run_logs_data,
                        columns=TransactionController.run_logs_data_columns,
                        pagination=True,
                        search=True,
                        sort=True,
                        align='center',
                        resizable=True,
                        key="coll_id",

                    ),
                    style={"max-width": "33.5em", "overflow-x": "auto"},
                    direction="column",

                ),
                bg=rx.color(ThemeState.accent_color, 3)

            ),
            rx.card(
                rx.text("Runs", align="right", weight="medium", size="5"),
                rx.flex(
                    rx.data_table(
                        data=TransactionController.run_data,
                        columns=TransactionController.run_data_columns,
                        pagination=True,
                        search=True,
                        sort=True,
                        align='center',
                        resizable=True,
                        key="run_id",
                    ),
                    style={"max-width": "33.5em", "overflow-x": "auto"},
                ),
                bg=rx.color(ThemeState.accent_color, 3)

            ),
            spacing="1",
            direction="row",
            align="start",
            align_items="start",
            justify="start",
        ),
        direction="column",
    )
