"""The dashboard page."""
from datetime import datetime
from typing import List

from performer_searcher.templates import template, ThemeState

import reflex as rx

from config import CONFIG
from services import MongoService

mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)


class TransactionController(rx.State):
    run_logs_data: List[str] = []
    run_logs_data_columns: List[str] = ["description", "period_start", "period_end"]
    logs_info_style = "hidden"
    transaction_type = "All"
    entity_type = "All"
    processing = False

    def get_run_logs_data(self):
        query = {}
        if self.transaction_type == "Added":
            query["operation"] = "insert"
        elif self.transaction_type == "Modified":
            query["operation"] = "update"

        if self.entity_type == "Tender":
            query["collection"] = "tenders"
        elif self.entity_type == "Performer":
            query["collection"] = "entities"

        try:
            self.processing = True
            yield
            self.run_logs_data = []
            pipeline = [
                {
                    "$match": query  # Match documents based on the query filters
                },
                {
                    "$lookup": {
                        "from": "runs",  # Specify the collection to join with
                        "localField": "run_id",  # Field from the current collection
                        "foreignField": "run_id",  # Field from the other collection
                        "as": "joined_data"  # Name for the output array
                    }
                }
            ]
            run_logs = sorted(list(mongo.db.get_collection(CONFIG.MONGO.RUNS_LOGS_COLLECTION).aggregate(pipeline)),
                              key=lambda x: x.get("joined_data", [])[0].get("start", 0), reverse=True)
            if len(run_logs) == 0: self.logs_info_style = "visible"
            for log in run_logs[:100]:
                print(log)
                key_id = ""
                if log["collection"] == "tenders":
                    try:
                        key_id = mongo.find_one(CONFIG.MONGO.TENDERS_COLLECTION, {"_id": log["object_id"]})["tenderID"]
                        if log["operation"] == "insert":
                            self.run_logs_data.append(
                                [str(f"Tender with ID: {key_id} added"),
                                 str(log["joined_data"][0]["start"]).split(".")[0],
                                 str(log["joined_data"][0]["end"]).split(".")[0]])
                        elif log["operation"] == "update":
                            self.run_logs_data.append(
                                [str(f"Tender with ID: {key_id} updated"),
                                 str(log["joined_data"][0]["start"]).split(".")[0],
                                 str(log["joined_data"][0]["end"]).split(".")[0]])
                    except:
                        pass

                elif log["collection"] == "entities":
                    try:
                        key_id = mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"_id": log["object_id"]})["edrpou"]
                        if log["operation"] == "insert":
                            self.run_logs_data.append(
                                [str(f"Entity with EDRPOU: {key_id} added"),
                                 str(log["joined_data"][0]["start"]).split(".")[0],
                                 str(log["joined_data"][0]["end"]).split(".")[0]])
                        elif log["operation"] == "update":
                            self.run_logs_data.append(
                                [str(f"Entity with EDRPOU: {key_id} updated"),
                                 str(log["joined_data"][0]["start"]).split(".")[0],
                                 str(log["joined_data"][0]["end"]).split(".")[0]])
                    except:
                        pass
        except:
            pass
        self.processing = False

    def change_logs_info_style(self, value):
        self.logs_info_style = value


@template(route="/transactions", title="Transaction Logs")
def transtactions() -> rx.Component:
    """The dashboard page.

    Returns:
        The UI for the dashboard page.
    """
    return rx.flex(
        rx.flex(
            rx.flex(
                rx.text("Transaction type"),
                rx.select(["All", "Added", "Modified"], placeholder="Select transaction type",
                          on_change=TransactionController.set_transaction_type,
                          default_value="All", width="8em"),
                direction="row",
                spacing="3",
                align="center"
            ),
            rx.flex(
                rx.text("Entity type"),
                rx.select(["All", "Tender", "Performer"], placeholder="Select entity type",
                          on_change=TransactionController.set_entity_type,
                          default_value="All", width="8em"),
                direction="row",
                spacing="3",
                align="center"
            ),
            rx.button("Get Logs", on_click=TransactionController.get_run_logs_data, style={"max-width": "8em"}, ),
            rx.flex(
                rx.flex(
                    rx.image(src="/info.png", style={"width": "2.5em", "height": "2.5em"}),
                    rx.text("Zero Logs Found", size="2", weight="medium"),
                    direction="row",
                    align="center",
                    spacing="2"
                ),
                rx.button("Close", on_click=[TransactionController.change_logs_info_style("hidden")],
                          bg="lightgray",
                          style={"color": "black", "border": f"0.13em solid gray",
                                 "border-radius": "0.5em"}),
                style={"z-index": "25", "position": "absolute", "padding": "0.6em", "margin-top": "0em",
                       "visibility": TransactionController.logs_info_style,
                       "border": f"0.13em solid gray",
                       "border-radius": "0.5em", "margin-top": "9.5em", "margin-left": "18em"},
                bg=rx.color("gray", 5),
                direction="column",
                spacing="2",

            ),
            align="center",
            justify="center",
            spacing="7",
            direction="row",
            bg=rx.color(ThemeState.accent_color, 3),
            style={"max-width": "40em", "border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                   "border-radius": "0.5em", "margin-bottom": "0.5em", "padding": "0.5em"}

        ),
        rx.flex(
            rx.card(
                rx.cond(
                    TransactionController.processing,
                    rx.chakra.circular_progress(is_indeterminate=True, color="gray"),
                ),
                rx.flex(
                    rx.data_table(
                        data=TransactionController.run_logs_data,
                        columns=TransactionController.run_logs_data_columns,
                        pagination=True,
                        search=True,
                        sort=True,
                        align='center',
                        resizable=True,
                        key="coll_id"
                    ),
                    style={"max-width": "67em", "overflow-x": "auto"},
                    direction="column",

                ),
                bg=rx.color(ThemeState.accent_color, 3)

            ),
            spacing="1",
            direction="row",
            align="start",
            align_items="start",
            justify="start",
            style={"border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                   "border-radius": "0.5em"}
        ),
        direction="column",
    )
