from performer_searcher.templates import template, ThemeState

import reflex as rx
import sys

sys.path.append("../../../src")

from config import CONFIG
from services import ClickhouseService

clickhouse = ClickhouseService(CONFIG.CLICKHOUSE.HOST, CONFIG.CLICKHOUSE.USER, CONFIG.CLICKHOUSE.PASSWORD)


class ExportController(rx.State):
    table_to_export: str = "All"
    output_format: str = "json"
    limit: str = ""
    file_name: str

    def export(self):
        if self.limit == "":
            limit = ""
        else:
            limit = "LIMIT " + self.limit
        if self.table_to_export == "All":
            for table in get_table_names():
                query = f"SELECT * FROM {table} {limit}"
                df = clickhouse.query(query)
                if self.output_format == "json":
                    df.to_json(f"{self.file_name + table}.{self.output_format}", orient="records", lines=True)
                if self.output_format == "csv":
                    df.to_csv(f"{self.file_name + table}.{self.output_format}")
                if self.output_format == "parquet":
                    df.to_parquet(f"{self.file_name + table}.{self.output_format}")
        else:
            query = f"SELECT * FROM {self.table_to_export} {limit}"
            df = clickhouse.query(query)
            if self.output_format == "json":
                df.to_json(f"{self.file_name}.{self.output_format}", orient="records", lines=True)
            if self.output_format == "csv":
                df.to_csv(f"{self.file_name}.{self.output_format}")
            if self.output_format == "parquet":
                df.to_parquet(f"{self.file_name}.{self.output_format}")


def get_table_names():
    return [item[0] for item in clickhouse.query(
        "SELECT table FROM system.tables WHERE is_temporary = 0 AND database = 'default'").values.tolist()]


@template(route="/export", title="Export")
def export() -> rx.Component:
    return rx.flex(
        rx.flex(
            rx.flex(
                rx.text("Table to export"),
                rx.select(["All"] + get_table_names(), default_value="All",
                          on_change=ExportController.set_table_to_export,
                          width="11em"),
                direction="column",
                align="start",
                justify="center",
                spacing="1",

            ),
            rx.flex(
                rx.text("Rows limit"),
                rx.input(placeholder="Rows to limit", on_change=ExportController.set_limit, style={"width": "11em"},
                         type="number"),
                direction="column",
                align="start",
                justify="center",
                spacing="1"
            ),
            rx.flex(
                rx.text("Filename"),
                rx.input(placeholder="Name of file", on_change=ExportController.set_file_name, style={"width": "11em"},
                         type="string"),
                direction="column",
                align="start",
                justify="center",
                spacing="1"
            ),
            rx.flex(
                rx.text("Select output format"),
                rx.select(["json", "csv", "parquet"], default_value="json",
                          on_change=ExportController.set_output_format,
                          width="11em"),
                direction="column",
                align="start",
                justify="center",
                spacing="1"
            ),
            rx.button("Export", size="4", on_click=ExportController.export),
            align="center",
            justify="center",
            spacing="7",
            style={"border": f"0.1em solid {rx.color(ThemeState.accent_color)}",
                   "border-radius": "0.5em", "padding": "1em"},
            bg=rx.color(ThemeState.accent_color, 3)
        ),
        style={"width": "67em"},
        align="center",
        justify="center",
    )
