import json
from typing import List, Dict

from performer_searcher.templates import template

import reflex as rx

Measurement = Dict[str, str]
Attribute = Dict[str, str]
Dimension = Dict[str, List[Attribute]]
Cube = Dict[str, List[Dimension]]

class ExportController(rx.State):
    file = open("/Users/erikhpetrushynets/Documents/kursova/tenders-analysis/src/searchers/performer_searcher/assets/metadata.json").read()
    data: Dict[str, List[Cube]] = json.loads(file)

    cubes: List[Cube] = data.get("cubes", [])


def get_fact_opened_cube(cube: Cube) -> rx.Component:
    measurement_components = rx.foreach(
        cube["measurements"],
        lambda measurement: get_measurement_component(measurement)
    )
    dimension_components = rx.foreach(
        cube["dimensions"],
        lambda dimension: get_dimension_component(dimension)
    )

    return rx.flex(
        rx.text(cube["name"]),
        measurement_components,
        dimension_components
    )


def get_measurement_component(measurement: Measurement) -> rx.Component:
    return rx.text(measurement["name"])


def get_dimension_component(dimension: Dimension) -> rx.Component:
    attribute_components = rx.foreach(
        dimension["attributes"],
        lambda attribute: rx.text(attribute["name"])
    )

    return rx.flex(
        rx.text(dimension["name"]),
        attribute_components
    )


@template(route="/export", title="Export")
def export() -> rx.Component:
    return rx.flex(
        rx.foreach(
            ExportController.cubes,
            get_fact_opened_cube
        )
    )
