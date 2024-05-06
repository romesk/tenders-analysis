import json
from typing import List, Dict

from performer_searcher.templates import template

import reflex as rx

Measurement = Dict[str, str]
Attribute = Dict[str, str]
Dimension = Dict[str, List[Attribute]]
Cube = Dict[str, List[Dimension]]
depth = 1


class RadixFormSubmissionState(rx.State):
    form_data: dict
    cube_value: str
    select_cube: str

    def handle_submit(self, form_data: dict):
        """Handle the form submit."""
        self.form_data = form_data

        # Initialize an empty list to store the selected checkboxes
        selected_checkboxes = []

        # Iterate over the form data
        for key, value in form_data.items():
            # Check if the value is True (checkbox is selected)
            if value:
                # Append the key (checkbox name) to the selected checkboxes list
                selected_checkboxes.append(key)

        # Print the selected checkboxes
        print("Query:", generate_sql_query(selected_checkboxes))


# Read data from JSON file
with open(
        "/Users/erikhpetrushynets/Documents/kursova/tenders-analysis/src/searchers/performer_searcher/assets/metadata.json") as file:
    data: Dict[str, List[Cube]] = json.load(file)

cubes: List[Cube] = data.get("cubes", [])


def get_fact_opened_cube(cube: Cube) -> rx.Component:
    # Helper function to get columns from related dimensions
    import json

    # Assuming the rest of the code remains the same as provided in the previous message

    def get_related_dimension_columns(cube_name, column, depth, hierarchy_names=None, full_path=None,
                                      parent_foreign_table=None):
        related_columns = []
        if hierarchy_names is None:
            hierarchy_names = [cube_name]

        if column.get("foreign_key"):
            hierarchy_names.append(f"({column['name']}, {column['foreign_key']}){column['foreign_table']}")

            # Check if the current foreign table is the same as the parent foreign table
            if parent_foreign_table and column["foreign_table"] == parent_foreign_table:
                # Remove the last item from the hierarchy names
                hierarchy_names.pop()

            # Iterate over dimensions to find the related dimension
            for related_dim in cube.get("dimensions", []):
                if related_dim["name"] == column["foreign_table"]:
                    # Get the index of the current dimension
                    related_dim_index = cube["dimensions"].index(related_dim)

                    # Iterate over columns in the related dimension
                    for related_column in related_dim.get("columns", []):
                        checkbox_name = f"{''.join(hierarchy_names)}|{related_column['name']}"

                        if not related_column.get("foreign_key"):
                            related_columns.append(
                                rx.flex(
                                    rx.checkbox(
                                        name=checkbox_name
                                    ),
                                    rx.text(f"{''.join(hierarchy_names)}|{related_column['name']}"),
                                    direction="row",
                                    spacing="2",
                                    style={"margin-left": f"{2 + (2 * depth)}em"}  # Adjusted margin based on depth
                                )
                            )
                        else:
                            related_columns.append(
                                rx.flex(
                                    rx.text(f"({'_'.join(hierarchy_names)}) {related_column['name']}"),
                                    direction="row",
                                    spacing="2",
                                    style={"margin-left": f"{2 + (2 * depth)}em"}  # Adjusted margin based on depth
                                )
                            )
                        # Recursively search for joined columns in related dimensions
                        related_columns.extend(
                            get_related_dimension_columns(None, related_column, depth + 1, hierarchy_names, full_path,
                                                          column["foreign_table"]))

                    # Remove the last hierarchy name and full path when returning one step up
                    hierarchy_names.pop()

                    # Break the loop after processing the related dimension
                    break

        return related_columns

    # Define measurement components
    measurement_components = [
        # Get measurement components for each measurement in the cube
        get_measurement_component(meas) for meas in cube.get("measurements", [])
    ]

    # Define components for columns and their associated dimension columns
    component_list = []
    for col in cube.get("columns", []):
        # Display the primary column
        if col["foreign_table"]:
            column_component = rx.flex(
                rx.text(f"{col['name']}"),
                direction="row",
                spacing="2",
                style={"margin-left": "2em"}
            )
        else:
            column_component = rx.flex(
                rx.checkbox(name=f"{col['name']}"),
                rx.text(f"{col['name']}"),
                direction="row",
                spacing="2",
                style={"margin-left": "2em"}
            )
        component_list.append(column_component)

        # Display columns from related dimensions
        related_columns = get_related_dimension_columns(cube["name"], col, depth)
        component_list.extend(related_columns)

    return measurement_components, component_list


def get_measurement_component(measurement: Measurement) -> rx.Component:
    # Define checkbox component for measurement
    return rx.flex(
        rx.checkbox(),
        measurement.get("name", "Unknown Measurement"),
        direction="row",
        spacing="2",
        align="center",
        style={"margin-left": "2em"}
    )


def get_dimension_component(coll_key: str, dimension: Dimension) -> rx.Component:
    # Define checkbox component for dimension
    return rx.flex(
        rx.checkbox(),
        rx.text(f"({coll_key}) {dimension.get('name', 'Unknown Dimension')}"),
        direction="row",
        spacing="2"
    )


@template(route="/export", title="Export")
def export() -> rx.Component:
    # Create a list to store all components
    all_components = []

    # Iterate over cubes and get components for each cube
    for cube in cubes:
        measurement_components, component_list = get_fact_opened_cube(cube)

        # Return Flex container with all components
        return rx.flex(
            rx.form.root(
                rx.text(cube["name"], weight="medium", size="6", style={"margin-bottom": "1em"}),
                rx.text("Measurements", weight="medium", size="4", style={"margin-bottom": "0.5em"}),
                *measurement_components,
                rx.text("Dimensions", weight="medium", size="4", style={"margin-bottom": "0.5em", "margin-top": "0.5em"}),
                *component_list,
                rx.form.submit(
                    rx.button("Export"),
                    as_child=True,
                    style={"margin-top": "1em"}
                ),
                direction="column",
                spacing="4",
                on_submit=RadixFormSubmissionState.handle_submit,
            )
        )


def generate_sql_query(checked_checkboxes):
    pass
