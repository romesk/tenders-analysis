from typing import Iterable
import clickhouse_connect


class ClickhouseService:
    """
    This class is responsible for the interaction with Clickhouse.
    """

    def __init__(self, host: str, user: str, password: str) -> None:
        self._client = clickhouse_connect.get_client(host=host, username=user, password=password, secure=True, verify=False)

    def insert(self, table_name: str, values: dict, columns: str | Iterable[str] = "*") -> None:
        """
        Insert a record into a Clickhouse table.
        """

        return self._client.insert(table_name, values, columns)

    def remove(self, table_name: str, column_name: str, column_value: str):
        return self._client.query(f"DELETE FROM {table_name} WHERE {column_name} = '{column_value}'")

    def query(self, query):
        return self._client.query_df(query)
