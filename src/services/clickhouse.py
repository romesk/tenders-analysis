from typing import Iterable
import clickhouse_connect


class ClickhouseService:
    """
    This class is responsible for the interaction with Clickhouse.
    """

    def __init__(self, host: str, user: str, password: str) -> None:
        self._client = clickhouse_connect.get_client(host=host, username=user, password=password, secure=True)

    def insert(self, table_name: str, values: dict, columns: str | Iterable[str] = "*") -> None:
        """
        Insert a record into a Clickhouse table.
        """

        return self._client.insert(table_name, values, columns)

    def upsert_by_str_column(self, table_name: str, values: dict, column: str = None, culumn_value: str = None,
                             columns: str | Iterable[str] = "*"):
        try:
            self._client.query(f"DELETE FROM {table_name} WHERE {column}='{culumn_value}'")
        except:
            pass
        self.insert(table_name, values, columns)