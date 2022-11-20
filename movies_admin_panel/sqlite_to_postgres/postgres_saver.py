import logging
from dataclasses import asdict, astuple, dataclass
from typing import Iterator

from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_values

from config import TABLES

logger = logging.getLogger(__name__)

DATA_SAVED = 'Data of table {table} is successfully loaded in Postgres.'
ERROR_TO_INSERT_DATA = 'Error to save data in table {table}: {error}.'


class PostgresSaver:

    def __init__(self, pg_conn: _connection) -> None:
        self.connection = pg_conn
        self.cursor = pg_conn.cursor()

    def save_all_data(self, table: str, data: Iterator[dataclass]) -> None:
        """Load data in Postgres table"""
        dataclasses_fields = asdict(TABLES[table]()).keys()
        fields = ', '.join(dataclasses_fields)
        try:
            insert_query = ("INSERT INTO content.{table} ({fields})"
                            " VALUES %s ON CONFLICT (id) DO NOTHING;").format(
                            table=table, fields=fields)
            execute_values(
                self.cursor,
                insert_query,
                [astuple(row) for row in data]
            )
            self.connection.commit()
            logger.debug(DATA_SAVED.format(table=table))
        except Exception as error:
            logger.error(ERROR_TO_INSERT_DATA.format(table=table, error=error))
