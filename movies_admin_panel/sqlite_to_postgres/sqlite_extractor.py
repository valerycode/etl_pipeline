import logging
import sqlite3
from dataclasses import dataclass
from typing import Iterator

from config import BATCH_SIZE, TABLES

logger = logging.getLogger(__name__)

DATA_IS_FETCHED = 'Data of table {table} is extracted from SQLite.'
ERROR_TO_GET_DATA = 'Error to get data from table {table}: {error}.'


class SQLiteExtractor:

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.cursor = connection.cursor()
        self.size = BATCH_SIZE

    def extract_movies(self, table: str) -> Iterator[dataclass]:
        """Extract data from SQLite table"""
        table_class = TABLES.get(table)
        try:
            self.cursor.execute(
                "SELECT * FROM {table};".format(table=table)
            )
            while rows := self.cursor.fetchmany(BATCH_SIZE):
                yield from [table_class(*row) for row in rows]
            logger.debug(DATA_IS_FETCHED.format(table=table))
        except Exception as error:
            logger.error(
                ERROR_TO_GET_DATA.format(table=table, error=error)
            )
