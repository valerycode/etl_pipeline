import logging
from datetime import datetime
from typing import Iterator

import psycopg2
from backoff import backoff
from config import BATCH_SIZE, postgres_dsl
from psycopg2.extras import RealDictCursor
from queries import FILMWORKS_QUERY

logger = logging.getLogger(__name__)

POSTGRES_CONNECTION = 'Connection to Postgres succeeded.'


class PostgresExtractor:
    """A class to extract Postgres data and transform it to pydantic format."""
    def __init__(self) -> None:
        self.connection = None
        self.cursor = None

    @backoff()
    def connect_to_postgres(self) -> None:
        self.connection = psycopg2.connect(
            **postgres_dsl, cursor_factory=RealDictCursor)
        self.cursor = self.connection.cursor()
        logger.info(POSTGRES_CONNECTION)

    @backoff()
    def extract_movies(self, date_last_modified: datetime) -> Iterator:
        """Extract data from Postgres"""
        self.cursor.execute(FILMWORKS_QUERY, (date_last_modified, ) * 3)
        while rows := self.cursor.fetchmany(BATCH_SIZE):
            yield rows
