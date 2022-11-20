import logging
import sqlite3
from contextlib import closing

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import DB_PATH, TABLES, conn_context, dsl
from sqlite_to_postgres.postgres_saver import PostgresSaver
from sqlite_to_postgres.sqlite_extractor import SQLiteExtractor

logging.basicConfig(
    level=logging.DEBUG,
    filename='load_data_logs.log',
    format='%(asctime)s, %(levelname)s, %(message)s, %(name)s',
    filemode='w'
)

logger = logging.getLogger(__name__)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Load data from SQLite to Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    for table in TABLES:
        data = sqlite_extractor.extract_movies(table)
        postgres_saver.save_all_data(table, data)


if __name__ == '__main__':
    try:
        # комментарий для ревьюера: использую closing для закрытия pg_conn
        with conn_context(DB_PATH) as sqlite_conn,\
                closing(psycopg2.connect(
                    **dsl, cursor_factory=DictCursor)) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
    except sqlite3.Error as error:
        logger.error(f'SQLite database connection error: {error}')
    except psycopg2.DatabaseError as error:
        logger.error(f'PostgreSQL database connection error: {error}')
    except Exception as error:
        logger.error(
            f'In the performance of load_from_sqlite'
            f' function error occurs: {error}'
        )
