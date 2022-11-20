import logging
import sqlite3
from contextlib import closing
from dataclasses import asdict

import psycopg2 as psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from sqlite_to_postgres.config import DB_PATH, TABLES, conn_context, dsl

logging.basicConfig(
    level=logging.DEBUG,
    filename='tests_logs.log',
    format='%(asctime)s, %(levelname)s, %(message)s, %(name)s',
    filemode='w'
)

logger = logging.getLogger(__name__)


TABLE_SIZE_ERROR = ('Размеры таблицы {pg_table} в Postgres и'
                    ' таблицы {sqlite_table} в SQLite не совпадают.')
COLUMNS_ERROR = 'Название столбцов таблиц не совпадает.'
TABLE_NAMES_ERROR = ('Список таблиц в Postgres: {pg_tables} и'
                     ' список таблиц в SQLite: {sqlite_tables} не совпадают.')
TABLES_COUNT = ('Количество таблиц в Postgres и SQLite различается.'
                ' В Postgres количество найденных таблиц - {pg_tables_count},'
                ' а в SQLite - {sqlite_tables_count}.')
TABLES_ARE_THE_SAME = 'Названия таблиц в SQLite и Postgres совпадают.'
TABLES_COUNT_IS_THE_SAME = 'Количество таблиц в SQLite и Postgres совпадает.'
ROWS_COUNT_IN_TABLE = ('Количество строк в таблице {sqlite_table} в'
                       ' SQLite и в таблице {pg_table} в Postgres совпадает.')
ROW_DATA_IS_DIFFERENT = ('Данные {sqlite_data} в таблице {sqlite_table} в'
                         ' SQLite и данные {pg_data} в таблице {pg_table}'
                         ' в Postgres не совпадают.')
ROW_DATA_IS_THE_SAME = ('Данные в таблице {sqlite_table} в SQLite'
                        ' и в таблице {pg_table} в Postgres совпадают.')


class CheckConsistency:
    """Класс для проверки консистентности данных"""

    def __init__(self, pg_conn: _connection,
                 connection: sqlite3.Connection) -> None:
        self.tables = (
            ('content.genre', 'genre'),
            ('content.film_work', 'film_work'),
            ('content.person', 'person'),
            ('content.genre_film_work', 'genre_film_work'),
            ('content.person_film_work', 'person_film_work'),
        )
        self.pg_cursor = pg_conn.cursor()
        self.sqlite_cursor = connection.cursor()

    def test_table_names_and_count(self) -> None:
        """Проверяет название и количество таблиц в в Postgres и SQLite"""
        pg_query = ("SELECT table_name FROM information_schema.tables"
                    " WHERE table_schema = 'content' ORDER BY table_name;")
        sqlite_query = ("SELECT name FROM sqlite_master"
                        " WHERE type='table' ORDER BY name;")
        self.pg_cursor.execute(pg_query)
        self.sqlite_cursor.execute(sqlite_query)
        sqlite_tables = [_[0] for _ in self.sqlite_cursor.fetchall()]
        pg_tables = [_[0] for _ in self.pg_cursor.fetchall()]
        assert pg_tables == sqlite_tables, (TABLE_NAMES_ERROR.format(
            pg_tables=pg_tables, sqlite_tables=sqlite_tables))
        logger.debug(TABLES_ARE_THE_SAME)
        assert len(pg_tables) == len(sqlite_tables), (TABLES_COUNT.format(
            pg_tables_count=len(pg_tables),
            sqlite_tables_count=len(sqlite_tables))
        )
        logger.debug(TABLES_COUNT_IS_THE_SAME)

    def test_table_size(self) -> None:
        """Проверяет количество строк в таблицах в Postgres и SQLite"""
        query = "SELECT count(*) FROM {table}"
        for table in self.tables:
            self.pg_cursor.execute(query.format(table=table[0]))
            postgres_table_size = self.pg_cursor.fetchone()[0]
            self.sqlite_cursor.execute(query.format(table=table[1]))
            sqlite_table_size = self.sqlite_cursor.fetchone()[0]
            assert postgres_table_size == sqlite_table_size, (
                TABLE_SIZE_ERROR.format(
                    pg_table=table[0], sqlite_table=table[1]
                )
            )
            logger.debug(ROWS_COUNT_IN_TABLE.format(
                sqlite_table=table[1], pg_table=table[0]))

    def test_data(self):
        """Сравнивает содержимое таблиц в Postgres и SQLite"""
        for table in self.tables:
            dataclasses_fields = asdict(TABLES[table[1]]())
            [dataclasses_fields.pop(key, None) for key in [
                'created', 'modified']]
            fields = ', '.join(dataclasses_fields.keys())
            query = "SELECT {fields} FROM {table};"
            sqlite_data = []
            pg_data = []
            self.sqlite_cursor.execute(query.format(
                fields=fields, table=table[1]))
            for row in self.sqlite_cursor.fetchall():
                sqlite_data.append([*row])
            self.pg_cursor.execute(query.format(
                fields=fields, table=table[0]))
            for pg_row in self.pg_cursor.fetchall():
                pg_data.append(pg_row)
            assert sqlite_data == pg_data, (ROW_DATA_IS_DIFFERENT.format(
                sqlite_data=sqlite_data,
                sqlite_table=table[1],
                pg_data=pg_data,
                pg_table=table[0]
            )
            )
            logger.debug(ROW_DATA_IS_THE_SAME.format(
                sqlite_table=table[1], pg_table=table[0]))


def check_consistency(connection: sqlite3.Connection, pg_conn: _connection):
    """Проверяет идентичность данных в таблицах SQLite и Postgres"""
    checker = CheckConsistency(pg_conn, connection)
    checker.test_table_names_and_count()
    checker.test_table_size()
    checker.test_data()


if __name__ == '__main__':
    with conn_context(DB_PATH) as sqlite_conn, closing(
            psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        check_consistency(sqlite_conn, pg_conn)
