import os
import sqlite3
from contextlib import contextmanager

from dotenv import load_dotenv

from sqlite_to_postgres.tables import (Filmwork, Genre, GenreFilmwork, Person,
                                       PersonFilmwork)

load_dotenv()


DB_NAME = os.environ.get('DB_NAME')
USER = os.environ.get('DB_USER')
PASSWORD = os.environ.get('DB_PASSWORD')
HOST = os.environ.get('DB_HOST', '127.0.0.1')
PORT = os.environ.get('DB_HOST', 5432)
DB_PATH = os.environ.get('DB_PATH', 'db.sqlite')
BATCH_SIZE = os.environ.get('DB_PATH', 1000)

TABLES = {
    'genre': Genre,
    'film_work': Filmwork,
    'person': Person,
    'genre_film_work': GenreFilmwork,
    'person_film_work': PersonFilmwork
}

dsl = {
    'dbname': DB_NAME,
    'user': USER,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT
}


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
