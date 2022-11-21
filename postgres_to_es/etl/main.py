import logging
from datetime import datetime
from time import sleep

from config import etl_settings
from extract_from_postgres import PostgresExtractor
from load_to_elasticsearch import ElasticsearchLoader
from state import JsonFileStorage, State
from transform_data import DataTransform

logging.basicConfig(
    level=etl_settings.LOGGING_LEVEL,
    filename=etl_settings.FILENAME,
    format="%(asctime)s, %(levelname)s, %(message)s, %(name)s",
    filemode=etl_settings.FILEMODE,
)

logger = logging.getLogger(__name__)

LOAD_MESSAGE = "Load in Elasticsearch {number} documents."
ERROR_MESSAGE = "ETL process failed. Error occurs: {error}."


class ETL:
    def __init__(self):
        self.postgres = PostgresExtractor()
        self.elastic = ElasticsearchLoader()
        self.transform = DataTransform()
        self.state = State(JsonFileStorage(etl_settings.STATE_FILE_NAME))

    def load_data_from_postgres_to_elastic(self):
        """Load data from Postgres to Elasticsearch"""
        last_modified = self.state.get_state("modified")
        date_last_modified = last_modified if last_modified else datetime.min
        count = 0
        for films in self.postgres.extract_movies(date_last_modified):
            self.state.set_state("modified", datetime.now().isoformat())
            es_films = self.transform.validate_and_transform_data(films)
            self.elastic.load_data_to_elastic(es_films)
            count += len(films)
            logger.debug(LOAD_MESSAGE.format(number=count))


def main():
    while True:
        etl = ETL()
        try:
            etl.postgres.connect_to_postgres()
            etl.elastic.connect()
            etl.elastic.create_index()
            etl.load_data_from_postgres_to_elastic()
        except Exception as error:
            logger.error(ERROR_MESSAGE.format(error=error))
        finally:
            etl.postgres.connection.close()
            sleep(etl_settings.TIME_INTERVAL)


if __name__ == "__main__":
    main()
