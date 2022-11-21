import logging
from http import HTTPStatus

from backoff import backoff
from config import es_settings
from elasticsearch import (
    ConnectionError,
    ConnectionTimeout,
    Elasticsearch,
    RequestError,
    SerializationError,
    TransportError,
)
from elasticsearch.helpers import bulk
from es_index import MOVIES_INDEX

from postgres_to_es.etl.models import ESFilmworkData

logger = logging.getLogger(__name__)

INDEX_CREATED = (
    "Index {name} is created." " Response from Elasticsearch: {response}."
)
PING_MESSAGE = "Ping from Elasticsearch server: {message}."


class ElasticsearchLoader:
    """A class to get data and load in Elasticsearch."""

    def __init__(self) -> None:
        self.client = None

    @backoff(
        exceptions=(ConnectionError, TransportError, ConnectionTimeout),
        logger=logger,
    )
    def connect(self):
        self.client = Elasticsearch(**es_settings.dict())
        logging.info(PING_MESSAGE.format(message=self.client.ping()))

    @backoff(exceptions=(RequestError,), logger=logger)
    def create_index(self) -> None:
        if not self.client.indices.exists(index="movies"):
            response = self.client.indices.create(
                index="movies",
                ignore=HTTPStatus.BAD_REQUEST.value,
                body=MOVIES_INDEX,
            )
            logger.debug(
                INDEX_CREATED.format(name="movies", response=response)
            )

    @backoff(exceptions=(SerializationError,), logger=logger)
    def load_data_to_elastic(self, data: list[ESFilmworkData]) -> None:
        documents = [
            {"_index": "movies", "_id": row.id, "_source": row.dict()}
            for row in data
        ]
        bulk(self.client, documents)
