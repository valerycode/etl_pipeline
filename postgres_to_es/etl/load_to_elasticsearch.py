import logging

from backoff import backoff
from config import MOVIES_INDEX, elasticsearch_dsl
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from postgres_to_es.etl.models import ESFilmworkData

logger = logging.getLogger(__name__)

INDEX_CREATED = ('Index {name} is created.'
                 ' Response from Elasticsearch: {response}.')
PING_MESSAGE = 'Ping from Elasticsearch server: {message}.'


class ElasticsearchLoader:
    """A class to get data and load in Elasticsearch."""
    @backoff()
    def __init__(self) -> None:
        self.client = Elasticsearch(**elasticsearch_dsl)
        logging.info(PING_MESSAGE.format(message=self.client.ping()))

    @backoff()
    def create_index(self) -> None:
        if not self.client.indices.exists(index='movies'):
            response = self.client.indices.create(
                index='movies', ignore=400, body=MOVIES_INDEX)
            logger.debug(INDEX_CREATED.format(name='movies', response=response))

    @backoff()
    def load_data_to_elastic(self, data: list[ESFilmworkData]) -> None:
        documents = [
            {
                '_index': 'movies',
                '_id': row.id,
                '_source': row.dict()
            } for row in data
        ]
        bulk(self.client, documents)
