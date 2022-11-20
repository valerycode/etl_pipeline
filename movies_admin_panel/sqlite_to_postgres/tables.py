import uuid
from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass
class Filmwork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    title: str = field(default='')
    description: str = field(default='')
    creation_date: date = field(default='')
    file_path: str = field(default='')
    rating: float = field(default=0.0)
    type: str = field(default='')
    created: datetime = field(default_factory=datetime.now)
    modified: datetime = field(default_factory=datetime.now)


@dataclass
class Genre:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = field(default='')
    description: str = field(default='')
    created: datetime = field(default_factory=datetime.now)
    modified: datetime = field(default_factory=datetime.now)


@dataclass
class Person:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    full_name: str = field(default='')
    created: datetime = field(default_factory=datetime.now)
    modified: datetime = field(default_factory=datetime.now)


@dataclass
class GenreFilmwork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now)


@dataclass
class PersonFilmwork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    role: str = field(default='')
    created: datetime = field(default_factory=datetime.now)
