import asyncio
import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest
from peewee import sort_models

from peewee_async.databases import AioDatabase
from peewee_async.testing import TransactionTestCase
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import ALL_MODELS


@pytest.fixture
def enable_debug_log_level() -> Generator[None, None, None]:
    logger = logging.getLogger("peewee.async")
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield

    logger.removeHandler(handler)
    logger.setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def _get_db(name: str) -> AioDatabase:
    params = DB_DEFAULTS[name]
    return DB_CLASSES[name](**params)


def _clear_tables(database: AioDatabase) -> None:
    with database.allow_sync():
        for model in reversed(sort_models(ALL_MODELS)):
            model.delete().execute()


@contextmanager
def bound_models(database: AioDatabase) -> Generator[None]:
    for model in ALL_MODELS:
        model._meta.database = database

    yield

    for model in ALL_MODELS:
        model._meta.database = None


@pytest.fixture(scope="session", autouse=True)
async def create_tables() -> AsyncGenerator[None, None]:

    databases = [_get_db(name) for name in ("psycopg-pool", "mysql-pool")]
    for database in databases:
        with bound_models(database), database.allow_sync():
            for model in ALL_MODELS:
                model.create_table(True)
    yield

    for database in databases:
        with bound_models(database), database.allow_sync():
            for model in reversed(sort_models(ALL_MODELS)):
                model.drop_table(True)


@asynccontextmanager
async def reset_models(database: AioDatabase, use_transaction: bool = False) -> AsyncGenerator[None, None]:
    if use_transaction:
        async with TransactionTestCase(database):
            yield
    else:
        yield
        _clear_tables(database)


def pytest_configure(config: Any) -> None:
    config.addinivalue_line("markers", "use_transaction: mark test to run in transaction")


@pytest.fixture
async def db(request: pytest.FixtureRequest) -> AsyncGenerator[AioDatabase, None]:
    database = _get_db(request.param)
    use_transcation = request.keywords.get("use_transaction", False)

    with bound_models(database):
        async with reset_models(database, use_transcation):
            yield database

    await database.aio_close()


PG_DBS = [
    "postgres-pool",
    "postgres-pool-ext",
    "psycopg-pool",
]

MYSQL_DBS = ["mysql-pool"]


dbs_mysql = pytest.mark.parametrize("db", MYSQL_DBS, indirect=["db"])


dbs_postgres = pytest.mark.parametrize("db", PG_DBS, indirect=["db"])


dbs_all = pytest.mark.parametrize("db", PG_DBS + MYSQL_DBS, indirect=["db"])
transaction_methods = pytest.mark.parametrize("transaction_method", ["aio_transaction", "aio_atomic"])
