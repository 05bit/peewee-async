import asyncio
import logging
from collections.abc import AsyncGenerator, Generator

import pytest
from peewee import sort_models

from peewee_async.databases import AioDatabase
from peewee_async.utils import aiomysql, aiopg, psycopg
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


@pytest.fixture
async def db(request: pytest.FixtureRequest) -> AsyncGenerator[AioDatabase, None]:
    db = request.param
    if db.startswith("postgres") and aiopg is None:
        pytest.skip("aiopg is not installed")
    if db.startswith("mysql") and aiomysql is None:
        pytest.skip("aiomysql is not installed")
    if db.startswith("psycopg") and psycopg is None:
        pytest.skip("psycopg is not installed")

    params = DB_DEFAULTS[db]
    database = DB_CLASSES[db](**params)

    with database.allow_sync():
        for model in ALL_MODELS:
            model._meta.database = database
            model.create_table(True)

    yield database

    with database.allow_sync():
        for model in reversed(sort_models(ALL_MODELS)):
            model.delete().execute()
            model._meta.database = None
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
