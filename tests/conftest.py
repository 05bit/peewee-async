import asyncio

import pytest
from peewee import sort_models

from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import ALL_MODELS
from peewee_async.utils import aiopg, aiomysql


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def db(request):
    db = request.param
    if db.startswith('postgres') and aiopg is None:
        pytest.skip("aiopg is not installed")
    if db.startswith('mysql') and aiomysql is None:
        pytest.skip("aiomysql is not installed")

    params = DB_DEFAULTS[db]
    database = DB_CLASSES[db](**params)
    database._allow_sync = False
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
    "postgres-pool-ext"
]

MYSQL_DBS = ["mysql-pool"]


dbs_postgres = pytest.mark.parametrize(
    "db", PG_DBS, indirect=["db"]
)


dbs_all = pytest.mark.parametrize(
    "db", PG_DBS + MYSQL_DBS, indirect=["db"]
)


