import asyncio

import pytest
from peewee import sort_models

import peewee_async
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import ALL_MODELS

try:
    import aiopg
except ImportError:
    aiopg = None

try:
    import aiomysql
except ImportError:
    aiomysql = None


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
    "postgres",
    "postgres-ext",
    "postgres-pool",
    "postgres-pool-ext"
]

MYSQL_DBS = ["mysql", "mysql-pool"]


dbs_postgres = pytest.mark.parametrize(
    "db", PG_DBS, indirect=["db"]
)


dbs_all = pytest.mark.parametrize(
    "db", PG_DBS + MYSQL_DBS, indirect=["db"]
)

# MANAGERS fixtures will be removed in v1.0.0
postgres_only = pytest.mark.parametrize(
    "manager", PG_DBS, indirect=["manager"]
)

mysql_only = pytest.mark.parametrize(
    "manager", MYSQL_DBS, indirect=["manager"]
)

manager_for_all_dbs = pytest.mark.parametrize(
    "manager", PG_DBS + MYSQL_DBS, indirect=["manager"]
)


@pytest.fixture
async def manager(request):
    db = request.param
    if db.startswith('postgres') and aiopg is None:
        pytest.skip("aiopg is not installed")
    if db.startswith('mysql') and aiomysql is None:
        pytest.skip("aiomysql is not installed")

    params = DB_DEFAULTS[db]
    database = DB_CLASSES[db](**params)
    database._allow_sync = False
    manager = peewee_async.Manager(database)
    with manager.allow_sync():
        for model in ALL_MODELS:
            model._meta.database = database
            model.create_table(True)

    yield peewee_async.Manager(database)

    with manager.allow_sync():
        for model in reversed(sort_models(ALL_MODELS)):
            model.delete().execute()
            model._meta.database = None
    await database.close_async()
