import asyncio

import pytest

import peewee_async
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import TestModel, UUIDTestModel, TestModelAlpha, TestModelBeta, TestModelGamma, CompositeTestModel

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
def manager(request):
    db = request.param
    if db.startswith('postgres') and aiopg is None:
        pytest.skip("aiopg is not installed")
    if db.startswith('mysql') and aiomysql is None:
        pytest.skip("aiomysql is not installed")

    params = DB_DEFAULTS[db]
    database = DB_CLASSES[db](**params)
    models = [TestModel, UUIDTestModel, TestModelAlpha,
              TestModelBeta, TestModelGamma, CompositeTestModel]
    manager = peewee_async.Manager(database)
    with manager.allow_sync():
        for model in models:
            model._meta.database = database
            model.create_table(True)

    yield peewee_async.Manager(database)
    for model in reversed(models):
        model.drop_table(fail_silently=True)
        model._meta.database = None


PG_DBS = [
    "postgres",
    "postgres-ext",
    "postgres-pool",
    "postgres-pool-ext"
]

MYSQL_DBS = ["mysql", "mysql-pool"]


postgres_only = pytest.mark.parametrize(
    "manager", PG_DBS, indirect=["manager"]
)

mysql_only = pytest.mark.parametrize(
    "manager", MYSQL_DBS, indirect=["manager"]
)

all_dbs = pytest.mark.parametrize(
    "manager", PG_DBS + MYSQL_DBS, indirect=["manager"]
)
