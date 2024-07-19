import pytest
from peewee import sort_models

import peewee_async
from peewee_async.utils import aiopg, aiomysql
from tests.conftest import PG_DBS, MYSQL_DBS
from tests.db_config import DB_DEFAULTS, DB_CLASSES
from tests.models import ALL_MODELS


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


manager_for_all_dbs = pytest.mark.parametrize(
    "manager", PG_DBS + MYSQL_DBS, indirect=["manager"]
)
mysql_only = pytest.mark.parametrize(
    "manager", MYSQL_DBS, indirect=["manager"]
)
postgres_only = pytest.mark.parametrize(
    "manager", PG_DBS, indirect=["manager"]
)
