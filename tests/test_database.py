import pytest

from peewee_async import connection_context
from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, MYSQL_DBS, PG_DBS
from tests.db_config import DB_DEFAULTS, DB_CLASSES
from tests.models import TestModel


@dbs_all
async def test_nested_connection(db):
    async with db.aio_connection() as connection_1:
        async with connection_1.cursor() as cursor:
            await cursor.execute("SELECT 1")
        await TestModel.aio_get_or_none(id=5)
        async with db.aio_connection() as connection_2:
            assert connection_1 is connection_2
            _connection = connection_context.get().connection
            assert _connection is connection_2
            async with connection_2.cursor() as cursor:
                await cursor.execute("SELECT 1")
    assert connection_context.get() is None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_db_should_connect_manually_after_close(db):
    await TestModel.aio_create(text='test')

    await db.aio_close()
    with pytest.raises(RuntimeError):
        await TestModel.aio_get_or_none(text='test')
    await db.aio_connect()

    assert await TestModel.aio_get_or_none(text='test') is not None


@dbs_all
async def test_is_connected(db):
    assert db.is_connected is False

    await db.aio_connect()
    assert db.is_connected is True

    await db.aio_close()
    assert db.is_connected is False


@dbs_all
async def test_aio_close_idempotent(db):
    assert db.is_connected is False

    await db.aio_close()
    assert db.is_connected is False

    await db.aio_close()
    assert db.is_connected is False


@pytest.mark.parametrize('db_name', PG_DBS + MYSQL_DBS)
async def test_deferred_init(db_name):
    database: AioDatabase = DB_CLASSES[db_name](None)

    with pytest.raises(Exception, match='Error, database must be initialized before creating a connection pool'):
        await database.aio_execute_sql(sql='SELECT 1;')

    database.init(**DB_DEFAULTS[db_name])

    await database.aio_execute_sql(sql='SELECT 1;')
