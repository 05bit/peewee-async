import pytest

from peewee_async import connection_context
from tests.conftest import dbs_all
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
