from typing import Any

import peewee as pw
import pytest
from peewee import OperationalError

from peewee_async import connection_context
from peewee_async.aio_model import AioModel
from peewee_async.databases import AioDatabase
from tests.conftest import MYSQL_DBS, PG_DBS, dbs_all, dbs_mysql, dbs_postgres
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import IntegerTestModel, TestModel


@dbs_all
async def test_nested_connection(db: AioDatabase) -> None:
    async with db.aio_connection() as connection_1:
        async with connection_1.cursor() as cursor:
            await cursor.execute("SELECT 1")
        await TestModel.aio_get_or_none(id=5)
        async with db.aio_connection() as connection_2:
            assert connection_1 is connection_2
            _connection_context = connection_context.get()
            assert _connection_context is not None
            _connection = _connection_context.connection
            assert _connection is connection_2
            async with connection_2.cursor() as cursor:
                await cursor.execute("SELECT 1")
    assert connection_context.get() is None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_db_should_connect_manually_after_close(db: AioDatabase) -> None:
    await TestModel.aio_create(text="test")

    await db.aio_close()
    with pytest.raises((RuntimeError, OperationalError)):
        await TestModel.aio_get_or_none(text="test")
    await db.aio_connect()

    assert await TestModel.aio_get_or_none(text="test") is not None


@dbs_all
async def test_is_connected(db: AioDatabase) -> None:
    assert db.is_connected is False

    await db.aio_connect()
    assert db.is_connected is True

    await db.aio_close()
    assert db.is_connected is False


@dbs_all
async def test_aio_close_idempotent(db: AioDatabase) -> None:
    assert db.is_connected is False

    await db.aio_close()
    assert db.is_connected is False

    await db.aio_close()
    assert db.is_connected is False


@pytest.mark.parametrize("db_name", PG_DBS + MYSQL_DBS)
async def test_deferred_init(db_name: str) -> None:
    database: AioDatabase = DB_CLASSES[db_name](None)

    with pytest.raises(Exception, match="Error, database must be initialized before creating a connection pool"):
        await database.aio_execute_sql(sql="SELECT 1;")

    db_params: dict[str, Any] = DB_DEFAULTS[db_name]
    database.init(**db_params)

    await database.aio_execute_sql(sql="SELECT 1;")
    await database.aio_close()


@dbs_mysql
async def test_mysql_params(db: AioDatabase) -> None:
    async with db.aio_connection() as connection_1:
        assert connection_1.autocommit_mode is True  # type: ignore
    assert db.pool_backend.pool._recycle == 2  # type: ignore
    assert db.pool_backend.pool.minsize == 0  # type: ignore
    assert db.pool_backend.pool.maxsize == 5  # type: ignore


@pytest.mark.parametrize("db", ["aiopg-pool"], indirect=["db"])
async def test_pg_ext_json_hstore__params(db: AioDatabase) -> None:
    await db.aio_connect()
    assert db.pool_backend.pool._enable_json is True  # type: ignore
    assert db.pool_backend.pool._enable_hstore is False  # type: ignore
    assert db.pool_backend.pool._timeout == 30  # type: ignore
    assert db.pool_backend.pool._recycle == 1.5  # type: ignore
    assert db.pool_backend.pool._recycle == 1.5  # type: ignore
    assert db.pool_backend.pool.minsize == 0  # type: ignore
    assert db.pool_backend.pool.maxsize == 5  # type: ignore


@pytest.mark.parametrize("db", ["psycopg-pool"], indirect=["db"])
async def test_psycopg__params(db: AioDatabase) -> None:
    await db.aio_connect()
    assert db.pool_backend.pool.min_size == 0  # type: ignore
    assert db.pool_backend.pool.max_size == 5  # type: ignore
    assert db.pool_backend.pool.max_lifetime == 15  # type: ignore


@dbs_all
async def test_aio_get_tables(db: AioDatabase) -> None:
    # TODO check with scheme
    tables = set(await db.aio_get_tables())
    assert {"testmodelalpha", "testmodelbeta", "testmodelgamma"} <= tables


@dbs_all
async def test_aio_table_exists(db: AioDatabase) -> None:
    assert await db.aio_table_exists("compositetestmodel") is True
    assert await db.aio_table_exists(IntegerTestModel) is True
    assert await db.aio_table_exists("unknown") is False


@dbs_postgres
async def test_aio_sequence_exists(db: AioDatabase) -> None:
    assert await db.aio_sequence_exists("testmodel_id_seq") is True
    assert await db.aio_sequence_exists("unknown") is False


@dbs_all
async def test_create_drop_tables(db: AioDatabase) -> None:
    class SomeModel1(AioModel):
        text = pw.CharField(index=True)

        class Meta:
            database = db

    class SomeModel2(AioModel):
        text = pw.CharField(index=True)

        class Meta:
            database = db

    await db.aio_create_tables([SomeModel1, SomeModel2])

    assert await SomeModel1.aio_table_exists() is True
    assert await SomeModel2.aio_table_exists() is True

    await db.aio_drop_tables([SomeModel1, SomeModel2])

    assert await SomeModel1.aio_table_exists() is False
    assert await SomeModel2.aio_table_exists() is False
