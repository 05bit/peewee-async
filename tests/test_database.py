from typing import Any, Dict

import pytest
from peewee import OperationalError

from peewee_async import connection_context
from peewee_async.databases import AioDatabase
from tests.conftest import MYSQL_DBS, PG_DBS, dbs_all, dbs_mysql
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import TestModel


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

    db_params: Dict[str, Any] = DB_DEFAULTS[db_name]
    database.init(**db_params)

    await database.aio_execute_sql(sql="SELECT 1;")
    await database.aio_close()


@pytest.mark.parametrize("db_name", ["postgres-pool", "postgres-pool-ext", "mysql-pool"])
async def test_deprecated_min_max_connections_param(db_name: str) -> None:
    default_params = DB_DEFAULTS[db_name].copy()
    del default_params["pool_params"]
    default_params["min_connections"] = 1
    default_params["max_connections"] = 3
    db_cls = DB_CLASSES[db_name]
    database = db_cls(**default_params)
    await database.aio_connect()

    assert database.pool_backend.pool.minsize == 1  # type: ignore
    assert database.pool_backend.pool.maxsize == 3  # type: ignore

    await database.aio_close()


@dbs_mysql
async def test_mysql_params(db: AioDatabase) -> None:
    async with db.aio_connection() as connection_1:
        assert connection_1.autocommit_mode is True  # type: ignore
    assert db.pool_backend.pool._recycle == 2  # type: ignore
    assert db.pool_backend.pool.minsize == 0  # type: ignore
    assert db.pool_backend.pool.maxsize == 5  # type: ignore


@pytest.mark.parametrize("db", ["postgres-pool"], indirect=["db"])
async def test_pg_json_hstore__params(db: AioDatabase) -> None:
    await db.aio_connect()
    assert db.pool_backend.pool._enable_json is False  # type: ignore
    assert db.pool_backend.pool._enable_hstore is False  # type: ignore
    assert db.pool_backend.pool._timeout == 30  # type: ignore
    assert db.pool_backend.pool._recycle == 1.5  # type: ignore
    assert db.pool_backend.pool.minsize == 0  # type: ignore
    assert db.pool_backend.pool.maxsize == 5  # type: ignore


@pytest.mark.parametrize("db", ["postgres-pool-ext"], indirect=["db"])
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
