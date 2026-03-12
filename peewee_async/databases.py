import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any

import peewee
from playhouse import postgres_ext as ext

from peewee_async.result_wrappers import fetch_models

from .connection import ConnectionContextManager, connection_context
from .pool import MysqlPoolBackend, PoolBackend, PostgresqlPoolBackend, PsycopgPoolBackend
from .transactions import Transaction
from .utils import CursorProtocol, __log__

FetchResults = Callable[["AioDatabase", CursorProtocol], Awaitable[Any]]


def fetchmany(count: int | None) -> FetchResults:

    async def _fetch_results(db: "AioDatabase", cursor: CursorProtocol) -> Sequence[Any]:
        if count == 1:
            return await cursor.fetchone()
        if count is not None:
            return await cursor.fetchmany(count)
        return await cursor.fetchall()

    return _fetch_results


fetchone = fetchmany(1)
fetchall = fetchmany(None)


class AioDatabase(peewee.Database):
    """Base async database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param pool_params: parameters that are passed to the pool

    Example::

        database = Psycopg3Database(
            'database': 'postgres',
            'host': '127.0.0.1',
            'port': 5432,
            'password': 'postgres',
            'user': 'postgres',
            'pool_params': {
                "min_size": 0,
                "max_size": 5,
                'max_lifetime': 15
            }
        )

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/api.html#Database
    """

    _allow_sync = False  # whether sync queries are allowed

    pool_backend_cls: type[PoolBackend]
    pool_backend: PoolBackend

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.pool_params: dict[str, Any] = {}
        super().__init__(*args, **kwargs)

    def init_pool_params_defaults(self) -> None:
        pass

    def init_pool_params(self) -> None:
        self.init_pool_params_defaults()
        pool_params = self.connect_params.pop("pool_params", {})
        self.pool_params.update(pool_params)
        self.pool_params.update(self.connect_params)

    def init(self, database: str | None, **kwargs: Any) -> None:
        super().init(database, **kwargs)
        self.init_pool_params()
        self.pool_backend = self.pool_backend_cls(database=self.database, **self.pool_params)

    async def aio_connect(self) -> None:
        """Creates a connection pool"""
        if self.deferred:
            raise Exception("Error, database must be initialized before creating a connection pool")
        await self.pool_backend.connect()

    @property
    def is_connected(self) -> bool:
        """Checks if pool is connected"""
        return self.pool_backend.is_connected

    async def aio_close(self) -> None:
        """Close pool backend. The pool is closed until you run aio_connect manually."""

        if self.deferred:
            raise Exception("Error, database must be initialized before creating a connection pool")

        await self.pool_backend.close()

    def aio_atomic(self) -> AbstractAsyncContextManager[None]:
        """Create an async context-manager which runs any queries in the wrapped block
        in a transaction (or save-point if blocks are nested).
        Calls to :meth:`.aio_atomic()` can be nested.
        """
        return self._aio_atomic(use_savepoint=True)

    def aio_transaction(self) -> AbstractAsyncContextManager[None]:
        """Create an async context-manager that runs all queries in the wrapped block in a transaction.

        Calls to :meth:`.aio_transaction()` cannot be nested. If so OperationalError will be raised.
        """
        return self._aio_atomic(use_savepoint=False)

    @contextlib.asynccontextmanager
    async def _aio_atomic(self, use_savepoint: bool = False) -> AsyncIterator[None]:
        async with self.aio_connection() as connection:
            _connection_context = connection_context.get()
            assert _connection_context is not None

            _is_root = not _connection_context.transaction_is_opened
            _is_nested = _connection_context.transaction_is_opened

            if _is_nested and not use_savepoint:
                raise peewee.OperationalError("Transaction already opened")
            try:
                async with Transaction(connection, is_savepoint=_is_nested):
                    if _is_root:
                        _connection_context.transaction_is_opened = True
                    yield
            finally:
                if _is_root:
                    _connection_context.transaction_is_opened = False

    def set_allow_sync(self, value: bool) -> None:
        """Allow or forbid sync queries for the database. See also
        the :meth:`.allow_sync()` context manager.
        """
        self._allow_sync = value

    @contextlib.contextmanager
    def allow_sync(self) -> Iterator[None]:
        """Allow sync queries within context. Close sync
        connection on exit if connected.

        Example::

            with database.allow_sync():
                PageBlock.create_table(True)
        """
        old_allow_sync = self._allow_sync
        self._allow_sync = True

        try:
            yield
        except:
            raise
        finally:
            self._allow_sync = old_allow_sync
            self.close()

    def execute_sql(self, *args: Any, **kwargs: Any) -> Any:
        """Sync execute SQL query, `allow_sync` must be set to True."""
        assert self._allow_sync, (
            "Error, sync query is not allowed! Call the `.set_allow_sync()` or use the `.allow_sync()` context manager."
        )
        return super().execute_sql(*args, **kwargs)

    def aio_connection(self) -> ConnectionContextManager:
        if self.deferred:
            raise Exception("Error, database must be initialized before creating a connection pool")

        return ConnectionContextManager(self.pool_backend)

    async def aio_execute_sql(
        self, sql: str, params: Sequence[Any] | None = None, fetch_results: FetchResults | None = None
    ) -> Any:
        __log__.debug((sql, params))
        with peewee.__exception_wrapper__:
            async with self.aio_connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(sql, params or ())
                    if fetch_results is not None:
                        return await fetch_results(self, cursor)

    async def aio_execute(self, query: Any, fetch_results: FetchResults | None = None) -> Any:
        """Execute *SELECT*, *INSERT*, *UPDATE* or *DELETE* query asyncronously.

        :param query: peewee query instance created with ``Model.select()``,
                      ``Model.update()`` etc.
        :param fetch_results: function with cursor param. It let you get data manually and
                              don't need to close cursor It will be closed automatically.
        :return: result depends on query type, it's the same as for sync `query.execute()`
        """
        ctx = self.get_sql_context()
        sql, params = ctx.sql(query).query()
        fetch_results = fetch_results or getattr(query, "fetch_results", None)
        return await self.aio_execute_sql(sql, params, fetch_results=fetch_results)

    async def aio_last_insert_id(self, cursor: CursorProtocol, query: peewee.Insert) -> int:
        return cursor.lastrowid

    async def aio_rows_affected(self, cursor: CursorProtocol) -> int:
        return cursor.rowcount

    async def aio_sequence_exists(self, seq: str) -> bool:
        raise NotImplementedError

    async def aio_get_tables(self, schema: str | None = None) -> list[str]:
        raise NotImplementedError

    async def aio_table_exists(self, table_name: Any, schema: str | None = None) -> bool:
        if peewee.is_model(table_name):
            model = table_name
            table_name = model._meta.table_name
            schema = model._meta.schema
        return table_name in await self.aio_get_tables(schema=schema)


class AioPostgresDatabase(AioDatabase):
    async def aio_last_insert_id(self, cursor: CursorProtocol, query: peewee.Insert) -> Any:
        if query._query_type == peewee.Insert.SIMPLE:
            try:
                return (await cursor.fetchmany(1))[0][0]
            except (IndexError, KeyError, TypeError):
                return None
        return await fetch_models(cursor, query)

    async def aio_sequence_exists(self, sequence: str) -> bool:
        res = await self.aio_execute_sql(
            """
            SELECT COUNT(*) FROM pg_class, pg_namespace
            WHERE relkind='S'
                AND pg_class.relnamespace = pg_namespace.oid
                AND relname=%s""",
            [
                sequence,
            ],
            fetch_results=fetchone,
        )
        return bool(res[0])

    async def aio_get_tables(self, schema: str | None = None) -> list[str]:
        query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s ORDER BY tablename"
        return [row for (row,) in await self.aio_execute_sql(query, (schema or "public",), fetch_results=fetchall)]


class Psycopg3Database(AioPostgresDatabase, ext.Psycopg3Database):
    """Extension for `playhouse.Psycopg3Database` providing extra methods
    for managing async connection based on psycopg3 pool backend.

    Example::

        database = Psycopg3Database(
            'database': 'postgres',
            'host': '127.0.0.1',
            'port': 5432,
            'password': 'postgres',
            'user': 'postgres',
            'pool_params': {
                "min_size": 0,
                "max_size": 5,
                'max_lifetime': 15
            }
        )

    See also:
    https://docs.peewee-orm.com/en/4.0.0/peewee/api.html#PostgresqlDatabase
    https://www.psycopg.org/psycopg3/docs/advanced/pool.html
    """

    pool_backend_cls = PsycopgPoolBackend


class PostgresqlDatabase(AioPostgresDatabase, ext.PostgresqlExtDatabase):
    """Extension for `playhouse.PostgresqlDatabase` providing extra methods
    for managing async connection based on aiopg pool backend.


    Example::

        database = PostgresqlDatabase(
            'database': 'postgres',
            'host': '127.0.0.1',
            'port':5432,
            'password': 'postgres',
            'user': 'postgres',
            'pool_params': {
                "minsize": 0,
                "maxsize": 5,
                "timeout": 30,
                'pool_recycle': 1.5
            }
        )

    See also:
    https://docs.peewee-orm.com/en/4.0.0/peewee/api.html#PostgresqlDatabase
    https://aiopg.readthedocs.io/en/stable/
    """

    pool_backend_cls = PostgresqlPoolBackend

    def init_pool_params_defaults(self) -> None:
        self.pool_params.update({"enable_json": True, "enable_hstore": self._register_hstore})


class MySQLDatabase(AioDatabase, peewee.MySQLDatabase):
    """MySQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    Example::

        database = MySQLDatabase(
            'database': 'mysql',
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': 'mysql',
            'connect_timeout': 30,
            "pool_params": {
                "minsize": 0,
                "maxsize": 5,
                "pool_recycle": 2
            }
        )

    See also:
    https://docs.peewee-orm.com/en/4.0.0/peewee/api.html#MySQLDatabase
    https://aiomysql.readthedocs.io/en/stable/
    """

    pool_backend_cls = MysqlPoolBackend

    def init_pool_params_defaults(self) -> None:
        self.pool_params.update({"autocommit": True})

    async def aio_get_tables(self, schema: str | None = None) -> list[str]:
        query = (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_type != %s "
            "ORDER BY table_name"
        )
        return [row for (row,) in await self.aio_execute_sql(query, ("VIEW",), fetch_results=fetchall)]
