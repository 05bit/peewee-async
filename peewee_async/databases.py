import contextlib
import logging
from typing import Type, Optional, Any, AsyncIterator, Iterator, Dict, List

import peewee
from playhouse import postgres_ext as ext

from .connection import connection_context, ConnectionContextManager
from .pool import PoolBackend, PostgresqlPoolBackend, MysqlPoolBackend
from .transactions import Transaction
from .utils import aiopg, aiomysql, __log__, FetchResults


class AioDatabase(peewee.Database):
    """Base async database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param pool_params: parameters that are passed to the pool
    :param min_connections: min connections pool size. Alias for pool_params.minsize
    :param max_connections: max connections pool size. Alias for pool_params.maxsize

    Example::

        database = PooledPostgresqlExtDatabase(
            'test',
            'min_connections': 1,
            'max_connections': 5,
            'pool_params': {"timeout": 30, 'pool_recycle': 1.5}
        )

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/api.html#Database
    """

    _allow_sync = True  # whether sync queries are allowed

    pool_backend_cls: Type[PoolBackend]
    pool_backend: PoolBackend

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.pool_params: Dict[str, Any] = {}
        super().__init__(*args, **kwargs)

    def init_pool_params_defaults(self) -> None:
        pass

    def init_pool_params(self) -> None:
        self.init_pool_params_defaults()
        self.pool_params.update(
            {
                "minsize": self.connect_params.pop("min_connections", 1),
                "maxsize": self.connect_params.pop("max_connections", 20),
            }
        )
        pool_params = self.connect_params.pop('pool_params', {})
        self.pool_params.update(pool_params)
        self.pool_params.update(self.connect_params)

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        super().init(database, **kwargs)
        self.init_pool_params()
        self.pool_backend = self.pool_backend_cls(
            database=self.database,
            **self.pool_params
        )

    async def aio_connect(self) -> None:
        """Creates a connection pool
        """
        if self.deferred:
            raise Exception('Error, database must be initialized before creating a connection pool')
        await self.pool_backend.connect()

    @property
    def is_connected(self) -> bool:
        """Checks if pool is connected
        """
        return self.pool_backend.is_connected

    async def aio_close(self) -> None:
        """Terminate pool backend. The pool is closed until you run aio_connect manually
        """
        if self.deferred:
            raise Exception('Error, database must be initialized before creating a connection pool')

        await self.pool_backend.terminate()

    @contextlib.asynccontextmanager
    async def aio_atomic(self) -> AsyncIterator[None]:
        """Similar to peewee `Database.atomic()` method, but returns
        asynchronous context manager.
        """
        async with self.aio_connection() as connection:
            _connection_context = connection_context.get()
            assert _connection_context is not None
            begin_transaction = _connection_context.transaction_is_opened is False
            try:
                async with Transaction(connection, is_savepoint=begin_transaction is False):
                    _connection_context.transaction_is_opened = True
                    yield
            finally:
                if begin_transaction is True:
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
        """Sync execute SQL query, `allow_sync` must be set to True.
        """
        assert self._allow_sync, (
            "Error, sync query is not allowed! Call the `.set_allow_sync()` "
            "or use the `.allow_sync()` context manager.")
        if self._allow_sync in (logging.ERROR, logging.WARNING):
            logging.log(
                self._allow_sync,
                "Error, sync query is not allowed: %s %s" %
                (str(args), str(kwargs))
            )
        return super().execute_sql(*args, **kwargs)

    def aio_connection(self) -> ConnectionContextManager:
        if self.deferred:
            raise Exception('Error, database must be initialized before creating a connection pool')

        return ConnectionContextManager(self.pool_backend)

    async def aio_execute_sql(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        fetch_results: Optional[FetchResults] = None
    ) -> Any:
        __log__.debug((sql, params))
        with peewee.__exception_wrapper__:
            async with self.aio_connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(sql, params or ())
                    if fetch_results is not None:
                        return await fetch_results(cursor)

    async def aio_execute(self, query: Any, fetch_results: Optional[FetchResults] = None) -> Any:
        """Execute *SELECT*, *INSERT*, *UPDATE* or *DELETE* query asyncronously.

        :param query: peewee query instance created with ``Model.select()``,
                      ``Model.update()`` etc.
        :param fetch_results: function with cursor param. It let you get data manually and
                              don't need to close cursor It will be closed automatically.
        :return: result depends on query type, it's the same as for sync `query.execute()`
        """
        ctx = self.get_sql_context()
        sql, params = ctx.sql(query).query()
        fetch_results = fetch_results or getattr(query, 'fetch_results', None)
        return await self.aio_execute_sql(sql, params, fetch_results=fetch_results)


class PooledPostgresqlDatabase(AioDatabase, peewee.PostgresqlDatabase):
    """Extension for `peewee.PostgresqlDatabase` providing extra methods
    for managing async connection.

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/api.html#PostgresqlDatabase
    """

    pool_backend_cls = PostgresqlPoolBackend

    def init_pool_params_defaults(self) -> None:
        self.pool_params.update({"enable_json": False, "enable_hstore": False})

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        if not aiopg:
            raise Exception("Error, aiopg is not installed!")
        super().init(database, **kwargs)


class PooledPostgresqlExtDatabase(
    PooledPostgresqlDatabase,
    ext.PostgresqlExtDatabase
):
    """PosgtreSQL database extended driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    JSON fields support is enabled by default, HStore supports is disabled by
    default, but can be enabled through pool_params or with ``register_hstore=False`` argument.

    Example::

        database = PooledPostgresqlExtDatabase('test', register_hstore=False,
                                               max_connections=20)

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """
    def init_pool_params_defaults(self) -> None:
        self.pool_params.update({
            "enable_json": True,
            "enable_hstore": self._register_hstore
        })


class PooledMySQLDatabase(AioDatabase, peewee.MySQLDatabase):
    """MySQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    Example::

        database = PooledMySQLDatabase('test', max_connections=10)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """
    pool_backend_cls = MysqlPoolBackend

    def init_pool_params_defaults(self) -> None:
        self.pool_params.update({"autocommit": True})

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")
        super().init(database, **kwargs)
