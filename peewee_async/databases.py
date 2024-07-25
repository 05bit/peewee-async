import contextlib
import logging
from typing import Type, Optional, Any, AsyncIterator, Iterator

import peewee
from playhouse import postgres_ext as ext

from .connection import connection_context, ConnectionContextManager
from .pool import PoolBackend, PostgresqlPoolBackend, MysqlPoolBackend
from .transactions import Transaction
from .utils import psycopg2, aiopg, pymysql, aiomysql, __log__


class AioDatabase:
    _allow_sync = True  # whether sync queries are allowed

    pool_backend_cls: Type[PoolBackend]

    def __init__(self, database: Optional[str], **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        if not database:
            raise Exception("Deferred initialization is not supported")
        self.pool_backend = self.pool_backend_cls(
            database=self.database,
            **self.connect_params_async
        )

    async def aio_connect(self) -> None:
        """Creates a connection pool
        """
        await self.pool_backend.connect()

    @property
    def is_connected(self) -> bool:
        """Checks if pool is connected
        """
        return self.pool_backend.is_connected

    async def aio_close(self) -> None:
        """Terminate pool backend. The pool is closed until you run aio_connect manually
        """
        await self.pool_backend.terminate()

    @contextlib.asynccontextmanager
    async def aio_atomic(self) -> AsyncIterator[None]:
        """Similar to peewee `Database.atomic()` method, but returns
        asynchronous context manager.
        """
        async with self.aio_connection() as connection:
            _connection_context = connection_context.get()
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

    def execute_sql(self, *args, **kwargs):
        """Sync execute SQL query, `allow_sync` must be set to True.
        """
        assert self._allow_sync, (
            "Error, sync query is not allowed! Call the `.set_allow_sync()` "
            "or use the `.allow_sync()` context manager.")
        if self._allow_sync in (logging.ERROR, logging.WARNING):
            logging.log(self._allow_sync,
                        "Error, sync query is not allowed: %s %s" %
                        (str(args), str(kwargs)))
        return super().execute_sql(*args, **kwargs)

    def aio_connection(self) -> ConnectionContextManager:
        return ConnectionContextManager(self.pool_backend)

    async def aio_execute_sql(self, sql: str, params=None, fetch_results=None):
        __log__.debug(sql, params)
        with peewee.__exception_wrapper__:
            async with self.aio_connection() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(sql, params or ())
                    if fetch_results is not None:
                        return await fetch_results(cursor)

    async def aio_execute(self, query, fetch_results=None):
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


class AioPostgresqlMixin(AioDatabase):
    """Mixin for `peewee.PostgresqlDatabase` providing extra methods
    for managing async connection.
    """

    pool_backend_cls = PostgresqlPoolBackend

    if psycopg2:
        Error = psycopg2.Error

    def init_async(self, enable_json: bool = False, enable_hstore: bool =False) -> None:
        if not aiopg:
            raise Exception("Error, aiopg is not installed!")
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore

    @property
    def connect_params_async(self):
        """Connection parameters for `aiopg.Connection`
        """
        kwargs = self.connect_params.copy()
        kwargs.update({
            'minsize': self.min_connections,
            'maxsize': self.max_connections,
            'enable_json': self._enable_json,
            'enable_hstore': self._enable_hstore,
        })
        return kwargs


class PooledPostgresqlDatabase(AioPostgresqlMixin, peewee.PostgresqlDatabase):
    """PosgreSQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    Example::

        database = PooledPostgresqlDatabase('test', max_connections=20)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#PostgresqlDatabase
    """
    def init(self, database: Optional[str], **kwargs: Any) -> None:
        self.min_connections = kwargs.pop('min_connections', 1)
        self.max_connections = kwargs.pop('max_connections', 20)
        super().init(database, **kwargs)
        self.init_async()


class PooledPostgresqlExtDatabase(
    AioPostgresqlMixin,
    ext.PostgresqlExtDatabase
):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    JSON fields support is always enabled, HStore supports is enabled by
    default, but can be disabled with ``register_hstore=False`` argument.

    :param max_connections: connections pool size

    Example::

        database = PooledPostgresqlExtDatabase('test', register_hstore=False,
                                               max_connections=20)

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        self.min_connections = kwargs.pop('min_connections', 1)
        self.max_connections = kwargs.pop('max_connections', 20)
        connection_timeout = kwargs.pop('connection_timeout', None)
        super().init(database, **kwargs)
        self.init_async(
            enable_json=True,
            enable_hstore=self._register_hstore
        )


class PooledMySQLDatabase(AioDatabase, peewee.MySQLDatabase):
    """MySQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    Example::

        database = PooledMySQLDatabase('test', max_connections=10)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """
    pool_backend_cls = MysqlPoolBackend

    if pymysql:
        Error = pymysql.Error

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")
        self.min_connections = kwargs.pop('min_connections', 1)
        self.max_connections = kwargs.pop('max_connections', 20)
        super().init(database, **kwargs)

    @property
    def connect_params_async(self):
        """Connection parameters for `aiomysql.Connection`
        """
        kwargs = self.connect_params.copy()
        kwargs.update({
            'minsize': self.min_connections,
            'maxsize': self.max_connections,
            'autocommit': True,
        })
        return kwargs
