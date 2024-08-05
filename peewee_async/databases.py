import contextlib
import logging
import warnings
from typing import Dict, Type, Optional, Any, AsyncIterator, Iterator

import peewee
from playhouse import postgres_ext as ext

from peewee_async_compat import _patch_query_with_compat_methods, savepoint
from .connection import connection_context, ConnectionContextManager
from .pool import PoolBackend, PostgresqlPoolBackend, MysqlPoolBackend
from .transactions import Transaction
from .utils import psycopg2, aiopg, pymysql, aiomysql, __log__


class AioDatabase(peewee.Database):
    _allow_sync = True  # whether sync queries are allowed

    pool_backend_cls: Type[PoolBackend]
    pool_backend: PoolBackend

    @property
    def connect_params_async(self) -> Dict[str, Any]:
        ...

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        connection_timeout = kwargs.pop('connection_timeout', None)
        if connection_timeout is not None:
            warnings.warn(
                "`connection_timeout` is deprecated, use `connect_timeout` instead.",
                DeprecationWarning
            )
            kwargs['connect_timeout'] = connection_timeout
        super().init(database, **kwargs)
        self.pool_backend = self.pool_backend_cls(
            database=self.database,
            **self.connect_params_async
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
        # To make `Database.aio_execute` compatible with peewee's sync queries we
        # apply optional patching, it will do nothing for Aio-counterparts:
        _patch_query_with_compat_methods(query, None)
        ctx = self.get_sql_context()
        sql, params = ctx.sql(query).query()
        fetch_results = fetch_results or getattr(query, 'fetch_results', None)
        return await self.aio_execute_sql(sql, params, fetch_results=fetch_results)

    #### Deprecated methods ####
    def __setattr__(self, name, value) -> None:
        if name == 'allow_sync':
            warnings.warn(
                "`.allow_sync` setter is deprecated, use either the "
                "`.allow_sync()` context manager or `.set_allow_sync()` "
                "method.", DeprecationWarning)
            self._allow_sync = value
        else:
            super().__setattr__(name, value)

    def atomic_async(self) -> Any:
        """Similar to peewee `Database.atomic()` method, but returns
        asynchronous context manager.
        """
        warnings.warn(
            "`atomic_async` is deprecated, use `aio_atomic` instead.",
            DeprecationWarning
        )
        return self.aio_atomic()

    def savepoint_async(self, sid=None) -> Any:
        """Similar to peewee `Database.savepoint()` method, but returns
        asynchronous context manager.
        """
        warnings.warn(
            "`savepoint` is deprecated, use `aio_atomic` instead.",
            DeprecationWarning
        )
        return savepoint(self, sid=sid)

    async def connect_async(self) -> None:
        warnings.warn(
            "`connect_async` is deprecated, use `aio_connect` instead.",
            DeprecationWarning
        )
        await self.aio_connect()

    async def close_async(self) -> None:
        warnings.warn(
            "`close_async` is deprecated, use `aio_close` instead.",
            DeprecationWarning
        )
        await self.aio_close()

    def transaction_async(self) -> Any:
        """Similar to peewee `Database.transaction()` method, but returns
        asynchronous context manager.
        """
        warnings.warn(
            "`atomic_async` is deprecated, use `aio_atomic` instead.",
            DeprecationWarning
        )
        return self.aio_atomic()


class AioPostgresqlMixin(AioDatabase, peewee.PostgresqlDatabase):
    """Extension for `peewee.PostgresqlDatabase` providing extra methods
    for managing async connection.
    """

    _enable_json: bool
    _enable_hstore: bool

    pool_backend_cls = PostgresqlPoolBackend

    if psycopg2:
        Error = psycopg2.Error

    def init_async(self, enable_json: bool = False, enable_hstore: bool = False) -> None:
        if not aiopg:
            raise Exception("Error, aiopg is not installed!")
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore


class PooledPostgresqlDatabase(AioPostgresqlMixin, peewee.PostgresqlDatabase):
    """PostgreSQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    Example::

        database = PooledPostgresqlDatabase('test', max_connections=20)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#PostgresqlDatabase
    """
    min_connections: int = 1
    max_connections: int = 20

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        if min_connections := kwargs.pop('min_connections', False):
            self.min_connections = min_connections

        if max_connections := kwargs.pop('max_connections', False):
            self.max_connections = max_connections

        self.init_async()
        super().init(database, **kwargs)

    @property
    def connect_params_async(self):
        """Connection parameters for `aiopg.Connection`
        """
        kwargs = self.connect_params.copy()
        kwargs.update(
            {
                'minsize': self.min_connections,
                'maxsize': self.max_connections,
                'enable_json': self._enable_json,
                'enable_hstore': self._enable_hstore,
            }
        )
        return kwargs


class PooledPostgresqlExtDatabase(
    PooledPostgresqlDatabase,
    ext.PostgresqlExtDatabase
):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    JSON fields support is always enabled, HStore supports is enabled by
    default, but can be disabled with ``register_hstore=False`` argument.

    Example::

        database = PooledPostgresqlExtDatabase('test', register_hstore=False,
                                               max_connections=20)

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        self.init_async(
            enable_json=True,
            enable_hstore=self._register_hstore
        )
        super().init(database, **kwargs)


class PooledMySQLDatabase(AioDatabase, peewee.MySQLDatabase):
    """MySQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    Example::

        database = PooledMySQLDatabase('test', max_connections=10)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """
    min_connections: int = 1
    max_connections: int = 20

    pool_backend_cls = MysqlPoolBackend

    if pymysql:
        Error = pymysql.Error

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")

        if min_connections := kwargs.pop('min_connections', False):
            self.min_connections = min_connections

        if max_connections := kwargs.pop('max_connections', False):
            self.max_connections = max_connections

        super().init(database, **kwargs)

    @property
    def connect_params_async(self) -> Dict[str, Any]:
        """Connection parameters for `aiomysql.Connection`
        """
        kwargs = self.connect_params.copy()
        kwargs.update(
            {
                'minsize': self.min_connections,
                'maxsize': self.max_connections,
                'autocommit': True,
            }
        )
        return kwargs


# DEPRECATED Databases


class PostgresqlDatabase(PooledPostgresqlDatabase):
    """PosgreSQL database driver providing **single drop-in sync** connection
    and **single async connection** interface.

    Example::

        database = PostgresqlDatabase('test')

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#PostgresqlDatabase
    """

    min_connections: int = 1
    max_connections: int = 1

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        warnings.warn(
            "`PostgresqlDatabase` is deprecated, use `PooledPostgresqlDatabase` instead.",
            DeprecationWarning
        )
        super().init(database, **kwargs)


class MySQLDatabase(PooledMySQLDatabase):
    """MySQL database driver providing **single drop-in sync** connection
    and **single async connection** interface.

    Example::

        database = MySQLDatabase('test')

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """

    min_connections: int = 1
    max_connections: int = 1

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        warnings.warn(
            "`MySQLDatabase` is deprecated, use `PooledMySQLDatabase` instead.",
            DeprecationWarning
        )
        super().init(database, **kwargs)


class PostgresqlExtDatabase(PooledPostgresqlExtDatabase):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **single async connection** interface.

    JSON fields support is always enabled, HStore supports is enabled by
    default, but can be disabled with ``register_hstore=False`` argument.

    Example::

        database = PostgresqlExtDatabase('test', register_hstore=False)

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """

    min_connections: int = 1
    max_connections: int = 1

    def init(self, database: Optional[str], **kwargs: Any) -> None:
        warnings.warn(
            "`PostgresqlExtDatabase` is deprecated, use `PooledPostgresqlExtDatabase` instead.",
            DeprecationWarning
        )
        super().init(database, **kwargs)
