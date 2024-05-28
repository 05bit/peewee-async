"""
peewee-async
============

Asynchronous interface for `peewee`_ ORM powered by `asyncio`_:
https://github.com/05bit/peewee-async

.. _peewee: https://github.com/coleifer/peewee
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Licensed under The MIT License (MIT)

Copyright (c) 2014, Alexey Kinëv <rudy@05bit.com>

"""
import abc
import asyncio
import contextlib
import logging
import uuid
import warnings
from contextvars import ContextVar
from importlib.metadata import version
from typing import Optional, Type

import peewee
from playhouse.db_url import register_database
from peewee_async_compat import Manager, count, execute, prefetch, scalar, savepoint, atomic, transaction
from peewee_async_compat import _patch_query_with_compat_methods

try:
    import aiopg
    import psycopg2
except ImportError:
    aiopg = None
    psycopg2 = None

try:
    import aiomysql
    import pymysql
except ImportError:
    aiomysql = None
    pymysql = None

try:
    asyncio_current_task = asyncio.current_task
except AttributeError:
    asyncio_current_task = asyncio.Task.current_task

__version__ = version('peewee-async')


__all__ = [
    # TODO: Define new classes here
    # ...
    'PostgresqlDatabase',
    'PooledPostgresqlDatabase',
    'MySQLDatabase',
    'PooledMySQLDatabase',
    'Transaction',
    'AioModel',

    # Compatibility API (deprecated in v1.0 release)
    'Manager',
    'execute',
    'count',
    'scalar',
    'prefetch',
    'atomic',
    'transaction',
    'savepoint',
]

__log__ = logging.getLogger('peewee.async')
__log__.addHandler(logging.NullHandler())


class ConnectionContext:
    def __init__(self, connection):
        self.connection = connection
        # needs for to know whether begin a transaction  or create a savepoint
        self.transaction_is_opened = False

connection_context: ContextVar[Optional[ConnectionContext]] = ContextVar("connection_context", default=None)

###################
# Result wrappers #
###################


class RowsCursor(object):
    def __init__(self, rows, description):
        self._rows = rows
        self.description = description
        self._idx = 0

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def close(self):
        pass


class AsyncQueryWrapper:
    """Async query results wrapper for async `select()`. Internally uses
    results wrapper produced by sync peewee select query.

    Arguments:

        result_wrapper -- empty results wrapper produced by sync `execute()`
        call cursor -- async cursor just executed query

    To retrieve results after async fetching just iterate over this class
    instance, like you generally iterate over sync results wrapper.
    """
    def __init__(self, *, cursor=None, query=None):
        self._cursor = cursor
        self._rows = []
        self._result_cache = None
        self._result_wrapper = self._get_result_wrapper(query)

    def __iter__(self):
        return iter(self._result_wrapper)

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, idx):
        # NOTE: side effects will appear when both
        # iterating and accessing by index!
        if self._result_cache is None:
            self._result_cache = list(self)
        return self._result_cache[idx]

    def _get_result_wrapper(self, query):
        """Get result wrapper class.
        """
        cursor = RowsCursor(self._rows, self._cursor.description)
        return query._get_cursor_wrapper(cursor)

    async def fetchone(self):
        """Fetch single row from the cursor.
        """
        row = await self._cursor.fetchone()
        if not row:
            raise GeneratorExit
        self._rows.append(row)

    async def fetchall(self):
        try:
            while True:
                await self.fetchone()
        except GeneratorExit:
            pass

    @classmethod
    async def make_for_all_rows(cls, cursor, query):
        result = AsyncQueryWrapper(cursor=cursor, query=query)
        await result.fetchall()
        return result


###############
# Transaction #
###############

class Transaction:

    def __init__(self, connection, is_savepoint=False):
        self.connection = connection
        if is_savepoint:
            self.savepoint = f"PWASYNC__{uuid.uuid4().hex}"
        else:
            self.savepoint = None

    @property
    def is_savepoint(self):
        return self.savepoint is not None

    async def execute(self, sql):
        async with self.connection.cursor() as cursor:
            await cursor.execute(sql)

    async def begin(self):
        sql = "BEGIN"
        if self.savepoint:
            sql = f"SAVEPOINT {self.savepoint}"
        return await self.execute(sql)

    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    async def commit(self):

        sql = "COMMIT"
        if self.savepoint:
            sql = f"RELEASE SAVEPOINT {self.savepoint}"
        return await self.execute(sql)

    async def rollback(self):
        sql = "ROLLBACK"
        if self.savepoint:
            sql = f"ROLLBACK TO SAVEPOINT {self.savepoint}"
        return await self.execute(sql)


class ConnectionContextManager:
    def __init__(self, pool_backend):
        self.pool_backend = pool_backend
        self.connection_context = connection_context.get()
        self.resuing_connection = self.connection_context is not None

    async def __aenter__(self):
        if self.connection_context is not None:
            connection = self.connection_context.connection
        else:
            connection = await self.pool_backend.acquire()
            self.connection_context = ConnectionContext(connection)
            connection_context.set(self.connection_context)
        return connection

    async def __aexit__(self, *args):
        if self.resuing_connection is False:
            self.pool_backend.release(self.connection_context.connection)
            connection_context.set(None)


class PoolBackend(metaclass=abc.ABCMeta):
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database=None, **kwargs):
        self.pool = None
        self.database = database
        self.connect_params = kwargs
        self._connection_lock = asyncio.Lock()

    @property
    def is_connected(self):
        return self.pool is not None and self.pool.closed is False

    def has_acquired_connections(self):
        return self.pool is not None and len(self.pool._used) > 0

    async def connect(self):
        async with self._connection_lock:
            if self.is_connected is False:
                await self.create()

    async def acquire(self):
        """Acquire connection from pool.
        """
        if self.pool is None:
            await self.connect()
        return await self.pool.acquire()

    def release(self, conn):
        """Release connection to pool.
        """
        self.pool.release(conn)

    @abc.abstractmethod
    async def create(self):
        """Create connection pool asynchronously.
        """
        raise NotImplementedError

    async def terminate(self):
        """Terminate all pool connections.
        """
        if self.pool is not None:
            self.pool.terminate()
            await self.pool.wait_closed()


############
# Database #
############

class AioDatabase:
    _allow_sync = True  # whether sync queries are allowed

    pool_backend_cls: Type[PoolBackend]

    def __init__(self, database, **kwargs):
        super().__init__(database, **kwargs)
        self.pool_backend = self.pool_backend_cls(
            database=self.database,
            **self.connect_params_async
        )

    async def aio_connect(self):
        """Set up async connection on default event loop.
        """
        if self.deferred:
            raise Exception("Error, database not properly initialized "
                            "before opening connection")
        await self.pool_backend.connect()

    @property
    def is_connected(self):
        return self.pool_backend.is_connected

    async def aio_close(self):
        """Close async connection.
        """
        await self.pool_backend.terminate()

    @contextlib.asynccontextmanager
    async def aio_atomic(self):
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

    def set_allow_sync(self, value):
        """Allow or forbid sync queries for the database. See also
        the :meth:`.allow_sync()` context manager.
        """
        self._allow_sync = value

    @contextlib.contextmanager
    def allow_sync(self):
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
            try:
                self.close()
            except self.Error:
                pass  # already closed

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
        # To make `Database.aio_execute` compatible with peewee's sync queries we
        # apply optional patching, it will do nothing for Aio-counterparts:
        _patch_query_with_compat_methods(query, None)
        ctx = self.get_sql_context()
        sql, params = ctx.sql(query).query()
        fetch_results = fetch_results or getattr(query, 'fetch_results', None)
        return await self.aio_execute_sql(sql, params, fetch_results=fetch_results)

    #### Deprecated methods ####
    def __setattr__(self, name, value):
        if name == 'allow_sync':
            warnings.warn(
                "`.allow_sync` setter is deprecated, use either the "
                "`.allow_sync()` context manager or `.set_allow_sync()` "
                "method.", DeprecationWarning)
            self._allow_sync = value
        else:
            super().__setattr__(name, value)

    def atomic_async(self):
        """Similar to peewee `Database.atomic()` method, but returns
        asynchronous context manager.
        """
        warnings.warn(
            "`atomic_async` is deprecated, use `aio_atomic` instead.",
            DeprecationWarning
        )
        return self.aio_atomic()

    def savepoint_async(self, sid=None):
        """Similar to peewee `Database.savepoint()` method, but returns
        asynchronous context manager.
        """
        warnings.warn(
            "`savepoint` is deprecated, use `aio_atomic` instead.",
            DeprecationWarning
        )
        return savepoint(self, sid=sid)

    async def connect_async(self):
        warnings.warn(
            "`connect_async` is deprecated, use `aio_connect` instead.",
            DeprecationWarning
        )
        await self.aio_connect()

    async def close_async(self):
        warnings.warn(
            "`close_async` is deprecated, use `aio_close` instead.",
            DeprecationWarning
        )
        await self.aio_close()

    def transaction_async(self):
        """Similar to peewee `Database.transaction()` method, but returns
        asynchronous context manager.
        """
        warnings.warn(
            "`atomic_async` is deprecated, use `aio_atomic` instead.",
            DeprecationWarning
        )
        return self.aio_atomic()

##############
# PostgreSQL #
##############


class PostgresqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool.
    """

    async def create(self):
        """Create connection pool asynchronously.
        """
        if "connect_timeout" in self.connect_params:
            self.connect_params['timeout'] = self.connect_params.pop("connect_timeout")
        self.pool = await aiopg.create_pool(
            database=self.database,
            **self.connect_params
        )


class AioPostgresqlMixin(AioDatabase):
    """Mixin for `peewee.PostgresqlDatabase` providing extra methods
    for managing async connection.
    """

    pool_backend_cls = PostgresqlPoolBackend

    if psycopg2:
        Error = psycopg2.Error

    def init_async(self, enable_json=False, enable_hstore=False):
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

    async def last_insert_id_async(self, cursor):
        """Get ID of last inserted row.

        NOTE: it's not clear, when this code is executed?
        """
        # try:
        #     return cursor if query_type else cursor[0][0]
        # except (IndexError, KeyError, TypeError):
        #     pass
        return cursor.lastrowid


class PostgresqlDatabase(AioPostgresqlMixin, peewee.PostgresqlDatabase):
    """PosgreSQL database driver providing **single drop-in sync** connection
    and **single async connection** interface.

    Example::

        database = PostgresqlDatabase('test')

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#PostgresqlDatabase
    """
    def init(self, database, **kwargs):
        self.min_connections = 1
        self.max_connections = 1
        super().init(database, **kwargs)
        self.init_async()


register_database(PostgresqlDatabase, 'postgres+async', 'postgresql+async')


class PooledPostgresqlDatabase(AioPostgresqlMixin, peewee.PostgresqlDatabase):
    """PosgreSQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    Example::

        database = PooledPostgresqlDatabase('test', max_connections=20)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#PostgresqlDatabase
    """
    def init(self, database, **kwargs):
        self.min_connections = kwargs.pop('min_connections', 1)
        self.max_connections = kwargs.pop('max_connections', 20)
        connection_timeout = kwargs.pop('connection_timeout', None)
        if connection_timeout is not None:
            warnings.warn(
                "`connection_timeout` is deprecated, use `connect_timeout` instead.",
                DeprecationWarning
            )
            kwargs['connect_timeout'] = connection_timeout
        super().init(database, **kwargs)
        self.init_async()


register_database(PooledPostgresqlDatabase, 'postgres+pool+async', 'postgresql+pool+async')


#########
# MySQL #
#########


class MysqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool.
    """

    async def create(self):
        """Create connection pool asynchronously.
        """
        self.pool = await aiomysql.create_pool(
            db=self.database, **self.connect_params
        )


class MySQLDatabase(AioDatabase, peewee.MySQLDatabase):
    """MySQL database driver providing **single drop-in sync** connection
    and **single async connection** interface.

    Example::

        database = MySQLDatabase('test')

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """
    pool_backend_cls = MysqlPoolBackend

    if pymysql:
        Error = pymysql.Error

    def init(self, database, **kwargs):
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")
        self.min_connections = 1
        self.max_connections = 1
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

    async def last_insert_id_async(self, cursor):
        """Get ID of last inserted row.
        """
        return cursor.lastrowid


register_database(MySQLDatabase, 'mysql+async')


class PooledMySQLDatabase(MySQLDatabase):
    """MySQL database driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    Example::

        database = MySQLDatabase('test', max_connections=10)

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """
    def init(self, database, **kwargs):
        min_connections = kwargs.pop('min_connections', 1)
        max_connections = kwargs.pop('max_connections', 10)
        super().init(database, **kwargs)
        self.min_connections = min_connections
        self.max_connections = max_connections


register_database(PooledMySQLDatabase, 'mysql+pool+async')


class AioQueryMixin:
    @peewee.database_required
    async def aio_execute(self, database):
        return await database.aio_execute(self)

    async def make_async_query_wrapper(self, cursor):
        return await AsyncQueryWrapper.make_for_all_rows(cursor, self)


class AioModelDelete(peewee.ModelDelete, AioQueryMixin):
    async def fetch_results(self, cursor):
        if self._returning:
            return await self.make_async_query_wrapper(cursor)
        return cursor.rowcount


class AioModelUpdate(peewee.ModelUpdate, AioQueryMixin):

    async def fetch_results(self, cursor):
        if self._returning:
            return await self.make_async_query_wrapper(cursor)
        return cursor.rowcount


class AioModelInsert(peewee.ModelInsert, AioQueryMixin):
    async def fetch_results(self, cursor):
        if self._returning is not None and len(self._returning) > 1:
            return await self.make_async_query_wrapper(cursor)

        if self._returning:
            row = await cursor.fetchone()
            return row[0] if row else None
        else:
            return await self._database.last_insert_id_async(cursor)


class AioModelRaw(peewee.ModelRaw, AioQueryMixin):
    async def fetch_results(self, cursor):
        return await self.make_async_query_wrapper(cursor)


class AioSelectMixin(AioQueryMixin):

    async def fetch_results(self, cursor):
        return await self.make_async_query_wrapper(cursor)

    @peewee.database_required
    async def aio_scalar(self, database, as_tuple=False):
        """
        Get single value from ``select()`` query, i.e. for aggregation.

        :return: result is the same as after sync ``query.scalar()`` call
        """
        async def fetch_results(cursor):
            return await cursor.fetchone()

        rows = await database.aio_execute(self, fetch_results=fetch_results)

        return rows[0] if rows and not as_tuple else rows

    async def aio_get(self, database=None):
        clone = self.paginate(1, 1)
        try:
            return (await clone.aio_execute(database))[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist('%s instance matching query does '
                                          'not exist:\nSQL: %s\nParams: %s' %
                                          (clone.model, sql, params))

    @peewee.database_required
    async def aio_count(self, database, clear_limit=False):
        clone = self.order_by().alias('_wrapped')
        if clear_limit:
            clone._limit = clone._offset = None
        try:
            if clone._having is None and clone._group_by is None and \
               clone._windows is None and clone._distinct is None and \
               clone._simple_distinct is not True:
                clone = clone.select(peewee.SQL('1'))
        except AttributeError:
            pass
        return await AioSelect([clone], [peewee.fn.COUNT(peewee.SQL('1'))]).aio_scalar(database)


class AioSelect(peewee.Select, AioSelectMixin):
    pass


class AioModelSelect(peewee.ModelSelect, AioSelectMixin):
    pass


class AioModel(peewee.Model):
    """Async version of **peewee.Model** that allows to execute queries asynchronously
    with **aio_execute** method

    Example::

        class User(peewee_async.AioModel):
            username = peewee.CharField(max_length=40, unique=True)

        await User.select().where(User.username == 'admin').aio_execute()

    Also it provides async versions of **peewee.Model** shortcuts

    Example::

        user = await User.aio_get(User.username == 'user')
    """

    @classmethod
    def select(cls, *fields):
        is_default = not fields
        if not fields:
            fields = cls._meta.sorted_fields
        return AioModelSelect(cls, fields, is_default=is_default)

    @classmethod
    def update(cls, __data=None, **update):
        return AioModelUpdate(cls, cls._normalize_data(__data, update))

    @classmethod
    def insert(cls, __data=None, **insert):
        return AioModelInsert(cls, cls._normalize_data(__data, insert))

    @classmethod
    def insert_many(cls, rows, fields=None):
        return AioModelInsert(cls, insert=rows, columns=fields)

    @classmethod
    def insert_from(cls, query, fields):
        columns = [getattr(cls, field) if isinstance(field, str)
                   else field for field in fields]
        return AioModelInsert(cls, insert=query, columns=columns)

    @classmethod
    def raw(cls, sql, *params):
        return AioModelRaw(cls, sql, params)

    @classmethod
    def delete(cls):
        return AioModelDelete(cls)

    async def aio_delete_instance(self, recursive=False, delete_nullable=False):
        if recursive:
            dependencies = self.dependencies(delete_nullable)
            for query, fk in reversed(list(dependencies)):
                print(query, fk)
                model = fk.model
                if fk.null and not delete_nullable:
                    await model.update(**{fk.name: None}).where(query).aio_execute()
                else:
                    await model.delete().where(query).aio_execute()
        return await type(self).delete().where(self._pk_expr()).aio_execute()

    @classmethod
    async def aio_get(cls, *query, **filters):
        """Async version of **peewee.Model.get**"""
        sq = cls.select()
        if query:
            if len(query) == 1 and isinstance(query[0], int):
                sq = sq.where(cls._meta.primary_key == query[0])
            else:
                sq = sq.where(*query)
        if filters:
            sq = sq.filter(**filters)
        return await sq.aio_get()

    @classmethod
    async def aio_get_or_none(cls, *query, **filters):
        """
        Async version of **peewee.Model.get_or_none**
        """
        try:
            return await cls.aio_get(*query, **filters)
        except cls.DoesNotExist:
            return None

    @classmethod
    async def aio_create(cls, **data):
        """INSERT new row into table and return corresponding model instance."""
        obj = cls(**data)
        pk = await cls.insert(**dict(obj.__data__)).aio_execute()
        if obj._pk is None:
            obj._pk = pk
        return obj
