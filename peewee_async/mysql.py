try:
    import aiomysql
except ImportError:
    aiomysql = None

import asyncio
import peewee
from playhouse.db_url import register_database

from .database import AsyncDatabase


class AsyncMySQLConnection:
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database=None, loop=None, timeout=None, **kwargs):
        self.pool = None
        self.loop = loop
        self.database = database
        self.timeout = timeout
        self.connect_kwargs = kwargs

    @asyncio.coroutine
    def acquire(self):
        """Acquire connection from pool.
        """
        return (yield from self.pool.acquire())

    def release(self, conn):
        """Release connection to pool.
        """
        self.pool.release(conn)

    @asyncio.coroutine
    def connect(self):
        """Create connection pool asynchronously.
        """
        self.pool = yield from aiomysql.create_pool(
            loop=self.loop,
            db=self.database,
            connect_timeout=self.timeout,
            **self.connect_kwargs)

    @asyncio.coroutine
    def close(self):
        """Terminate all pool connections.
        """
        self.pool.terminate()
        yield from self.pool.wait_closed()

    @asyncio.coroutine
    def cursor(self, conn=None, *args, **kwargs):
        """Get cursor for connection from pool.
        """
        in_transaction = conn is not None
        if not conn:
            conn = yield from self.acquire()
        cursor = yield from conn.cursor(*args, **kwargs)
        # NOTE: `cursor.release` is an awaitable object!
        cursor.release = self.release_cursor(
            cursor, in_transaction=in_transaction)
        return cursor

    @asyncio.coroutine
    def release_cursor(self, cursor, in_transaction=False):
        """Release cursor coroutine. Unless in transaction,
        the connection is also released back to the pool.
        """
        conn = cursor.connection
        yield from cursor.close()
        if not in_transaction:
            self.release(conn)

class MySQLDatabase(AsyncDatabase, peewee.MySQLDatabase):
    """MySQL database driver providing **single drop-in sync** connection
    and **single async connection** interface.

    Example::

        database = MySQLDatabase('test')

    See also:
    http://peewee.readthedocs.io/en/latest/peewee/api.html#MySQLDatabase
    """
    if aiomysql:
        import pymysql
        Error = pymysql.Error

    def init(self, database, **kwargs):
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")
        self.min_connections = 1
        self.max_connections = 1
        self._async_conn_cls = kwargs.pop('async_conn', AsyncMySQLConnection)
        super().init(database, **kwargs)

    @property
    def connect_kwargs_async(self):
        """Connection parameters for `aiomysql.Connection`
        """
        kwargs = self.connect_kwargs.copy()
        kwargs.update({
            'minsize': self.min_connections,
            'maxsize': self.max_connections,
            'autocommit': True,
        })
        return kwargs

    @asyncio.coroutine
    def last_insert_id_async(self, cursor, model):
        """Get ID of last inserted row.
        """
        if model._meta.auto_increment:
            return cursor.lastrowid

    @property
    def use_speedups(self):
        return False

    @use_speedups.setter
    def use_speedups(self, value):
        pass


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
