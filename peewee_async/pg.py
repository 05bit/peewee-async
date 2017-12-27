try:
    import aiopg
except ImportError:
    aiopg = None

import asyncio
import peewee
from playhouse.db_url import register_database

from .database import AsyncDatabase


class AsyncPostgresqlConnection:
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database=None, loop=None, timeout=None, **kwargs):
        self.pool = None
        self.loop = loop
        self.database = database
        self.timeout = timeout or aiopg.DEFAULT_TIMEOUT
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
        self.pool = yield from aiopg.create_pool(
            loop=self.loop,
            timeout=self.timeout,
            database=self.database,
            **self.connect_kwargs)

    @asyncio.coroutine
    def close(self):
        """Terminate all pool connections.
        """
        self.pool.terminate()
        yield from self.pool.wait_closed()

    @asyncio.coroutine
    def cursor(self, conn=None, *args, **kwargs):
        """Get a cursor for the specified transaction connection
        or acquire from the pool.
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
        cursor.close()
        if not in_transaction:
            self.release(conn)


class AsyncPostgresqlMixin(AsyncDatabase):
    """Mixin for `peewee.PostgresqlDatabase` providing extra methods
    for managing async connection.
    """
    if aiopg:
        import psycopg2
        Error = psycopg2.Error

    def init_async(self, conn_cls=AsyncPostgresqlConnection,
                   enable_json=False, enable_hstore=False):
        if not aiopg:
            raise Exception("Error, aiopg is not installed!")
        self._async_conn_cls = conn_cls
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore

    @property
    def connect_kwargs_async(self):
        """Connection parameters for `aiopg.Connection`
        """
        kwargs = self.connect_kwargs.copy()
        kwargs.update({
            'minsize': self.min_connections,
            'maxsize': self.max_connections,
            'enable_json': self._enable_json,
            'enable_hstore': self._enable_hstore,
        })
        return kwargs

    @asyncio.coroutine
    def last_insert_id_async(self, cursor, model):
        """Get ID of last inserted row.

        NOTE: it's a copy-paste, not sure how to make it better
        https://github.com/05bit/peewee/blob/2.3.2/peewee.py#L2907
        """
        meta = model._meta
        schema = ''
        if meta.schema:
            schema = '%s.' % meta.schema

        if meta.primary_key.sequence:
            seq = meta.primary_key.sequence
        elif meta.auto_increment:
            seq = '%s_%s_seq' % (meta.db_table, meta.primary_key.db_column)
        else:
            seq = None

        if seq:
            yield from cursor.execute("SELECT CURRVAL('%s\"%s\"')" % (schema, seq))
            result = (yield from cursor.fetchone())[0]
            return result


class PostgresqlDatabase(AsyncPostgresqlMixin, peewee.PostgresqlDatabase):
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

    @property
    def use_speedups(self):
        return False

    @use_speedups.setter
    def use_speedups(self, value):
        pass


register_database(PostgresqlDatabase, 'postgres+async', 'postgresql+async')


class PooledPostgresqlDatabase(AsyncPostgresqlMixin, peewee.PostgresqlDatabase):
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
        super().init(database, **kwargs)
        self.init_async()

    @property
    def use_speedups(self):
        return False

    @use_speedups.setter
    def use_speedups(self, value):
        pass


register_database(PooledPostgresqlDatabase, 'postgres+pool+async', 'postgresql+pool+async')
