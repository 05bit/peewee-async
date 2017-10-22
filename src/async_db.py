import asyncio
import contextlib
import warnings
import logging

from . import transactions
from .utils import TaskLocals


class AsyncDatabase:
    _loop = None        # asyncio event loop
    _allow_sync = True  # whether sync queries are allowed
    _async_conn = None  # async connection
    _async_wait = None  # connection waiter
    _task_data = None   # asyncio per-task data

    def __setattr__(self, name, value):
        if name == 'allow_sync':
            warnings.warn(
                "`.allow_sync` setter is deprecated, use either the "
                "`.allow_sync()` context manager or `.set_allow_sync()` "
                "method.", DeprecationWarning)
            self._allow_sync = value
        else:
            super().__setattr__(name, value)

    @property
    def loop(self):
        """Get the event loop.

        If no event loop is provided explicitly on creating
        the instance, just return the current event loop.
        """
        return self._loop or asyncio.get_event_loop()

    @asyncio.coroutine
    def connect_async(self, loop=None, timeout=None):
        """Set up async connection on specified event loop or
        on default event loop.
        """
        if self.deferred:
            raise Exception("Error, database not properly initialized "
                            "before opening connection")

        if self._async_conn:
            return
        elif self._async_wait:
            yield from self._async_wait
        else:
            self._loop = loop
            self._async_wait = asyncio.Future(loop=self._loop)

            conn = self._async_conn_cls(
                database=self.database,
                loop=self._loop,
                timeout=timeout,
                **self.connect_kwargs_async)

            try:
                yield from conn.connect()
            except:
                self._async_wait.cancel()
                self._async_wait = None
                raise
            else:
                self._task_data = TaskLocals(loop=self._loop)
                self._async_conn = conn
                self._async_wait.set_result(True)

    @asyncio.coroutine
    def cursor_async(self):
        """Acquire async cursor.
        """
        yield from self.connect_async(loop=self._loop)

        if self.transaction_depth_async() > 0:
            conn = self.transaction_conn_async()
        else:
            conn = None

        try:
            return (yield from self._async_conn.cursor(conn=conn))
        except:
            yield from self.close_async()
            raise

    @asyncio.coroutine
    def close_async(self):
        """Close async connection.
        """
        if self._async_wait:
            yield from self._async_wait
        if self._async_conn:
            conn = self._async_conn
            self._async_conn = None
            self._async_wait = None
            self._task_data = None
            yield from conn.close()

    @asyncio.coroutine
    def push_transaction_async(self):
        """Increment async transaction depth.
        """
        yield from self.connect_async(loop=self.loop)
        depth = self.transaction_depth_async()
        if not depth:
            conn = yield from self._async_conn.acquire()
            self._task_data.set('conn', conn)
        self._task_data.set('depth', depth + 1)

    @asyncio.coroutine
    def pop_transaction_async(self):
        """Decrement async transaction depth.
        """
        depth = self.transaction_depth_async()
        if depth > 0:
            depth -= 1
            self._task_data.set('depth', depth)
            if depth == 0:
                conn = self._task_data.get('conn')
                self._async_conn.release(conn)
        else:
            raise ValueError("Invalid async transaction depth value")

    def transaction_depth_async(self):
        """Get async transaction depth.
        """
        return self._task_data.get('depth', 0) if self._task_data else 0

    def transaction_conn_async(self):
        """Get async transaction connection.
        """
        return self._task_data.get('conn', None) if self._task_data else None

    def transaction_async(self):
        """Similar to peewee `Database.transaction()` method, but returns
        asynchronous context manager.
        """
        return transactions.transaction(self)

    def atomic_async(self):
        """Similar to peewee `Database.atomic()` method, but returns
        asynchronous context manager.
        """
        return transactions.atomic(self)

    def savepoint_async(self, sid=None):
        """Similar to peewee `Database.savepoint()` method, but returns
        asynchronous context manager.
        """
        return transactions.savepoint(self, sid=sid)

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
            try:
                self.close()
            except self.Error:
                pass  # already closed

        self._allow_sync = old_allow_sync

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


@contextlib.contextmanager
def sync_unwanted(database):
    """Context manager for preventing unwanted sync queries.
    `UnwantedSyncQueryError` exception will raise on such query.

    NOTE: sync_unwanted() context manager is **deprecated**, use
    database's `.allow_sync()` context manager or `Manager.allow_sync()`
    context manager.
    """
    warnings.warn("sync_unwanted() context manager is deprecated, "
                  "use database's `.allow_sync()` context manager or "
                  "`Manager.allow_sync()` context manager. ",
                  DeprecationWarning)
    old_allow_sync = database._allow_sync
    database._allow_sync = False
    yield
    database._allow_sync = old_allow_sync


class UnwantedSyncQueryError(Exception):
    """Exception which is raised when performing unwanted sync query.

    NOTE: UnwantedSyncQueryError is deprecated, `assert` is used instead.
    """
    def __init__(self, *args, **kwargs):
        warnings.warn("UnwantedSyncQueryError is deprecated, "
                      "assert is used instead.",
                      DeprecationWarning)
