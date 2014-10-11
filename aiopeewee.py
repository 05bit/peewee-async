"""
aiopeewee = asyncio + peewee
============================

Asynchronous interface for **[peewee](https://github.com/coleifer/peewee)**
orm powered by **[asyncio](https://docs.python.org/3/library/asyncio.html)**.
Current state: **proof of concept**.

Copyright 2014 Alexey Kinev, 05Bit http://05bit.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import asyncio
import aiopg
import peewee
import contextlib

__all__ = (
    'create',
    'delete',
    'delete_instance',
    'select',
    # 'count',
    # 'scalar',
    'save',
    'insert',
    'update',

    'PostgresqlDatabase',
    'PooledPostgresqlDatabase',

    'sync_unwanted',
    'UnwantedSyncQuery',
)


@asyncio.coroutine
def create(model, **query):
    """
    Create object asynchronously.
    
    NOTE: copy-paste, try to avoid
    https://github.com/05bit/peewee/blob/2.3.2/peewee.py#L3372
    """
    inst = model(**query)
    yield from save(inst, force_insert=True)
    inst.prepared()
    return inst


@asyncio.coroutine
def delete_instance(obj, recursive=False, delete_nullable=False):
    """
    Delete object asynchronously.

    NOTE: design decision is nessecary to avoid copy-paste from
          `peewee.Model.delete_instance()`
          https://github.com/05bit/peewee/blob/master/peewee.py#L3547
    """
    if recursive:
        dependencies = obj.dependencies(delete_nullable)
        for query, fk in reversed(list(dependencies)):
            model = fk.model_class
            if fk.null and not delete_nullable:
                yield from update(model.update(**{fk.name: None}).where(query))
            else:
                yield from delete(model.delete().where(query))
    result = yield from delete(obj.delete().where(obj.pk_expr()))
    return result


@asyncio.coroutine
def select(query_):
    """
    Perform SELECT query asynchronously.
    """
    query = query_.clone()

    # Perform async query
    cursor = yield from _execute(query)

    # Perform fake execute: we only need result wrapper class
    # TODO: sugest to expose some API in peewee?
    query._execute = lambda: None
    result_wrapper = query.execute()

    # Async fetch results
    class AsyncQueryResult(result_wrapper.__class__):
        def __iter__(self):
            return iter(self._result_cache)

        def __next__(self):
            raise NotImplementedError

        @asyncio.coroutine
        def iterate(self):
            row = yield from self.cursor.fetchone()

            if not row:
                self._populated = True
                raise GeneratorExit
            elif not self._initialized:
                self.initialize(self.cursor.description)
                self._initialized = True

            obj = self.process_row(row)
            self._result_cache.append(obj)

    result = AsyncQueryResult(query.model_class, cursor,
                              query.get_query_meta())

    try:
        while True:
            yield from result.iterate()
    except GeneratorExit:
        pass

    # Release cursor
    cursor.release()

    return result


@asyncio.coroutine
def count(query):
    raise NotImplementedError


@asyncio.coroutine
def scalar(query):
    raise NotImplementedError


@asyncio.coroutine
def save(obj, force_insert=False, only=None):
    """
    NOTE: it's a copy-paste, not sure how to make it better
    https://github.com/05bit/peewee/blob/2.3.2/peewee.py#L3486
    """
    field_dict = dict(obj._data)
    pk_field = obj._meta.primary_key
    if only:
        field_dict = obj._prune_fields(field_dict, only)
    if obj.get_id() is not None and not force_insert:
        if not isinstance(pk_field, peewee.CompositeKey):
            field_dict.pop(pk_field.name, None)
        else:
            field_dict = obj._prune_fields(field_dict, obj.dirty_fields)
        rows = yield from update(obj.update(**field_dict).where(obj.pk_expr()))
    else:
        pk = obj.get_id()
        pk_from_cursor = yield from insert(obj.insert(**field_dict))
        if pk_from_cursor is not None:
            pk = pk_from_cursor
        obj.set_id(pk)  # Do not overwrite current ID with a None value.
        rows = 1
    obj._dirty.clear()
    return rows


@asyncio.coroutine
def insert(query):
    """
    Perform INSERT query asynchronously. Returns last insert ID.
    """
    assert isinstance(query, peewee.InsertQuery), ("Error, trying to run insert coroutine"
                                                   "with wrong query class %s" % str(query))
    cursor = yield from _execute(query)
    result = yield from query.database.last_insert_id_async(cursor, query.model_class)
    cursor.release()
    return result


@asyncio.coroutine
def update(query):
    """
    Perform UPDATE query asynchronously. Returns number of rows updated.
    """
    assert isinstance(query, peewee.UpdateQuery), ("Error, trying to run update coroutine"
                                                   "with wrong query class %s" % str(query))
    cursor = yield from _execute(query)
    rowcount = cursor.rowcount
    cursor.release()
    return rowcount


@asyncio.coroutine
def delete(query):
    """
    Perform DELETE query asynchronously. Returns number of rows deleted.
    """
    assert isinstance(query, peewee.DeleteQuery), ("Error, trying to run delete coroutine"
                                                   "with wrong query class %s" % str(query))
    cursor = yield from _execute(query)
    rowcount = cursor.rowcount
    cursor.release()
    return rowcount


@asyncio.coroutine
def _execute(query):
    """
    Execute query and return cursor.
    """
    conn = query.database.async
    assert conn, "Error, database is not connected"
    cursor = yield from conn.cursor()
    yield from cursor.execute(*query.sql())
    return cursor


class PostgresqlDatabase(peewee.PostgresqlDatabase):
    """
    Drop-in replacement for default `peewee.PostgresqlDatabase` backend with
    extra `asyncio` powered interface.
    """
    def __init__(self, database, threadlocals=True, autocommit=True,
                 fields=None, ops=None, autorollback=True, **connect_kwargs):
        # Base sync connection
        super().__init__(database, threadlocals=True, autocommit=autocommit,
                         fields=fields, ops=ops, autorollback=autorollback,
                         **connect_kwargs)

        # Async connection
        self.async = None
        self._sync_unwanted = False
        self._loop = None

    def connect(self, loop=None):
        """
        Connect is sync or async mode. For async connection you should
        provide `loop` parameter.
        """
        if loop:
            if self.async: # and not self.async.closed:
                raise Exception("Error, async connection is already opened")

            self._loop = loop if loop else asyncio.get_event_loop()
            return self.connect_async()
        else:
            if not self.is_closed():
                raise Exception("Error, sync connection is already opened")

            super().connect()

    @asyncio.coroutine
    def connect_async(self):
        timeout = aiopg.DEFAULT_TIMEOUT
        conn = AsyncPostgresDatabase(self.database, timeout, self._loop, **self.connect_kwargs)
        yield from conn.connect()
        self.async = conn

    def close(self):
        """
        Close both sync and async connections.
        """
        super().close()

        if self.async:
            self.async.close()
            self.async = None
            self._loop = None

    def execute_sql(self, *args, **kwargs):
        if self._sync_unwanted:
            raise UnwantedSyncQuery("Error, unwanted sync query", args, kwargs)
        return super().execute_sql(*args, **kwargs)

    @asyncio.coroutine
    def last_insert_id_async(self, cursor, model):
        """
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
            result = yield from cursor.fetchone()
            return result


class PooledPostgresqlDatabase(PostgresqlDatabase):
    def __init__(self, *args, **kwargs):
        self.max_connections = kwargs.pop('max_connections', 20)
        self.min_connections = kwargs.pop('min_connections', self.max_connections)
        super().__init__(*args, **kwargs)

    @asyncio.coroutine
    def connect_async(self):
        minsize = self.min_connections
        maxsize = self.max_connections
        timeout = aiopg.DEFAULT_TIMEOUT
        pool = PooledAsyncPostgresDatabase(self.database, minsize, maxsize,
                                           self._loop, timeout, **self.connect_kwargs)
        yield from pool.connect()
        self.async = pool


class AsyncPostgresDatabase:
    """
    Asynchronous single connection database interface.

    Async methods:

        connect()
        cursor()
    """
    def __init__(self, database, timeout, loop, **connect_kwargs):
        self._conn = None
        self._loop = loop if loop else asyncio.get_event_loop()
        self.database = database
        self.timeout = timeout
        self.waiter = asyncio.Future(loop=loop)
        self.connect_kwargs = connect_kwargs

        dsn = 'dbname=%s' % self.database
        for k in ('user', 'password', 'host', 'port'):
            if self.connect_kwargs.get(k, None):
                dsn += ' %s=%s' % (k, connect_kwargs.pop(k))
        self.dsn = dsn

    @asyncio.coroutine
    def connect(self):
        self._conn = yield from aiopg.connect(dsn=self.dsn, timeout=self.timeout,
                                              loop=self._loop, enable_json=False,
                                              **self.connect_kwargs)

    def close(self):
        self._conn.close()

    @asyncio.coroutine
    def cursor(self, *args, **kwargs):
        cursor = yield from self._conn.cursor(*args, **kwargs)
        cursor.release = lambda: None
        return cursor


class PooledAsyncPostgresDatabase:
    """
    Asynchronous pooled connection database interface.

    Async methods:

        connect()
        cursor()
    """
    def __init__(self, database, minsize, maxsize, loop, timeout, **connect_kwargs):
        self._pool = None
        self._loop = loop if loop else asyncio.get_event_loop()
        self.database = database
        self.minsize = minsize
        self.maxsize = maxsize
        self.connect_kwargs = connect_kwargs
        self.timeout = timeout

        dsn = 'dbname=%s' % database
        for k in ('user', 'password', 'host', 'port'):
            if connect_kwargs.get(k, None):
                dsn += ' %s=%s' % (k, connect_kwargs.pop(k))
        self.dsn = dsn

    @asyncio.coroutine
    def connect(self):
        self._pool = yield from aiopg.create_pool(
            dsn=self.dsn, minsize=self.minsize, maxsize=self.maxsize,
            loop=self._loop, timeout=self.timeout, enable_json=False,
            **self.connect_kwargs)

    @asyncio.coroutine
    def cursor(self, *args, **kwargs):
        conn = yield from self._pool.acquire()
        cursor = yield from conn.cursor(*args, **kwargs)
        cursor.release = lambda: all((cursor.close(), self._pool.release(conn)))
        return cursor

    def close(self):
        """
        Hmmm... :) How to close all pool connections?..
        """
        for c in self._pool._used:
            c.close()


@contextlib.contextmanager
def sync_unwanted(database, flag=True):
    """
    Context manager for preventing unwanted sync queries.
    `UnwantedSyncQuery` exception will raise on such query.
    """
    database._sync_unwanted = flag
    yield
    database._sync_unwanted = False


class UnwantedSyncQuery(Exception):
    """
    Exception raised when performing unwanted sync query.
    """
    pass
