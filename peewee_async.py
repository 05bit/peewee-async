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
import asyncio
import contextlib
import functools
import logging
import uuid
import warnings

import peewee
from playhouse.db_url import register_database

IntegrityErrors = (peewee.IntegrityError,)

try:
    import aiopg
    import psycopg2
    IntegrityErrors += (psycopg2.errors.UniqueViolation,)
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

__version__ = '0.7.0'

__all__ = [
    # High level API ###

    'Manager',
    'PostgresqlDatabase',
    'PooledPostgresqlDatabase',
    'MySQLDatabase',
    'PooledMySQLDatabase',

    # Low level API ###

    'execute',
    'count',
    'scalar',
    'atomic',
    'transaction',
    'savepoint',

    # Deprecated ###

    'get_object',
    'create_object',
    'delete_object',
    'update_object',
    'sync_unwanted',
    'UnwantedSyncQueryError',
]

__log__ = logging.getLogger('peewee.async')
__log__.addHandler(logging.NullHandler())


#################
# Async manager #
#################


class Manager:
    """Async peewee models manager.

    :param loop: (optional) asyncio event loop
    :param database: (optional) async database driver

    Example::

        class User(peewee.Model):
            username = peewee.CharField(max_length=40, unique=True)

        objects = Manager(PostgresqlDatabase('test'))

        async def my_async_func():
            user0 = await objects.create(User, username='test')
            user1 = await objects.get(User, id=user0.id)
            user2 = await objects.get(User, username='test')
            # All should be the same
            print(user1.id, user2.id, user3.id)

    If you don't pass database to constructor, you should define
    ``database`` as a class member like that::

        database = PostgresqlDatabase('test')

        class MyManager(Manager):
            database = database

        objects = MyManager()

    """
    #: Async database driver for manager. Must be provided
    #: in constructor or as a class member.
    database = None

    def __init__(self, database=None, *, loop=None):
        assert database or self.database, \
               ("Error, database must be provided via "
                "argument or class member.")

        self.database = database or self.database
        self._loop = loop
        self._timeout = getattr(database, 'timeout', None)

        attach_callback = getattr(self.database, 'attach_callback', None)
        if attach_callback:
            attach_callback(lambda db: setattr(db, '_loop', loop))
        else:
            self.database._loop = loop

    @property
    def loop(self):
        """Get the event loop.

        If no event loop is provided explicitly on creating
        the instance, just return the current event loop.
        """
        return self._loop or asyncio.get_event_loop()

    @property
    def is_connected(self):
        """Check if database is connected.
        """
        return self.database._async_conn is not None

    async def get(self, source_, *args, **kwargs):
        """Get the model instance.

        :param source_: model or base query for lookup

        Example::

            async def my_async_func():
                obj1 = await objects.get(MyModel, id=1)
                obj2 = await objects.get(MyModel, MyModel.id==1)
                obj3 = await objects.get(MyModel.select().where(MyModel.id==1))

        All will return `MyModel` instance with `id = 1`
        """
        await self.connect()

        if isinstance(source_, peewee.Query):
            query = source_
            model = query.model
        else:
            query = source_.select()
            model = source_

        conditions = list(args) + [(getattr(model, k) == v)
                                   for k, v in kwargs.items()]

        if conditions:
            query = query.where(*conditions)

        try:
            result = await self.execute(query)
            return list(result)[0]
        except IndexError:
            raise model.DoesNotExist

    async def create(self, model_, **data):
        """Create a new object saved to database.
        """
        inst = model_(**data)
        query = model_.insert(**dict(inst.__data__))

        pk = await self.execute(query)
        if inst._pk is None:
            inst._pk = pk
        return inst

    async def get_or_create(self, model_, defaults=None, **kwargs):
        """Try to get an object or create it with the specified defaults.

        Return 2-tuple containing the model instance and a boolean
        indicating whether the instance was created.
        """
        try:
            return (await self.get(model_, **kwargs)), False
        except model_.DoesNotExist:
            data = defaults or {}
            data.update({k: v for k, v in kwargs.items() if '__' not in k})
            return (await self.create(model_, **data)), True

    async def update(self, obj, only=None):
        """Update the object in the database. Optionally, update only
        the specified fields. For creating a new object use :meth:`.create()`

        :param only: (optional) the list/tuple of fields or
                     field names to update
        """
        field_dict = dict(obj.__data__)
        pk_field = obj._meta.primary_key

        if only:
            self._prune_fields(field_dict, only)

        if obj._meta.only_save_dirty:
            self._prune_fields(field_dict, obj.dirty_fields)

        if obj._meta.composite_key:
            for pk_part_name in pk_field.field_names:
                field_dict.pop(pk_part_name, None)
        else:
            field_dict.pop(pk_field.name, None)

        query = obj.update(**field_dict).where(obj._pk_expr())
        result = await self.execute(query)
        obj._dirty.clear()
        return result

    async def delete(self, obj, recursive=False, delete_nullable=False):
        """Delete object from database.
        """
        if recursive:
            dependencies = obj.dependencies(delete_nullable)
            for cond, fk in reversed(list(dependencies)):
                model = fk.model
                if fk.null and not delete_nullable:
                    sq = model.update(**{fk.name: None}).where(cond)
                else:
                    sq = model.delete().where(cond)
                await self.execute(sq)

        query = obj.delete().where(obj._pk_expr())
        return (await self.execute(query))

    async def create_or_get(self, model_, **kwargs):
        """Try to create new object with specified data. If object already
        exists, then try to get it by unique fields.
        """
        try:
            return (await self.create(model_, **kwargs)), True
        except IntegrityErrors:
            query = []
            for field_name, value in kwargs.items():
                field = getattr(model_, field_name)
                if field.unique or field.primary_key:
                    query.append(field == value)
            return (await self.get(model_, *query)), False

    async def execute(self, query):
        """Execute query asyncronously.
        """
        query = self._swap_database(query)
        return (await execute(query))

    async def prefetch(self, query, *subqueries):
        """Asynchronous version of the `prefetch()` from peewee.

        :return: Query that has already cached data for subqueries
        """
        query = self._swap_database(query)
        subqueries = map(self._swap_database, subqueries)
        return (await prefetch(query, *subqueries))

    async def count(self, query, clear_limit=False):
        """Perform *COUNT* aggregated query asynchronously.

        :return: number of objects in ``select()`` query
        """
        query = self._swap_database(query)
        return (await count(query, clear_limit=clear_limit))

    async def scalar(self, query, as_tuple=False):
        """Get single value from ``select()`` query, i.e. for aggregation.

        :return: result is the same as after sync ``query.scalar()`` call
        """
        query = self._swap_database(query)
        return (await scalar(query, as_tuple=as_tuple))

    async def connect(self):
        """Open database async connection if not connected.
        """
        await self.database.connect_async(loop=self.loop, timeout=self._timeout)

    async def close(self):
        """Close database async connection if connected.
        """
        await self.database.close_async()

    def atomic(self):
        """Similar to `peewee.Database.atomic()` method, but returns
        **asynchronous** context manager.

        Example::

            async with objects.atomic():
                await objects.create(
                    PageBlock, key='intro',
                    text="There are more things in heaven and earth, "
                         "Horatio, than are dreamt of in your philosophy.")
                await objects.create(
                    PageBlock, key='signature', text="William Shakespeare")
        """
        return atomic(self.database)

    def transaction(self):
        """Similar to `peewee.Database.transaction()` method, but returns
        **asynchronous** context manager.
        """
        return transaction(self.database)

    def savepoint(self, sid=None):
        """Similar to `peewee.Database.savepoint()` method, but returns
        **asynchronous** context manager.
        """
        return savepoint(self.database, sid=sid)

    def allow_sync(self):
        """Allow sync queries within context. Close the sync
        database connection on exit if connected.

        Example::

            with objects.allow_sync():
                PageBlock.create_table(True)
        """
        return self.database.allow_sync()

    def _swap_database(self, query):
        """Swap database for query if swappable. Return **new query**
        with swapped database.

        This is experimental feature which allows us to have multiple
        managers configured against different databases for single model
        definition.

        The essential limitation though is that database backend have
        to be **the same type** for model and manager!
        """
        database = _query_db(query)

        if database == self.database:
            return query

        if self._subclassed(peewee.PostgresqlDatabase, database,
                            self.database):
            can_swap = True
        elif self._subclassed(peewee.MySQLDatabase, database,
                              self.database):
            can_swap = True
        else:
            can_swap = False

        if can_swap:
            # **Experimental** database swapping!
            query = query.clone()
            query._database = self.database
            return query

        assert False, (
            "Error, query's database and manager's database are "
            "different. Query: %s Manager: %s" % (database, self.database)
        )

        return None

    @staticmethod
    def _subclassed(base, *classes):
        """Check if all classes are subclassed from base.
        """
        return all(map(lambda obj: isinstance(obj, base), classes))

    @staticmethod
    def _prune_fields(field_dict, only):
        """Filter fields data **in place** with `only` list.

        Example::

            self._prune_fields(field_dict, ['slug', 'text'])
            self._prune_fields(field_dict, [MyModel.slug])
        """
        fields = [(isinstance(f, str) and f or f.name) for f in only]
        for f in list(field_dict.keys()):
            if f not in fields:
                field_dict.pop(f)
        return field_dict


#################
# Async queries #
#################


async def execute(query):
    """Execute *SELECT*, *INSERT*, *UPDATE* or *DELETE* query asyncronously.

    :param query: peewee query instance created with ``Model.select()``,
                  ``Model.update()`` etc.
    :return: result depends on query type, it's the same as for sync
        ``query.execute()``
    """
    if isinstance(query, (peewee.Select, peewee.ModelCompoundSelectQuery)):
        coroutine = select
    elif isinstance(query, peewee.Update):
        coroutine = update
    elif isinstance(query, peewee.Insert):
        coroutine = insert
    elif isinstance(query, peewee.Delete):
        coroutine = delete
    else:
        coroutine = raw_query

    return (await coroutine(query))


async def create_object(model, **data):
    """Create object asynchronously.

    :param model: mode class
    :param data: data for initializing object
    :return: new object saved to database
    """
    # NOTE! Here are internals involved:
    #
    # - obj._data
    # - obj._get_pk_value()
    # - obj._set_pk_value()
    # - obj._prepare_instance()
    #
    warnings.warn("create_object() is deprecated, Manager.create() "
                  "should be used instead",
                  DeprecationWarning)

    obj = model(**data)

    pk = await insert(model.insert(**dict(obj.__data__)))

    if obj._pk is None:
        obj._pk = pk
    return obj


async def get_object(source, *args):
    """Get object asynchronously.

    :param source: mode class or query to get object from
    :param args: lookup parameters
    :return: model instance or raises ``peewee.DoesNotExist`` if object not
        found
    """
    warnings.warn("get_object() is deprecated, Manager.get() "
                  "should be used instead",
                  DeprecationWarning)

    if isinstance(source, peewee.Query):
        query = source
        model = query.model
    else:
        query = source.select()
        model = source

    # Return first object from query
    for obj in (await select(query.where(*args))):
        return obj

    # No objects found
    raise model.DoesNotExist


async def delete_object(obj, recursive=False, delete_nullable=False):
    """Delete object asynchronously.

    :param obj: object to delete
    :param recursive: if ``True`` also delete all other objects depends on
        object
    :param delete_nullable: if `True` and delete is recursive then delete even
        'nullable' dependencies

    For details please check out `Model.delete_instance()`_ in peewee docs.

    .. _Model.delete_instance(): http://peewee.readthedocs.io/en/latest/peewee/
        api.html#Model.delete_instance
    """
    warnings.warn("delete_object() is deprecated, Manager.delete() "
                  "should be used instead",
                  DeprecationWarning)

    # Here are private calls involved:
    # - obj._pk_expr()
    if recursive:
        dependencies = obj.dependencies(delete_nullable)
        for query, fk in reversed(list(dependencies)):
            model = fk.model
            if fk.null and not delete_nullable:
                await update(model.update(**{fk.name: None}).where(query))
            else:
                await delete(model.delete().where(query))
    result = await delete(obj.delete().where(obj._pk_expr()))
    return result


async def update_object(obj, only=None):
    """Update object asynchronously.

    :param obj: object to update
    :param only: list or tuple of fields to updata, is `None` then all fields
        updated

    This function does the same as `Model.save()`_ for already saved object,
        but it doesn't invoke ``save()`` method on model class. That is
        important to know if you overrided save method for your model.

    .. _Model.save(): http://peewee.readthedocs.io/en/latest/peewee/
        api.html#Model.save
    """
    # Here are private calls involved:
    #
    # - obj._data
    # - obj._meta
    # - obj._prune_fields()
    # - obj._pk_expr()
    # - obj._dirty.clear()
    #
    warnings.warn("update_object() is deprecated, Manager.update() "
                  "should be used instead",
                  DeprecationWarning)

    field_dict = dict(obj.__data__)
    pk_field = obj._meta.primary_key

    if only:
        field_dict = obj._prune_fields(field_dict, only)

    if not isinstance(pk_field, peewee.CompositeKey):
        field_dict.pop(pk_field.name, None)
    else:
        field_dict = obj._prune_fields(field_dict, obj.dirty_fields)
    rows = await update(obj.update(**field_dict).where(obj._pk_expr()))

    obj._dirty.clear()
    return rows


async def select(query):
    """Perform SELECT query asynchronously.
    """
    assert isinstance(query, peewee.SelectQuery),\
        ("Error, trying to run select coroutine"
         "with wrong query class %s" % str(query))

    cursor = await _execute_query_async(query)

    result = AsyncQueryWrapper(cursor=cursor, query=query)

    try:
        while True:
            await result.fetchone()
    except GeneratorExit:
        pass
    finally:
        await cursor.release()

    return result


async def insert(query):
    """Perform INSERT query asynchronously. Returns last insert ID.
    This function is called by object.create for single objects only.
    """
    assert isinstance(query, peewee.Insert),\
        ("Error, trying to run insert coroutine"
         "with wrong query class %s" % str(query))

    cursor = await _execute_query_async(query)

    try:
        if query._returning:
            row = await cursor.fetchone()
            result = row[0]
        else:
            database = _query_db(query)
            last_id = await database.last_insert_id_async(cursor)
            result = last_id
    finally:
        await cursor.release()

    return result


async def update(query):
    """Perform UPDATE query asynchronously. Returns number of rows updated.
    """
    assert isinstance(query, peewee.Update),\
        ("Error, trying to run update coroutine"
         "with wrong query class %s" % str(query))

    cursor = await _execute_query_async(query)
    rowcount = cursor.rowcount

    await cursor.release()
    return rowcount


async def delete(query):
    """Perform DELETE query asynchronously. Returns number of rows deleted.
    """
    assert isinstance(query, peewee.Delete),\
        ("Error, trying to run delete coroutine"
         "with wrong query class %s" % str(query))

    cursor = await _execute_query_async(query)
    rowcount = cursor.rowcount

    await cursor.release()
    return rowcount


async def count(query, clear_limit=False):
    """Perform *COUNT* aggregated query asynchronously.

    :return: number of objects in ``select()`` query
    """
    clone = query.clone()
    if query._distinct or query._group_by or query._limit or query._offset:
        if clear_limit:
            clone._limit = clone._offset = None
        sql, params = clone.sql()
        wrapped = 'SELECT COUNT(1) FROM (%s) AS wrapped_select' % sql
        raw = query.model.raw(wrapped, *params)
        return (await scalar(raw)) or 0
    else:
        clone._returning = [peewee.fn.Count(peewee.SQL('*'))]
        clone._order_by = None
        return (await scalar(clone)) or 0


async def scalar(query, as_tuple=False):
    """Get single value from ``select()`` query, i.e. for aggregation.

    :return: result is the same as after sync ``query.scalar()`` call
    """
    cursor = await _execute_query_async(query)

    try:
        row = await cursor.fetchone()
    finally:
        await cursor.release()

    if row and not as_tuple:
        return row[0]
    else:
        return row


async def raw_query(query):
    assert isinstance(query, peewee.RawQuery),\
        ("Error, trying to run raw_query coroutine"
         "with wrong query class %s" % str(query))

    cursor = await _execute_query_async(query)

    result = AsyncQueryWrapper(cursor=cursor, query=query)
    try:
        while True:
            await result.fetchone()
    except GeneratorExit:
        pass
    finally:
        await cursor.release()

    return result


async def prefetch(sq, *subqueries):
    """Asynchronous version of the `prefetch()` from peewee.
    """
    if not subqueries:
        result = await execute(sq)
        return result

    fixed_queries = peewee.prefetch_add_subquery(sq, subqueries)
    deps = {}
    rel_map = {}

    for pq in reversed(fixed_queries):
        query_model = pq.model
        if pq.fields:
            for rel_model in pq.rel_models:
                rel_map.setdefault(rel_model, [])
                rel_map[rel_model].append(pq)

        deps[query_model] = {}
        id_map = deps[query_model]
        has_relations = bool(rel_map.get(query_model))

        result = await execute(pq.query)

        for instance in result:
            if pq.fields:
                pq.store_instance(instance, id_map)
            if has_relations:
                for rel in rel_map[query_model]:
                    rel.populate_instance(instance, deps[rel.model])

    return result


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


############
# Database #
############

class AsyncDatabase:
    _loop = None  # asyncio event loop
    _timeout = None  # connection timeout
    _allow_sync = True  # whether sync queries are allowed
    _async_conn = None  # async connection
    _async_wait = None  # connection waiter
    _task_data = None  # asyncio per-task data

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

    async def connect_async(self, loop=None, timeout=None):
        """Set up async connection on specified event loop or
        on default event loop.
        """
        if self.deferred:
            raise Exception("Error, database not properly initialized "
                            "before opening connection")

        if self._async_conn:
            return
        elif self._async_wait:
            await self._async_wait
        else:
            self._loop = loop
            self._async_wait = asyncio.Future(loop=self._loop)

            if not timeout and self._timeout:
                timeout = self._timeout

            conn = self._async_conn_cls(
                database=self.database,
                loop=self._loop,
                timeout=timeout,
                **self.connect_params_async)

            try:
                await conn.connect()
            except Exception as e:
                if not self._async_wait.done():
                    self._async_wait.set_exception(e)
                self._async_wait = None
                raise
            else:
                self._task_data = TaskLocals(loop=self._loop)
                self._async_conn = conn
                self._async_wait.set_result(True)

    async def cursor_async(self):
        """Acquire async cursor.
        """
        await self.connect_async(loop=self._loop)

        if self.transaction_depth_async() > 0:
            conn = self.transaction_conn_async()
        else:
            conn = None

        try:
            return (await self._async_conn.cursor(conn=conn))
        except:
            await self.close_async()
            raise

    async def close_async(self):
        """Close async connection.
        """
        if self._async_wait:
            await self._async_wait
        if self._async_conn:
            conn = self._async_conn
            self._async_conn = None
            self._async_wait = None
            self._task_data = None
            await conn.close()

    async def push_transaction_async(self):
        """Increment async transaction depth.
        """
        await self.connect_async(loop=self.loop)
        depth = self.transaction_depth_async()
        if not depth:
            conn = await self._async_conn.acquire()
            self._task_data.set('conn', conn)
        self._task_data.set('depth', depth + 1)

    async def pop_transaction_async(self):
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
        return transaction(self)

    def atomic_async(self):
        """Similar to peewee `Database.atomic()` method, but returns
        asynchronous context manager.
        """
        return atomic(self)

    def savepoint_async(self, sid=None):
        """Similar to peewee `Database.savepoint()` method, but returns
        asynchronous context manager.
        """
        return savepoint(self, sid=sid)

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


##############
# PostgreSQL #
##############


class AsyncPostgresqlConnection:
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database=None, loop=None, timeout=None, **kwargs):
        self.pool = None
        self.loop = loop
        self.database = database
        self.timeout = timeout or aiopg.DEFAULT_TIMEOUT
        self.connect_params = kwargs

    async def acquire(self):
        """Acquire connection from pool.
        """
        return (await self.pool.acquire())

    def release(self, conn):
        """Release connection to pool.
        """
        self.pool.release(conn)

    async def connect(self):
        """Create connection pool asynchronously.
        """
        self.pool = await aiopg.create_pool(
            loop=self.loop,
            timeout=self.timeout,
            database=self.database,
            **self.connect_params)

    async def close(self):
        """Terminate all pool connections.
        """
        self.pool.terminate()
        await self.pool.wait_closed()

    async def cursor(self, conn=None, *args, **kwargs):
        """Get a cursor for the specified transaction connection
        or acquire from the pool.
        """
        in_transaction = conn is not None
        if not conn:
            conn = await self.acquire()
        cursor = await conn.cursor(*args, **kwargs)
        cursor.release = functools.partial(
            self.release_cursor, cursor,
            in_transaction=in_transaction)
        return cursor

    async def release_cursor(self, cursor, in_transaction=False):
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
    if psycopg2:
        Error = psycopg2.Error

    def init_async(self, conn_cls=AsyncPostgresqlConnection,
                   enable_json=False, enable_hstore=False):
        if not aiopg:
            raise Exception("Error, aiopg is not installed!")
        self._async_conn_cls = conn_cls
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


class PooledPostgresqlDatabase(AsyncPostgresqlMixin,
                               peewee.PostgresqlDatabase):
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
        self._timeout = kwargs.pop('connection_timeout', aiopg.DEFAULT_TIMEOUT)
        super().init(database, **kwargs)
        self.init_async()

    @property
    def use_speedups(self):
        return False

    @use_speedups.setter
    def use_speedups(self, value):
        pass


register_database(PooledPostgresqlDatabase, 'postgres+pool+async',
                  'postgresql+pool+async')


#########
# MySQL #
#########


class AsyncMySQLConnection:
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database=None, loop=None, timeout=None, **kwargs):
        self.pool = None
        self.loop = loop
        self.database = database
        self.timeout = timeout
        self.connect_params = kwargs

    async def acquire(self):
        """Acquire connection from pool.
        """
        return (await self.pool.acquire())

    def release(self, conn):
        """Release connection to pool.
        """
        self.pool.release(conn)

    async def connect(self):
        """Create connection pool asynchronously.
        """
        self.pool = await aiomysql.create_pool(
            loop=self.loop,
            db=self.database,
            connect_timeout=self.timeout,
            **self.connect_params)

    async def close(self):
        """Terminate all pool connections.
        """
        self.pool.terminate()
        await self.pool.wait_closed()

    async def cursor(self, conn=None, *args, **kwargs):
        """Get cursor for connection from pool.
        """
        in_transaction = conn is not None
        if not conn:
            conn = await self.acquire()
        cursor = await conn.cursor(*args, **kwargs)
        cursor.release = functools.partial(
            self.release_cursor, cursor,
            in_transaction=in_transaction)
        return cursor

    async def release_cursor(self, cursor, in_transaction=False):
        """Release cursor coroutine. Unless in transaction,
        the connection is also released back to the pool.
        """
        conn = cursor.connection
        await cursor.close()
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
    if pymysql:
        Error = pymysql.Error

    def init(self, database, **kwargs):
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")
        self.min_connections = 1
        self.max_connections = 1
        self._async_conn_cls = kwargs.pop('async_conn', AsyncMySQLConnection)
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


##############
# Sync utils #
##############


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


################
# Transactions #
################


class transaction:
    """Asynchronous context manager (`async with`), similar to
    `peewee.transaction()`. Will start new `asyncio` task for
    transaction if not started already.
    """
    def __init__(self, db):
        self.db = db
        self.loop = db.loop

    async def commit(self, begin=True):
        await _run_no_result_sql(self.db, 'COMMIT')
        if begin:
            await _run_no_result_sql(self.db, 'BEGIN')

    async def rollback(self, begin=True):
        await _run_no_result_sql(self.db, 'ROLLBACK')
        if begin:
            await _run_no_result_sql(self.db, 'BEGIN')

    async def __aenter__(self):
        if not asyncio_current_task(loop=self.loop):
            raise RuntimeError("The transaction must run within a task")
        await self.db.push_transaction_async()
        if self.db.transaction_depth_async() == 1:
            await _run_no_result_sql(self.db, 'BEGIN')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                await self.rollback(False)
            elif self.db.transaction_depth_async() == 1:
                try:
                    await self.commit(False)
                except:
                    await self.rollback(False)
                    raise
        finally:
            # transaction depth may be zero if database gone
            depth = self.db.transaction_depth_async()
            if depth > 0:
                await self.db.pop_transaction_async()


class savepoint:
    """Asynchronous context manager (`async with`), similar to
    `peewee.savepoint()`.
    """
    def __init__(self, db, sid=None):
        self.db = db
        self.sid = sid or 's' + uuid.uuid4().hex
        self.quoted_sid = self.sid.join(self.db.quote)

    async def commit(self):
        await _run_no_result_sql(
            self.db, 'RELEASE SAVEPOINT %s;' % self.quoted_sid)

    async def rollback(self):
        await _run_no_result_sql(
            self.db, 'ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    async def __aenter__(self):
        await _run_no_result_sql(
            self.db, 'SAVEPOINT %s;' % self.quoted_sid)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                await self.rollback()
            else:
                try:
                    await self.commit()
                except:
                    await self.rollback()
                    raise
        finally:
            pass


class atomic:
    """Asynchronous context manager (`async with`), similar to
    `peewee.atomic()`.
    """
    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        if self.db.transaction_depth_async() > 0:
            self._ctx = self.db.savepoint_async()
        else:
            self._ctx = self.db.transaction_async()
        return (await self._ctx.__aenter__())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._ctx.__aexit__(exc_type, exc_val, exc_tb)


####################
# Internal helpers #
####################


def _query_db(query):
    """Get database instance bound to query. This helper
    incapsulates internal peewee's access to database.
    """
    return query._database


async def _run_sql(database, operation, *args, **kwargs):
    """Run SQL operation (query or command) against database.
    """
    __log__.debug((operation, args, kwargs))

    with peewee.__exception_wrapper__:
        cursor = await database.cursor_async()

        try:
            await cursor.execute(operation, *args, **kwargs)
        except:
            await cursor.release()
            raise

        return cursor


async def _run_no_result_sql(database, operation, *args, **kwargs):
    cursor = await _run_sql(database, operation, *args, **kwargs)
    await cursor.release()


async def _execute_query_async(query):
    """Execute query and return cursor object.
    """
    database = _query_db(query)
    return (await _run_sql(database, *query.sql()))


class TaskLocals:
    """Simple `dict` wrapper to get and set values on per `asyncio`
    task basis.

    The idea is similar to thread-local data, but actually *much* simpler.
    It's no more than a "sugar" class. Use `get()` and `set()` method like
    you would to for `dict` but values will be get and set in the context
    of currently running `asyncio` task.

    When task is done, all saved values are removed from stored data.
    """
    def __init__(self, loop):
        self.loop = loop
        self.data = {}

    def get(self, key, *val):
        """Get value stored for current running task. Optionally
        you may provide the default value. Raises `KeyError` when
        can't get the value and no default one is provided.
        """
        data = self.get_data()
        if data is not None:
            return data.get(key, *val)
        if val:
            return val[0]
        raise KeyError(key)

    def set(self, key, val):
        """Set value stored for current running task.
        """
        data = self.get_data(True)
        if data is not None:
            data[key] = val
        else:
            raise RuntimeError("No task is currently running")

    def get_data(self, create=False):
        """Get dict stored for current running task. Return `None`
        or an empty dict if no data was found depending on the
        `create` argument value.

        :param create: if argument is `True`, create empty dict
                       for task, default: `False`
        """
        task = asyncio_current_task(loop=self.loop)
        if task:
            task_id = id(task)
            if create and task_id not in self.data:
                self.data[task_id] = {}
                task.add_done_callback(self.del_data)
            return self.data.get(task_id)
        return None

    def del_data(self, task):
        """Delete data for task from stored data dict.
        """
        del self.data[id(task)]
