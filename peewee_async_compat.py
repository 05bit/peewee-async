"""
Compatibility layer for `peewee_async` to navigate smooth migration towards v1.0.

In the initial implementation the `Manager` class was introduced to avoid models
subclassing (which is not always possible or just undesirable). The newer interface
relies more to models subclassing, please check the `peewee_async.AioModel`.

Licensed under The MIT License (MIT)

Copyright (c) 2024, Alexey Kinëv <rudy@05bit.com>

"""
import uuid
import warnings
from functools import partial

import peewee

from peewee_async.aio_model import AioModelSelect, AioModelInsert, AioModelUpdate, AioModelDelete

IntegrityErrors = (peewee.IntegrityError,)

try:
    import aiopg
    import psycopg2
    IntegrityErrors += (psycopg2.IntegrityError,)
except ImportError:
    aiopg = None
    psycopg2 = None

__all__ = [
    'Manager',
    'count',
    'prefetch',
    'execute',
    'scalar',
    'atomic',
    'transaction',
    'savepoint',
]


def _patch_query_with_compat_methods(query, async_query_cls):
    """
    Patches original peewee's query with methods from AioQueryMixin, etc.

    This is the central (hacky) place where we glue the new classes with older style
    `Manager` interface that operates on original peewee's models and query classes.

    Methods added to peewee's original query:

    - aio_execute
    - fetch_results
    - make_async_query_wrapper
    - aio_get (for SELECT)
    - aio_scalar (for SELECT)
    """

    if getattr(query, 'aio_execute', None):
        # No need to patch
        return

    if async_query_cls is None:
        if isinstance(query, peewee.RawQuery):
            async_query_cls = AioModelSelect
        if isinstance(query, peewee.SelectBase):
            async_query_cls = AioModelSelect
        elif isinstance(query, peewee.Update):
            async_query_cls = AioModelUpdate
        elif isinstance(query, peewee.Delete):
            async_query_cls = AioModelDelete
        elif isinstance(query, peewee.Insert):
            async_query_cls = AioModelInsert

    query.aio_execute = partial(async_query_cls.aio_execute, query)
    query.fetch_results = partial(async_query_cls.fetch_results, query)
    query.make_async_query_wrapper = partial(async_query_cls.make_async_query_wrapper, query)

    if async_query_cls is AioModelSelect:
        query.aio_get = partial(async_query_cls.aio_get, query)
        query.aio_scalar = partial(async_query_cls.aio_scalar, query)
        query.aio_count = partial(async_query_cls.aio_count, query)


def _query_db(query):
    """
    Get database instance bound to query. This helper
    incapsulates internal peewee's access to database.
    """
    return query._database


async def count(query, clear_limit=False):
    """
    Perform *COUNT* aggregated query asynchronously.

    :return: number of objects in `select()` query
    """
    warnings.warn(
        "`count` is deprecated, use `query.aio_count` method.",
        DeprecationWarning
    )
    _patch_query_with_compat_methods(query, AioModelSelect)
    return await query.aio_count(clear_limit=clear_limit)


async def prefetch(sq, *subqueries, prefetch_type):
    """Asynchronous version of the `prefetch()` from peewee."""
    warnings.warn(
        "`prefetch` method is deprecated, use `AioModel.aio_prefetch` or aio_prefetch instead.",
        DeprecationWarning
    )
    database = _query_db(sq)
    if not subqueries:
        result = await database.aio_execute(sq)
        return result

    fixed_queries = peewee.prefetch_add_subquery(sq, subqueries, prefetch_type)
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
        database = _query_db(pq.query)
        result = await database.aio_execute(pq.query)

        for instance in result:
            if pq.fields:
                pq.store_instance(instance, id_map)
            if has_relations:
                for rel in rel_map[query_model]:
                    rel.populate_instance(instance, deps[rel.model])

    return result


async def execute(query):
    warnings.warn(
        "`execute` is deprecated, use `database.aio_execute` method.",
        DeprecationWarning
    )
    database = _query_db(query)
    return await database.aio_execute(query)


async def scalar(query, as_tuple=False):
    warnings.warn(
        "`scalar` is deprecated, use `query.aio_scalar` method.",
        DeprecationWarning
    )
    _patch_query_with_compat_methods(query, AioModelSelect)
    return await query.aio_scalar(as_tuple=as_tuple)


class Manager:
    """
    Async peewee model's manager.

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

    def __init__(self, database=None):
        assert database or self.database, \
               ("Error, database must be provided via "
                "argument or class member.")

        self.database = database or self.database

    @property
    def is_connected(self):
        """Check if database is connected.
        """
        return self.database.is_connected

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
        warnings.warn(
            "`get` method is deprecated, use `AioModel.aio_get` instead.",
            DeprecationWarning
        )
        await self.connect()

        if isinstance(source_, peewee.Query):
            query = source_
            model = query.model
        else:
            query = source_.select()
            model = source_

        conditions = list(args) + [(getattr(model, k) == v) for k, v in kwargs.items()]

        if conditions:
            query = query.where(*conditions)

        try:
            result = await self.execute(query)
            return list(result)[0]
        except IndexError:
            raise model.DoesNotExist

    async def create(self, model_, **data):
        """Create a new object saved to database."""
        warnings.warn(
            "`create` method is deprecated, use `AioModel.aio_create` instead.",
            DeprecationWarning
        )
        obj = model_(**data)
        query = model_.insert(**dict(obj.__data__))

        pk = await self.execute(query)
        if obj._pk is None:
            obj._pk = pk

        return obj

    async def get_or_create(self, model_, defaults=None, **kwargs):
        """
        Try to get an object or create it with the specified defaults.

        Return 2-tuple containing the model instance and a boolean
        indicating whether the instance was created.
        """
        warnings.warn(
            "`get_or_create` method is deprecated, use `AioModel.aio_get_or_create` instead.",
            DeprecationWarning
        )
        try:
            return (await self.get(model_, **kwargs)), False
        except model_.DoesNotExist:
            data = defaults or {}
            data.update({k: v for k, v in kwargs.items() if '__' not in k})
            return (await self.create(model_, **data)), True

    async def get_or_none(self, model_, *args, **kwargs):
        """Try to get an object and return None if it doesn't exist."""
        warnings.warn(
            "`get_or_none` method is deprecated, use `AioModel.aio_get_or_none` instead.",
            DeprecationWarning
        )
        try:
            return (await self.get(model_, *args, **kwargs))
        except model_.DoesNotExist:
            pass

    async def update(self, obj, only=None):
        """
        Update the object in the database. Optionally, update only
        the specified fields. For creating a new object use :meth:`.create()`

        :param only: (optional) the list/tuple of fields or
                     field names to update
        """
        warnings.warn(
            "`update` method is deprecated, use `AioModel.aio_save` method instead.",
            DeprecationWarning
        )
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
        """Delete object from database."""
        warnings.warn(
            "`delete` method is deprecated, use `AioModel.aio_delete_instance` instead.",
            DeprecationWarning
        )
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
        """
        Try to create new object with specified data. If object already
        exists, then try to get it by unique fields.
        """
        warnings.warn(
            "`create_or_get` method is deprecated, use `AioModel.aio_get_or_create` instead.",
            DeprecationWarning
        )
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
        """Execute query asyncronously."""
        warnings.warn(
            "`execute` method is deprecated, use `AioModel.aio_execute` or database.aio_execute instead.",
            DeprecationWarning
        )
        return await self.database.aio_execute(query)

    async def prefetch(self, query, *subqueries, prefetch_type=peewee.PREFETCH_TYPE.JOIN):
        """
        Asynchronous version of the `prefetch()` from peewee.

        :return: Query that has already cached data for subqueries
        """
        return (await prefetch(query, *subqueries, prefetch_type=prefetch_type))

    async def count(self, query, clear_limit=False):
        """
        Perform *COUNT* aggregated query asynchronously.

        :return: number of objects in ``select()`` query
        """
        return (await count(query, clear_limit=clear_limit))

    async def scalar(self, query, as_tuple=False):
        """
        Get single value from ``select()`` query, i.e. for aggregation.

        :return: result is the same as after sync ``query.scalar()`` call
        """
        return (await scalar(query, as_tuple=as_tuple))

    async def connect(self):
        """Open database async connection if not connected."""
        await self.database.connect_async()

    async def close(self):
        """Close database async connection if connected."""
        await self.database.close_async()

    def atomic(self):
        """
        Similar to `peewee.Database.atomic()` method, but returns
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
        warnings.warn(
            "`atomic` is deprecated, use `database.aio_atomic` method.",
            DeprecationWarning
        )
        return self.database.aio_atomic()

    def transaction(self):
        """
        Similar to `peewee.Database.transaction()` method, but returns
        **asynchronous** context manager.
        """
        warnings.warn(
            "`transaction` is deprecated, use `database.aio_atomic` method.",
            DeprecationWarning
        )
        return self.database.aio_atomic()

    def savepoint(self, sid=None):
        """
        Similar to `peewee.Database.savepoint()` method, but returns
        **asynchronous** context manager.
        """
        return savepoint(self.database, sid=sid)

    def allow_sync(self):
        """
        Allow sync queries within context. Close the sync
        database connection on exit if connected.

        Example::

            with objects.allow_sync():
                PageBlock.create_table(True)
        """
        return self.database.allow_sync()

    @staticmethod
    def _prune_fields(field_dict, only):
        """
        Filter fields data **in place** with `only` list.

        Example::

            self._prune_fields(field_dict, ['slug', 'text'])
            self._prune_fields(field_dict, [MyModel.slug])
        """
        fields = [(isinstance(f, str) and f or f.name) for f in only]
        for f in list(field_dict.keys()):
            if f not in fields:
                field_dict.pop(f)
        return field_dict


def transaction(db):
    """Asynchronous context manager (`async with`), similar to
    `peewee.transaction()`. Will start new `asyncio` task for
    transaction if not started already.
    """
    warnings.warn(
        "`transaction` is deprecated, use `database.aio_atomic` or `Transaction` class instead.",
        DeprecationWarning
    )
    return db.aio_atomic()


def atomic(db):
    """Asynchronous context manager (`async with`), similar to
    `peewee.atomic()`.
    """
    warnings.warn(
        "`atomic` is deprecated, use `database.aio_atomic` or `Transaction` class instead.",
        DeprecationWarning
    )
    return db.aio_atomic()


class _savepoint:
    """Asynchronous context manager (`async with`), similar to
    `peewee.savepoint()`.
    """
    def __init__(self, db, sid=None):

        self.db = db
        self.sid = sid or 's' + uuid.uuid4().hex
        self.quoted_sid = self.sid.join(self.db.quote)

    async def commit(self):
        await self.db.aio_execute_sql('RELEASE SAVEPOINT %s;' % self.quoted_sid)

    async def rollback(self):
        await self.db.aio_execute_sql('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    async def __aenter__(self):
        await self.db.aio_execute_sql('SAVEPOINT %s;' % self.quoted_sid)
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


def savepoint(db, sid=None):
    warnings.warn(
        "`savepoint` is deprecated, use `database.aio_atomic` or `Transaction` class instead.",
        DeprecationWarning
    )
    return _savepoint(db.db, sid)
