import asyncio
import peewee

from .queries import execute, prefetch, count, scalar
from .transactions import transaction, atomic, savepoint


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

    @asyncio.coroutine
    def get(self, source_, *args, **kwargs):
        """Get the model instance.

        :param source_: model or base query for lookup

        Example::

            async def my_async_func():
                obj1 = await objects.get(MyModel, id=1)
                obj2 = await objects.get(MyModel, MyModel.id == 1)
                obj3 = await objects.get(MyModel.select().where(MyModel.id == 1))

        All will return `MyModel` instance with `id = 1`
        """
        yield from self.connect()

        if isinstance(source_, peewee.Query):
            query = source_
            model = query.model_class
        else:
            query = source_.select()
            model = source_

        conditions = list(args) + [(getattr(model, k) == v)
            for k, v in kwargs.items()]

        if conditions:
            query = query.where(*conditions)

        try:
            result = yield from self.execute(query)
            return list(result)[0]
        except IndexError:
            raise model.DoesNotExist

    @asyncio.coroutine
    def create(self, model_, **data):
        """Create a new object saved to database.
        """
        inst = model_(**data)
        query = model_.insert(**dict(inst._data))

        pk = yield from self.execute(query)
        if pk is None:
            pk = inst._get_pk_value()
        inst._set_pk_value(pk)

        inst._prepare_instance()
        return inst

    @asyncio.coroutine
    def get_or_create(self, model_, defaults=None, **kwargs):
        """Try to get an object or create it with the specified defaults.

        Return 2-tuple containing the model instance and a boolean
        indicating whether the instance was created.
        """
        try:
            return (yield from self.get(model_, **kwargs)), False
        except model_.DoesNotExist:
            data = defaults or {}
            data.update({k: v for k, v in kwargs.items()
                if not '__' in k})
            return (yield from self.create(model_, **data)), True

    @asyncio.coroutine
    def update(self, obj, only=None):
        """Update the object in the database. Optionally, update only
        the specified fields. For creating a new object use :meth:`.create()`

        :param only: (optional) the list/tuple of fields or
                     field names to update
        """
        field_dict = dict(obj._data)
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
        result = yield from self.execute(query)
        obj._dirty.clear()
        return result

    @asyncio.coroutine
    def delete(self, obj, recursive=False, delete_nullable=False):
        """Delete object from database.
        """
        if recursive:
            dependencies = obj.dependencies(delete_nullable)
            for cond, fk in reversed(list(dependencies)):
                model = fk.model_class
                if fk.null and not delete_nullable:
                    sq = model.update(**{fk.name: None}).where(cond)
                else:
                    sq = model.delete().where(cond)
                yield from self.execute(sq)

        query = obj.delete().where(obj._pk_expr())
        return (yield from self.execute(query))

    @asyncio.coroutine
    def create_or_get(self, model_, **kwargs):
        """Try to create new object with specified data. If object already
        exists, then try to get it by unique fields.
        """
        try:
            return (yield from self.create(model_, **kwargs)), True
        except peewee.IntegrityError:
            query = []
            for field_name, value in kwargs.items():
                field = getattr(model_, field_name)
                if field.unique or field.primary_key:
                    query.append(field == value)
            return (yield from self.get(model_, *query)), False

    @asyncio.coroutine
    def execute(self, query):
        """Execute query asyncronously.
        """
        query = self._swap_database(query)
        return (yield from execute(query))

    @asyncio.coroutine
    def prefetch(self, query, *subqueries):
        """Asynchronous version of the `prefetch()` from peewee.

        :return: Query that has already cached data for subqueries
        """
        query = self._swap_database(query)
        subqueries = map(self._swap_database, subqueries)
        return (yield from prefetch(query, *subqueries))

    @asyncio.coroutine
    def count(self, query, clear_limit=False):
        """Perform *COUNT* aggregated query asynchronously.

        :return: number of objects in ``select()`` query
        """
        query = self._swap_database(query)
        return (yield from count(query, clear_limit=clear_limit))

    @asyncio.coroutine
    def scalar(self, query, as_tuple=False):
        """Get single value from ``select()`` query, i.e. for aggregation.

        :return: result is the same as after sync ``query.scalar()`` call
        """
        query = self._swap_database(query)
        return (yield from scalar(query, as_tuple=as_tuple))

    @asyncio.coroutine
    def connect(self):
        """Open database async connection if not connected.
        """
        yield from self.database.connect_async(loop=self.loop)

    @asyncio.coroutine
    def close(self):
        """Close database async connection if connected.
        """
        yield from self.database.close_async()

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
        if query.database == self.database:
            return query
        elif self._subclassed(peewee.PostgresqlDatabase,
                              query.database,
                              self.database):
            can_swap = True
        elif self._subclassed(peewee.MySQLDatabase,
                              query.database,
                              self.database):
            can_swap = True
        else:
            can_swap = False

        if can_swap:
            # **Experimental** database swapping!
            query = query.clone()
            query.database = self.database
            return query
        else:
            assert False, (
                "Error, query's database and manager's database are "
                "different. Query: %s Manager: %s" % (
                    query.database, self.database
                )
            )

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
            if not f in fields:
                field_dict.pop(f)
        return field_dict
