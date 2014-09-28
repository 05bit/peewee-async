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

    cursor = yield from AsyncQueryExecutor(query).execute()

    query._execute = lambda: None
    result_wrapper = query.execute()

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
    cursor = yield from AsyncQueryExecutor(query).execute()
    result = yield from query.database.last_insert_id_async(cursor, query.model_class)
    return result


@asyncio.coroutine
def update(query):
    """
    Perform UPDATE query asynchronously. Returns number of rows updated.
    """
    assert isinstance(query, peewee.UpdateQuery), ("Error, trying to run update coroutine"
                                                   "with wrong query class %s" % str(query))
    cursor = yield from AsyncQueryExecutor(query).execute()
    return cursor.rowcount


@asyncio.coroutine
def delete(query):
    """
    Perform DELETE query asynchronously. Returns number of rows deleted.
    """
    assert isinstance(query, peewee.DeleteQuery), ("Error, trying to run delete coroutine"
                                                   "with wrong query class %s" % str(query))
    cursor = yield from AsyncQueryExecutor(query).execute()
    return cursor.rowcount


class AsyncQueryExecutor:
    """
    Query wrapper for asynchronous request execution.
    """
    def __init__(self, query):
        self.query = query

    def get_conn(self):
        conn = self.query.database.async
        assert conn, "Error, database is not connected"
        return conn

    def sql(self):
        return self.query.sql()

    @asyncio.coroutine
    def execute(self):
        cursor = yield from self.get_conn().cursor()
        yield from cursor.execute(*self.sql())
        return cursor


class PostgresDatabase(peewee.PostgresqlDatabase):
    """
    Drop-in replacement for default `peewee.PostgresqlDatabase` backend with
    extra `asyncio` powered interface.
    """
    def __init__(self, database, threadlocals=True, autocommit=True,
                 fields=None, ops=None, autorollback=False, **connect_kwargs):
        # Base sync connection
        super().__init__(database, threadlocals=True, autocommit=autocommit,
                         fields=fields, ops=ops, autorollback=autorollback, **connect_kwargs)

        # Async connection
        self.loop = None
        self.async = None

    def connect(self, loop=None):
        """
        Connect is sync or async mode. For async connection you should
        provide `loop` parameter.
        """
        if loop:
            if self.async and not self.async.closed:
                raise Exception("Error, async connection is already opened")

            @asyncio.coroutine
            def do_connect():
                """
                Ref: https://github.com/aio-libs/aiopg/blob/master/aiopg/connection.py#L17
                """
                self.loop = loop if loop else asyncio.get_event_loop()

                timeout = aiopg.DEFAULT_TIMEOUT
                wait = asyncio.Future(loop=loop)
                conn = AsyncPostgresDatabase(self.database, loop, timeout,
                                             wait, **self.connect_kwargs)

                yield from conn._poll(wait, timeout)

                self.async = conn

            return do_connect()
        else:
            if not self.is_closed():
                raise Exception("Error, sync connection is already opened")

            super().connect()

    def close(self):
        """
        Close both sync and async connections.
        """
        super().close()

        if self.async:
            self.async.close()
            self.async = None
            self.loop = None

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
            result = yield from cursor.fetchone()#[0]
            return result


class AsyncPostgresDatabase(aiopg.Connection):
    """
    Asynchronous database interface, subclass of `aiopg.Connection`
    providing thin interface adapter for `peewee`.
    """
    def __init__(self, database, loop, timeout, waiter, **kwargs):
        self.loop = loop if loop else asyncio.get_event_loop()

        dsn = 'dbname=%s' % database
        for k in ('user', 'password', 'host', 'port'):
            if kwargs.get(k, None):
                dsn += ' %s=%s' % (k, kwargs.pop(k))

        super().__init__(dsn, loop, timeout, waiter, **kwargs)


#
# Self-tests
#

import unittest

database = PostgresDatabase('test')


class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database


class AsyncPostgresTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls, *args, **kwargs):
        # Sync connect 
        database.connect()

        # Clean up after possible errors
        TestModel.drop_table(True)

        # Create table with sync connection
        TestModel.create_table()

        # Create at least one object
        obj = TestModel.create(text='[sync] Hello!')

    @classmethod
    def tearDownClass(cls, *args, **kwargs):
        # Finally, clean up
        TestModel.drop_table()        

    def setUp(self):
        # New loop for each test
        self.loop = asyncio.new_event_loop()

    def run_until_complete(self, coroutine):
        @asyncio.coroutine
        def do():
            yield from database.connect(loop=self.loop)
            result = yield from coroutine
            database.close()
            return result
        return self.loop.run_until_complete(do())

    #
    # Test methods
    #

    def test_create_obj(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_create_obj]')
        self.assertTrue(not obj1.id is None)

        # Async create
        @asyncio.coroutine
        def do():
            obj2 = TestModel.create(text='[async] [test_create_obj]')
            self.assertTrue(not obj2.id is None)
        self.loop.run_until_complete(do())


    def test_fetch_obj(self):
        # Sync fetch
        q1 = TestModel.select()
        len1 = len([o for o in q1])
        self.assertTrue(len1 > 0)

        # Async fetch
        @asyncio.coroutine
        def do():
            result = yield from select(TestModel.select())
            return result
        q2 = self.run_until_complete(do())
        len2 = len([o for o in q2])
        self.assertTrue(len2 > 0)

        # Results should be the same
        self.assertEqual(len1, len2)
        for o1, o2 in zip(q1, q2):
            self.assertEqual(o1, o2)

    def test_update_obj(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_update_obj]')
        self.assertEqual(obj1.text, '[sync] [test_update_obj]')

        # Sync update
        upd1 = (TestModel.update(text='[sync] [test_update_obj] [update]')
                         .where(TestModel.id == obj1.id).execute())
        self.assertEqual(upd1, 1)

        # Async update
        @asyncio.coroutine
        def do():
            query = (TestModel.update(text='[async] [test_update_obj] [update]')
                              .where(TestModel.id == obj1.id))
            result = yield from update(query)
            return result
        upd2 = self.run_until_complete(do())
        self.assertEqual(upd2, 1)
        self.assertEqual(TestModel.get(id=obj1.id).text,
                         '[async] [test_update_obj] [update]')

    def test_delete_obj(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_delete_obj]')

        # Async delete
        @asyncio.coroutine
        def do():
            result = yield from delete_instance(obj1)
            return result
        del1 = self.run_until_complete(do())
        self.assertEqual(del1, 1)
        try:
            TestModel.get(id=obj1.id)
            self.assertTrue(False, "Error, object wasn't deleted")
        except TestModel.DoesNotExist:
            pass

    def test_save_obj(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_save_obj]')

        # Async save
        @asyncio.coroutine
        def do():
            obj1.text = '[async] [test_save_obj]'
            result = yield from save(obj1)
            return result
        sav1 = self.run_until_complete(do())
        self.assertEqual(sav1, 1)
        self.assertEqual(TestModel.get(id=obj1.id).text,
                         '[async] [test_save_obj]')
        
    def test_create_obj(self):
        # Async create
        @asyncio.coroutine
        def do():
            result = yield from create(TestModel, text='[async] [test_create_obj]')
            return result
        obj1 = self.run_until_complete(do())
        self.assertTrue(not obj1.id is None)
