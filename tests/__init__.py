"""
peewee-async tests
==================

Create tests.ini file to configure tests.

"""
import asyncio
import contextlib
import json
import logging
import os
import sys
import unittest
import uuid

import peewee

import peewee_async
import peewee_asyncext

##########
# Config #
##########

# logging.basicConfig(level=logging.DEBUG)

DB_DEFAULTS = {
    'postgres': {
        'database': 'test',
        'host': '127.0.0.1',
        # 'port': 5432,
        'user': 'postgres',
    },
    'postgres-ext': {
        'database': 'test',
        'host': '127.0.0.1',
        # 'port': 5432,
        'user': 'postgres',
    },
    'postgres-pool': {
        'database': 'test',
        'host': '127.0.0.1',
        # 'port': 5432,
        'user': 'postgres',
        'max_connections': 4,
    },
    'postgres-pool-ext': {
        'database': 'test',
        'host': '127.0.0.1',
        # 'port': 5432,
        'user': 'postgres',
        'max_connections': 4,
    },
    'mysql': {
        'database': 'test',
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
    },
    'mysql-pool': {
        'database': 'test',
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
    }
}

DB_OVERRIDES = {}

DB_CLASSES = {
    'postgres': peewee_async.PostgresqlDatabase,
    'postgres-ext': peewee_asyncext.PostgresqlExtDatabase,
    'postgres-pool': peewee_async.PooledPostgresqlDatabase,
    'postgres-pool-ext': peewee_asyncext.PooledPostgresqlExtDatabase,
    'mysql': peewee_async.MySQLDatabase,
    'mysql-pool': peewee_async.PooledMySQLDatabase
}

try:
    import aiopg
except ImportError:
    aiopg = None

try:
    import aiomysql
except ImportError:
    aiomysql = None


def setUpModule():
    try:
        with open('tests.json', 'r') as tests_fp:
            DB_OVERRIDES.update(json.load(tests_fp))
    except FileNotFoundError:
        print("'tests.json' file not found, will use defaults")

    if not aiopg:
        print("aiopg is not installed, ignoring PostgreSQL tests")
        for key in list(DB_CLASSES.keys()):
            if key.startswith('postgres'):
                DB_CLASSES.pop(key)

    if not aiomysql:
        print("aiomysql is not installed, ignoring MySQL tests")
        for key in list(DB_CLASSES.keys()):
            if key.startswith('mysql'):
                DB_CLASSES.pop(key)

    loop = asyncio.new_event_loop()
    all_databases = load_databases(only=None)
    for key, database in all_databases.items():
        connect = database.connect_async(loop=loop)
        loop.run_until_complete(connect)
        if database._async_conn is not None:
            disconnect = database.close_async()
            loop.run_until_complete(disconnect)
        else:
            print("Can't setup connection for %s" % key)
            DB_CLASSES.pop(key)


def load_managers(*, loop, only):
    managers = {}
    for key in DB_CLASSES:
        if only and key not in only:
            continue
        params = DB_DEFAULTS.get(key) or {}
        params.update(DB_OVERRIDES.get(key) or {})
        database = DB_CLASSES[key](**params)
        managers[key] = peewee_async.Manager(database, loop=loop)
    return managers


def load_databases(*, only):
    databases = {}
    for key in DB_CLASSES:
        if only and key not in only:
            continue
        params = DB_DEFAULTS.get(key) or {}
        params.update(DB_OVERRIDES.get(key) or {})
        databases[key] = DB_CLASSES[key](**params)
    return databases


##########
# Models #
##########


class TestModel(peewee.Model):
    text = peewee.CharField(max_length=100, unique=True)
    data = peewee.TextField(default='')

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class TestModelAlpha(peewee.Model):
    text = peewee.CharField()

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class TestModelBeta(peewee.Model):
    alpha = peewee.ForeignKeyField(TestModelAlpha, backref='betas')
    text = peewee.CharField()

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class TestModelGamma(peewee.Model):
    text = peewee.CharField()
    beta = peewee.ForeignKeyField(TestModelBeta, backref='gammas')

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class UUIDTestModel(peewee.Model):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField()

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class CompositeTestModel(peewee.Model):
    """A simple "through" table for many-to-many relationship."""
    uuid = peewee.ForeignKeyField(UUIDTestModel)
    alpha = peewee.ForeignKeyField(TestModelAlpha)

    class Meta:
        primary_key = peewee.CompositeKey('uuid', 'alpha')


####################
# Base tests class #
####################


class BaseManagerTestCase(unittest.TestCase):
    only = None

    models = [TestModel, UUIDTestModel, TestModelAlpha,
              TestModelBeta, TestModelGamma, CompositeTestModel]

    @classmethod
    @contextlib.contextmanager
    def manager(cls, objects, allow_sync=False):
        for model in cls.models:
            model._meta.database = objects.database
        if allow_sync:
            with objects.allow_sync():
                yield
        else:
            yield

    def setUp(self):
        """Setup the new event loop, and database configs, reset counter.
        """
        self.run_count = 0
        self.loop = asyncio.new_event_loop()
        self.managers = load_managers(loop=self.loop, only=self.only)

        # Clean up before tests
        for _, objects in self.managers.items():
            objects.database.set_allow_sync(False)
            with self.manager(objects, allow_sync=True):
                for model in self.models:
                    model.create_table(True)
                for model in reversed(self.models):
                    model.delete().execute()

    def tearDown(self):
        """Check if test was actually passed by counter, clean up.
        """
        self.assertEqual(len(self.managers), self.run_count)

        for _, objects in self.managers.items():
            self.loop.run_until_complete(objects.close())
        self.loop.close()

        for _, objects in self.managers.items():
            with self.manager(objects, allow_sync=True):
                for model in reversed(self.models):
                    model.drop_table(fail_silently=True)

        self.managers = None

    def run_with_managers(self, test, exclude=None):
        """Run test coroutine against available Manager instances.

            test -- coroutine with single parameter, Manager instance
            exclude -- exclude list or string for manager key

        Example:

            async def test(objects):
                # ...

            run_with_managers(test, exclude=['mysql', 'mysql-pool'])
        """
        for key, objects in self.managers.items():
            if exclude is None or (key not in exclude):
                with self.manager(objects, allow_sync=False):
                    self.loop.run_until_complete(test(objects))
                with self.manager(objects, allow_sync=True):
                    for model in reversed(self.models):
                        model.delete().execute()
            self.run_count += 1


################
# Common tests #
################


class DatabaseTestCase(unittest.TestCase):
    def test_deferred_init(self):
        for key in DB_CLASSES:
            params = DB_DEFAULTS.get(key) or {}
            params.update(DB_OVERRIDES.get(key) or {})

            database = DB_CLASSES[key](None)
            self.assertTrue(database.deferred)

            database.init(**params)
            self.assertTrue(not database.deferred)

            TestModel._meta.database = database
            TestModel.create_table(True)
            TestModel.drop_table(True)

    def test_proxy_database(self):
        loop = asyncio.new_event_loop()
        database = peewee.Proxy()
        TestModel._meta.database = database
        objects = peewee_async.Manager(database, loop=loop)

        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)
            await objects.get(TestModel, text=text)

        for key in DB_CLASSES:
            params = DB_DEFAULTS.get(key) or {}
            params.update(DB_OVERRIDES.get(key) or {})
            database.initialize(DB_CLASSES[key](**params))

            TestModel.create_table(True)
            loop.run_until_complete(test(objects))
            loop.run_until_complete(objects.close())
            TestModel.drop_table(True)

        loop.close()


class OlderTestCase(unittest.TestCase):
    # only = ['postgres', 'postgres-ext', 'postgres-pool', 'postgres-pool-ext']
    only = None

    models = [TestModel, UUIDTestModel, TestModelAlpha,
              TestModelBeta, TestModelGamma]

    @classmethod
    @contextlib.contextmanager
    def current_database(cls, database, allow_sync=False):
        for model in cls.models:
            model._meta.database = database
        yield

    @classmethod
    def setUpClass(cls, *args, **kwargs):
        """Configure database managers, create test tables.
        """
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)
        cls.databases = load_databases(only=cls.only)

        for k, database in cls.databases.items():
            database.set_allow_sync(True)
            with cls.current_database(database):
                for model in cls.models:
                    model.create_table(True)
            database.set_allow_sync(False)

    @classmethod
    def tearDownClass(cls, *args, **kwargs):
        """Remove all test tables and close connections.
        """
        for _, database in cls.databases.items():
            cls.loop.run_until_complete(database.close_async())
        cls.loop.close()

        for _, database in cls.databases.items():
            database.set_allow_sync(True)
            with cls.current_database(database):
                for model in reversed(cls.models):
                    model.drop_table(fail_silently=True)
            database.set_allow_sync(False)
        cls.databases = None

    def setUp(self):
        """Reset all data.
        """
        self.run_count = 0
        for k, database in self.databases.items():
            with self.current_database(database):
                database.set_allow_sync(True)
                for model in reversed(self.models):
                    model.delete().execute()
                database.set_allow_sync(False)

    def tearDown(self):
        """Check if test was actually passed by counter.
        """
        self.assertEqual(len(self.databases), self.run_count)

    def run_with_databases(self, test, exclude=None):
        """Run test coroutine against available databases.
        """
        for k, database in self.databases.items():
            if exclude is None or (k not in exclude):
                with self.current_database(database):
                    database.set_allow_sync(False)
                    self.loop.run_until_complete(test(database))
                    database.set_allow_sync(True)
                    for model in reversed(self.models):
                        model.delete().execute()
                    database.set_allow_sync(False)
            self.run_count += 1

    def test_create_obj(self):
        async def test(database):
            text = "Test %s" % uuid.uuid4()
            obj = await peewee_async.create_object(TestModel, text=text)
            self.assertTrue(obj is not None)
            self.assertEqual(obj.text, text)

        self.run_with_databases(test)

    def test_get_and_delete_obj(self):
        async def test(database):
            text = "Test %s" % uuid.uuid4()
            obj1 = await peewee_async.create_object(
                TestModel, text=text)

            obj2 = await peewee_async.get_object(
                TestModel, TestModel.id == obj1.id)

            await peewee_async.delete_object(obj2)

            try:
                obj3 = await peewee_async.get_object(
                    TestModel, TestModel.id == obj1.id)
            except TestModel.DoesNotExist:
                obj3 = None
            self.assertTrue(obj3 is None, "Error, object wasn't deleted")

        self.run_with_databases(test)

    def test_get_and_update_obj(self):
        async def test(database):
            text = "Test %s" % uuid.uuid4()
            obj1 = await peewee_async.create_object(
                TestModel, text=text)

            obj1.text = "Test update object"
            await peewee_async.update_object(obj1)

            obj2 = await peewee_async.get_object(
                TestModel, TestModel.id == obj1.id)
            self.assertEqual(obj2.text, "Test update object")

        self.run_with_databases(test)


class ManagerTestCase(BaseManagerTestCase):
    # only = ['postgres', 'postgres-ext', 'postgres-pool', 'postgres-pool-ext']
    only = None

    def test_connect_close(self):
        async def get_conn(objects):
            await objects.connect()
            # await asyncio.sleep(0.05, loop=self.loop)
            # NOTE: "private" member access
            return objects.database._async_conn

        async def test(objects):
            c1 = await get_conn(objects)
            c2 = await get_conn(objects)
            self.assertEqual(c1, c2)
            self.assertTrue(objects.is_connected)

            await objects.close()
            self.assertTrue(not objects.is_connected)

            done, not_done = await asyncio.wait([
                get_conn(objects),
                get_conn(objects),
                get_conn(objects),
            ], loop=self.loop)

            conn = next(iter(done)).result()
            self.assertEqual(len(done), 3)
            self.assertTrue(objects.is_connected)
            self.assertTrue(all(map(lambda t: t.result() == conn, done)))

            await objects.close()
            self.assertTrue(not objects.is_connected)

        self.run_with_managers(test)

    def test_many_requests(self):
        async def test(objects):
            max_connections = getattr(objects.database, 'max_connections', 1)
            text = "Test %s" % uuid.uuid4()
            obj = await objects.create(TestModel, text=text)
            n = 2 * max_connections  # number of requests
            done, not_done = await asyncio.wait(
                [objects.get(TestModel, id=obj.id) for _ in range(n)],
                loop=self.loop)
            self.assertEqual(len(done), n)

        self.run_with_managers(test)

    def test_create_obj(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj = await objects.create(TestModel, text=text)
            self.assertTrue(obj is not None)
            self.assertEqual(obj.text, text)

        self.run_with_managers(test)

    def test_create_or_get(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1, created1 = await objects.create_or_get(
                TestModel, text=text, data="Data 1")
            obj2, created2 = await objects.create_or_get(
                TestModel, text=text, data="Data 2")

            self.assertTrue(created1)
            self.assertTrue(not created2)
            self.assertEqual(obj1, obj2)
            self.assertEqual(obj1.data, "Data 1")
            self.assertEqual(obj2.data, "Data 1")

        self.run_with_managers(test)

    def test_get_or_create(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()

            obj1, created1 = await objects.get_or_create(
                TestModel, text=text, defaults={'data': "Data 1"})
            obj2, created2 = await objects.get_or_create(
                TestModel, text=text, defaults={'data': "Data 2"})

            self.assertTrue(created1)
            self.assertTrue(not created2)
            self.assertEqual(obj1, obj2)
            self.assertEqual(obj1.data, "Data 1")
            self.assertEqual(obj2.data, "Data 1")

        self.run_with_managers(test)

    def test_create_uuid_obj(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj = await objects.create(UUIDTestModel, text=text)
            self.assertEqual(len(str(obj.id)), 36)

        self.run_with_managers(test, exclude=['mysql', 'mysql-pool'])

    def test_get_obj_by_id(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = await objects.create(TestModel, text=text)
            obj2 = await objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj1, obj2)
            self.assertEqual(obj1.id, obj2.id)

        self.run_with_managers(test)

    def test_get_obj_by_uuid(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = await objects.create(UUIDTestModel, text=text)
            obj2 = await objects.get(UUIDTestModel, id=obj1.id)
            self.assertEqual(obj1, obj2)
            self.assertEqual(len(str(obj1.id)), 36)

        self.run_with_managers(test)

    def test_raw_query(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)

            result1 = await objects.execute(TestModel.raw(
                'select id, text from testmodel'))
            result1 = list(result1)
            self.assertEqual(len(result1), 1)
            self.assertTrue(isinstance(result1[0], TestModel))

            result2 = await objects.execute(TestModel.raw(
                'select id, text from testmodel').tuples())
            result2 = list(result2)
            self.assertEqual(len(result2), 1)
            self.assertTrue(isinstance(result2[0], tuple))

            result3 = await objects.execute(TestModel.raw(
                'select id, text from testmodel').dicts())
            result3 = list(result3)
            self.assertEqual(len(result3), 1)
            self.assertTrue(isinstance(result3[0], dict))

        self.run_with_managers(test)

    def test_select_many_objects(self):
        async def test(objects):
            text = "Test 1"
            obj1 = await objects.create(TestModel, text=text)
            text = "Test 2"
            obj2 = await objects.create(TestModel, text=text)

            select1 = [obj1, obj2]
            len1 = len(select1)

            select2 = await objects.execute(
                TestModel.select().order_by(TestModel.text))
            len2 = len([o for o in select2])

            self.assertEqual(len1, len2)
            for o1, o2 in zip(select1, select2):
                self.assertEqual(o1, o2)

        self.run_with_managers(test)

    def test_indexing_result(self):
        async def test(objects):
            await objects.create(TestModel, text="Test 1")
            obj = await objects.create(TestModel, text="Test 2")
            result = await objects.execute(
                TestModel.select().order_by(TestModel.text))
            self.assertEqual(obj, result[1])

        self.run_with_managers(test)

    def test_multiple_iterate_over_result(self):
        async def test(objects):
            obj1 = await objects.create(TestModel, text="Test 1")
            obj2 = await objects.create(TestModel, text="Test 2")
            result = await objects.execute(
                TestModel.select().order_by(TestModel.text))
            self.assertEqual(list(result), [obj1, obj2])
            self.assertEqual(list(result), [obj1, obj2])

        self.run_with_managers(test)

    def test_insert_many_rows_query(self):
        async def test(objects):
            select1 = await objects.execute(TestModel.select())
            self.assertEqual(len(select1), 0)

            query = TestModel.insert_many([
                {'text': "Test %s" % uuid.uuid4()},
                {'text': "Test %s" % uuid.uuid4()},
            ])
            last_id = await objects.execute(query)
            self.assertTrue(last_id is not None)

            select2 = await objects.execute(TestModel.select())
            self.assertEqual(len(select2), 2)

        self.run_with_managers(test)

    def test_insert_one_row_query(self):
        async def test(objects):
            query = TestModel.insert(text="Test %s" % uuid.uuid4())
            last_id = await objects.execute(query)
            self.assertTrue(last_id is not None)
            select1 = await objects.execute(TestModel.select())
            self.assertEqual(len(select1), 1)

        self.run_with_managers(test)

    def test_insert_one_row_uuid_query(self):
        async def test(objects):
            query = UUIDTestModel.insert(text="Test %s" % uuid.uuid4())
            last_id = await objects.execute(query)
            self.assertEqual(len(str(last_id)), 36)

        self.run_with_managers(test, exclude=['mysql', 'mysql-pool'])

    def test_update_query(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = await objects.create(TestModel, text=text)

            query = TestModel.update(text="Test update query") \
                             .where(TestModel.id == obj1.id)

            upd1 = await objects.execute(query)
            self.assertEqual(upd1, 1)

            obj2 = await objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj2.text, "Test update query")

        self.run_with_managers(test)

    def test_update_obj(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = await objects.create(TestModel, text=text)

            obj1.text = "Test update object"
            await objects.update(obj1)

            obj2 = await objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj2.text, "Test update object")

        self.run_with_managers(test)

    def test_delete_obj(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = await objects.create(TestModel, text=text)

            obj2 = await objects.get(TestModel, id=obj1.id)

            await objects.delete(obj2)
            try:
                obj3 = await objects.get(TestModel, id=obj1.id)
            except TestModel.DoesNotExist:
                obj3 = None
            self.assertTrue(obj3 is None, "Error, object wasn't deleted")

        self.run_with_managers(test)

    def test_scalar_query(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)

            fn = peewee.fn.Count(TestModel.id)
            count = await objects.scalar(TestModel.select(fn))
            self.assertEqual(count, 2)

        self.run_with_managers(test)

    def test_count_query(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)

            count = await objects.count(TestModel.select())
            self.assertEqual(count, 3)

        self.run_with_managers(test)

    def test_count_query_with_limit(self):
        async def test(objects):
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            await objects.create(TestModel, text=text)

            count = await objects.count(TestModel.select().limit(1))
            self.assertEqual(count, 1)

        self.run_with_managers(test)

    def test_prefetch(self):
        async def test(objects):
            alpha_1 = await objects.create(
                TestModelAlpha, text='Alpha 1')
            alpha_2 = await objects.create(
                TestModelAlpha, text='Alpha 2')

            beta_11 = await objects.create(
                TestModelBeta, alpha=alpha_1, text='Beta 11')
            beta_12 = await objects.create(
                TestModelBeta, alpha=alpha_1, text='Beta 12')
            _ = await objects.create(
                TestModelBeta, alpha=alpha_2, text='Beta 21')
            _ = await objects.create(
                TestModelBeta, alpha=alpha_2, text='Beta 22')

            gamma_111 = await objects.create(
                TestModelGamma, beta=beta_11, text='Gamma 111')
            gamma_112 = await objects.create(
                TestModelGamma, beta=beta_11, text='Gamma 112')

            result = await objects.prefetch(
                TestModelAlpha.select(),
                TestModelBeta.select(),
                TestModelGamma.select())

            self.assertEqual(tuple(result),
                             (alpha_1, alpha_2))

            self.assertEqual(tuple(result[0].betas),
                             (beta_11, beta_12))

            self.assertEqual(tuple(result[0].betas[0].gammas),
                             (gamma_111, gamma_112))

        self.run_with_managers(test)

    def test_composite_key(self):
        async def test(objects):
            obj_uuid = await objects.create(UUIDTestModel, text='UUID')
            obj_alpha = await objects.create(TestModelAlpha, text='Alpha')
            comp = await objects.create(CompositeTestModel,
                                        uuid=obj_uuid,
                                        alpha=obj_alpha)
            self.assertEqual((obj_uuid, obj_alpha), comp.get_id())
        self.run_with_managers(test)


######################
# Transactions tests #
######################


class FakeUpdateError(Exception):
    """Fake error while updating database.
    """
    pass


class ManagerTransactionsTestCase(BaseManagerTestCase):
    # only = ['postgres', 'postgres-ext', 'postgres-pool', 'postgres-pool-ext']
    only = None

    def test_atomic_success(self):
        """Successful update in transaction.
        """
        async def test(objects):
            obj = await objects.create(TestModel, text='FOO')
            obj_id = obj.id

            async with objects.atomic():
                obj.text = 'BAR'
                await objects.update(obj)

            res = await objects.get(TestModel, id=obj_id)
            self.assertEqual(res.text, 'BAR')

        self.run_with_managers(test)

    def test_atomic_failed(self):
        """Failed update in transaction.
        """
        async def test(objects):
            obj = await objects.create(TestModel, text='FOO')
            obj_id = obj.id

            try:
                async with objects.atomic():
                    obj.text = 'BAR'
                    await objects.update(obj)
                    raise FakeUpdateError()
            except FakeUpdateError as e:
                error = True
                res = await objects.get(TestModel, id=obj_id)

            self.assertTrue(error)
            self.assertEqual(res.text, 'FOO')

        self.run_with_managers(test)

    def test_several_transactions(self):
        """Run several transactions in parallel tasks.
        """
        wait = lambda tasks: self.loop.run_until_complete(
            asyncio.wait([
                self.loop.create_task(t) for t in tasks
            ], loop=self.loop))

        async def t1(objects):
            async with objects.atomic():
                self.assertEqual(objects.database.transaction_depth_async(), 1)
                await asyncio.sleep(0.25, loop=self.loop)

        async def t2(objects):
            async with objects.atomic():
                self.assertEqual(objects.database.transaction_depth_async(), 1)
                await asyncio.sleep(0.0625, loop=self.loop)

        async def t3(objects):
            async with objects.atomic():
                self.assertEqual(objects.database.transaction_depth_async(), 1)
                await asyncio.sleep(0.125, loop=self.loop)

        for _, objects in self.managers.items():
            wait([
                t1(objects),
                t2(objects),
                t3(objects),
            ])

            with self.manager(objects, allow_sync=True):
                for model in reversed(self.models):
                    model.delete().execute()

            self.run_count += 1

    def test_atomic_fail_with_disconnect(self):
        """Database gone in transaction.
        """
        async def test(objects):
            error = False
            try:
                async with objects.atomic():
                    await objects.database.close_async()
                    raise FakeUpdateError()
            except FakeUpdateError:
                error = True

            self.assertTrue(error)

        self.run_with_managers(test)
