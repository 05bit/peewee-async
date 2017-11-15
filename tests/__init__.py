"""
peewee-async tests
==================

Create tests.ini file to configure tests.

"""
import os
import sys
import json
import logging
import asyncio
import contextlib
import unittest
import uuid
import peewee
import peewee_async
import peewee_asyncext

##########
# Config #
##########

# logging.basicConfig(level=logging.DEBUG)

defaults = {
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
        # 'port': 3306,
        'user': 'root',
    },
    'mysql-pool': {
        'database': 'test',
        'host': '127.0.0.1',
        # 'port': 3306,
        'user': 'root',
    }
}

overrides = {}

try:
    import aiopg
except ImportError:
    print("aiopg is not installed, ignoring PostgreSQL tests")
    for k in list(defaults.keys()):
        if k.startswith('postgres'):
            defaults.pop(k)


try:
    import aiomysql
except ImportError:
    print("aiomysql is not installed, ignoring MySQL tests")
    for k in list(defaults.keys()):
        if k.startswith('mysql'):
            defaults.pop(k)


db_classes = {
    'postgres': peewee_async.PostgresqlDatabase,
    'postgres-ext': peewee_asyncext.PostgresqlExtDatabase,
    'postgres-pool': peewee_async.PooledPostgresqlDatabase,
    'postgres-pool-ext': peewee_asyncext.PooledPostgresqlExtDatabase,
    'mysql': peewee_async.MySQLDatabase,
    'mysql-pool': peewee_async.PooledMySQLDatabase
}


def setUpModule():
    try:
        with open('tests.json', 'r') as f:
            overrides.update(json.load(f))
    except:
        print("'tests.json' file not found, will use defaults")


def load_managers(*, managers=None, loop=None, only=None):
    config = dict(defaults)
    for k in list(config.keys()):
        if only and not k in only:
            continue
        config[k].update(overrides.get(k, {}))
        database = db_classes[k](**config[k])
        managers[k] = peewee_async.Manager(database, loop=loop)


def load_databases(*, databases=None, only=None):
    config = dict(defaults)
    for k in list(config.keys()):
        if only and not k in only:
            continue
        config[k].update(overrides.get(k, {}))
        databases[k] = db_classes[k](**config[k])


##########
# Models #
##########


class TestModel(peewee.Model):
    text = peewee.CharField(max_length=100, unique=True)
    data = peewee.TextField(default='')


class TestModelAlpha(peewee.Model):
    text = peewee.CharField()


class TestModelBeta(peewee.Model):
    alpha = peewee.ForeignKeyField(TestModelAlpha, related_name='betas')
    text = peewee.CharField()


class TestModelGamma(peewee.Model):
    text = peewee.CharField()
    beta = peewee.ForeignKeyField(TestModelBeta, related_name='gammas')


class UUIDTestModel(peewee.Model):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField()


####################
# Base tests class #
####################


class BaseManagerTestCase(unittest.TestCase):
    only = None

    models = [TestModel, UUIDTestModel, TestModelAlpha,
              TestModelBeta, TestModelGamma]

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

        self.managers = {}
        self.loop = asyncio.new_event_loop()
        # self.loop.set_debug(True)

        load_managers(managers=self.managers,
                      loop=self.loop,
                      only=self.only)

        for k, objects in self.managers.items():
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

        for k, objects in self.managers.items():
            self.loop.run_until_complete(objects.close())
        self.loop.close()

        for k, objects in self.managers.items():
            with self.manager(objects, allow_sync=True):
                for model in reversed(self.models):
                    model.drop_table(fail_silently=True)

        self.managers = None

    def run_with_managers(self, test, exclude=None):
        """Run test coroutine against available Manager instances.

            test -- coroutine with single parameter, Manager instance
            exclude -- exclude list or string for manager key

        Example:

            @asyncio.coroutine
            def test(objects):
                # ...

            run_with_managers(test, exclude=['mysql', 'mysql-pool'])
        """
        for k, objects in self.managers.items():
            if exclude is None or (not k in exclude):
                with self.manager(objects):
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
        config = dict(defaults)
        for k in list(config.keys()):
            config[k].update(overrides.get(k, {}))

            database = db_classes[k](None)
            self.assertTrue(database.deferred)

            database.init(**config[k])
            self.assertTrue(not database.deferred)

            TestModel._meta.database = database
            TestModel.create_table(True)
            TestModel.drop_table(True)

    def test_proxy_database(self):
        loop = asyncio.new_event_loop()
        database = peewee.Proxy()
        TestModel._meta.database = database
        objects = peewee_async.Manager(database, loop=loop)

        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)
            obj = yield from objects.get(TestModel, text=text)

        config = dict(defaults)
        for k in list(config.keys()):
            config[k].update(overrides.get(k, {}))
            database.initialize(db_classes[k](**config[k]))

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
        cls.databases = {}
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)

        load_databases(databases=cls.databases,
                       only=cls.only)

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
        for k, database in cls.databases.items():
            cls.loop.run_until_complete(database.close_async())
        cls.loop.close()

        for k, database in cls.databases.items():
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
            if exclude is None or (not k in exclude):
                with self.current_database(database):
                    database.set_allow_sync(False)
                    self.loop.run_until_complete(test(database))
                    database.set_allow_sync(True)
                    for model in reversed(self.models):
                        model.delete().execute()
                    database.set_allow_sync(False)
            self.run_count += 1

    def test_create_obj(self):
        @asyncio.coroutine
        def test(database):
            text = "Test %s" % uuid.uuid4()
            obj = yield from peewee_async.create_object(TestModel, text=text)
            self.assertTrue(obj is not None)
            self.assertEqual(obj.text, text)

        self.run_with_databases(test)

    def test_get_and_delete_obj(self):
        @asyncio.coroutine
        def test(database):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from peewee_async.create_object(
                TestModel, text=text)

            obj2 = yield from peewee_async.get_object(
                TestModel, TestModel.id == obj1.id)

            yield from peewee_async.delete_object(obj2)

            try:
                obj3 = yield from peewee_async.get_object(
                    TestModel, TestModel.id == obj1.id)
            except TestModel.DoesNotExist:
                obj3 = None
            self.assertTrue(obj3 is None, "Error, object wasn't deleted")

        self.run_with_databases(test)

    def test_get_and_update_obj(self):
        @asyncio.coroutine
        def test(database):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from peewee_async.create_object(
                TestModel, text=text)

            obj1.text = "Test update object"
            yield from peewee_async.update_object(obj1)

            obj2 = yield from peewee_async.get_object(
                TestModel, TestModel.id == obj1.id)
            self.assertEqual(obj2.text, "Test update object")

        self.run_with_databases(test)


class ManagerTestCase(BaseManagerTestCase):
    # only = ['postgres', 'postgres-ext', 'postgres-pool', 'postgres-pool-ext']
    only = None

    def test_connect_close(self):
        @asyncio.coroutine
        def get_conn(objects):
            yield from objects.connect()
            # yield from asyncio.sleep(0.05, loop=self.loop)
            # NOTE: "private" member access
            return objects.database._async_conn

        @asyncio.coroutine
        def test(objects):
            c1 = yield from get_conn(objects)
            c2 = yield from get_conn(objects)
            self.assertEqual(c1, c2)
            self.assertTrue(objects.is_connected)

            yield from objects.close()
            self.assertTrue(not objects.is_connected)

            done, not_done = yield from asyncio.wait([
                get_conn(objects),
                get_conn(objects),
                get_conn(objects),
            ], loop=self.loop)

            conn = next(iter(done)).result()
            self.assertEqual(len(done), 3)
            self.assertTrue(objects.is_connected)
            self.assertTrue(all(map(lambda t: t.result() == conn, done)))

            yield from objects.close()
            self.assertTrue(not objects.is_connected)

        self.run_with_managers(test)

    def test_many_requests(self):
        @asyncio.coroutine
        def test(objects):
            max_connections = getattr(objects.database, 'max_connections', 1)
            text = "Test %s" % uuid.uuid4()
            obj = yield from objects.create(TestModel, text=text)
            n = 2 * max_connections # number of requests
            done, not_done = yield from asyncio.wait(
                [objects.get(TestModel, id=obj.id) for _ in range(n)],
                loop=self.loop)
            self.assertEqual(len(done), n)

        self.run_with_managers(test)

    def test_create_obj(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj = yield from objects.create(TestModel, text=text)
            self.assertTrue(obj is not None)
            self.assertEqual(obj.text, text)

        self.run_with_managers(test)

    def test_create_or_get(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1, created1 = yield from objects.create_or_get(
                TestModel, text=text, data="Data 1")
            obj2, created2 = yield from objects.create_or_get(
                TestModel, text=text, data="Data 2")

            self.assertTrue(created1)
            self.assertTrue(not created2)
            self.assertEqual(obj1, obj2)
            self.assertEqual(obj1.data, "Data 1")
            self.assertEqual(obj2.data, "Data 1")

        self.run_with_managers(test)

    def test_get_or_create(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()

            obj1, created1 = yield from objects.get_or_create(
                TestModel, text=text, defaults={'data': "Data 1"})
            obj2, created2 = yield from objects.get_or_create(
                TestModel, text=text, defaults={'data': "Data 2"})

            self.assertTrue(created1)
            self.assertTrue(not created2)
            self.assertEqual(obj1, obj2)
            self.assertEqual(obj1.data, "Data 1")
            self.assertEqual(obj2.data, "Data 1")

        self.run_with_managers(test)

    def test_create_uuid_obj(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj = yield from objects.create(UUIDTestModel, text=text)
            self.assertEqual(len(str(obj.id)), 36)

        self.run_with_managers(test, exclude=['mysql', 'mysql-pool'])

    def test_get_obj_by_id(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from objects.create(TestModel, text=text)

            obj2 = yield from objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj1, obj2)
            self.assertEqual(obj1.id, obj2.id)

        self.run_with_managers(test)

    def test_get_obj_by_uuid(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from objects.create(UUIDTestModel, text=text)

            obj2 = yield from objects.get(UUIDTestModel, id=obj1.id)
            self.assertEqual(obj1, obj2)
            self.assertEqual(len(str(obj1.id)), 36)

        self.run_with_managers(test)

    def test_raw_query(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            result1 = yield from objects.execute(TestModel.raw(
                'select id, text from testmodel'))
            result1 = list(result1)
            self.assertEqual(len(result1), 1)
            self.assertTrue(isinstance(result1[0], TestModel))

            result2 = yield from objects.execute(TestModel.raw(
                'select id, text from testmodel').tuples())
            result2 = list(result2)
            self.assertEqual(len(result2), 1)
            self.assertTrue(isinstance(result2[0], tuple))

            result3 = yield from objects.execute(TestModel.raw(
                'select id, text from testmodel').dicts())
            result3 = list(result3)
            self.assertEqual(len(result3), 1)
            self.assertTrue(isinstance(result3[0], dict))

        self.run_with_managers(test)

    def test_select_many_objects(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test 1"
            obj1 = yield from objects.create(TestModel, text=text)
            text = "Test 2"
            obj2 = yield from objects.create(TestModel, text=text)

            select1 = [obj1, obj2]
            len1 = len(select1)

            select2 = yield from objects.execute(
                TestModel.select().order_by(TestModel.text))
            len2 = len([o for o in select2])

            self.assertEqual(len1, len2)
            for o1, o2 in zip(select1, select2):
                self.assertEqual(o1, o2)

        self.run_with_managers(test)

    def test_insert_many_rows_query(self):
        @asyncio.coroutine
        def test(objects):
            select1 = yield from objects.execute(TestModel.select())
            self.assertEqual(len(select1), 0)

            query = TestModel.insert_many([
                {'text': "Test %s" % uuid.uuid4()},
                {'text': "Test %s" % uuid.uuid4()},
            ])
            last_id = yield from objects.execute(query)
            self.assertTrue(last_id is not None)

            select2 = yield from objects.execute(TestModel.select())
            self.assertEqual(len(select2), 2)

        self.run_with_managers(test)

    def test_insert_one_row_query(self):
        @asyncio.coroutine
        def test(objects):
            query = TestModel.insert(text="Test %s" % uuid.uuid4())
            last_id = yield from objects.execute(query)
            self.assertTrue(last_id is not None)
            select1 = yield from objects.execute(TestModel.select())
            self.assertEqual(len(select1), 1)

        self.run_with_managers(test)

    def test_insert_one_row_uuid_query(self):
        @asyncio.coroutine
        def test(objects):
            query = UUIDTestModel.insert(text="Test %s" % uuid.uuid4())
            last_id = yield from objects.execute(query)
            self.assertEqual(len(str(last_id)), 36)

        self.run_with_managers(test, exclude=['mysql', 'mysql-pool'])

    def test_update_query(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from objects.create(TestModel, text=text)

            query = TestModel.update(text="Test update query") \
                             .where(TestModel.id == obj1.id)
            
            upd1 = yield from objects.execute(query)
            self.assertEqual(upd1, 1)

            obj2 = yield from objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj2.text, "Test update query")

        self.run_with_managers(test)

    def test_update_obj(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from objects.create(TestModel, text=text)

            obj1.text = "Test update object"
            yield from objects.update(obj1)

            obj2 = yield from objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj2.text, "Test update object")

        self.run_with_managers(test)

    def test_delete_obj(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj1 = yield from objects.create(TestModel, text=text)

            obj2 = yield from objects.get(TestModel, id=obj1.id)

            yield from objects.delete(obj2)
            try:
                obj3 = yield from objects.get(TestModel, id=obj1.id)
            except TestModel.DoesNotExist:
                obj3 = None
            self.assertTrue(obj3 is None, "Error, object wasn't deleted")

        self.run_with_managers(test)

    def test_scalar_query(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            fn = peewee.fn.Count(TestModel.id)
            count = yield from objects.scalar(TestModel.select(fn))
            self.assertEqual(count, 2)

        self.run_with_managers(test)

    def test_count_query(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            count = yield from objects.count(TestModel.select())
            self.assertEqual(count, 3)
            
        self.run_with_managers(test)

    def test_prefetch(self):
        @asyncio.coroutine
        def test(objects):
            alpha_1 = yield from objects.create(TestModelAlpha,
                                                text='Alpha 1')
            alpha_2 = yield from objects.create(TestModelAlpha,
                                                text='Alpha 2')

            beta_11 = yield from objects.create(TestModelBeta,
                                                alpha=alpha_1,
                                                text='Beta 11')
            beta_12 = yield from objects.create(TestModelBeta,
                                                alpha=alpha_1,
                                                text='Beta 12')
            beta_21 = yield from objects.create(TestModelBeta,
                                                alpha=alpha_2,
                                                text='Beta 21')
            beta_22 = yield from objects.create(TestModelBeta,
                                                alpha=alpha_2,
                                                text='Beta 22')

            gamma_111 = yield from objects.create(TestModelGamma,
                                                  beta=beta_11,
                                                  text='Gamma 111')
            gamma_112 = yield from objects.create(TestModelGamma,
                                                  beta=beta_11,
                                                  text='Gamma 112')
            
            result = yield from objects.prefetch(
                TestModelAlpha.select(),
                TestModelBeta.select(),
                TestModelGamma.select())

            result = tuple(result)

            self.assertEqual(result,
                             (alpha_1, alpha_2))

            self.assertEqual(tuple(result[0].betas_prefetch),
                            (beta_11, beta_12))

            self.assertEqual(tuple(result[0].betas_prefetch[0].gammas_prefetch),
                             (gamma_111, gamma_112))

        self.run_with_managers(test)

    def test_aggregate_rows(self):
        @asyncio.coroutine
        def test(objects):
            alpha = yield from objects.create(TestModelAlpha,
                                              text='Alpha 10')

            beta_1 = yield from objects.create(TestModelBeta,
                                               alpha=alpha,
                                               text='Beta 110')
            beta_2 = yield from objects.create(TestModelBeta,
                                               alpha=alpha,
                                               text='Beta 120')

            gamma_1 = yield from objects.create(TestModelGamma,
                                                beta=beta_1,
                                                text='Gamma 1110')
            gamma_2 = yield from objects.create(TestModelGamma,
                                                beta=beta_1,
                                                text='Gamma 1120')

            result = yield from objects.get((
                TestModelAlpha
                .select(TestModelAlpha, TestModelBeta, TestModelGamma)
                .join(TestModelBeta)
                .join(TestModelGamma)
                .where(TestModelAlpha.text == 'Alpha 10')
                .order_by(TestModelAlpha.text, TestModelBeta.text, TestModelGamma.text)
                .aggregate_rows()))

            self.assertEqual(result, alpha)

            self.assertEqual(result.betas, [beta_1, beta_2])

            self.assertEqual(result.betas[0].gammas, [gamma_1, gamma_2])

        self.run_with_managers(test)


#####################
# Python 3.5+ tests #
#####################

if sys.version_info >= (3, 5):
    from .tests_py35 import *
