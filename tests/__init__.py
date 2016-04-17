"""
peewee-async tests
==================

Create tests.ini file to configure tests.

"""
import os
import asyncio
import configparser
import sys
import urllib.parse
import unittest
import uuid
import peewee
import peewee_async
import peewee_asyncext

##########
# Config #
##########

ini = configparser.ConfigParser()

deafults = {
    'postgres': {
        'database': 'test',
        'host': '127.0.0.1',
        'port': 5432,
        'user': 'postgres',
    },
    'postgres-ext': {
        'database': 'test',
        'host': '127.0.0.1',
        'port': 5432,
        'user': 'postgres',
    },
    'postgres-pool': {
        'database': 'test',
        'host': '127.0.0.1',
        'port': 5432,
        'user': 'postgres',
        'max_connections': 2,
    },
    'postgres-pool-ext': {
        'database': 'test',
        'host': '127.0.0.1',
        'port': 5432,
        'user': 'postgres',
        'max_connections': 2,
    }
}

db_classes = {
    'postgres': peewee_async.PostgresqlDatabase,
    'postgres-ext': peewee_asyncext.PostgresqlExtDatabase,
    'postgres-pool': peewee_async.PooledPostgresqlDatabase,
    'postgres-pool-ext': peewee_asyncext.PooledPostgresqlExtDatabase,
}


def setUpModule():
    ini.read(['tests.ini'])


def load_managers(*, managers=None, loop=None):
    config = dict(deafults)

    for k in list(config.keys()):
        try:
            config.update(dict(**ini[k]))
        except KeyError:
            pass

        db_class = db_classes[k]
        database = db_class(**config[k])
        managers[k] = peewee_async.Manager(database, loop=loop)


##########
# Models #
##########


class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = peewee_async.AutoDatabase


class TestModelAlpha(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = peewee_async.AutoDatabase


class TestModelBeta(peewee.Model):
    alpha = peewee.ForeignKeyField(TestModelAlpha, related_name='betas')
    text = peewee.CharField()

    class Meta:
        database = peewee_async.AutoDatabase


class TestModelGamma(peewee.Model):
    text = peewee.CharField()
    beta = peewee.ForeignKeyField(TestModelBeta, related_name='gammas')

    class Meta:
        database = peewee_async.AutoDatabase


class UUIDTestModel(peewee.Model):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField()

    class Meta:
        database = peewee_async.AutoDatabase


####################
# Base tests class #
####################


class BaseManagerTestCase(unittest.TestCase):
    managers = {}

    models = [TestModel, UUIDTestModel, TestModelAlpha,
              TestModelBeta, TestModelGamma]

    @classmethod
    def setUpClass(cls, *args, **kwargs):
        cls.loop = asyncio.new_event_loop()
        load_managers(managers=cls.managers, loop=cls.loop)

    @classmethod
    def tearDownClass(cls, *args, **kwargs):
        for k, objects in cls.managers.items():
            cls.loop.run_until_complete(objects.close())

    def setUp(self):
        self.run_count = 0

        for k, objects in self.managers.items():
            for model in self.models:
                with objects.allow_sync():
                    objects.database.drop_table(
                        model, fail_silently=True, cascade=True)
                    objects.database.create_table(model)

    def tearDown(self):
        self.assertTrue(len(self.managers) == self.run_count)

        for k, objects in self.managers.items():
            for model in self.models:
                with objects.allow_sync():
                    objects.database.drop_table(
                        model, fail_silently=True, cascade=True)

    def run_with_managers(self, test, only=None):
        """Run test coroutine against available Manager instances.

            test -- coroutine with single parameter, Manager instance
            only -- list of keys to filter managers, e.g. ['postgres-ext']
        """
        run = lambda c: self.loop.run_until_complete(c)
        for k, objects in self.managers.items():
            if only is None or (k in only):
                run(test(objects))
                run(self.clean_up(objects))

            self.run_count += 1

    @asyncio.coroutine
    def clean_up(self, objects):
        """Clean all tables against objects manager.
        """
        for model in reversed(self.models):
            yield from objects.execute(model.delete())


################
# Common tests #
################


class ManagerTestCase(BaseManagerTestCase):
    def test_connect_close(self):
        @asyncio.coroutine
        def test(objects):
            yield from objects.connect()
            self.assertTrue(objects.is_connected)
            yield from objects.connect()
            self.assertTrue(objects.is_connected)
            yield from objects.close()
            self.assertTrue(not objects.is_connected)
            yield from objects.close()
            self.assertTrue(not objects.is_connected)

        self.run_with_managers(test)

    def test_create_obj(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj = yield from objects.create(TestModel, text=text)
            self.assertTrue(obj is not None)

        self.run_with_managers(test)

    def test_create_uuid_obj(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            obj = yield from objects.create(UUIDTestModel, text=text)
            self.assertEqual(len(str(obj.id)), 36)

        self.run_with_managers(test)

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

        self.run_with_managers(test)

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


class PostgresInitTestCase(unittest.TestCase):
    def test_deferred_init(self):
        config = dict(deafults)

        for k in list(config.keys()):
            try:
                config.update(dict(**ini[k]))
            except KeyError:
                pass

            db_class = db_classes[k]
            database = db_class(None)
            self.assertTrue(database.deferred)

            database.init(**config[k])
            self.assertTrue(not database.deferred)


#####################
# Python 3.5+ tests #
#####################

if sys.version_info >= (3, 5):
    from .tests_py35 import *
