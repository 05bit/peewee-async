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

# import logging
# logging.basicConfig(level=logging.DEBUG)


class ProxyDatabase(object):
    """Proxy database for deferred initialization.
    """
    def __init__(self):
        self.conn = None

    def __getattr__(self, attr):
        if self.conn is None:
            raise AttributeError('Cannot use uninitialized Proxy.')
        return getattr(self.conn, attr)

    def __setattr__(self, attr, value):
        if attr == 'conn':
            return super(ProxyDatabase, self).__setattr__(attr, value)
        elif (self.conn is None) and (attr != 'conn'):
            raise AttributeError('Cannot use uninitialized Proxy.')
        else:
            return setattr(self.conn, attr, value)        

# Shortcuts
execute = peewee_async.execute
count = peewee_async.count
scalar = peewee_async.scalar
get_object = peewee_async.get_object
create_object = peewee_async.create_object
delete_object = peewee_async.delete_object
update_object = peewee_async.update_object
sync_unwanted = peewee_async.sync_unwanted

# Globals
db_params = {}
database = ProxyDatabase()
managers = {}


def setUpModule():
    global db_params
    global database

    ini = configparser.ConfigParser()
    ini.read(['tests.ini'])

    try:
        config = dict(**ini['tests'])
    except KeyError:
        config = {}

    config.setdefault('database', 'test')
    config.setdefault('host', '127.0.0.1')
    config.setdefault('port', None)
    config.setdefault('user', 'postgres')
    config.setdefault('password', '')

    if 'DATABASE_URL' in os.environ:
        url = urllib.parse.urlparse(os.environ['DATABASE_URL'])
        config['user'] = url.username or config['user']
        config['host'] = url.host or config['host']
        config['port'] = url.port or config['port']

    db_params = config.copy()
    use_ext = db_params.pop('use_ext', False)
    use_pool = False

    if 'max_connections' in db_params:
        db_params['max_connections'] = int(db_params['max_connections'])
        use_pool = db_params['max_connections'] > 1
        if not use_pool:
            db_params.pop('max_connections')

    if use_pool:
        if use_ext:
            db_cls = peewee_asyncext.PooledPostgresqlExtDatabase
        else:
            db_cls = peewee_async.PooledPostgresqlDatabase
    else:
        if use_ext:
            db_cls = peewee_asyncext.PostgresqlExtDatabase
        else:
            db_cls = peewee_async.PostgresqlDatabase

    database.conn = db_cls(**db_params)

    init_managers(ini)


def init_managers(ini):
    if managers:
        return

    config = {
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

    classes = {
        'postgres': peewee_async.PostgresqlDatabase,
        'postgres-ext': peewee_asyncext.PostgresqlExtDatabase,
        'postgres-pool': peewee_async.PooledPostgresqlDatabase,
        'postgres-pool-ext': peewee_asyncext.PooledPostgresqlExtDatabase,
    }

    for k in list(config.keys()):
        try:
            config.update(dict(**ini[k]))
        except KeyError:
            pass

        db_cls = classes[k]
        db = db_cls(**config[k])
        managers[k] = peewee_async.Manager(db)


class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database


class TestModelAlpha(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database


class TestModelBeta(peewee.Model):
    alpha = peewee.ForeignKeyField(TestModelAlpha, related_name='betas')
    text = peewee.CharField()

    class Meta:
        database = database


class TestModelGamma(peewee.Model):
    text = peewee.CharField()
    beta = peewee.ForeignKeyField(TestModelBeta, related_name='gammas')

    class Meta:
        database = database


class UUIDTestModel(peewee.Model):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField()

    class Meta:
        database = database


class ManagerTestCase(unittest.TestCase):
    models = [TestModel, UUIDTestModel, TestModelAlpha,
              TestModelBeta, TestModelGamma]

    @classmethod
    def setUpClass(cls, *args, **kwargs):
        cls.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls, *args, **kwargs):
        for k in managers:
            cls.loop.run_until_complete(managers[k].close())

    def setUp(self):
        self.run_count = 0
        for k in managers:
            for model in self.models:
                managers[k].database.create_table(model, safe=True)

    def tearDown(self):
        self.assertTrue(len(managers) == self.run_count)
        for k in managers:
            managers[k].database.drop_tables(
                self.models, safe=True, cascade=True)

    def run_with_managers(self, test, only=None):
        """Run test coroutine against available Manager instances.

            test -- coroutine with single parameter, Manager instance
            only -- list of keys to filter managers, e.g. ['postgres-ext']
        """
        run = lambda c: self.loop.run_until_complete(c)
        for k in managers:
            objects = managers[k]

            # Swap sync database
            for model in self.models:
                model._meta.database = objects.database

            # Run async test
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
            TestModel.create(text="Test %s" % uuid.uuid4())
            TestModel.create(text="Test %s" % uuid.uuid4())

            # Reference sync select
            select1 = TestModel.select()
            len1 = len([o for o in select1])

            # Async select
            objects.database.allow_sync = False
            select2 = yield from objects.execute(TestModel.select())
            len2 = len([o for o in select2])

            # Should be identical
            self.assertTrue(len2 > 0)
            self.assertEqual(len1, len2)
            for o1, o2 in zip(select1, select2):
                self.assertEqual(o1, o2)

            objects.database.allow_sync = True

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

            objects.database.allow_sync = False

            query = TestModel.update(text="Test update async") \
                             .where(TestModel.id == obj1.id)
            
            upd1 = yield from objects.execute(query)
            self.assertEqual(upd1, 1)

            obj2 = yield from objects.get(TestModel, id=obj1.id)
            self.assertEqual(obj2.text, "Test update async")

            objects.database.allow_sync = True

        self.run_with_managers(test)

    def test_scalar_query(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            fn = peewee.fn.Count(TestModel.id)
            count1 = TestModel.select(fn).scalar()
            count2 = yield from objects.scalar(TestModel.select(fn))
            self.assertEqual(count1, count2)

        self.run_with_managers(test)

    def test_count_query(self):
        @asyncio.coroutine
        def test(objects):
            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            count0 = TestModel.select().count()

            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            count1 = TestModel.select().count()

            text = "Test %s" % uuid.uuid4()
            yield from objects.create(TestModel, text=text)

            count2 = yield from objects.count(TestModel.select())

            self.assertEqual(count0, 1)
            self.assertEqual(count1, 2)
            self.assertEqual(count2, 3)
            
        self.run_with_managers(test)

    def test_prefetch(self):
        @asyncio.coroutine
        def test(objects):
            alpha_1 = TestModelAlpha.create(text='Alpha 1')
            alpha_2 = TestModelAlpha.create(text='Alpha 2')
            beta_11 = TestModelBeta.create(text='Beta 1', alpha=alpha_1)
            beta_12 = TestModelBeta.create(text='Beta 2', alpha=alpha_1)
            beta_21 = TestModelBeta.create(text='Beta 1', alpha=alpha_2)
            beta_22 = TestModelBeta.create(text='Beta 2', alpha=alpha_2)
            gamma_111 = TestModelGamma.create(text='Gamma 1', beta=beta_11)
            gamma_112 = TestModelGamma.create(text='Gamma 2', beta=beta_11)

            objects.database.allow_sync = False

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

            objects.database.allow_sync = True

        self.run_with_managers(test)


class PostgresInitTestCase(unittest.TestCase):
    def test_deferred_init(self):
        db = peewee_async.PooledPostgresqlDatabase(None)
        self.assertTrue(db.deferred)

        db.init(**db_params)
        self.assertTrue(not db.deferred)

        loop = asyncio.get_event_loop()
        run = lambda coroutine: loop.run_until_complete(coroutine)

        run(db.connect_async(loop=loop))
        run(db.connect_async(loop=loop)) # Should not fail connect again
        run(db.close_async())
        run(db.close_async())# Should not fail closing again


class BaseAsyncPostgresTestCase(unittest.TestCase):
    db_tables = [TestModel, UUIDTestModel, TestModelAlpha,
                 TestModelBeta, TestModelGamma]

    @classmethod
    def setUpClass(cls, *args, **kwargs):
        # Sync connect 
        database.connect()

        # Async connect
        cls.loop = asyncio.get_event_loop()
        cls.loop.run_until_complete(database.connect_async(loop=cls.loop))

        # Clean up after possible errors
        for table in reversed(cls.db_tables):
            table.drop_table(True, cascade=True)

        # Create tables with sync connection
        for table in cls.db_tables:
            table.create_table()

        # Create at least one object per model
        cls.obj = TestModel.create(text='[sync] Hello!')
        cls.uuid_obj = UUIDTestModel.create(text='[sync] Hello!')

        cls.alpha_1 = TestModelAlpha.create(text='Alpha 1')
        cls.alpha_2 = TestModelAlpha.create(text='Alpha 2')

        cls.beta_11 = TestModelBeta.create(text='Beta 1', alpha=cls.alpha_1)
        cls.beta_12 = TestModelBeta.create(text='Beta 2', alpha=cls.alpha_1)

        cls.beta_21 = TestModelBeta.create(text='Beta 1', alpha=cls.alpha_2)
        cls.beta_22 = TestModelBeta.create(text='Beta 2', alpha=cls.alpha_2)

        cls.gamma_111 = TestModelGamma.create(text='Gamma 1', beta=cls.beta_11)
        cls.gamma_112 = TestModelGamma.create(text='Gamma 2', beta=cls.beta_11)

    @classmethod
    def tearDownClass(cls, *args, **kwargs):
        # Finally, clean up
        for table in reversed(cls.db_tables):
            table.drop_table()

        # Close database
        database.close()

        # Async disconnect
        cls.loop.run_until_complete(database.close_async())

    def run_until_complete(self, coroutine):
        result = self.loop.run_until_complete(coroutine)
        return result


class AsyncPostgresTestCase(BaseAsyncPostgresTestCase):
    def test_delete_obj(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_delete_obj]')

        # Async delete
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                result = yield from delete_object(obj1)
            return result

        del1 = self.run_until_complete(test())
        self.assertEqual(del1, 1)
        try:
            TestModel.get(id=obj1.id)
            self.assertTrue(False, "Error, object wasn't deleted")
        except TestModel.DoesNotExist:
            pass

    def test_update_obj(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_save_obj]')

        # Async save
        @asyncio.coroutine
        def test():
            obj1.text = '[async] [test_save_obj]'
            with sync_unwanted(database):
                result = yield from update_object(obj1)
            return result

        sav1 = self.run_until_complete(test())
        self.assertEqual(sav1, 1)
        self.assertEqual(TestModel.get(id=obj1.id).text,
                         '[async] [test_save_obj]')


if sys.version_info >= (3, 5):
    from .tests_py35 import *
