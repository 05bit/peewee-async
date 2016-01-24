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
config = {}
database = ProxyDatabase()


def setUpModule():
    global config
    global database

    ini_config = configparser.ConfigParser()
    ini_config.read(['tests.ini'])

    try:
        config = dict(**ini_config['tests'])
    except KeyError:
        pass

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

    db_cfg = config.copy()
    use_ext = db_cfg.pop('use_ext', False)

    if 'max_connections' in db_cfg:
        db_cfg['max_connections'] = int(db_cfg['max_connections'])
        use_pool = db_cfg['max_connections'] > 1
    else:
        use_pool = False

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

    database.conn = db_cls(**db_cfg)


class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database


class UUIDTestModel(peewee.Model):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField()

    class Meta:
        database = database


class BaseAsyncPostgresTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls, *args, **kwargs):
        # Sync connect 
        database.connect()

        # Async connect
        cls.loop = asyncio.get_event_loop()
        @asyncio.coroutine
        def test():
            yield from database.connect_async(loop=cls.loop)
        cls.loop.run_until_complete(test())

        # Clean up after possible errors
        TestModel.drop_table(True)
        UUIDTestModel.drop_table(True)

        # Create table with sync connection
        TestModel.create_table()
        UUIDTestModel.create_table()

        # Create at least one object per model
        cls.obj = TestModel.create(text='[sync] Hello!')
        cls.uuid_obj = UUIDTestModel.create(text='[sync] Hello!')

    @classmethod
    def tearDownClass(cls, *args, **kwargs):
        # Finally, clean up
        TestModel.drop_table()

        # Close database
        database.close()

    def run_until_complete(self, coroutine):
        result = self.loop.run_until_complete(coroutine)
        return result


class AsyncPostgresTestCase(BaseAsyncPostgresTestCase):
    def test_get_obj(self):
        # Async get
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                obj = yield from get_object(TestModel, TestModel.id == self.obj.id)
            self.assertEqual(obj.text, self.obj.text)
            return obj

        obj = self.run_until_complete(test())
        self.assertTrue(obj is not None)

    def test_get_uuid_obj(self):
        # Async get
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                obj = yield from get_object(UUIDTestModel, UUIDTestModel.id == self.uuid_obj.id)
            self.assertEqual(obj.text, self.obj.text)
            return obj

        obj = self.run_until_complete(test())
        self.assertTrue(obj is not None)

    def test_create_obj(self):
        # Async create
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                obj = yield from create_object(TestModel, text='[async] [test_create_obj]')
            self.assertTrue(obj.id is not None)
            return obj

        self.run_until_complete(test())

    def test_create_uuid_obj(self):
        # Async create
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                obj = yield from create_object(UUIDTestModel, text='[async] [test_create_uuid_obj]')
            self.assertTrue(obj.id is not None)
            return obj

        self.run_until_complete(test())

    def test_select_query(self):
        # Sync select
        q1 = TestModel.select()
        len1 = len([o for o in q1])
        self.assertTrue(len1 > 0)

        # Async select
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                result = yield from execute(TestModel.select())
            return result

        q2 = self.run_until_complete(test())
        len2 = len([o for o in q2])
        self.assertTrue(len2 > 0)

        # Results should be the same
        self.assertEqual(len1, len2)
        for o1, o2 in zip(q1, q2):
            self.assertEqual(o1, o2)

    def test_insert_many_rows_query(self):
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                query = TestModel.insert_many([
                    {'text': '[async] [test_insert_many_1]'},
                    {'text': '[async] [test_insert_many_2]'}
                ])
                result = yield from execute(query)
            return result

        last_id = self.run_until_complete(test())
        self.assertTrue(last_id is not None)

    def test_insert_one_row_query(self):
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                query = TestModel.insert(text='[async] [test_insert_one_query]')
                result = yield from execute(query)
            return result

        last_id = self.run_until_complete(test())
        self.assertTrue(last_id is not None)

    def test_insert_one_row_uuid_query(self):
        @asyncio.coroutine
        def test():
            with sync_unwanted(database):
                query = UUIDTestModel.insert(text='[async] [test_insert_uuid_query]')
                result = yield from execute(query)
            return result

        last_id = self.run_until_complete(test())
        self.assertTrue(last_id is not None)

    def test_update_query(self):
        # Sync create
        obj1 = TestModel.create(text='[sync] [test_update_obj]')
        self.assertEqual(obj1.text, '[sync] [test_update_obj]')

        # Sync update
        upd1 = (TestModel.update(text='[sync] [test_update_obj] [update]')
                         .where(TestModel.id == obj1.id).execute())
        self.assertEqual(upd1, 1)

        # Async update
        @asyncio.coroutine
        def test():
            query = (TestModel.update(text='[async] [test_update_obj] [update]')
                              .where(TestModel.id == obj1.id))
            with sync_unwanted(database):
                result = yield from execute(query)
            return result

        upd2 = self.run_until_complete(test())
        self.assertEqual(upd2, 1)
        self.assertEqual(TestModel.get(id=obj1.id).text,
                         '[async] [test_update_obj] [update]')

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

    def test_scalar_query(self):
        # Async scalar query
        @asyncio.coroutine
        def test():
            count1 = TestModel.select(peewee.fn.Count(TestModel.id)).scalar()
            with sync_unwanted(database):
                count2 = yield from scalar(TestModel.select(peewee.fn.Count(TestModel.id)))
            self.assertEqual(count1, count2)
            return True

        self.run_until_complete(test())

    def test_count_query(self):
        # Async count query
        @asyncio.coroutine
        def test():
            count0 = TestModel.select().count()
            TestModel.create(text='[sync] [test_count_query]')
            count1 = TestModel.select().count()

            with sync_unwanted(database):
                count2 = yield from count(TestModel.select())
                self.assertEqual(count2, count1)
                self.assertEqual(count2, count0 + 1)

                count3 = yield from count(TestModel.select().limit(1))
                self.assertEqual(count3, 1)
            
            return True

        self.run_until_complete(test())


if sys.version_info >= (3, 5):
    from .tests_py35 import *
