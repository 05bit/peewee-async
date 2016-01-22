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



class AsyncPostgresTestCase(unittest.TestCase):
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
        self.assertTrue(result)
        return result

    #
    # Test methods
    #
    def test_atomic_manager(self):
        async def test():
            with sync_unwanted(database):
                obj = await create_object(TestModel, text='FOO')
                obj_id = obj.id
                async with database.async_atomic():
                    obj.text = "foo"
                    result = await update_object(obj)
                res = await get_object(TestModel, TestModel.id == obj_id)
                self.assertEqual(result, 1)
                self.assertEqual(res.text, "foo")

                try:
                    async with database.async_atomic():
                        res.text = "BAR"
                        await update_object(obj)
                        raise Exception("Don't try to save it!")
                except Exception as e:
                    res = await get_object(TestModel, TestModel.id == obj_id)
                    self.assertEqual(res.text, "foo")

            return True

        self.run_until_complete(test())


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
