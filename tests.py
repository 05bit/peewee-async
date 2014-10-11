import asyncio
import configparser
import sys
import unittest
import peewee

# Testing module
import aiopeewee

# Shortcuts
create = aiopeewee.create
update = aiopeewee.update
select = aiopeewee.select
insert = aiopeewee.insert
delete = aiopeewee.delete
delete_instance = aiopeewee.delete_instance
save = aiopeewee.save

# Configure tests
ini_config = configparser.ConfigParser()
ini_config.read(['tests.ini'])

try:
    config = dict(**ini_config['tests'])
except KeyError:
    config = {}

config.setdefault('db', 'test')

if 'pool_size' in config:
    max_connections = int(config['pool_size'])
    database = aiopeewee.PooledPostgresqlDatabase(config['db'], max_connections=max_connections)
else:
    database = aiopeewee.PostgresqlDatabase(config['db'])


#
# Tests
#

class TestModel(peewee.Model):
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
        def do():
            yield from database.connect_async(loop=cls.loop)
        cls.loop.run_until_complete(do())

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

        # Close database
        database.close()

    def run_until_complete(self, coroutine):
        @asyncio.coroutine
        def do():
            result = yield from coroutine
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


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
