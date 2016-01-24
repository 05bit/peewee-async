"""
peewee-async tests for Python 3.5+
==================================

These tests should be run only for Python 3.5+, older versions
will just fail with `SyntaxError` while importing this module.

"""
import peewee_async
from . import database,\
    BaseAsyncPostgresTestCase,\
    TestModel,\
    UUIDTestModel

# Shortcuts
execute = peewee_async.execute
count = peewee_async.count
scalar = peewee_async.scalar
get_object = peewee_async.get_object
create_object = peewee_async.create_object
delete_object = peewee_async.delete_object
update_object = peewee_async.update_object
sync_unwanted = peewee_async.sync_unwanted


class FakeUpdateError(Exception):
    """Fake error while updating database.
    """
    pass


class AsyncPostgresTransactionsTestCase(BaseAsyncPostgresTestCase):
    def test_atomic_success(self):
        """Successful update in transaction.
        """
        async def test():
            with sync_unwanted(database):
                obj = await create_object(TestModel, text='FOO')
                obj_id = obj.id

                async with database.async_atomic():
                    obj.text = 'BAR'
                    await update_object(obj)

                res = await get_object(TestModel, TestModel.id == obj_id)
                self.assertEqual(res.text, 'BAR')

        self.run_until_complete(test())

    def test_atomic_failed(self):
        """Failed update in transaction.
        """
        async def test():
            with sync_unwanted(database):
                obj = await create_object(TestModel, text='FOO')
                obj_id = obj.id

                try:
                    async with database.async_atomic():
                        obj.text = 'BAR'
                        await update_object(obj)
                        raise FakeUpdateError()
                except FakeUpdateError as e:
                    update_error = True
                    res = await get_object(TestModel, TestModel.id == obj_id)
                
                self.assertTrue(update_error)
                self.assertEqual(res.text, 'FOO')

        self.run_until_complete(test())
