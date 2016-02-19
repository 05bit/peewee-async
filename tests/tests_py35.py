"""
peewee-async tests for Python 3.5+
==================================

These tests should be run only for Python 3.5+, older versions
will just fail with `SyntaxError` while importing this module.

"""
import asyncio
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

                async with database.atomic_async():
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
                    async with database.atomic_async():
                        obj.text = 'BAR'
                        await update_object(obj)
                        raise FakeUpdateError()
                except FakeUpdateError as e:
                    update_error = True
                    res = await get_object(TestModel, TestModel.id == obj_id)
                
                self.assertTrue(update_error)
                self.assertEqual(res.text, 'FOO')

        self.run_until_complete(test())

    def test_several_transactions(self):
        """Run several transactions in parallel tasks.
        """
        async def t1():
            async with database.atomic_async():
                self.assertEqual(database.transaction_depth(), 1)
                await asyncio.sleep(0.5)

        async def t2():
            async with database.atomic_async():
                self.assertEqual(database.transaction_depth(), 1)
                await asyncio.sleep(1.0)

        async def t3():
            async with database.atomic_async():
                self.assertEqual(database.transaction_depth(), 1)
                await asyncio.sleep(1.5)

        self.run_until_complete(asyncio.wait([
            self.loop.create_task(t1()),
            self.loop.create_task(t2()),
            self.loop.create_task(t3()),
        ]))
