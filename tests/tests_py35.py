"""
peewee-async tests for Python 3.5+
==================================

These tests should be run only for Python 3.5+, older versions
will just fail with `SyntaxError` while importing this module.

"""
import asyncio
from . import TestModel
from . import BaseManagerTestCase


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
        run = lambda c: self.loop.run_until_complete(c)

        wait = lambda tasks: run(asyncio.wait([self.loop.create_task(t)
            for t in tasks], loop=self.loop))

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

        for k, objects in self.managers.items():
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
