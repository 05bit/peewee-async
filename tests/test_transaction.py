import asyncio

from tests.conftest import all_dbs
from tests.models import TestModel


class FakeUpdateError(Exception):
    """Fake error while updating database.
    """
    pass


@all_dbs
async def test_atomic_success(manager):
    obj = await manager.create(TestModel, text='FOO')
    obj_id = obj.id

    async with manager.atomic():
        obj.text = 'BAR'
        await manager.update(obj)

    res = await manager.get(TestModel, id=obj_id)
    assert res.text == 'BAR'


@all_dbs
async def test_atomic_fail_with_disconnect(manager):
    """Database gone in transaction.
    """

    error = False
    try:
        async with manager.atomic():
            await manager.database.close_async()
            raise FakeUpdateError()
    except FakeUpdateError:
        error = True

    assert error is True


@all_dbs
async def test_atomic_failed(manager):
    """Failed update in transaction.
    """

    obj = await manager.create(TestModel, text='FOO')
    obj_id = obj.id

    try:
        async with manager.atomic():
            obj.text = 'BAR'
            await manager.update(obj)
            raise FakeUpdateError()
    except FakeUpdateError as e:
        error = True
        res = await manager.get(TestModel, id=obj_id)

    assert error is True
    assert res.text == 'FOO'


@all_dbs
async def test_several_transactions(manager):
    """Run several transactions in parallel tasks.
    """

    async def t1():
        async with manager.atomic():
            assert manager.database.transaction_depth_async() == 1
            await asyncio.sleep(0.25)

    async def t2():
        async with manager.atomic():
            assert manager.database.transaction_depth_async() == 1
            await asyncio.sleep(0.0625)

    async def t3():
        async with manager.atomic():
            assert manager.database.transaction_depth_async() == 1
            await asyncio.sleep(0.125)

    await asyncio.gather(t1(), t2(), t3())
