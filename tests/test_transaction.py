from tests.conftest import dbs_all
from tests.models import TestModel
from peewee_async import Transaction
import asyncio


@dbs_all
async def test_savepoint_success(db):

    async with db.aio_atomic():
        await TestModel.aio_create(text='FOO')

        async with db.aio_atomic():
            await TestModel.update(text="BAR").aio_execute()

    assert await TestModel.aio_get_or_none(text="BAR") is not None
    assert db.aio_pool.has_acquired_connections() is False


@dbs_all
async def test_transaction_success(db):
    async with db.aio_atomic():
        await TestModel.aio_create(text='FOO')

    assert TestModel.aio_get_or_none(text="FOO") is not None
    assert db.aio_pool.has_acquired_connections() is False


@dbs_all
async def test_savepoint_rollback(db):
    await TestModel.aio_create(text='FOO', data="")

    async with db.aio_atomic():
        await TestModel.update(data="BAR").aio_execute()

        try:
            async with db.aio_atomic():
                await TestModel.aio_create(text='FOO')
        except:
            pass

    assert TestModel.aio_get_or_none(data="BAR") is not None
    assert db.aio_pool.has_acquired_connections() is False


@dbs_all
async def test_transaction_rollback(db):
    await TestModel.aio_create(text='FOO', data="")

    try:
        async with db.aio_atomic():
            await TestModel.update(data="BAR").aio_execute()
            assert await TestModel.aio_get_or_none(data="BAR") is not None
            await TestModel.aio_create(text='FOO')
    except:
        pass

    assert await TestModel.aio_get_or_none(data="BAR") is None
    assert db.aio_pool.has_acquired_connections() is False


@dbs_all
async def test_several_transactions(db):
    """Run several transactions in parallel tasks.
    """

    async def t1():
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO1', data="")

    async def t2():
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO2', data="")
            try:
                async with db.aio_atomic():
                    await TestModel.aio_create(text='FOO2', data="not_created")
            except:
                pass

    async def t3():
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO3', data="")
            async with db.aio_atomic():
                await TestModel.update(data="BAR").where(TestModel.text == 'FOO3').aio_execute()

    await asyncio.gather(t1(), t2(), t3())

    assert await TestModel.aio_get_or_none(text="FOO1") is not None
    assert await TestModel.aio_get_or_none(text="FOO2", data="") is not None
    assert await TestModel.aio_get_or_none(text="FOO3", data="BAR") is not None
    assert db.aio_pool.has_acquired_connections() is False


@dbs_all
async def test_transaction_manual_work(db):
    async with db.aio_connection() as connection:
        tr = Transaction(connection)
        await tr.begin()
        await TestModel.aio_create(text='FOO')
        assert TestModel.aio_get_or_none(text="FOO") is not None
        try:
            await TestModel.aio_create(text='FOO')
        except:
            await tr.rollback()
        else:
            await tr.commit()

    assert await TestModel.aio_get_or_none(text="FOO") is None
    assert db.aio_pool.has_acquired_connections() is False


@dbs_all
async def test_savepoint_manual_work(db):
    async with db.aio_connection() as connection:
        tr = Transaction(connection)
        await tr.begin()
        await TestModel.aio_create(text='FOO')
        assert TestModel.aio_get_or_none(text="FOO") is not None

        savepoint = Transaction(connection, is_savepoint=True)
        await savepoint.begin()
        try:
            await TestModel.aio_create(text='FOO')
        except:
            await savepoint.rollback()
        else:
            await savepoint.commit()
        await tr.commit()

    assert await TestModel.aio_get_or_none(text="FOO") is not None
    assert db.aio_pool.has_acquired_connections() is False
