import asyncio

import pytest
from peewee import IntegrityError
from pytest_mock import MockerFixture

from peewee_async import Transaction
from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all
from tests.models import TestModel


class FakeConnectionError(Exception):
    pass


@dbs_all
async def test_transaction_error_on_begin(db: AioDatabase, mocker: MockerFixture, enable_debug_log_level) -> None:
    mocker.patch.object(Transaction, "begin", side_effect=FakeConnectionError)
    with pytest.raises(FakeConnectionError):
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO')
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_transaction_error_on_commit(db: AioDatabase, mocker: MockerFixture) -> None:
    mocker.patch.object(Transaction, "commit", side_effect=FakeConnectionError)
    with pytest.raises(FakeConnectionError):
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO')
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_transaction_error_on_rollback(db: AioDatabase, mocker: MockerFixture) -> None:
    await TestModel.aio_create(text='FOO', data="")
    mocker.patch.object(Transaction, "rollback", side_effect=FakeConnectionError)
    with pytest.raises(FakeConnectionError):
        async with db.aio_atomic():
            await TestModel.update(data="BAR").aio_execute()
            assert await TestModel.aio_get_or_none(data="BAR") is not None
            await TestModel.aio_create(text='FOO')

    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_transaction_success(db: AioDatabase) -> None:
    async with db.aio_atomic():
        await TestModel.aio_create(text='FOO')

    assert await TestModel.aio_get_or_none(text="FOO") is not None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_transaction_rollback(db: AioDatabase) -> None:
    await TestModel.aio_create(text='FOO', data="")

    with pytest.raises(IntegrityError):
        async with db.aio_atomic():
            await TestModel.update(data="BAR").aio_execute()
            assert await TestModel.aio_get_or_none(data="BAR") is not None
            await TestModel.aio_create(text='FOO')

    assert await TestModel.aio_get_or_none(data="BAR") is None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_several_transactions(db: AioDatabase) -> None:
    """Run several transactions in parallel tasks.
    """

    async def t1() -> None:
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO1', data="")

    async def t2() -> None:
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO2', data="")
            with pytest.raises(IntegrityError):
                async with db.aio_atomic():
                    await TestModel.aio_create(text='FOO2', data="not_created")

    async def t3() -> None:
        async with db.aio_atomic():
            await TestModel.aio_create(text='FOO3', data="")
            async with db.aio_atomic():
                await TestModel.update(data="BAR").where(TestModel.text == 'FOO3').aio_execute()

    await asyncio.gather(t1(), t2(), t3())

    assert await TestModel.aio_get_or_none(text="FOO1") is not None
    assert await TestModel.aio_get_or_none(text="FOO2", data="") is not None
    assert await TestModel.aio_get_or_none(text="FOO3", data="BAR") is not None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_transaction_manual_work(db: AioDatabase) -> None:
    async with db.aio_connection() as connection:
        tr = Transaction(connection)
        await tr.begin()
        await TestModel.aio_create(text='FOO')
        assert await TestModel.aio_get_or_none(text="FOO") is not None
        try:
            await TestModel.aio_create(text='FOO')
        except:
            await tr.rollback()
        else:
            await tr.commit()

    assert await TestModel.aio_get_or_none(text="FOO") is None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_savepoint_success(db: AioDatabase) -> None:
    async with db.aio_atomic():
        await TestModel.aio_create(text='FOO')

        async with db.aio_atomic():
            await TestModel.update(text="BAR").aio_execute()

    assert await TestModel.aio_get_or_none(text="BAR") is not None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_savepoint_rollback(db: AioDatabase) -> None:
    await TestModel.aio_create(text='FOO', data="")

    async with db.aio_atomic():
        await TestModel.update(data="BAR").aio_execute()

        with pytest.raises(IntegrityError):
            async with db.aio_atomic():
                await TestModel.aio_create(text='FOO')

    assert await TestModel.aio_get_or_none(data="BAR") is not None
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_savepoint_manual_work(db: AioDatabase) -> None:
    async with db.aio_connection() as connection:
        tr = Transaction(connection)
        await tr.begin()
        await TestModel.aio_create(text='FOO')
        assert await TestModel.aio_get_or_none(text="FOO") is not None

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
    assert db.pool_backend.has_acquired_connections() is False


@dbs_all
async def test_acid_when_connetion_has_been_broken(db: AioDatabase) -> None:
    async def restart_connections(event_for_lock: asyncio.Event) -> None:
        event_for_lock.set()
        await asyncio.sleep(0.05)

        # Using an event, we force tasks to wait until a certain coroutine
        # This is necessary to reproduce a case when connections reopened during transaction

        # Somebody decides to close all connections and open again
        event_for_lock.clear()

        await db.aio_close()
        await db.aio_connect()

        event_for_lock.set()
        return None

    async def insert_records(event_for_wait: asyncio.Event) -> None:
        await event_for_wait.wait()
        async with db.aio_atomic():
            # BEGIN
            # INSERT 1
            await TestModel.aio_create(text="1")

            await asyncio.sleep(0.05)
            # wait for db close all connections and open again
            await event_for_wait.wait()

            # This row should not be inserted because the connection of the current transaction has been closed
            # # INSERT 2
            await TestModel.aio_create(text="2")

        return None

    event = asyncio.Event()

    await asyncio.gather(
        restart_connections(event),
        insert_records(event),
        return_exceptions=True,
    )

    # The transaction has not been committed
    assert len(list(await TestModel.select().aio_execute())) in (0, 2)
    assert db.pool_backend.has_acquired_connections() is False

