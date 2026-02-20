from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import TracebackType
from unittest import mock

from peewee_async import Transaction
from peewee_async.databases import AioDatabase
from peewee_async.utils import ConnectionProtocol


class TransactionTestCase:
    """
    Asynchronous context manager for simplifying ORM testing.

    Resetting the database to a known state upon exit.

    This class:

        - Wraps the enclosed code block in a single transaction.
        - Uses one shared connection for all queries within the context.
        - Disables usage of ``aio_atomic`` and ``aio_transaction`` methods
          inside the managed block.

    Example::

        async with TransactionTestCase(database):
            await TestModel.aio_create(text="Test 1")
            assert await TestModel.select().aio_exists()

        assert not await TestModel.select().aio_exists()

    """

    def __init__(self, database: AioDatabase) -> None:
        self.database = database

    @asynccontextmanager
    async def _disable_transactions(self) -> AsyncIterator[None]:
        @asynccontextmanager
        async def patched__aio_atomic(use_savepoint: bool = False) -> AsyncIterator[None]:
            raise ValueError("Using transactions 'aio_atomic' and 'aio_transcation' is disabled.")
            yield

        with mock.patch.object(self.database, "_aio_atomic", patched__aio_atomic):
            yield

    @asynccontextmanager
    async def _make_global_connection(self) -> AsyncIterator[ConnectionProtocol]:
        global_connection = await self.database.pool_backend.acquire()

        @asynccontextmanager
        async def patched_aio_connection() -> AsyncIterator[ConnectionProtocol]:
            yield global_connection

        with mock.patch.object(self.database, "aio_connection", patched_aio_connection):
            try:
                yield global_connection
            finally:
                await self.database.pool_backend.release(global_connection)

    @asynccontextmanager
    async def _run_in_single_transaction(self, connection: ConnectionProtocol) -> AsyncIterator[None]:
        trx = Transaction(connection, is_savepoint=False)
        await trx.begin()
        try:
            yield
        finally:
            await trx.rollback()

    @asynccontextmanager
    async def _patch(self) -> AsyncIterator[None]:
        async with (
            self._disable_transactions(),
            self._make_global_connection() as global_connection,
            self._run_in_single_transaction(global_connection),
        ):
            yield

    async def __aenter__(self) -> None:
        self._ctx = self._patch()
        await self._ctx.__aenter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        await self._ctx.__aexit__(exc_type, exc_val, exc_tb)
