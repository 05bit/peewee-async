Testing
=======

For testing purposes, the ``TransactionTestCase`` class can be used.
Its behavior is similar to the eponymous class in Django: it starts a
transaction at the beginning of a test and rolls it back at the end.
As a result, no changes are persisted after the test finishes.

Example usage:

.. code-block:: python

    import pytest
    from peewee_async.testing import TransactionTestCase
    from typing import AsyncGenerator


    @pytest.fixture
    async def clear_tables() -> AsyncGenerator[None, None]:
        yield
        for model in all_your_models:
            await model.delete().aio_execute()


    @pytest.fixture(autouse=True)
    async def in_transaction(
        request: pytest.FixtureRequest,
    ) -> AsyncGenerator[None, None]:
        # In some cases, TransactionTestCase cannot be used,
        # so we fall back to the clear_tables fixture.
        if "clear_tables" in request.fixturenames:
            yield
        else:
            async with TransactionTestCase(database):
                yield


    async def test_model_created() -> None:
        # This test runs inside a transaction and is rolled back on exit.
        # No records will remain in the TestModel table after the test.
        await TestModel.aio_create(text="Test 1")

        assert await TestModel.aio_exists()


    async def test_model_sync_created(clear_tables: None) -> None:
        # TransactionTestCase cannot be used with synchronous queries,
        # so we use the clear_tables fixture instead.
        TestModel.create(text="Test 1")

        assert TestModel.exists()


Implementation details
----------------------

Internally, the context manager works as follows:

1. A dedicated global connection is created.
2. The ``aio_connection`` attribute is patched so that any query executed uses only this connection.
3. The ``aio_atomic`` and ``aio_transaction`` methods are patched to raise
   an error if they are used. This prevents nested transaction usage in tests.
4. A transaction is started.
5. The code inside the context manager is executed.
6. The transaction is rolled back, the connection is released, and all patches
   are reverted.


Caveats
-------

``TransactionTestCase`` should not be used in the following situations:

1. When synchronous queries are used.
2. If an error occurs in the database during test execution, such as a constraint violation.
3. If the ``aio_atomic`` or ``aio_transaction`` methods are used — this will raise a ``ValueError``.
4. If a transaction is started by any means other than the ``aio_atomic`` or ``aio_transaction`` methods.