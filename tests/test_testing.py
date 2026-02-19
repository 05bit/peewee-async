import pytest

from peewee_async.databases import AioDatabase
from peewee_async.testing import TransactionTestCase
from tests.conftest import dbs_all, transaction_methods
from tests.models import TestModel


@transaction_methods
@dbs_all
async def test_transcations_disabled(db: AioDatabase, transaction_method: str) -> None:
    async with TransactionTestCase(db):
        with pytest.raises(ValueError):
            async with getattr(db, transaction_method)():
                pass
    async with getattr(db, transaction_method)():
        # not raised
        pass


@dbs_all
async def test_integration(db: AioDatabase) -> None:

    async with TransactionTestCase(db):
        await TestModel.aio_create(text="Test 1")
        assert await TestModel.select().aio_exists()

    assert not await TestModel.select().aio_exists()
