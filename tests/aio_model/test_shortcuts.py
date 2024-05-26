import pytest
from peewee import fn

from tests.conftest import dbs_all
from tests.models import TestModel, IntegerTestModel


@dbs_all
async def test_aio_get(db):
    obj1 = await TestModel.aio_create(text="Test 1")
    obj2 = await TestModel.aio_create(text="Test 2")

    result = await TestModel.aio_get(TestModel.id == obj1.id)
    assert result.id == obj1.id

    result = await TestModel.aio_get(TestModel.text == "Test 2")
    assert result.id == obj2.id

    with pytest.raises(TestModel.DoesNotExist):
        await TestModel.aio_get(TestModel.text == "unknown")


@dbs_all
async def test_aio_get_or_none(db):
    obj1 = await TestModel.aio_create(text="Test 1")

    result = await TestModel.aio_get_or_none(TestModel.id == obj1.id)
    assert result.id == obj1.id

    result = await TestModel.aio_get_or_none(TestModel.text == "unknown")
    assert result is None


@dbs_all
async def test_aio_scalar(db):
    await IntegerTestModel.aio_create(num=1)
    await IntegerTestModel.aio_create(num=2)

    assert await IntegerTestModel.select(fn.MAX(IntegerTestModel.num)).aio_scalar() == 2

    assert await IntegerTestModel.select(
        fn.MAX(IntegerTestModel.num),fn.Min(IntegerTestModel.num)
    ).aio_scalar(as_tuple=True) == (2, 1)

    assert await TestModel.select().aio_scalar() is None
