import pytest

from tests.conftest import dbs_all
from tests.models import TestModel


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
