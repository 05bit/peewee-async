import uuid

import peewee
import pytest
from peewee import fn

from tests.conftest import dbs_all
from tests.models import TestModel, IntegerTestModel, TestModelAlpha, TestModelBeta


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


@dbs_all
async def test_count_query(db):

    for num in range(5):
        await IntegerTestModel.aio_create(num=num)
    count = await IntegerTestModel.select().limit(3).aio_count()
    assert count == 3


@dbs_all
async def test_count_query_clear_limit(db):

    for num in range(5):
        await IntegerTestModel.aio_create(num=num)
    count = await IntegerTestModel.select().limit(3).aio_count(clear_limit=True)
    assert count == 5


@dbs_all
async def test_aio_delete_instance(db):
    text = "Test %s" % uuid.uuid4()
    obj1 = await TestModel.aio_create(text=text)
    obj2 = await TestModel.aio_get(id=obj1.id)

    await obj2.aio_delete_instance()

    obj3 = await TestModel.aio_get_or_none(id=obj1.id)
    assert obj3 is None


@dbs_all
async def test_aio_delete_instance_with_fk(db):
    alpha = await TestModelAlpha.aio_create(text="test")
    beta = await TestModelBeta.aio_create(alpha=alpha, text="test")

    await alpha.aio_delete_instance(recursive=True)

    assert await TestModelAlpha.aio_get_or_none(id=alpha.id) is None
    assert await TestModelBeta.aio_get_or_none(id=beta.id) is None


@dbs_all
async def test_aio_save(db):
    t = TestModel(text="text", data="data")
    rows = await t.aio_save()
    assert rows == 1
    assert t.id is not None

    assert await TestModel.aio_get_or_none(text="text", data="data") is not None


@dbs_all
async def test_aio_save__force_insert(db):
    t = await TestModel.aio_create(text="text", data="data")
    t.data = "data2"
    await t.aio_save()

    assert await TestModel.aio_get_or_none(text="text", data="data2") is not None

    with pytest.raises(peewee.IntegrityError):
        await t.aio_save(force_insert=True)


@dbs_all
async def test_aio_get_or_create__get(db):
    t1 = await TestModel.aio_create(text="text", data="data")
    t2, created = await TestModel.aio_get_or_create(text="text")
    assert t1.id == t2.id
    assert created is False


@dbs_all
async def test_aio_get_or_create__created(db):
    t2, created = await TestModel.aio_get_or_create(text="text")
    assert t2.text == "text"
    assert created is True
