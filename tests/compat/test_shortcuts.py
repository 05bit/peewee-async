import uuid

import peewee

from tests.conftest import manager_for_all_dbs
from tests.models import CompatTestModel


@manager_for_all_dbs
async def test_get_or_none(manager):
    """Test get_or_none manager function."""
    text1 = "Test %s" % uuid.uuid4()
    text2 = "Test %s" % uuid.uuid4()

    obj1 = await manager.create(CompatTestModel, text=text1)
    obj2 = await manager.get_or_none(CompatTestModel, text=text1)
    obj3 = await manager.get_or_none(CompatTestModel, text=text2)

    assert obj1 == obj2
    assert obj1 is not None
    assert obj2 is not None
    assert obj3 is None


@manager_for_all_dbs
async def test_count_query_with_limit(manager):
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)

    count = await manager.count(CompatTestModel.select().limit(1))
    assert count == 1


@manager_for_all_dbs
async def test_count_query(manager):
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)

    count = await manager.count(CompatTestModel.select())
    assert count == 3


@manager_for_all_dbs
async def test_scalar_query(manager):

    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)

    fn = peewee.fn.Count(CompatTestModel.id)
    count = await manager.scalar(CompatTestModel.select(fn))

    assert count == 2


@manager_for_all_dbs
async def test_create_obj(manager):

    text = "Test %s" % uuid.uuid4()
    obj = await manager.create(CompatTestModel, text=text)
    assert obj is not None
    assert obj.text == text


@manager_for_all_dbs
async def test_delete_obj(manager):
    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(CompatTestModel, text=text)
    obj2 = await manager.get(CompatTestModel, id=obj1.id)

    await manager.delete(obj2)

    obj3 = await manager.get_or_none(CompatTestModel, id=obj1.id)
    assert obj3 is None
