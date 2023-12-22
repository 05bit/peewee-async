import uuid

from tests.conftest import all_dbs
from tests.models import TestModel, UUIDTestModel, TestModelAlpha, CompositeTestModel


@all_dbs
async def test_get_or_none(manager):
    """Test get_or_none manager function."""
    text1 = "Test %s" % uuid.uuid4()
    text2 = "Test %s" % uuid.uuid4()

    obj1 = await manager.create(TestModel, text=text1)
    obj2 = await manager.get_or_none(TestModel, text=text1)
    obj3 = await manager.get_or_none(TestModel, text=text2)

    assert obj1 == obj2
    assert obj1 is not None
    assert obj2 is not None
    assert obj3 is None

@all_dbs
async def test_composite_key(manager):
    obj_uuid = await manager.create(UUIDTestModel, text='UUID')
    obj_alpha = await manager.create(TestModelAlpha, text='Alpha')
    comp = await manager.create(CompositeTestModel, uuid=obj_uuid, alpha=obj_alpha)
    assert (obj_uuid, obj_alpha) == (comp.uuid, comp.alpha)


@all_dbs
async def test_count_query_with_limit(manager):
    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)

    count = await manager.count(TestModel.select().limit(1))
    assert count ==  1

@all_dbs
async def test_count_query(manager):
    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)

    count = await manager.count(TestModel.select())
    assert count == 3
