import uuid

import peewee
from tests.conftest import manager_for_all_dbs
from tests.models import CompatTestModel


@manager_for_all_dbs
async def test_create_select_compat_mode(manager):
    obj1 = await manager.create(CompatTestModel, text="Test 1")
    obj2 = await manager.create(CompatTestModel, text="Test 2")
    query = CompatTestModel.select().order_by(CompatTestModel.text)
    assert isinstance(query, peewee.ModelSelect)
    result = await manager.execute(query)
    assert list(result) == [obj1, obj2]


@manager_for_all_dbs
async def test_compound_select_compat_mode(manager):
    obj1 = await manager.create(CompatTestModel, text="Test 1")
    obj2 = await manager.create(CompatTestModel, text="Test 2")
    query = (
        CompatTestModel.select().where(CompatTestModel.id == obj1.id) |
        CompatTestModel.select().where(CompatTestModel.id == obj2.id)
    )
    assert isinstance(query, peewee.ModelCompoundSelectQuery)
    result = await manager.execute(query)
    assert len(list(result)) == 2
    assert obj1 in list(result)
    assert obj2 in list(result)


@manager_for_all_dbs
async def test_raw_select_compat_mode(manager):
    obj1 = await manager.create(CompatTestModel, text="Test 1")
    obj2 = await manager.create(CompatTestModel, text="Test 2")
    query = CompatTestModel.raw(
        'SELECT id, text, data FROM compattestmodel m ORDER BY m.text'
    )
    assert isinstance(query, peewee.ModelRaw)
    result = await manager.execute(query)
    assert list(result) == [obj1, obj2]


@manager_for_all_dbs
async def test_update_compat_mode(manager):
    obj_draft = await manager.create(CompatTestModel, text="Draft 1")
    obj_draft.text = "Final result"
    await manager.update(obj_draft)
    obj = await manager.get(CompatTestModel, id=obj_draft.id)
    assert obj.text == "Final result"


@manager_for_all_dbs
async def test_count_compat_mode(manager):
    obj = await manager.create(CompatTestModel, text="Unique title %s" % uuid.uuid4())
    search = CompatTestModel.select().where(CompatTestModel.text == obj.text)
    count = await manager.count(search)
    assert count == 1


@manager_for_all_dbs
async def test_delete_compat_mode(manager):
    obj = await manager.create(CompatTestModel, text="Expired item %s" % uuid.uuid4())
    search = CompatTestModel.select().where(CompatTestModel.id == obj.id)
    count_before = await manager.count(search)
    assert count_before == 1
    await manager.delete(obj)
    count_after = await manager.count(search)
    assert count_after == 0
