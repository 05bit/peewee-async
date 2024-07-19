import uuid

from tests.compat.conftest import manager_for_all_dbs, postgres_only
from tests.models import TestModel


@manager_for_all_dbs
async def test_update__count(manager):
    for n in range(3):
        await manager.create(TestModel, text=f"{n}")
    query = TestModel.update(data="new_data")

    count = await manager.execute(query)

    assert count == 3


@manager_for_all_dbs
async def test_update__field_updated(manager):
    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(TestModel, text=text)
    query = TestModel.update(text="Test update query").where(TestModel.id == obj1.id)

    await manager.execute(query)

    obj2 = await manager.get(TestModel, id=obj1.id)
    assert obj2.text == "Test update query"


@postgres_only
async def test_update__returning_model(manager):
    await manager.create(TestModel, text="text1", data="data")
    await manager.create(TestModel, text="text2", data="data")
    new_data = "New_data"
    query = TestModel.update(data=new_data).where(TestModel.data == "data").returning(TestModel)

    wrapper = await manager.execute(query)

    result = [m.data for m in wrapper]
    assert [new_data, new_data] == result
