import uuid

from tests.conftest import postgres_only, all_dbs
from tests.models import TestModel


@all_dbs
async def test_count(manager):
    for n in range(3):
        await manager.create(TestModel, text=f"{n}")

    query = TestModel.update(data="new_data")

    count = await manager.execute(query)
    assert count == 3


@all_dbs
async def test_updated(manager):
    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(TestModel, text=text)

    query = TestModel.update(text="Test update query").where(TestModel.id == obj1.id)

    await manager.execute(query)

    obj2 = await manager.get(TestModel, id=obj1.id)
    assert obj2.text == "Test update query"


@postgres_only
async def test_update_returning_query(manager):
    await manager.create(TestModel, text="text1", data="data")
    await manager.create(TestModel, text="text2", data="data")

    new_data = "New_data"
    query = TestModel.update(data=new_data).where(TestModel.data == "data").returning(TestModel)

    wrapper = await manager.execute(query)
    result = [m.data for m in wrapper]
    assert [new_data, new_data] == result
