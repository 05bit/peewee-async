import uuid

from tests.conftest import postgres_only, all_dbs
from tests.models import TestModel


@all_dbs
async def test_update__count(manager):
    for n in range(3):
        await TestModel.aio_create(text=f"{n}")
    count = await TestModel.update(data="new_data").aio_execute()

    assert count == 3


@all_dbs
async def test_update__field_updated(manager):
    text = "Test %s" % uuid.uuid4()
    obj1 = await TestModel.aio_create(text=text)
    await TestModel.update(text="Test update query").where(TestModel.id == obj1.id).aio_execute()

    obj2 = await TestModel.aio_get(id=obj1.id)
    assert obj2.text == "Test update query"


@postgres_only
async def test_update__returning_model(manager):
    await TestModel.aio_create(text="text1", data="data")
    await TestModel.aio_create(text="text2", data="data")
    new_data = "New_data"
    wrapper = await TestModel.update(data=new_data).where(TestModel.data == "data").returning(TestModel).aio_execute()

    result = [m.data for m in wrapper]
    assert [new_data, new_data] == result
