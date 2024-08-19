import uuid

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, dbs_postgres
from tests.models import TestModel


@dbs_all
async def test_update__count(db: AioDatabase) -> None:
    for n in range(3):
        await TestModel.aio_create(text=f"{n}")
    count = await TestModel.update(data="new_data").aio_execute()

    assert count == 3


@dbs_all
async def test_update__field_updated(db: AioDatabase) -> None:
    text = "Test %s" % uuid.uuid4()
    obj1 = await TestModel.aio_create(text=text)
    await TestModel.update(text="Test update query").where(TestModel.id == obj1.id).aio_execute()

    obj2 = await TestModel.aio_get(id=obj1.id)
    assert obj2.text == "Test update query"


@dbs_postgres
async def test_update__returning_model(db: AioDatabase) -> None:
    await TestModel.aio_create(text="text1", data="data")
    await TestModel.aio_create(text="text2", data="data")
    new_data = "New_data"
    wrapper = await TestModel.update(data=new_data).where(TestModel.data == "data").returning(TestModel).aio_execute()

    result = [m.data for m in wrapper]
    assert [new_data, new_data] == result
