import uuid

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, dbs_postgres
from tests.models import TestModel
from tests.utils import model_has_fields


@dbs_all
async def test_delete__count(db: AioDatabase) -> None:
    query = TestModel.insert_many([
        {'text': "Test %s" % uuid.uuid4()},
        {'text': "Test %s" % uuid.uuid4()},
    ])
    await query.aio_execute()

    count = await TestModel.delete().aio_execute()

    assert count == 2


@dbs_all
async def test_delete__by_condition(db: AioDatabase) -> None:
    expected_text = "text1"
    deleted_text = "text2"
    query = TestModel.insert_many([
        {'text': expected_text},
        {'text': deleted_text},
    ])
    await query.aio_execute()

    await TestModel.delete().where(TestModel.text == deleted_text).aio_execute()

    res = await TestModel.select().aio_execute()
    assert len(res) == 1
    assert res[0].text == expected_text


@dbs_postgres
async def test_delete__return_model(db: AioDatabase) -> None:
    m = await TestModel.aio_create(text="text", data="data")

    res = await TestModel.delete().returning(TestModel).aio_execute()
    assert model_has_fields(res[0], {
        "id": m.id,
        "text": m.text,
        "data": m.data
    }) is True
