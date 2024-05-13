import uuid

from tests.conftest import postgres_only, manager_for_all_dbs
from tests.models import TestModel
from tests.utils import model_has_fields


@manager_for_all_dbs
async def test_delete__count(manager):
    query = TestModel.insert_many([
        {'text': "Test %s" % uuid.uuid4()},
        {'text': "Test %s" % uuid.uuid4()},
    ])
    await query.aio_execute()

    count = await TestModel.delete().aio_execute()

    assert count == 2


@manager_for_all_dbs
async def test_delete__by_condition(manager):
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


@postgres_only
async def test_delete__return_model(manager):
    m = await TestModel.aio_create(text="text", data="data")

    res = await TestModel.delete().returning(TestModel).aio_execute()
    assert model_has_fields(res[0], {
        "id": m.id,
        "text": m.text,
        "data": m.data
    }) is True
