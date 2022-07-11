import uuid

from tests.conftest import postgres_only, all_dbs
from tests.models import TestModel
from tests.utils import model_has_fields


@all_dbs
async def test_delete__count(manager):
    query = TestModel.insert_many([
        {'text': "Test %s" % uuid.uuid4()},
        {'text': "Test %s" % uuid.uuid4()},
    ])
    await manager.execute(query)

    count = await manager.execute(TestModel.delete())

    assert count == 2


@all_dbs
async def test_delete__by_condition(manager):
    expected_text = "text1"
    deleted_text = "text2"
    query = TestModel.insert_many([
        {'text': expected_text},
        {'text': deleted_text},
    ])
    await manager.execute(query)

    await manager.execute(TestModel.delete().where(TestModel.text == deleted_text))

    res = await manager.execute(TestModel.select())
    assert len(res) == 1
    assert res[0].text == expected_text


@postgres_only
async def test_delete__return_model(manager):
    m = await manager.create(TestModel, text="text", data="data")

    res = await manager.execute(TestModel.delete().returning(TestModel))
    assert model_has_fields(res[0], {
        "id": m.id,
        "text": m.text,
        "data": m.data
    }) is True
