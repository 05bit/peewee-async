import uuid

from tests.conftest import postgres_only, all_dbs
from tests.models import TestModel, UUIDTestModel
from tests.utils import model_has_fields


@all_dbs
async def test_insert_many(manager):
    query = TestModel.insert_many([
        {'text': "Test %s" % uuid.uuid4()},
        {'text': "Test %s" % uuid.uuid4()},
    ])

    last_id = await manager.execute(query)

    TestModel.get()
    res = await manager.execute(TestModel.select())
    assert len(res) == 2
    assert last_id in [m.id for m in res]


@all_dbs
async def test_insert__return_id(manager):
    query = TestModel.insert(text="Test %s" % uuid.uuid4())

    last_id = await manager.execute(query)

    res = await manager.execute(TestModel.select())
    obj = res[0]
    assert last_id == obj.id


@postgres_only
async def test_insert_on_conflict_ignore__last_id_is_none(manager):
    query = TestModel.insert(text="text").on_conflict_ignore()
    await manager.execute(query)

    last_id = await manager.execute(query)

    assert last_id is None


@postgres_only
async def test_insert_on_conflict_ignore__return_model(manager):
    query = TestModel.insert(text="text", data="data").on_conflict_ignore().returning(TestModel)

    res = await manager.execute(query)

    inserted = res[0]
    res = await manager.execute(TestModel.select())
    expected = res[0]

    assert model_has_fields(inserted, {
        "id": expected.id,
        "text": expected.text,
        "data": expected.data
    }) is True


@postgres_only
async def test_insert_on_conflict_ignore__inserted_once(manager):
    query = TestModel.insert(text="text").on_conflict_ignore()
    last_id = await manager.execute(query)

    await manager.execute(query)

    res = await manager.execute(TestModel.select())
    assert len(res) == 1
    assert res[0].id == last_id


@postgres_only
async def test_insert__uuid_pk(manager):
    query = UUIDTestModel.insert(text="Test %s" % uuid.uuid4())
    last_id = await manager.execute(query)
    assert len(str(last_id)) == 36


@postgres_only
async def test_insert__return_model(manager):
    text = "Test %s" % uuid.uuid4()
    data = "data"
    query = TestModel.insert(text=text, data=data).returning(TestModel)

    res = await manager.execute(query)

    inserted = res[0]
    assert model_has_fields(
        inserted, {"id": inserted.id, "text": text, "data": data}
    ) is True


@postgres_only
async def test_insert_many__return_model(manager):
    texts = [f"text{n}" for n in range(2)]
    query = TestModel.insert_many([
        {"text": text} for text in texts
    ]).returning(TestModel)

    res = await manager.execute(query)

    texts = [m.text for m in res]
    assert sorted(texts) == ["text0", "text1"]
