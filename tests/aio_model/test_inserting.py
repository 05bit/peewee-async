import uuid

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, dbs_postgres
from tests.models import TestModel, UUIDTestModel
from tests.utils import model_has_fields


@dbs_all
async def test_insert_many(db: AioDatabase) -> None:
    last_id = await TestModel.insert_many(
        [
            {"text": "Test %s" % uuid.uuid4()},
            {"text": "Test %s" % uuid.uuid4()},
        ]
    ).aio_execute()

    res = await TestModel.select().aio_execute()

    assert len(res) == 2
    assert last_id in [m.id for m in res]


@dbs_all
async def test_insert__return_id(db: AioDatabase) -> None:
    last_id = await TestModel.insert(text="Test %s" % uuid.uuid4()).aio_execute()

    res = await TestModel.select().aio_execute()
    obj = res[0]
    assert last_id == obj.id


@dbs_postgres
async def test_insert_on_conflict_ignore__last_id_is_none(db: AioDatabase) -> None:
    query = TestModel.insert(text="text").on_conflict_ignore()
    await query.aio_execute()

    last_id = await query.aio_execute()

    assert last_id is None


@dbs_postgres
async def test_insert_on_conflict_ignore__return_model(db: AioDatabase) -> None:
    query = TestModel.insert(text="text", data="data").on_conflict_ignore().returning(TestModel)

    res = await query.aio_execute()

    inserted = res[0]
    res = await TestModel.select().aio_execute()
    expected = res[0]

    assert model_has_fields(inserted, {"id": expected.id, "text": expected.text, "data": expected.data}) is True


@dbs_postgres
async def test_insert_on_conflict_ignore__inserted_once(db: AioDatabase) -> None:
    query = TestModel.insert(text="text").on_conflict_ignore()
    last_id = await query.aio_execute()

    await query.aio_execute()

    res = await TestModel.select().aio_execute()
    assert len(res) == 1
    assert res[0].id == last_id


@dbs_postgres
async def test_insert__uuid_pk(db: AioDatabase) -> None:
    query = UUIDTestModel.insert(text="Test %s" % uuid.uuid4())
    last_id = await query.aio_execute()
    assert len(str(last_id)) == 36


@dbs_postgres
async def test_insert__return_model(db: AioDatabase) -> None:
    text = "Test %s" % uuid.uuid4()
    data = "data"
    query = TestModel.insert(text=text, data=data).returning(TestModel)

    res = await query.aio_execute()

    inserted = res[0]
    assert model_has_fields(inserted, {"id": inserted.id, "text": text, "data": data}) is True


@dbs_postgres
async def test_insert_many__return_model(db: AioDatabase) -> None:
    texts = [f"text{n}" for n in range(2)]
    query = TestModel.insert_many([{"text": text} for text in texts]).returning(TestModel)

    res = await query.aio_execute()

    texts = [m.text for m in res]
    assert sorted(texts) == ["text0", "text1"]
