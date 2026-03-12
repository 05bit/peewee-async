import uuid

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, dbs_mysql, dbs_postgres
from tests.models import TestModel, UUIDTestModel
from tests.utils import model_has_fields

# pytestmark = pytest.mark.use_transaction


@dbs_postgres
async def test_insert_many__pg(db: AioDatabase) -> None:
    text1 = f"Test {uuid.uuid4()}"
    text2 = f"Test {uuid.uuid4()}"
    result = await TestModel.insert_many(
        [
            {"id": 1, "text": text1},
            {"id": 2, "text": text2},
        ]
    ).aio_execute()

    assert sorted(result) == [(1,), (2,)]

    assert await TestModel.aio_get_or_none(id=1, text=text1) is not None
    assert await TestModel.aio_get_or_none(id=2, text=text2) is not None


@dbs_mysql
async def test_insert_many__mysql(db: AioDatabase) -> None:
    text1 = f"Test {uuid.uuid4()}"
    text2 = f"Test {uuid.uuid4()}"
    result = await TestModel.insert_many(
        [
            {"id": 1, "text": text1},
            {"id": 2, "text": text2},
        ]
    ).aio_execute()

    assert result in [1, 2]
    assert await TestModel.aio_get_or_none(id=1, text=text1) is not None
    assert await TestModel.aio_get_or_none(id=2, text=text2) is not None


@dbs_postgres
async def test_insert_many__return_model(db: AioDatabase) -> None:
    texts = [f"text{n}" for n in range(2)]
    query = TestModel.insert_many([{"text": text} for text in texts]).returning(TestModel)

    res = await query.aio_execute()

    texts = [m.text for m in res]
    assert sorted(texts) == ["text0", "text1"]


@dbs_all
async def test_insert__as_row_count(db: AioDatabase) -> None:
    result = (
        await TestModel.insert_many(
            [
                {"id": 1, "text": "text1"},
                {"id": 2, "text": "text2"},
            ]
        )
        .as_rowcount()
        .aio_execute()
    )

    assert result == 2


@dbs_all
async def test_insert__return_id(db: AioDatabase) -> None:
    last_id = await TestModel.insert(text=f"Test {uuid.uuid4()}").aio_execute()

    res = await TestModel.select().aio_execute()
    obj = res[0]
    assert last_id == obj.id


@dbs_postgres
async def test_insert__return_model(db: AioDatabase) -> None:
    text = f"Test {uuid.uuid4()}"
    data = "data"
    query = TestModel.insert(text=text, data=data).returning(TestModel)

    res = await query.aio_execute()

    inserted = res[0]
    assert model_has_fields(inserted, {"id": inserted.id, "text": text, "data": data}) is True


@dbs_postgres
async def test_insert__uuid_pk(db: AioDatabase) -> None:
    uid = "f85d03b2-001c-4da6-92c5-c0c925af0f70"
    query = UUIDTestModel.insert(id=uid, text=f"Test {uuid.uuid4()}")
    last_id = await query.aio_execute()
    assert str(last_id) == uid


@dbs_postgres
async def test_insert_on_conflict_ignore__last_id_is_none(db: AioDatabase) -> None:
    await TestModel.aio_create(id=5, text="text")
    last_id = await TestModel.insert(id=5, text="text").on_conflict_ignore().aio_execute()
    assert last_id is None


@dbs_postgres
async def test_insert_on_conflict_ignore__return_model(db: AioDatabase) -> None:
    query = TestModel.insert(text="text", data="data").on_conflict_ignore().returning(TestModel)

    res = await query.aio_execute()

    inserted = res[0]
    res = await TestModel.select().aio_execute()
    expected = res[0]

    assert model_has_fields(inserted, {"id": expected.id, "text": expected.text, "data": expected.data}) is True
