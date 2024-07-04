from peewee_async.aio_model import AioModelCompoundSelectQuery, AioModelRaw
from tests.conftest import dbs_all
from tests.models import TestModel, TestModelAlpha, TestModelBeta


@dbs_all
async def test_select_w_join(db):
    alpha = await TestModelAlpha.aio_create(text="Test 1")
    beta = await TestModelBeta.aio_create(alpha_id=alpha.id, text="text")

    result = (await TestModelBeta.select(TestModelBeta, TestModelAlpha).join(
        TestModelAlpha,
        attr="joined_alpha",
    ).aio_execute())[0]

    assert result.id == beta.id
    assert result.joined_alpha.id == alpha.id


@dbs_all
async def test_raw_select(db):
    obj1 = await TestModel.aio_create(text="Test 1")
    obj2 = await TestModel.aio_create(text="Test 2")
    query = TestModel.raw(
        'SELECT id, text, data FROM testmodel m ORDER BY m.text'
    )
    assert isinstance(query, AioModelRaw)
    result = await query.aio_execute()
    assert list(result) == [obj1, obj2]


@dbs_all
async def test_union_all(db):
    obj1 = await TestModel.aio_create(text="1")
    obj2 = await TestModel.aio_create(text="2")
    query = (
        TestModel.select().where(TestModel.id == obj1.id) +
        TestModel.select().where(TestModel.id == obj2.id) +
        TestModel.select().where(TestModel.id == obj2.id)
    )
    result = await query.aio_execute()
    assert sorted(r.text for r in result) == ["1", "2", "2"]


@dbs_all
async def test_union(db):
    obj1 = await TestModel.aio_create(text="1")
    obj2 = await TestModel.aio_create(text="2")
    query = (
        TestModel.select().where(TestModel.id == obj1.id) |
        TestModel.select().where(TestModel.id == obj2.id) |
        TestModel.select().where(TestModel.id == obj2.id)
    )
    assert isinstance(query, AioModelCompoundSelectQuery)
    result = await query.aio_execute()
    assert sorted(r.text for r in result) == ["1", "2"]


@dbs_all
async def test_intersect(db):
    await TestModel.aio_create(text="1")
    await TestModel.aio_create(text="2")
    await TestModel.aio_create(text="3")
    query = (
        TestModel.select().where(
            (TestModel.text == "1") | (TestModel.text == "2")
        ) &
        TestModel.select().where(
            (TestModel.text == "2") | (TestModel.text == "3")
        )
    )
    result = await query.aio_execute()
    assert sorted(r.text for r in result) == ["2"]


@dbs_all
async def test_except(db):
    await TestModel.aio_create(text="1")
    await TestModel.aio_create(text="2")
    await TestModel.aio_create(text="3")
    query = (
        TestModel.select().where(
            (TestModel.text == "1") | (TestModel.text == "2") | (TestModel.text == "3")
        ) -
        TestModel.select().where(
            (TestModel.text == "2")
        )
    )
    result = await query.aio_execute()
    assert sorted(r.text for r in result) == ["1", "3"]