import peewee
from tests.conftest import all_dbs
from tests.models import TestModel, TestModelAlpha, TestModelBeta


@all_dbs
async def test_select_w_join(manager):
    alpha = await TestModelAlpha.aio_create(text="Test 1")
    beta = await TestModelBeta.aio_create(alpha_id=alpha.id, text="text")

    result = (await TestModelBeta.select(TestModelBeta, TestModelAlpha).join(
        TestModelAlpha,
        attr="joined_alpha",
    ).aio_execute())[0]

    assert result.id == beta.id
    assert result.joined_alpha.id == alpha.id


@all_dbs
async def test_select_compound(manager):
    obj1 = await manager.create(TestModel, text="Test 1")
    obj2 = await manager.create(TestModel, text="Test 2")
    query = (
        TestModel.select().where(TestModel.id == obj1.id) |
        TestModel.select().where(TestModel.id == obj2.id)
    )
    assert isinstance(query, peewee.ModelCompoundSelectQuery)
    # NOTE: Two `AioModelSelect` when joining via `|` produce `ModelCompoundSelectQuery`
    # without `aio_execute()` method, so using `database.aio_execute()` here.
    result = await manager.database.aio_execute(query)
    assert len(list(result)) == 2
    assert obj1 in list(result)
    assert obj2 in list(result)
