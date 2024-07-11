import uuid

import peewee
import pytest

import peewee_async
from tests.conftest import manager_for_all_dbs
from tests.models import CompatTestModel
from tests.models import TestModelAlpha, TestModelBeta, TestModelGamma, TestModel


@manager_for_all_dbs
async def test_get_or_none(manager):
    """Test get_or_none manager function."""
    text1 = "Test %s" % uuid.uuid4()
    text2 = "Test %s" % uuid.uuid4()

    obj1 = await manager.create(CompatTestModel, text=text1)
    obj2 = await manager.get_or_none(CompatTestModel, text=text1)
    obj3 = await manager.get_or_none(CompatTestModel, text=text2)

    assert obj1 == obj2
    assert obj1 is not None
    assert obj2 is not None
    assert obj3 is None


@manager_for_all_dbs
async def test_count_query_with_limit(manager):
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)

    count = await manager.count(CompatTestModel.select().limit(1))
    assert count == 1


@manager_for_all_dbs
async def test_count_query(manager):
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)

    count = await manager.count(CompatTestModel.select())
    assert count == 3


@manager_for_all_dbs
async def test_scalar_query(manager):

    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)
    text = "Test %s" % uuid.uuid4()
    await manager.create(CompatTestModel, text=text)

    fn = peewee.fn.Count(CompatTestModel.id)
    count = await manager.scalar(CompatTestModel.select(fn))

    assert count == 2


@manager_for_all_dbs
async def test_create_obj(manager):

    text = "Test %s" % uuid.uuid4()
    obj = await manager.create(CompatTestModel, text=text)
    assert obj is not None
    assert obj.text == text


@manager_for_all_dbs
async def test_delete_obj(manager):
    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(CompatTestModel, text=text)
    obj2 = await manager.get(CompatTestModel, id=obj1.id)

    await manager.delete(obj2)

    obj3 = await manager.get_or_none(CompatTestModel, id=obj1.id)
    assert obj3 is None


@manager_for_all_dbs
@pytest.mark.parametrize(
    "prefetch_type",
    peewee.PREFETCH_TYPE.values()
)
async def test_prefetch(manager, prefetch_type):
    alpha_1 = await manager.create(
        TestModelAlpha, text='Alpha 1')
    alpha_2 = await manager.create(
        TestModelAlpha, text='Alpha 2')

    beta_11 = await manager.create(
        TestModelBeta, alpha=alpha_1, text='Beta 11')
    beta_12 = await manager.create(
        TestModelBeta, alpha=alpha_1, text='Beta 12')
    _ = await manager.create(
        TestModelBeta, alpha=alpha_2, text='Beta 21')
    _ = await manager.create(
        TestModelBeta, alpha=alpha_2, text='Beta 22')

    gamma_111 = await manager.create(
        TestModelGamma, beta=beta_11, text='Gamma 111')
    gamma_112 = await manager.create(
        TestModelGamma, beta=beta_11, text='Gamma 112')

    result = await peewee_async.prefetch(
        TestModelAlpha.select().order_by(TestModelAlpha.id),
        TestModelBeta.select().order_by(TestModelBeta.id),
        TestModelGamma.select().order_by(TestModelGamma.id),
        prefetch_type=prefetch_type,
    )
    assert tuple(result) == (alpha_1, alpha_2)
    assert tuple(result[0].betas) == (beta_11, beta_12)
    assert tuple(result[0].betas[0].gammas) == (gamma_111, gamma_112)


@manager_for_all_dbs
async def test_update_obj(manager):

    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(TestModel, text=text)

    obj1.text = "Test update object"
    await manager.update(obj1)

    obj2 = await manager.get(TestModel, id=obj1.id)
    assert obj2.text == "Test update object"


@manager_for_all_dbs
async def test_create_or_get(manager):
    text = "Test %s" % uuid.uuid4()
    obj1, created1 = await manager.create_or_get(
        TestModel, text=text, data="Data 1")
    obj2, created2 = await manager.create_or_get(
        TestModel, text=text, data="Data 2")

    assert created1 is True
    assert created2 is False
    assert obj1 == obj2
    assert obj1.data == "Data 1"
    assert obj2.data == "Data 1"


@manager_for_all_dbs
async def test_get_or_create(manager):

    text = "Test %s" % uuid.uuid4()

    obj1, created1 = await manager.get_or_create(
        TestModel, text=text, defaults={'data': "Data 1"})
    obj2, created2 = await manager.get_or_create(
        TestModel, text=text, defaults={'data': "Data 2"})

    assert created1 is True
    assert created2 is False
    assert obj1 == obj2
    assert obj1.data == "Data 1"
    assert obj2.data == "Data 1"
