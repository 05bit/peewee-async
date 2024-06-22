import uuid

import peewee
import pytest

import peewee_async
from tests.conftest import manager_for_all_dbs
from tests.models import TestModel, TestModelAlpha, TestModelBeta, TestModelGamma


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


