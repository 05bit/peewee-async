import peewee
import pytest

from tests.conftest import all_dbs
from tests.models import TestModelAlpha, TestModelBeta, TestModelGamma


@all_dbs
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

    result = await manager.prefetch(
        TestModelAlpha.select().order_by(TestModelAlpha.id),
        TestModelBeta.select().order_by(TestModelBeta.id),
        TestModelGamma.select().order_by(TestModelGamma.id),
        prefetch_type=prefetch_type,
    )
    assert tuple(result) == (alpha_1, alpha_2)
    assert tuple(result[0].betas) == (beta_11, beta_12)
    assert tuple(result[0].betas[0].gammas) == (gamma_111, gamma_112)
