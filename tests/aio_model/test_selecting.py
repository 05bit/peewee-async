import uuid

import pytest

from tests.conftest import all_dbs, postgres_only
from tests.models import TestModel, TestModelAlpha, TestModelBeta


@all_dbs
async def test__select__w_join(manager):
    alpha = await TestModelAlpha.aio_create(text="Test 1")
    beta = await TestModelBeta.aio_create(alpha_id=alpha.id, text="text")

    result = (await TestModelBeta.select(TestModelBeta, TestModelAlpha).join(
        TestModelAlpha,
        attr="joined_alpha",
    ).aio_execute())[0]

    assert result.id == beta.id
    assert result.joined_alpha.id == alpha.id