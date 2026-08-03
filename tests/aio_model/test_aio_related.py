import pytest
from pytest_mock import MockFixture

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all
from tests.models import TestFkNullModel, TestModelAlpha, TestModelBeta

pytestmark = pytest.mark.use_transaction


@dbs_all
async def test_aio_fk_not_cached(db: AioDatabase, mocker: MockFixture) -> None:
    alpha = await TestModelAlpha.aio_create(text="Alpha")
    beta = await TestModelBeta.aio_create(alpha_id=alpha.id, text="Beta")

    spy_aio_get = mocker.spy(TestModelAlpha, "aio_get")
    loaded = await beta.aio_fk("alpha")
    assert loaded.id == alpha.id
    assert loaded.text == alpha.text
    spy_aio_get.assert_called_once()

    # access to sync interface should use __rel__ cache, no extra query
    spy_get = mocker.spy(TestModelAlpha, "get")
    assert beta.alpha.id == alpha.id
    spy_get.assert_not_called()


@dbs_all
async def test_aio_fk_via_field(db: AioDatabase, mocker: MockFixture) -> None:
    alpha = await TestModelAlpha.aio_create(text="Alpha")
    beta = await TestModelBeta.aio_create(alpha_id=alpha.id, text="Beta")

    loaded = await beta.aio_fk(TestModelBeta.alpha)
    assert loaded.id == alpha.id
    assert loaded.text == alpha.text


@dbs_all
async def test_aio_fk_error(db: AioDatabase) -> None:
    alpha = await TestModelAlpha.aio_create(text="Alpha")

    with pytest.raises(ValueError):
        await alpha.aio_fk(TestModelAlpha.text)


@dbs_all
async def test_aio_fk_cached(db: AioDatabase, mocker: MockFixture) -> None:
    alpha = await TestModelAlpha.aio_create(text="Alpha")
    beta = await TestModelBeta.aio_create(alpha=alpha, text="Beta")

    spy_aio_get = mocker.spy(TestModelAlpha, "aio_get")
    loaded = await beta.aio_fk("alpha")
    assert loaded.id == alpha.id
    spy_aio_get.assert_not_called()


@dbs_all
async def test_aio_fk_cached_via_join(db: AioDatabase, mocker: MockFixture) -> None:
    alpha = await TestModelAlpha.aio_create(text="Joined")
    await TestModelBeta.aio_create(alpha=alpha, text="Beta")

    result = (await TestModelBeta.select(TestModelBeta, TestModelAlpha).join(TestModelAlpha).aio_execute())[0]

    spy_get = mocker.spy(TestModelAlpha, "get")
    loaded = await result.aio_fk("alpha")
    assert loaded.id == alpha.id
    spy_get.assert_not_called()


@dbs_all
async def test_aio_fk_null(db: AioDatabase, mocker: MockFixture) -> None:
    beta = await TestFkNullModel.aio_create()

    loaded = await beta.aio_fk("alpha")
    assert loaded is None
    assert beta.alpha is None


@dbs_all
async def test_aio_fk_lazy_load(db: AioDatabase, mocker: MockFixture) -> None:
    alpha = await TestModelAlpha.aio_create(text="Alpha")
    beta = await TestModelBeta.aio_create(alpha_id=alpha.id, text="Beta")

    TestModelBeta.alpha.lazy_load = False
    with pytest.raises(ValueError):
        await beta.aio_fk(TestModelBeta.alpha)
