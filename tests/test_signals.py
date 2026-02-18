from collections.abc import Callable, Coroutine, Iterator
from contextlib import contextmanager
from typing import Any

from peewee_async.databases import AioDatabase
from peewee_async.signals import (
    AioModel,
    AioSignal,
    aio_post_delete,
    aio_post_save,
    aio_pre_delete,
    aio_pre_save,
    pre_init,
)
from tests.conftest import dbs_all
from tests.models import TestSignalModel


@contextmanager
def _connect(
    signal: AioSignal, receiver: Callable[..., Coroutine[Any, Any, Any]], sender: type[AioModel]
) -> Iterator[None]:
    signal.connect(receiver=receiver, sender=sender)
    yield
    signal.disconnect(receiver=receiver, sender=sender)


@dbs_all
async def test_aio_pre_save(db: AioDatabase) -> None:

    async def on_save_handler(model_class: type[TestSignalModel], instance: TestSignalModel, created: bool) -> None:
        assert await TestSignalModel.select().aio_exists() is False
        assert model_class is TestSignalModel
        assert isinstance(instance, TestSignalModel)
        assert created

    with _connect(aio_pre_save, receiver=on_save_handler, sender=TestSignalModel):
        await TestSignalModel.aio_create(text="aio_create")


@dbs_all
async def test_aio_post_save(db: AioDatabase) -> None:

    async def on_save_handler(model_class: type[TestSignalModel], instance: TestSignalModel, created: bool) -> None:
        assert await TestSignalModel.select().aio_exists() is True
        assert model_class is TestSignalModel
        assert isinstance(instance, TestSignalModel)
        assert created

    with _connect(aio_post_save, receiver=on_save_handler, sender=TestSignalModel):
        await TestSignalModel.aio_create(text="aio_create")


@dbs_all
async def test_aio_pre_delete(db: AioDatabase) -> None:

    t = await TestSignalModel.aio_create(text="aio_create")

    async def on_delete_handler(model_class: type[TestSignalModel], instance: TestSignalModel) -> None:
        assert await TestSignalModel.select().aio_exists() is True
        assert model_class is TestSignalModel
        assert isinstance(instance, TestSignalModel)

    with _connect(aio_pre_delete, receiver=on_delete_handler, sender=TestSignalModel):
        await t.aio_delete_instance()


@dbs_all
async def test_aio_post_delete(db: AioDatabase) -> None:

    t = await TestSignalModel.aio_create(text="aio_create")

    async def on_delete_handler(model_class: type[TestSignalModel], instance: TestSignalModel) -> None:
        assert await TestSignalModel.select().aio_exists() is False
        assert model_class is TestSignalModel
        assert isinstance(instance, TestSignalModel)

    with _connect(aio_post_delete, receiver=on_delete_handler, sender=TestSignalModel):
        await t.aio_delete_instance()


@dbs_all
def test_pre_init(db: AioDatabase) -> None:

    def on_init_handler(model_class: type[TestSignalModel], instance: TestSignalModel) -> None:
        assert model_class is TestSignalModel
        assert instance.text == "text"

    pre_init.connect(receiver=on_init_handler, sender=TestSignalModel)

    TestSignalModel(text="text")

    pre_init.disconnect(receiver=on_init_handler, sender=TestSignalModel)
