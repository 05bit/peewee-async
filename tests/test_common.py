import asyncio
import uuid
from typing import Any, Dict, Type

import peewee
import pytest

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import TestModel, CompositeTestModel


@dbs_all
async def test_composite_key(db: AioDatabase) -> None:
    task_id = 5
    product_type = "boots"
    comp = await CompositeTestModel.aio_create(task_id=task_id, product_type=product_type)
    assert comp.get_id() == (task_id, product_type)


@dbs_all
async def test_multiple_iterate_over_result(db: AioDatabase) -> None:

    obj1 = await TestModel.aio_create(text="Test 1")
    obj2 = await TestModel.aio_create(text="Test 2")

    result = await TestModel.select().order_by(TestModel.text).aio_execute()

    assert list(result) == [obj1, obj2]


@dbs_all
async def test_indexing_result(db: AioDatabase) -> None:

    await TestModel.aio_create(text="Test 1")
    obj = await TestModel.aio_create(text="Test 2")

    result = await TestModel.select().order_by(TestModel.text).aio_execute()
    assert obj == result[1]


@pytest.mark.parametrize(
    "params, db_cls",
    [
        (DB_DEFAULTS[name], db_cls) for name, db_cls in DB_CLASSES.items()
    ]
)
async def test_proxy_database(params: Dict[str, Any], db_cls: Type[AioDatabase]) -> None:
    database = peewee.Proxy()
    TestModel._meta.database = database

    database.initialize(db_cls(**params))

    TestModel.create_table(True)

    text = "Test %s" % uuid.uuid4()
    await TestModel.aio_create(text=text)
    await TestModel.aio_get(text=text)
    TestModel.drop_table(True)


@dbs_all
async def test_many_requests(db: AioDatabase) -> None:

    max_connections = getattr(dbs_all, 'max_connections', 1)
    text = "Test %s" % uuid.uuid4()
    obj = await TestModel.aio_create(text=text)
    n = 2 * max_connections  # number of requests
    done, not_done = await asyncio.wait(
        {asyncio.create_task(TestModel.aio_get(id=obj.id)) for _ in range(n)}
    )
    assert len(done) == n


@dbs_all
async def test_allow_sync(db: AioDatabase) -> None:
    with db.allow_sync():
        TestModel.create(text="text")
    assert await TestModel.aio_get_or_none(text="text") is not None
    assert db.is_closed() is True


@dbs_all
async def test_allow_sync_is_reverted_for_exc(db: AioDatabase) -> None:
    try:
        with db.allow_sync():
            ununique_text = "ununique_text"
            TestModel.create(text=ununique_text)
            TestModel.create(text=ununique_text)
    except peewee.IntegrityError:
        pass
    assert db._allow_sync is False
