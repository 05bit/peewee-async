import asyncio
import uuid

import peewee
import pytest

import peewee_async
from tests.conftest import manager_for_all_dbs
from tests.db_config import DB_CLASSES, DB_DEFAULTS
from tests.models import UUIDTestModel, TestModelAlpha, CompositeTestModel, TestModel


@manager_for_all_dbs
async def test_composite_key(manager):
    obj_uuid = await manager.create(UUIDTestModel, text='UUID')
    obj_alpha = await manager.create(TestModelAlpha, text='Alpha')
    comp = await manager.create(CompositeTestModel, uuid=obj_uuid, alpha=obj_alpha)
    assert (obj_uuid, obj_alpha) == (comp.uuid, comp.alpha)


@manager_for_all_dbs
async def test_multiple_iterate_over_result(manager):

    obj1 = await manager.create(TestModel, text="Test 1")
    obj2 = await manager.create(TestModel, text="Test 2")

    result = await manager.execute(
        TestModel.select().order_by(TestModel.text))

    assert list(result) == [obj1, obj2]


@manager_for_all_dbs
async def test_indexing_result(manager):

    await manager.create(TestModel, text="Test 1")
    obj = await manager.create(TestModel, text="Test 2")

    result = await manager.execute(
        TestModel.select().order_by(TestModel.text)
    )
    assert obj == result[1]


@manager_for_all_dbs
async def test_select_many_objects(manager):
    text = "Test 1"
    obj1 = await manager.create(TestModel, text=text)
    text = "Test 2"
    obj2 = await manager.create(TestModel, text=text)

    select1 = [obj1, obj2]
    len1 = len(select1)

    select2 = await manager.execute(
        TestModel.select().order_by(TestModel.text))
    len2 = len([o for o in select2])

    assert len1 == len2
    for o1, o2 in zip(select1, select2):
        assert o1 == o2


@manager_for_all_dbs
async def test_raw_query(manager):

    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)

    result1 = await manager.execute(TestModel.raw(
        'select id, text from testmodel'))
    result1 = list(result1)
    assert len(result1) == 1
    assert isinstance(result1[0], TestModel) is True

    result2 = await manager.execute(TestModel.raw(
        'select id, text from testmodel').tuples())
    result2 = list(result2)
    assert len(result2) == 1
    assert isinstance(result2[0], tuple) is True

    result3 = await manager.execute(TestModel.raw(
        'select id, text from testmodel').dicts())
    result3 = list(result3)
    assert len(result3) == 1
    assert isinstance(result3[0], dict) is True


@manager_for_all_dbs
async def test_get_obj_by_id(manager):
    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(TestModel, text=text)
    obj2 = await manager.get(TestModel, id=obj1.id)

    assert obj1 == obj2
    assert obj1.id == obj2.id


@manager_for_all_dbs
async def test_get_obj_by_uuid(manager):

    text = "Test %s" % uuid.uuid4()
    obj1 = await manager.create(UUIDTestModel, text=text)
    obj2 = await manager.get(UUIDTestModel, id=obj1.id)
    assert obj1 == obj2
    assert len(str(obj1.id)) == 36


@manager_for_all_dbs
async def test_create_uuid_obj(manager):

    text = "Test %s" % uuid.uuid4()
    obj = await manager.create(UUIDTestModel, text=text)
    assert len(str(obj.id)) == 36


@manager_for_all_dbs
async def test_allow_sync_is_reverted_for_exc(manager):
    try:
        with manager.allow_sync():
            ununique_text = "ununique_text"
            await manager.create(TestModel, text=ununique_text)
            await manager.create(TestModel, text=ununique_text)
    except peewee.IntegrityError:
        pass
    assert manager.database._allow_sync is False


@manager_for_all_dbs
async def test_many_requests(manager):

    max_connections = getattr(manager.database, 'max_connections', 1)
    text = "Test %s" % uuid.uuid4()
    obj = await manager.create(TestModel, text=text)
    n = 2 * max_connections  # number of requests
    done, not_done = await asyncio.wait(
        {asyncio.create_task(manager.get(TestModel, id=obj.id)) for _ in range(n)}
    )
    assert len(done) == n


@manager_for_all_dbs
async def test_connect_close(manager):

    async def get_conn(manager):
        await manager.connect()
        # await asyncio.sleep(0.05, loop=self.loop)
        # NOTE: "private" member access
        return manager.database.pool_backend


    c1 = await get_conn(manager)
    c2 = await get_conn(manager)
    assert c1 == c2

    assert manager.is_connected is True

    await manager.close()

    assert manager.is_connected is False

    done, not_done = await asyncio.wait({asyncio.create_task(get_conn(manager)) for _ in range(3)})

    conn = next(iter(done)).result()
    assert len(done) == 3
    assert manager.is_connected is True
    assert all(map(lambda t: t.result() == conn, done)) is True

    await manager.close()
    assert manager.is_connected is False


@pytest.mark.parametrize(
    "params, db_cls",
    [
        (DB_DEFAULTS[name], db_cls) for name, db_cls in DB_CLASSES.items()
    ]

)
async def test_deferred_init(params, db_cls):

    database = db_cls(None)
    assert database.deferred is True

    database.init(**params)
    assert database.deferred is False

    TestModel._meta.database = database
    TestModel.create_table(True)
    TestModel.drop_table(True)


@pytest.mark.parametrize(
    "params, db_cls",
    [
        (DB_DEFAULTS[name], db_cls) for name, db_cls in DB_CLASSES.items()
    ]
)
async def test_proxy_database(params, db_cls):
    database = peewee.Proxy()
    TestModel._meta.database = database
    manager = peewee_async.Manager(database)

    database.initialize(db_cls(**params))

    TestModel.create_table(True)

    text = "Test %s" % uuid.uuid4()
    await manager.create(TestModel, text=text)
    await manager.get(TestModel, text=text)
    TestModel.drop_table(True)
