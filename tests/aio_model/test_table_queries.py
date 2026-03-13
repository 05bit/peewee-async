import peewee as pw

from peewee_async.aio_model import AioModel
from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, dbs_postgres
from tests.models import TestModel


@dbs_all
async def test_aio_table_exists(db: AioDatabase) -> None:
    assert await TestModel.aio_table_exists() is True

    class NonExistedModel(AioModel):
        class Meta:
            database = db

    assert await NonExistedModel.aio_table_exists() is False


@dbs_all
async def test_aio_create_table__safe(db: AioDatabase) -> None:

    await TestModel.aio_create_table(safe=True)
    assert await TestModel.aio_table_exists() is True


@dbs_all
async def test_aio_create_table(db: AioDatabase) -> None:

    class SomeModel(AioModel):
        text = pw.CharField(index=True)

        class Meta:
            database = db

    await SomeModel.aio_create_table()
    assert await SomeModel.aio_table_exists() is True

    with db.allow_sync():
        assert 'somemodel_text' in [i.name for i in db.get_indexes("somemodel")]

    await SomeModel.aio_drop_table()
    assert await SomeModel.aio_table_exists() is False



@dbs_postgres
async def test_aio_create_table__sequences(db: AioDatabase) -> None:

    class SomeModel(AioModel):
        text = pw.CharField(index=True)

        class Meta:
            database = db

    await SomeModel.aio_create_table()
    assert await db.aio_sequence_exists("somemodel_id_seq") is True


    await SomeModel.aio_drop_table()
    assert await db.aio_sequence_exists("somemodel_id_seq") is False


@dbs_all
async def test_aio_truncate_table(db: AioDatabase) -> None:

    await TestModel.aio_create(text="text")
    assert await TestModel.select().aio_exists() is True

    await TestModel.aio_truncate_table()
    assert await TestModel.select().aio_exists() is False
