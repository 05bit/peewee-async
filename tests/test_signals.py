import uuid

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all, dbs_postgres
from tests.models import TestSignalModel
from tests.utils import model_has_fields
from peewee_async.signals import aio_pre_save




@dbs_all
async def test_aio_pre_save(db: AioDatabase) -> None:
    
    @aio_pre_save(sender=TestSignalModel)
    async def on_save_handler(model_class, instance, created):
        print(model_class, instance, created)

    await TestSignalModel.aio_create(text="text")