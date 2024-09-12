import asyncio
import peewee
import peewee_async

# Nothing special, just define model and database:

database = peewee_async.PooledPostgresqlExtDatabase('test')


class TestModel(peewee_async.AioModel):
    text = peewee.CharField()

    class Meta:
        database = database


# Look, sync code is working!

TestModel.create_table(True)
TestModel.create(text="Yo, I can do it sync!")
database.close()

# No need for sync anymore!

database.set_allow_sync(False)


async def handler():
    await TestModel.aio_create(text="Not bad. Watch this, I'm async!")
    all_objects = await TestModel.select().aio_execute()
    for obj in all_objects:
        print(obj.text)


loop = asyncio.get_event_loop()
loop.run_until_complete(handler())
loop.close()

# Clean up, can do it sync again:
with database.allow_sync():
    TestModel.drop_table(True)
