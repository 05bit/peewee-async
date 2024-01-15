More examples
=============


Using both sync and async calls
-------------------------------

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    database = peewee_async.PostgresqlDatabase('test')
    loop = asyncio.get_event_loop()

    class TestModel(peewee_async.AioModel):
        text = peewee.CharField()

        class Meta:
            database = database

    # Create table synchronously!
    TestModel.create_table(True)
    # This is optional: close sync connection
    database.close()

    async def my_handler():
        obj1 = TestModel.create(text="Yo, I can do it sync!")
        obj2 = await TestModel.aio_create(text="Not bad. Watch this, I'm async!")

        all_objects = await TestModel.select().aio_execute()
        for obj in all_objects:
            print(obj.text)

        await TestModel.delete().aio_execute()

    loop.run_until_complete(database.connect_async(loop=loop))
    loop.run_until_complete(my_handler())


Using transactions
------------------

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    # ... some init code ...

    async def test():
        obj = await TestModel.aio_create(text='FOO')
        obj_id = obj.id

        try:
            async with database.atomic_async():
                await TestModel.update(text='BAR').where(TestModel.id == obj_id).aio_execute()
                raise Exception('Fake error')
        except:
            res = await TestModel.aio_get(TestModel.id == obj_id)

        print(res.text) # Should print 'FOO', not 'BAR'

    loop.run_until_complete(test())
