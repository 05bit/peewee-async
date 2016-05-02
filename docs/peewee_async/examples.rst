More examples
=============

**TODO:** update examples to high-level API.

Using both sync and async calls
-------------------------------

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    database = peewee_async.PostgresqlDatabase('test')
    loop = asyncio.get_event_loop()

    class TestModel(peewee.Model):
        text = peewee.CharField()

        class Meta:
            database = database

    # Create table synchronously!
    TestModel.create_table(True)
    # This is optional: close sync connection
    database.close()

    @asyncio.coroutine
    def my_handler():
        obj1 = TestModel.create(text="Yo, I can do it sync!")
        obj2 = yield from peewee_async.create_object(TestModel, text="Not bad. Watch this, I'm async!")

        all_objects = yield from peewee_async.execute(TestModel.select())
        for obj in all_objects:
            print(obj.text)

        obj1.delete_instance()
        yield from peewee_async.delete_object(obj2)

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
        obj = await create_object(TestModel, text='FOO')
        obj_id = obj.id

        try:
            async with database.atomic_async():
                obj.text = 'BAR'
                await update_object(obj)
                raise Exception('Fake error')
        except:
            res = await get_object(TestModel, TestModel.id == obj_id)

        print(res.text) # Should print 'FOO', not 'BAR'

    loop.run_until_complete(test())
