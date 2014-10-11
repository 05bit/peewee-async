Usage examples
==============

Using both sync and async calls
-------------------------------

.. code-block:: python

    import asyncio
    import peewee
    import aiopeewee

    database = aiopeewee.PostgresqlDatabase('test')
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
        obj2 = yield from aiopeewee.create_object(TestModel, text="Not bad. Watch this, I'm async!")

        all_objects = yield from aiopeewee.execute(TestModel.select())
        for obj in all_objects:
            print(obj.text)

        obj1.delete_instance()
        yield from aiopeewee.delete_object(obj2)

    loop.run_until_complete(database.connect_async(loop=loop))
    loop.run_until_complete(my_handler())