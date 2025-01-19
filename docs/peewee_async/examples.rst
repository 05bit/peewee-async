Examples
=============


Using both sync and async calls
+++++++++++++++++++++++++++++++

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

    async with database.allow_sync():
        # Create table synchronously!
        TestModel.create_table(True)
        # sync connection is closed automatically on exit

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
++++++++++++++++++

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    # ... some init code ...

    async def test():
        obj = await TestModel.aio_create(text='FOO')
        obj_id = obj.id

        try:
            async with database.aio_atomic():
                await TestModel.update(text='BAR').where(TestModel.id == obj_id).aio_execute()
                raise Exception('Fake error')
        except:
            res = await TestModel.aio_get(TestModel.id == obj_id)

        print(res.text) # Should print 'FOO', not 'BAR'

    loop.run_until_complete(test())

Using async peewee with Tornado
+++++++++++++++++++++++++++++++

`Tornado`_ is a mature and powerful asynchronous web framework. It provides its own event loop, but there's an option to run Tornado on asyncio event loop. And that's exactly what we need!

.. _Tornado: http://www.tornadoweb.org

The complete working example is provided below. And here are some general notes:

1. **Be aware of current asyncio event loop!**

  In the provided example we use the default event loop everywhere, and that's OK. But if you see your application got silently stuck, that's most probably that some task is started on the different loop and will never complete as long as that loop is not running.

2. Tornado request handlers **does not** start asyncio tasks by default.

  The ``CreateHandler`` demostrates that, ``current_task()`` returns ``None`` until task is run explicitly.

3. Transactions **must** run within task context.

  All transaction operations have to be done within task. So if you need to run a transaction from Tornado handler, you have to wrap your call into task with ``create_task()`` or ``ensure_future()``.

  **Also note:** if you spawn an extra task during a transaction, it will run outside of that transaction.

.. literalinclude:: ../samples/tornado_sample.py
  :start-after: # Start example