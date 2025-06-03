Quickstart
====================

Let's provide an example::

    import asyncio
    import peewee
    import logging
    from peewee_async import PooledPostgresqlDatabase

    database = PooledPostgresqlDatabase('test')


    # Let's define a simple model:
    class PageBlock(peewee_async.AioModel):
        key = peewee.CharField(max_length=40, unique=True)
        text = peewee.TextField(default='')

        class Meta:
            database = database

-- as you can see, nothing special in this code, just plain ``peewee_async.AioModel`` definition and disabling sync queries.

Now we need to create a table for model::

    with database.allow_sync():
       PageBlock.create_table(True)

-- this code is **sync**, and will do **absolutely the same thing** as would do code with regular ``peewee.PostgresqlDatabase``. This is intentional, I believe there's just no need to run database initialization code asynchronously! *Less code, less errors*.

Finally, let's do something async::

    async def my_async_func():
        # Add new page block
        await PageBlock.aio_create(
            key='title',
            text="Peewee is AWESOME with async!"
        )

        # Get one by key
        title = await PageBlock.aio_get(key='title')
        print("Was:", title.text)

        # Save with new text using manager
        title.text = "Peewee is SUPER awesome with async!"
        await title.aio_save()
        print("New:", title.text)

    loop.run_until_complete(my_async_func())
    loop.close()

**That's it!** As you may notice there's no need to connect and re-connect before executing async queries! It's all automatic. But you can run ``AioDatabase.aio_connect()`` or ``AioDatabase.aio_close()`` when you need it.

And you can use methods from from **AioModel** for operations like selecting, deleting etc.
All of them you can find in the next section.


Using sync calls
+++++++++++++++++++++++++++++++

If you may notice in the example above if you need to run sync query you can use :py:meth:`~peewee_async.databases.AioDatabase.allow_sync` context manager:

.. code-block:: python

    with database.allow_sync():
       PageBlock.create_table(True)

Be careful when using such queries. It is not recommended to use them in an asynchronous application for the following reasons:

1. For each such query, a new connection to the database is open and closed upon its completion. Which is very expensive in terms of resources.
2. If such a query is executed for a long time, then the application will not be able to execute other coroutines until the query is completed

Synchronous queries should be used in tests or single-threaded tasks.