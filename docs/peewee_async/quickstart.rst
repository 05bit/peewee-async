Quickstart
====================

Let's provide an example::

    import asyncio
    import peewee
    import logging
    from peewee_async import PooledPostgresqlDatabase

    database = PooledPostgresqlDatabase('test')

    # Disable sync queries
    database.set_allow_sync(False)

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