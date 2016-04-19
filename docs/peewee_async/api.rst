High-level (new) API
====================

High-level API provides a single point for all async ORM calls. Meet the :class:`.Manager` class! The idea of ``Manager`` originally comes from `Django`_, but it's redesigned to meet new `asyncio`_ patterns.

First of all, once ``Manager`` is initialized with database and event loop, it's easy and safe to perform async calls. And all async operations and transactions management methods are bundled with a single object. No need to pass around database instance, event loop, etc.

Also there's no need to connect and re-connect before executing async queries with manager! It's all automatic. But you can run ``Manager.connect()`` or ``Manager.close()`` when you need it.

.. _peewee: https://github.com/coleifer/peewee
.. _Django: https://www.djangoproject.com
.. _asyncio: https://docs.python.org/3/library/asyncio.html

**Note:** code examples below are written for Python 3.5.x, it is possible to adapt them for Python 3.4.x by replacing `await` with `yield from` and `async def` with `@asyncio.coroutine` decorator. And async context managers like ``transaction()`` etc, are only possible in Python 3.5+

OK, let's provide an example::

    import asyncio
    import peewee
    import logging
    from peewee_async import Manager, PostgresqlDatabase

    loop = asyncio.new_event_loop() # Note: custom loop!
    database = PostgresqlDatabase('test')
    objects = Manager(database, loop=loop)

-- once ``objects`` is created with specified ``loop``, all database connections **automatically** will be set up on **that loop**. Sometimes, it's so easy to forget to pass custom loop instance, but now it's not a problem! Just initialize with an event loop once.

Let's define a simple model::

    class PageBlock(peewee.Model):
        key = peewee.CharField(max_length=40, unique=True)
        text = peewee.TextField(default='')

        class Meta:
            database = database

-- as you can see, nothing special in this code, just plain ``peewee.Model`` definition.

Now we need to create a table for model::

    PageBlock.create_table(True)

-- this code is **sync**, and will do **absolutely the same thing** as would do code with regular ``peewee.PostgresqlDatabase``. This is intentional, I believe there's just no need to run database initialization code asynchronously! *Less code, less errors*.

From now we may want **only async** calls and treat sync as unwanted or as errors::

    objects.database.allow_sync = False # this will raise AssertionError on ANY sync call

-- alternatevely we can set ``ERROR`` or ``WARNING`` loggin level to ``database.allow_sync``::

    objects.database.allow_sync = logging.ERROR

Finally, let's do something async::

    async def my_async_func():
        # Add new page block
        await objects.create_or_get(
            PageBlock, key='title',
            text="Peewee is AWESOME with async!")

        # Get one by key
        title = await objects.get(PageBlock, key='title')
        print("Was:", title.text)

        # Save with new text
        title.text = "Peewee is SUPER awesome with async!"
        await objects.update(title)
        print("New:", title.text)

    loop.run_until_complete(my_async_func())
    loop.close()

**That's it!**

Other methods for operations like selecting, deleting etc. are listed below.

Manager
-------

.. autoclass:: peewee_async.Manager

.. autoattribute:: peewee_async.Manager.database

.. automethod:: peewee_async.Manager.allow_sync

.. automethod:: peewee_async.Manager.get

.. automethod:: peewee_async.Manager.create

.. automethod:: peewee_async.Manager.update

.. automethod:: peewee_async.Manager.delete

.. automethod:: peewee_async.Manager.get_or_create

.. automethod:: peewee_async.Manager.create_or_get

.. automethod:: peewee_async.Manager.execute

.. automethod:: peewee_async.Manager.prefetch

.. automethod:: peewee_async.Manager.count

.. automethod:: peewee_async.Manager.scalar

.. automethod:: peewee_async.Manager.connect

.. automethod:: peewee_async.Manager.close

.. automethod:: peewee_async.Manager.atomic

.. automethod:: peewee_async.Manager.transaction

.. automethod:: peewee_async.Manager.savepoint


Databases
---------

.. autoclass:: peewee_async.PostgresqlDatabase
    :members: init

.. autoclass:: peewee_async.PooledPostgresqlDatabase
    :members: init

.. autoclass:: peewee_asyncext.PostgresqlExtDatabase
    :members: init

.. autoclass:: peewee_asyncext.PooledPostgresqlExtDatabase
    :members: init

.. autoclass:: peewee_async.MySQLDatabase
    :members: init

.. autoclass:: peewee_async.PooledMySQLDatabase
    :members: init
