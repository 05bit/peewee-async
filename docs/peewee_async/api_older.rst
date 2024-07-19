Deprecated API
=====================

**Note:** all query methods are **coroutines**.

Select, update, delete
----------------------

.. autofunction:: peewee_async.execute
.. autofunction:: peewee_async.prefetch

Transactions
------------

Transactions required Python 3.5+ to work, because their syntax is based on async context managers.

**Important note** transactions rely on data isolation on `asyncio` per-task basis.
That means, all queries for single transaction should be performed **within same task**.

.. autofunction:: peewee_async.atomic
.. autofunction:: peewee_async.savepoint
.. autofunction:: peewee_async.transaction

Aggregation
-----------

.. autofunction:: peewee_async.count
.. autofunction:: peewee_async.scalar

Databases
---------

.. autoclass:: peewee_async.PostgresqlDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async
    :noindex:

.. autoclass:: peewee_asyncext.PostgresqlExtDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async
    :noindex:

.. autoclass:: peewee_async.MySQLDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async
    :noindex:


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