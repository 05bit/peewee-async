Low-level (older) API
=====================

**Note:** all query methods are **coroutines**.

Select, update, delete
----------------------

.. autofunction:: peewee_async.execute
.. autofunction:: peewee_async.get_object
.. autofunction:: peewee_async.create_object
.. autofunction:: peewee_async.delete_object
.. autofunction:: peewee_async.update_object
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

.. autoclass:: peewee_async.PooledPostgresqlDatabase
    :members: connect_async
    :noindex:

.. autoclass:: peewee_asyncext.PostgresqlExtDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async
    :noindex:

.. autoclass:: peewee_asyncext.PooledPostgresqlExtDatabase
    :members: connect_async
    :noindex:
