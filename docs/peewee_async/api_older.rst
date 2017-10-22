Low-level (older) API
=====================

**Note:** all query methods are **coroutines**.

Select, update, delete
----------------------

.. autofunction:: src.queries.execute
.. autofunction:: src.queries.get_object
.. autofunction:: src.queries.create_object
.. autofunction:: src.queries.delete_object
.. autofunction:: src.queries.update_object
.. autofunction:: src.queries.prefetch

Transactions
------------

Transactions required Python 3.5+ to work, because their syntax is based on async context managers.

**Important note** transactions rely on data isolation on `asyncio` per-task basis.
That means, all queries for single transaction should be performed **within same task**.

.. autofunction:: src.transactions.atomic
.. autofunction:: src.transactions.savepoint
.. autofunction:: src.transactions.transaction

Aggregation
-----------

.. autofunction:: src.queries.count
.. autofunction:: src.queries.scalar

Databases
---------

.. autoclass:: src.postgres.PostgresqlDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async
    :noindex:

.. autoclass:: src.postgres.PooledPostgresqlDatabase
    :members: connect_async
    :noindex:

.. autoclass:: peewee_asyncext.PostgresqlExtDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async
    :noindex:

.. autoclass:: peewee_asyncext.PooledPostgresqlExtDatabase
    :members: connect_async
    :noindex:
