API Reference
=============

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

**Warning** For some reason doesn't work with pooled connection yet! This is bug, of course.

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

.. autoclass:: peewee_async.PooledPostgresqlDatabase
    :members: connect_async

.. autoclass:: peewee_asyncext.PostgresqlExtDatabase
    :members: connect_async, atomic_async, transaction_async, savepoint_async

.. autoclass:: peewee_asyncext.PooledPostgresqlExtDatabase
    :members: connect_async
