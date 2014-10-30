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

Aggregation
-----------

.. autofunction:: peewee_async.count
.. autofunction:: peewee_async.scalar

Databases
---------

.. autoclass:: peewee_async.PooledPostgresqlDatabase
    :members: connect_async

.. autoclass:: peewee_async.PostgresqlDatabase
    :members: connect_async


