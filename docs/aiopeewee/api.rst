API Reference
=============

**Note:** all query methods are **coroutines**.

Select, update, delete
----------------------

.. autofunction:: aiopeewee.execute
.. autofunction:: aiopeewee.get_object
.. autofunction:: aiopeewee.create_object
.. autofunction:: aiopeewee.delete_object
.. autofunction:: aiopeewee.update_object

Aggregation
-----------

.. autofunction:: aiopeewee.count
.. autofunction:: aiopeewee.scalar

Databases
---------

.. autoclass:: aiopeewee.PooledPostgresqlDatabase
    :members: connect_async

.. autoclass:: aiopeewee.PostgresqlDatabase
    :members: connect_async


