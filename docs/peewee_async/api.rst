API Documentation
====================


Databases
++++++++++

.. autoclass:: peewee_async.databases.AioDatabase

.. automethod:: peewee_async.databases.AioDatabase.aio_connect

.. autoproperty:: peewee_async.databases.AioDatabase.is_connected

.. automethod:: peewee_async.databases.AioDatabase.aio_close

.. automethod:: peewee_async.databases.AioDatabase.aio_execute

.. automethod:: peewee_async.databases.AioDatabase.set_allow_sync

.. automethod:: peewee_async.databases.AioDatabase.allow_sync

.. automethod:: peewee_async.databases.AioDatabase.aio_atomic

.. automethod:: peewee_async.databases.AioDatabase.aio_transaction

.. autoclass:: peewee_async.PsycopgDatabase
    :members: init

.. autoclass:: peewee_async.PooledPostgresqlDatabase
    :members: init

.. autoclass:: peewee_async.PooledPostgresqlExtDatabase
    :members: init

.. autoclass:: peewee_async.PooledMySQLDatabase
    :members: init

AioModel
++++++++++

.. autoclass:: peewee_async.AioModel

.. automethod:: peewee_async.AioModel.aio_get

.. automethod:: peewee_async.AioModel.aio_get_or_none

.. automethod:: peewee_async.AioModel.aio_create

.. automethod:: peewee_async.AioModel.aio_get_or_create

.. automethod:: peewee_async.AioModel.aio_delete_instance

.. automethod:: peewee_async.AioModel.aio_save

.. autofunction:: peewee_async.aio_prefetch

AioModelSelect
++++++++++++++

.. autoclass:: peewee_async.aio_model.AioModelSelect

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_peek

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_scalar

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_first

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_get

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_count

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_exists

.. automethod:: peewee_async.aio_model.AioModelSelect.aio_prefetch