Signal support
====================

    `Signal support`_ has been backported from the original peewee with a few differences. Models with hooks for signals are provided in
    ``peewee_async.signals``. To use the signals, you will need all of your project's
    models to be a subclass of ``peewee_async.signals.AioModel``, which overrides the
    necessary methods to provide support for the various signals. A handler for any signal except ``pre_init`` should be a coroutine function. For obvious reasons
    ``pre_init`` signal handler can be only a synchronious function.

.. code-block:: python

    from peewee_async.signals import AioModel, aio_post_save


    class MyModel(AioModel):
        data = IntegerField()

    @aio_post_save(sender=MyModel)
    async def on_save_handler(model_class, instance, created):
        await save_in_history_table(instance.data)


The following signals are provided:

``aio_pre_save``
    Called immediately before an object is saved to the database. Provides an
    additional keyword argument ``created``, indicating whether the model is being
    saved for the first time or updated.
``aio_post_save``
    Called immediately after an object is saved to the database. Provides an
    additional keyword argument ``created``, indicating whether the model is being
    saved for the first time or updated.
``aio_pre_delete``
    Called immediately before an object is deleted from the database when :py:meth:`Model.aio_delete_instance`
    is used.
``aio_post_delete``
    Called immediately after an object is deleted from the database when :py:meth:`Model.aio_delete_instance`
    is used.
``pre_init``
    Called when a model class is first instantiated. Can not be async.


.. _Signal support: https://docs.peewee-orm.com/en/latest/peewee/playhouse.html#signal-support
