Working with connections
========================

Every time some query is executed, for example, like this:

.. code-block:: python

    await MyModel.aio_get(id=1)


under the hood the execution runs under special async context manager:

.. code-block:: python

    async with database.aio_connection() as connection:
        await execute_your_query()

which takes a connection from the ``connection_context`` contextvar if it is not ``None`` or acquire from the pool otherwise. 
It releases the connection at the exit and set the ``connection_context`` to ``None``.


.. code-block:: python

    # acquire a connection and put it to connection_context context variable
    await MyModel.aio_get(id=1)
    # release the connection and set the connection_context to None

    # acquire a connection and put it to connection_context context variable
    await MyModel.aio_get(id=2)
    # release the connection and set the connection_context to None

    # acquire a connection and put it to connection_context context variable
    await MyModel.aio_get(id=3)
    # release the connection and set the connection_context to None

Connections manual management
++++++++++++++++++++++++++++

If you want to manage connections manually or you want to use one connection for a batch of queries you 
can run ``database.aio_connection`` by yourself and run the queries under the context manager.

.. code-block:: python

    # acquire a connection and put it to connection_context context variable
    async with database.aio_connection() as connection:
        
        # using the connection from the contextvar
        await MyModel.aio_get(id=1)

        # using the connection from the contextvar
        await MyModel.aio_get(id=2)

        # using the connection from the contextvar
        await MyModel.aio_get(id=3)
    # release the connection set connection_context to None at the exit of the async contextmanager

You can even run nested ``aio_connection`` context managers. 
In this case you will use one connection for the all managers from the highest context manager of the stack of calls 
and it will be closed when the highest manager is exited.