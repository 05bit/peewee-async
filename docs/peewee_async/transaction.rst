Transactions
=========================

Peewee-async provides several interfaces similiar to peewee for working with transactions. 


The most general interface are :py:meth:`~peewee_async.databases.AioDatabase.aio_atomic` and :py:meth:`~peewee_async.databases.AioDatabase.aio_transaction` methods which work as context managers.
The :py:meth:`~peewee_async.databases.AioDatabase.aio_atomic` method supports nested transactions and run the block of code in a transaction or savepoint, depending on the level of nesting.
If an exception occurs in a wrapped block, the current transaction/savepoint will be rolled back. Otherwise the statements will be committed at the end of the wrapped block.

.. code-block:: python

    async with db.aio_atomic(): # BEGIN
        await TestModel.aio_create(text='FOO') # INSERT INTO "testmodel" ("text", "data") VALUES ('FOO', '') RETURNING "testmodel"."id"

        async with db.aio_atomic(): # SAVEPOINT PWASYNC__e83bf5fc118f4e28b0fbdac90ab857ca
            await TestModel.update(text="BAR").aio_execute() # UPDATE "testmodel" SET "text" = 'BAR'
        # RELEASE SAVEPOINT PWASYNC__e83bf5fc118f4e28b0fbdac90ab857ca
    # COMMIT

The :py:meth:`~peewee_async.databases.AioDatabase.aio_transcation` method does not allow nested transactions and run the block of code in a transaction.

.. code-block:: python

    async with db.aio_atomic(): # BEGIN
        await TestModel.aio_create(text='FOO') # INSERT INTO "testmodel" ("text", "data") VALUES ('FOO', '') RETURNING "testmodel"."id"
    # COMMIT

Using nested :py:meth:`~peewee_async.databases.AioDatabase.aio_transcation` will lead to **OperationalError**.

Manual management
+++++++++++++++++

If you want to manage transactions manually you have to acquire a connection by yourself and manage the transaction inside the context manager of the connection:

.. code-block:: python

    from peewee_async import Transaction
    async with db.aio_connection() as connection:
        tr = Transaction(connection)
        await tr.begin() # BEGIN
        await TestModel.aio_create(text='FOO')
        try:
            await TestModel.aio_create(text='FOO')
        except:
            await tr.rollback() # ROLLBACK
        else:
            await tr.commit() # COMMIT

Raw sql for transactions
++++++++++++++++++++++++

if the above options are not enough for you you can always use raw sql for more opportunities:

.. code-block:: python

    async with db.aio_connection() as connection:
        await db.aio_execute_sql(sql="begin isolation level repeatable read;")
        await TestModel.aio_create(text='FOO')
        try:
            await TestModel.aio_create(text='FOO')
        except:
            await await db.aio_execute_sql(sql="ROLLBACK")
        else:
            await await db.aio_execute_sql(sql="COMMIT")

Just remember a transaction should work during one connection.

Aware different tasks when working with transactions.
+++++++++++++++++++++++++++++++++++++++++++++++++++++

As has been said a transaction should work during one connection. 
And as you know from :doc:`the connection section <./connection>` every connection is got from the contextvar variable which means every **asyncio.Task** has an own connection.
So this example will not work and may lead to bugs:

.. code-block:: python

    async with db.aio_atomic():
        await asyncio.gather(TestModel.aio_create(text='FOO1'), TestModel.aio_create(text='FOO2'), TestModel.aio_create(text='FOO3'))

Every sql query of the exmaple will run in the separate task which know nothing about started transaction in main task.
