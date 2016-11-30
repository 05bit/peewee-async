.. peewee-async documentation master file, created by
   sphinx-quickstart on Sat Oct 11 20:12:24 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

peewee-async
============

**peewee-async** is a library providing asynchronous interface powered by `asyncio`_ for `peewee`_ ORM.

.. _peewee: https://github.com/coleifer/peewee
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Current state: **alpha**, yet API seems fine and mostly stable.

In current version (0.5.x) new-high level API is introduced while older low-level API partially marked as deprecated.

* Works on Python 3.4+
* Has support for PostgreSQL via `aiopg`
* Has support for MySQL via `aiomysql`
* Single point for high-level async API
* Drop-in replacement for sync code, sync will remain sync
* Basic operations are supported
* Transactions support is present, yet not heavily tested

The source code is hosted on `GitHub`_.

.. _GitHub: https://github.com/05bit/peewee-async

Quickstart
----------

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    # Nothing special, just define model and database:

    database = peewee_async.PostgresqlDatabase('test')

    class TestModel(peewee.Model):
        text = peewee.CharField()

        class Meta:
            database = database

    # Look, sync code is working!

    TestModel.create_table(True)
    TestModel.create(text="Yo, I can do it sync!")
    database.close()

    # Create async models manager:

    objects = peewee_async.Manager(database)

    # No need for sync anymore!

    database.set_allow_sync(False)

    async def handler():
        await objects.create(TestModel, text="Not bad. Watch this, I'm async!")
        all_objects = await objects.execute(TestModel.select())
        for obj in all_objects:
            print(obj.text)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(handler())
    loop.close()

    # Clean up, can do it sync again:
    with objects.allow_sync():
        TestModel.drop_table(True)

    # Expected output:
    # Yo, I can do it sync!
    # Not bad. Watch this, I'm async!


Install
-------

Install latest version from PyPI.

For PostgreSQL:

.. code-block:: console

    pip install peewee-async aiopg

For MySQL:

.. code-block:: console

    pip install peewee-async aiomysql

Install from sources
++++++++++++++++++++

.. code-block:: console

    git clone https://github.com/05bit/peewee-async.git
    cd peewee-async
    python setup.py install

Running tests
+++++++++++++

Prepare environment for tests:

* Clone source code from GitHub as shown above
* Create PostgreSQL database for testing, i.e. named 'test'
* Create ``tests.json`` config file based on ``tests.json.sample``

Then run tests:

.. code-block:: console

    python setup.py test    

Report bugs and discuss
-----------------------

You are welcome to add discussion topics or bug reports to `tracker on GitHub`_!

.. _tracker on GitHub: https://github.com/05bit/peewee-async/issues

Contents
--------

.. toctree::
   :maxdepth: 2

   peewee_async/api
   peewee_async/api_older
   peewee_async/tornado
   peewee_async/examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

