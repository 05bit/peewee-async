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

* Works on Python 3.3+
* Databases supported: PostgreSQL
* Provides drop-in replacement for synchronous code

Source code hosted on `GitHub`_.

.. _GitHub: https://github.com/05bit/peewee-async

Quickstart
----------

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    database = peewee_async.PostgresqlDatabase('test')
    loop = asyncio.get_event_loop()

    class TestModel(peewee.Model):
        text = peewee.CharField()

        class Meta:
            database = database

    # Create table synchronously!
    TestModel.create_table(True)
    # This is optional: close sync connection
    database.close()

    @asyncio.coroutine
    def my_handler():
        TestModel.create(text="Yo, I can do it sync!")
        yield from peewee_async.create_object(TestModel, text="Not bad. Watch this, I'm async!")
        all_objects = yield from peewee_async.execute(TestModel.select())
        for obj in all_objects:
            print(obj.text)

    loop.run_until_complete(database.connect_async(loop=loop))
    loop.run_until_complete(my_handler())


Install
-------

Install latest version from PyPI:

.. code-block:: console

    pip install peewee-async

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
* Create ``tests.ini`` config file based on ``tests.ini.sample``

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
   peewee_async/examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

